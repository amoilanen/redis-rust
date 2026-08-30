use anyhow::anyhow;
use log::*;
use std::collections::HashMap;
use std::io::Write;
use std::net::TcpStream;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use rand::Rng;
use crate::commands::RedisCommand;
use crate::blocking::BlockingNotifier;
use crate::error::RedisError;
use crate::storage::Storage;

pub struct ServerState {
    pub port: usize,
    pub replica_of: Option<String>,
    pub master_replication_id: Option<String>,
    pub master_replication_offset: Option<usize>,
    pub replica_connections: Arc<Mutex<Vec<TcpStream>>>,
    pub blocking_notifier: Arc<BlockingNotifier>,
    /// The keyspace this server serves. Private so it is reached through
    /// [`ServerState::storage`], which hands out the bare keyspace:
    /// commands take that alone, never the whole server state.
    ///
    /// Not an `Arc` - the state itself is already shared as `Arc<ServerState>`,
    /// so a second refcount here would only ever be cloned alongside it.
    storage: Mutex<Storage>,
    /// How many bytes of the master's command stream this replica has
    /// processed - the offset it reports in `REPLCONF ACK`.
    ///
    /// Atomic rather than behind a `Mutex`: the connection to the master
    /// advances it from its own thread while client connections read it.
    replication_offset: AtomicUsize,
}

impl ServerState {

    const REPLICATION_ID_LENGTH: usize = 20;

    /// The keyspace this server serves.
    ///
    /// Returns the `Mutex` rather than a lock guard so callers keep control of
    /// when - and for how long - it is held; BLPOP in particular must drop that
    /// lock before parking on its receiver.
    pub fn storage(&self) -> &Mutex<Storage> {
        &self.storage
    }

    /// Bytes of the master's command stream processed so far.
    pub fn replication_offset(&self) -> usize {
        self.replication_offset.load(Ordering::SeqCst)
    }

    /// Counts a command received from the master towards the replication
    /// offset. Called once the command has been handled, so a `REPLCONF
    /// GETACK` still reports the offset as it stood before that request.
    pub fn advance_replication_offset(&self, bytes: usize) {
        self.replication_offset.fetch_add(bytes, Ordering::SeqCst);
    }

    pub fn is_master(&self) -> bool {
        self.replica_of.is_none()
    }

    pub fn is_replica(&self) -> bool {
        !self.is_master()
    }

    pub fn register_replica(
        &self,
        stream: &TcpStream
    ) -> Result<(), anyhow::Error> {
        self
            .replica_connections
            .lock()
            .map_err(|e| anyhow!("Failed to lock replica connections: {}", e))?
            .push(stream.try_clone()?);
        Ok(())
    }


    /// How many replicas are currently connected to this master - the number
    /// `WAIT` reports back.
    ///
    /// A replica is counted from the moment it completes `PSYNC` and is
    /// registered by [`register_replica`](Self::register_replica).
    pub fn replica_count(&self) -> Result<usize, anyhow::Error> {
        Ok(self
            .replica_connections
            .lock()
            .map_err(|e| anyhow!("Failed to lock replica connections: {}", e))?
            .len())
    }

    pub fn propagate_to_replicas(
        &self,
        command: &dyn RedisCommand
    ) -> Result<(), anyhow::Error> {
        let command_bytes = command.serialize();
        let mut replica_streams = self
            .replica_connections
            .lock()
            .map_err(|e| anyhow!("Failed to lock replica connections: {}", e))?;
        for replica_stream in replica_streams.iter_mut() {
            debug!("Propagating command to replica: {:?}", &command_bytes);
            replica_stream.write_all(&command_bytes)?;
        }
        Ok(())
    }

    pub fn get_replica_of_address(&self) -> Result<Option<String>, anyhow::Error> {
        match &self.replica_of {
            Some(replica_of) => {
                let error = RedisError { 
                    message: format!("Cannot parse replica_of {}", replica_of)
                };
                let mut replica_of_parts = replica_of.split(" ");
                let host = replica_of_parts.next().ok_or::<anyhow::Error>(error.clone().into())?;
                let port = replica_of_parts.next().ok_or::<anyhow::Error>(error.clone().into())?;
                Ok(Some(format!("{}:{}", host, port)))
            },
            None => {
                Ok(None)
            }
        }
    }

    fn generate_replication_id() -> String {
        let mut generator = rand::rng();
        let random_bytes: Vec<u8> = (0..ServerState::REPLICATION_ID_LENGTH).map(|_| generator.random()).collect();
        let formatted_bytes: String = random_bytes.iter().map(|x| format!("{:02x}", x)).collect();
        formatted_bytes
    }

    pub fn new<'a>(replica_of: Option<String>, port: usize) -> ServerState {
        let blocking = Arc::new(BlockingNotifier::new());
        let storage = Mutex::new(Storage::new(HashMap::new()));
        match replica_of {
            Some(replica_of) =>
                ServerState {
                    port,
                    replica_of: Some(replica_of),
                    master_replication_id: None,
                    master_replication_offset: None,
                    replica_connections: Arc::new(Mutex::new(Vec::new())),
                    blocking_notifier: blocking,
                    storage,
                    replication_offset: AtomicUsize::new(0),
                },
            None =>
                ServerState {
                    port,
                    replica_of: None,
                    master_replication_id: Some(ServerState::generate_replication_id()),
                    master_replication_offset: Some(0),
                    replica_connections: Arc::new(Mutex::new(Vec::new())),
                    blocking_notifier: blocking,
                    storage,
                    replication_offset: AtomicUsize::new(0),
                }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_set_replication_id_and_offset_for_master() -> Result<(), Box<dyn std::error::Error>> {
        let state = ServerState::new(None, 1234);
        assert_eq!(state.replica_of, None);
        assert_eq!(state.port, 1234);
        assert_eq!(state.master_replication_offset, Some(0));
        assert_eq!(state.master_replication_id.map(|x| x.len()).unwrap_or(0), 40);
        Ok(())
    }

    #[test]
    fn should_set_replication_id_and_offset_for_slave() -> Result<(), Box<dyn std::error::Error>> {
        let state = ServerState::new(Some("localhost 6379".to_owned()), 1234);
        assert_eq!(state.replica_of, Some("localhost 6379".to_owned()));
        assert_eq!(state.port, 1234);
        assert_eq!(state.master_replication_offset, None);
        assert_eq!(state.master_replication_id, None);
        Ok(())
    }

    #[test]
    fn should_count_no_replicas_on_a_fresh_master() {
        let state = ServerState::new(None, 1234);

        assert_eq!(state.replica_count().unwrap(), 0);
    }

    #[test]
    fn should_count_every_registered_replica() {
        let state = ServerState::new(None, 1234);
        // A registered replica is just a connection the master holds on to, so
        // any live stream stands in for one here.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();

        for expected_count in 1..=2 {
            let replica = TcpStream::connect(address).unwrap();
            state.register_replica(&replica).unwrap();

            assert_eq!(state.replica_count().unwrap(), expected_count);
        }
    }

    #[test]
    fn should_start_the_replication_offset_at_zero() {
        let state = ServerState::new(Some("localhost 6379".to_owned()), 1234);

        assert_eq!(state.replication_offset(), 0);
    }

    #[test]
    fn should_accumulate_the_replication_offset() {
        let state = ServerState::new(Some("localhost 6379".to_owned()), 1234);

        // The byte sizes of REPLCONF GETACK * and PING on the wire.
        state.advance_replication_offset(37);
        state.advance_replication_offset(14);

        assert_eq!(state.replication_offset(), 51);
    }
}

