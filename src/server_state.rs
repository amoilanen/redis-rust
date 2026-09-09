use anyhow::anyhow;
use log::*;
use std::collections::HashMap;
use std::io::Write;
use std::net::TcpStream;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};
use rand::Rng;
use crate::commands::RedisCommand;
use crate::blocking::BlockingNotifier;
use crate::error::RedisError;
use crate::protocol;
use crate::storage::Storage;

pub struct ReplicaLink {
    /// The master's end of the connection to the replica.
    stream: Mutex<TcpStream>,
    /// Where in the master's stream this replica was handed its snapshot.
    stream_offset_at_handshake: usize,
    /// How far into the master's stream this replica has confirmed processing,
    /// from its last `REPLCONF ACK <offset>`.
    acknowledged_offset: AtomicUsize,
}

impl ReplicaLink {
    fn new(stream: TcpStream, stream_offset_at_handshake: usize) -> ReplicaLink {
        ReplicaLink {
            stream: Mutex::new(stream),
            stream_offset_at_handshake,
            // Up to date the moment it is registered: the snapshot it was just
            // sent already holds every write propagated so far.
            acknowledged_offset: AtomicUsize::new(stream_offset_at_handshake),
        }
    }

    /// The offset in the master's stream this replica has last acknowledged.
    pub fn acknowledged_offset(&self) -> usize {
        self.acknowledged_offset.load(Ordering::SeqCst)
    }

    /// Takes note of the offset carried by a `REPLCONF ACK`, which counts from
    /// where this replica's own stream began.
    ///
    /// The offset only ever moves forward: several `GETACK`s can be in flight
    /// at once, and an answer to an older one must not un-acknowledge bytes a
    /// later answer already covered.
    fn record_acknowledgement(&self, offset: usize) {
        self.acknowledged_offset
            .fetch_max(self.stream_offset_at_handshake + offset, Ordering::SeqCst);
    }

    fn send(&self, bytes: &[u8]) -> Result<(), anyhow::Error> {
        self.stream
            .lock()
            .map_err(|e| anyhow!("Failed to lock a replica connection: {}", e))?
            .write_all(bytes)?;
        Ok(())
    }
}

pub struct ReplicaSlot {
    link: Mutex<Option<Arc<ReplicaLink>>>,
}

impl ReplicaSlot {
    pub fn new() -> ReplicaSlot {
        ReplicaSlot { link: Mutex::new(None) }
    }

    pub fn fill(&self, link: Arc<ReplicaLink>) -> Result<(), anyhow::Error> {
        *self.lock()? = Some(link);
        Ok(())
    }

    pub fn link(&self) -> Result<Option<Arc<ReplicaLink>>, anyhow::Error> {
        Ok(self.lock()?.clone())
    }

    fn lock(&self) -> Result<MutexGuard<'_, Option<Arc<ReplicaLink>>>, anyhow::Error> {
        self.link
            .lock()
            .map_err(|e| anyhow!("Failed to lock the replica slot: {}", e))
    }
}

impl Default for ReplicaSlot {
    fn default() -> Self {
        Self::new()
    }
}

pub struct ServerState {
    pub port: usize,
    pub replica_of: Option<String>,
    pub master_replication_id: Option<String>,
    pub master_replication_offset: Option<usize>,
    /// The replicas that have finished `PSYNC` and are being streamed to.
    ///
    /// Private: a replica is only ever reached through the methods below, so
    /// the lock is never held across anything but a write or a quick scan.
    replica_links: Mutex<Vec<Arc<ReplicaLink>>>,
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
    /// Bytes this master has written into the replication stream - the offset
    /// a replica has to reach to be up to date with it.
    ///
    /// The mirror image of `replication_offset`: that one counts what this
    /// node has consumed as a replica, this one what it has produced as a
    /// master, so a node only ever moves one of the two.
    propagated_offset: AtomicUsize,
    /// Wakes up whoever is waiting for replicas to catch up.
    ///
    /// The `usize` counts the acknowledgements recorded so far; the number
    /// itself is of no interest, but pairing the `Condvar` with a lock that
    /// every recorder takes is what stops an acknowledgement slipping in
    /// between a waiter's check and its sleep.
    acknowledgements: (Mutex<usize>, Condvar),
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

    pub fn propagated_offset(&self) -> usize {
        self.propagated_offset.load(Ordering::SeqCst)
    }

    fn replica_links(&self) -> Result<MutexGuard<'_, Vec<Arc<ReplicaLink>>>, anyhow::Error> {
        self.replica_links
            .lock()
            .map_err(|e| anyhow!("Failed to lock replica connections: {}", e))
    }

    /// Takes on a replica that has just finished its `PSYNC`.
    ///
    /// It joins the stream where the master stands now: the snapshot it was
    /// sent along with the FULLRESYNC already holds every write propagated so
    /// far, so it is up to date, and everything it goes on to acknowledge is
    /// counted from here.
    ///
    /// The returned link is the caller's handle on that replica - the
    /// connection reading its `REPLCONF ACK`s records them against it.
    pub fn register_replica(
        &self,
        stream: &TcpStream
    ) -> Result<Arc<ReplicaLink>, anyhow::Error> {
        let link = Arc::new(ReplicaLink::new(stream.try_clone()?, self.propagated_offset()));
        self.replica_links()?.push(Arc::clone(&link));
        Ok(link)
    }

    /// How many replicas are currently connected to this master.
    ///
    /// A replica is counted from the moment it completes `PSYNC` and is
    /// registered by [`register_replica`](Self::register_replica).
    pub fn replica_count(&self) -> Result<usize, anyhow::Error> {
        Ok(self.replica_links()?.len())
    }

    /// How many replicas have acknowledged processing the stream up to
    /// `offset` - the number `WAIT` answers with.
    fn replicas_acknowledged(&self, offset: usize) -> Result<usize, anyhow::Error> {
        Ok(self
            .replica_links()?
            .iter()
            .filter(|link| link.acknowledged_offset() >= offset)
            .count())
    }

    pub fn propagate_to_replicas(
        &self,
        command: &dyn RedisCommand
    ) -> Result<(), anyhow::Error> {
        let command_bytes = command.serialize();
        debug!("Propagating command to replicas: {:?}", &command_bytes);
        self.broadcast(&command_bytes)
    }

    /// Asks every replica how far it has got, with `REPLCONF GETACK *`.
    ///
    /// The request is itself part of the replication stream, so it advances
    /// this master's offset. What comes back does not answer the enlarged
    /// offset: a replica counts a command only once it has handled it, so its
    /// answer reports the offset as it stood before the request went out.
    fn request_acknowledgements(&self) -> Result<(), anyhow::Error> {
        debug!("Asking the replicas to acknowledge what they have processed");
        self.broadcast(
            &protocol::array(vec![
                protocol::bulk_string("REPLCONF"),
                protocol::bulk_string("GETACK"),
                protocol::bulk_string("*"),
            ])
            .serialize(),
        )
    }

    /// Writes `bytes` to every replica and counts them into the replication
    /// stream's offset.
    ///
    /// A replica whose socket has failed is dropped rather than failing the
    /// command being propagated: from the master's side a replica that cannot
    /// be written to has gone away, which is not an error of the client whose
    /// write happened to be in flight. The offset advances either way - it
    /// measures the stream, not the number of listeners.
    fn broadcast(&self, bytes: &[u8]) -> Result<(), anyhow::Error> {
        let mut links = self.replica_links()?;
        links.retain(|link| match link.send(bytes) {
            Ok(()) => true,
            Err(error) => {
                warn!("Dropping a replica that could not be written to: {}", error);
                false
            }
        });
        self.propagated_offset.fetch_add(bytes.len(), Ordering::SeqCst);
        Ok(())
    }

    /// Records a `REPLCONF ACK <offset>` a replica sent back, waking anyone
    /// waiting for it.
    pub fn record_acknowledgement(
        &self,
        link: &ReplicaLink,
        offset: usize,
    ) -> Result<(), anyhow::Error> {
        link.record_acknowledgement(offset);
        let (recorded, arrived) = &self.acknowledgements;
        // Taken after the offset is stored, so a waiter either sees the new
        // offset when it checks or is still holding this lock and gets woken.
        *recorded
            .lock()
            .map_err(|e| anyhow!("Failed to lock acknowledgements: {}", e))? += 1;
        arrived.notify_all();
        Ok(())
    }

    /// Waits until `wanted` replicas have processed everything written to
    /// them so far, giving up after `timeout` - or never, when there is none.
    ///
    /// This is `WAIT` itself: a replica reports where it has got to only when
    /// asked, so unless enough of them are already known to be up to date -
    /// nothing having been propagated since they last reported in - they are
    /// asked, and what is waited for is their answers.
    ///
    /// Returns how many had caught up by the time it stopped waiting, which is
    /// the answer whether it was satisfied or ran out of time. It can exceed
    /// `wanted`: more replicas than were asked about may be up to date.
    pub fn await_replicas(
        &self,
        wanted: usize,
        timeout: Option<Duration>,
    ) -> Result<usize, anyhow::Error> {
        // Everything written to the replicas so far: the point one has to have
        // reached to hold every write a client could have seen the effect of.
        let target_offset = self.propagated_offset();

        let acknowledged = self.replicas_acknowledged(target_offset)?;
        if acknowledged >= wanted {
            return Ok(acknowledged);
        }

        self.request_acknowledgements()?;
        self.await_acknowledgements(target_offset, wanted, timeout)
    }

    /// Blocks until `wanted` replicas have acknowledged `target_offset` or
    /// `timeout` runs out, reporting how many had by then.
    ///
    /// Deliberately not the one to ask them: the request goes out to sockets
    /// that may block, and doing that under the lock below would stall every
    /// acknowledgement the server is recording, not just this wait.
    fn await_acknowledgements(
        &self,
        target_offset: usize,
        wanted: usize,
        timeout: Option<Duration>,
    ) -> Result<usize, anyhow::Error> {
        let deadline = timeout.map(|timeout| Instant::now() + timeout);
        let (recorded, arrived) = &self.acknowledgements;
        // Held from here to the answer, bar the parking itself, so that an
        // acknowledgement cannot land in the gap between counting too few and
        // settling down to wait for more. The count is taken afresh each time
        // round rather than trusted from before the lock: answers to the
        // request just sent may already have arrived.
        let mut recorded = recorded
            .lock()
            .map_err(|e| anyhow!("Failed to lock acknowledgements: {}", e))?;

        loop {
            let acknowledged = self.replicas_acknowledged(target_offset)?;
            if acknowledged >= wanted {
                return Ok(acknowledged);
            }
            recorded = match deadline {
                None => arrived
                    .wait(recorded)
                    .map_err(|e| anyhow!("Failed to wait for acknowledgements: {}", e))?,
                Some(deadline) => {
                    let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                        return Ok(acknowledged);
                    };
                    let (recorded, wait) = arrived
                        .wait_timeout(recorded, remaining)
                        .map_err(|e| anyhow!("Failed to wait for acknowledgements: {}", e))?;
                    if wait.timed_out() {
                        // One last look: an acknowledgement may have landed as
                        // the wait was giving up.
                        return self.replicas_acknowledged(target_offset);
                    }
                    recorded
                }
            };
        }
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
                    replica_links: Mutex::new(Vec::new()),
                    blocking_notifier: blocking,
                    storage,
                    replication_offset: AtomicUsize::new(0),
                    propagated_offset: AtomicUsize::new(0),
                    acknowledgements: (Mutex::new(0), Condvar::new()),
                },
            None =>
                ServerState {
                    port,
                    replica_of: None,
                    master_replication_id: Some(ServerState::generate_replication_id()),
                    master_replication_offset: Some(0),
                    replica_links: Mutex::new(Vec::new()),
                    blocking_notifier: blocking,
                    storage,
                    replication_offset: AtomicUsize::new(0),
                    propagated_offset: AtomicUsize::new(0),
                    acknowledgements: (Mutex::new(0), Condvar::new()),
                }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;
    use std::thread;

    /// The wire size of the `REPLCONF GETACK *` a master sends out.
    const GETACK_BYTES: usize = 37;

    /// A command to propagate, standing in for any write: what it does to the
    /// keyspace is beside the point here, only that it lengthens the stream.
    fn ping() -> crate::commands::Ping {
        crate::commands::Ping {
            message: crate::protocol::array(vec![crate::protocol::bulk_string("PING")]),
        }
    }

    fn master_with_replicas(
        replicas: usize,
    ) -> (Arc<ServerState>, TcpListener, Vec<Arc<ReplicaLink>>) {
        let state = Arc::new(ServerState::new(None, 1234));
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let links = (0..replicas)
            .map(|_| {
                let replica = TcpStream::connect(address).unwrap();
                state.register_replica(&replica).unwrap()
            })
            .collect();
        (state, listener, links)
    }

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

    #[test]
    fn should_start_the_propagated_offset_at_zero() {
        let state = ServerState::new(None, 1234);

        assert_eq!(state.propagated_offset(), 0);
    }

    #[test]
    fn should_count_a_request_for_acknowledgements_into_the_propagated_offset() {
        // The request travels down the replication stream like any other
        // command, so the replicas' next answers have to account for it.
        let (state, _listener, _links) = master_with_replicas(1);

        state.request_acknowledgements().unwrap();

        assert_eq!(state.propagated_offset(), GETACK_BYTES);
    }

    #[test]
    fn should_register_a_replica_as_having_acknowledged_what_came_before_it() {
        // The snapshot a replica is given at PSYNC already holds every write
        // propagated so far, so it is up to date the moment it is registered
        // rather than trailing the master by the whole history.
        let (state, listener, _links) = master_with_replicas(0);
        state.request_acknowledgements().unwrap();

        let replica = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let link = state.register_replica(&replica).unwrap();

        assert_eq!(link.acknowledged_offset(), GETACK_BYTES);
        assert_eq!(state.replicas_acknowledged(GETACK_BYTES).unwrap(), 1);
    }

    #[test]
    fn should_rebase_what_a_late_replica_acknowledges_onto_the_masters_stream() {
        // A replica joining midway counts from zero: its FULLRESYNC starts it
        // at the snapshot it was handed, so the 37 bytes it reports having
        // processed are the 37 that followed the ones already in that
        // snapshot, not the first 37 the master ever wrote.
        let (state, listener, _links) = master_with_replicas(0);
        state.request_acknowledgements().unwrap();
        let replica = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let link = state.register_replica(&replica).unwrap();

        state.request_acknowledgements().unwrap();
        state.record_acknowledgement(&link, GETACK_BYTES).unwrap();

        assert_eq!(link.acknowledged_offset(), 2 * GETACK_BYTES);
        assert_eq!(state.replicas_acknowledged(state.propagated_offset()).unwrap(), 1);
    }

    #[test]
    fn should_count_only_the_replicas_that_reached_an_offset() {
        let (state, _listener, links) = master_with_replicas(2);

        state.record_acknowledgement(&links[0], 100).unwrap();
        state.record_acknowledgement(&links[1], 40).unwrap();

        assert_eq!(state.replicas_acknowledged(40).unwrap(), 2);
        assert_eq!(state.replicas_acknowledged(41).unwrap(), 1);
        assert_eq!(state.replicas_acknowledged(101).unwrap(), 0);
    }

    #[test]
    fn should_never_move_an_acknowledgement_backwards() {
        // Two GETACKs can be in flight at once, so the answer to the older one
        // may arrive last; it must not undo what the newer one acknowledged.
        let (state, _listener, links) = master_with_replicas(1);

        state.record_acknowledgement(&links[0], 100).unwrap();
        state.record_acknowledgement(&links[0], 40).unwrap();

        assert_eq!(links[0].acknowledged_offset(), 100);
    }

    #[test]
    fn should_not_wait_on_replicas_with_nothing_left_to_process() {
        // Nothing propagated since they were registered, so they are up to
        // date by definition: there is nothing to ask and nothing to wait for.
        let (state, _listener, _links) = master_with_replicas(2);

        // A timeout that would be plainly visible had it been waited out.
        let started_at = Instant::now();
        let caught_up = state.await_replicas(2, Some(Duration::from_secs(30))).unwrap();

        assert_eq!(caught_up, 2);
        assert!(started_at.elapsed() < Duration::from_secs(1));
    }

    #[test]
    fn should_report_how_many_replicas_caught_up_before_the_timeout() {
        // A write is pending and neither replica answers - there are no calls to record_acknowledgement - so the wait is spent
        // in full and reports the nil that made it.
        let (state, _listener, _links) = master_with_replicas(2);
        state.propagate_to_replicas(&ping()).unwrap();

        let caught_up = state.await_replicas(2, Some(Duration::from_millis(50))).unwrap();

        assert_eq!(caught_up, 0);
    }

    #[test]
    fn should_stop_waiting_when_a_late_acknowledgement_arrives() {
        let (state, _listener, links) = master_with_replicas(1);
        state.propagate_to_replicas(&ping()).unwrap();
        let propagated = state.propagated_offset();
        let acknowledging = {
            let state = Arc::clone(&state);
            let link = Arc::clone(&links[0]);
            thread::spawn(move || {
                thread::sleep(Duration::from_millis(20));
                state.record_acknowledgement(&link, propagated).unwrap();
            })
        };

        let started_at = Instant::now();
        let caught_up = state.await_replicas(1, Some(Duration::from_secs(30))).unwrap();

        assert_eq!(caught_up, 1);
        assert!(started_at.elapsed() < Duration::from_secs(1));
        acknowledging.join().unwrap();
    }
}

