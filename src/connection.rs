/// Connection handling for incoming Redis client connections.
///
/// This module handles incoming TCP connections, parses commands,
/// executes them, and sends responses back to clients.

use anyhow::anyhow;
use log::*;
use std::io::Write;
use std::net::TcpStream;
use std::sync::{Arc, Mutex};

use crate::protocol::{self, DataType};
use crate::error::RedisError;
use crate::io;
use crate::commands::{command, pubsub::{self, Subscriptions}, transaction::TransactionSlot};
use crate::storage::Storage;
use crate::server_state::{ReplicaSlot, ServerState};

/// Who is on the other end of a connection.
///
/// The two differ in more than whether replies are sent, so the distinction is
/// named rather than carried as a bare `should_reply` flag.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConnectionMode {
    /// A client issuing commands: every command is answered.
    ConnectedClient,
    /// The master this server replicates from. Its commands are applied
    /// silently - only the ones that insist on answering, such as `REPLCONF
    /// GETACK`, reply - and their byte sizes make up the replication offset.
    ConnectedMaster,
}

impl ConnectionMode {
    fn should_reply(self) -> bool {
        matches!(self, ConnectionMode::ConnectedClient)
    }

    fn counts_replication_offset(self) -> bool {
        matches!(self, ConnectionMode::ConnectedMaster)
    }
}

/// Everything one connection owns for as long as it lasts.
///
/// Created when the connection is accepted and dropped when it ends, which is
/// the whole lifecycle: a disconnect discards the open transaction and the
/// channels the client subscribed to for free, with no server-wide registry to
/// unregister from. The one thing other connections must reach - a replica's
/// socket - is registered separately on `ServerState`, and pruned there when a
/// write to it fails.
///
/// Each field keeps an `Arc` of its own rather than the state being shared
/// whole: `build_command` hands a command only the slot it works on, so a
/// command's fields still say which connection state it reaches for.
pub struct ConnectionState {
    /// Who is on the other end, which decides whether replies are sent and
    /// whether the peer's commands count towards the replication offset.
    pub mode: ConnectionMode,
    /// The at-most-one transaction opened by MULTI.
    pub transaction: Arc<TransactionSlot>,
    /// Empty until this connection turns out to be a replica's, which a PSYNC
    /// sent by the replica decides.
    pub replica: Arc<ReplicaSlot>,
    /// The channels SUBSCRIBE has taken on this connection.
    pub subscriptions: Arc<Subscriptions>,
}

impl ConnectionState {
    pub fn new(mode: ConnectionMode) -> Self {
        ConnectionState {
            mode,
            transaction: Arc::new(TransactionSlot::new()),
            replica: Arc::new(ReplicaSlot::new()),
            subscriptions: Arc::new(Subscriptions::new()),
        }
    }

    fn should_reply(&self) -> bool {
        self.mode.should_reply()
    }

    fn counts_replication_offset(&self) -> bool {
        self.mode.counts_replication_offset()
    }
}

/// Handles a single connection.
///
/// This function:
/// 1. Reads incoming messages from the peer
/// 2. Parses commands
/// 3. Executes commands
/// 4. Sends responses back where the role calls for them
/// 5. Propagates write commands to replicas if master
/// 6. Counts the master's commands towards the replication offset
///
/// # Arguments
/// * `stream` - TCP stream for the connection
/// * `server_state` - Server state (master/replica info, and the keyspace)
/// * `role` - Whether the peer is a client or the master this server replicates from
///
/// # Returns
/// Error if connection fails
pub fn handle_connection(
    stream: &mut TcpStream,
    server_state: &Arc<ServerState>,
    role: ConnectionMode,
) -> Result<(), anyhow::Error> {
    debug!("accepted new connection");

    // Owned here, so it lives exactly as long as the connection does.
    let connection_state = Arc::new(ConnectionState::new(role));

    loop {
        let received_messages: Vec<(DataType, usize)> = io::read_messages_with_lengths(stream)?;
        for (received_message, message_length) in received_messages.into_iter() {
            trace!(
                "Received: {}",
                String::from_utf8_lossy(&received_message.serialize()).replace("\r\n", "\\r\\n")
            );
            match &received_message {
                DataType::Array { elements: _ } => {
                    handle_command(stream, &received_message, server_state, &connection_state)?;
                    // After handling, so a REPLCONF GETACK reports the offset
                    // as it stood before that request - the request itself is
                    // only counted towards the next acknowledgement.
                    if connection_state.counts_replication_offset() {
                        server_state.advance_replication_offset(message_length);
                    }
                }
                DataType::Rdb { value } => {
                    // The snapshot is the starting point the offset counts
                    // from, so its bytes are not part of the offset.
                    handle_rdb_snapshot(value, server_state.storage())?;
                }
                DataType::SimpleString { value: _ } => {
                    handle_simple_string(&received_message)?;
                }
                _ => (),
            }
        }
    }
}

fn handle_command(
    stream: &mut TcpStream,
    received_message: &DataType,
    server_state: &Arc<ServerState>,
    connection_state: &Arc<ConnectionState>,
) -> Result<(), anyhow::Error> {
    let Some(command) =
        command::command_from_message(received_message, server_state, connection_state)?
    else {
        return Ok(());
    };
    let command_name = command.name();

    // Subscribed mode limits a connection to managing its subscriptions. The
    // check comes before queueing, as in Redis, so a transaction cannot be
    // used to slip a forbidden command past it.
    if connection_state.subscriptions.is_subscribed_mode()? {
        if let Err(redis_error) = pubsub::ensure_allowed_in_subscribed_mode(command_name) {
            debug!("Rejected {} in subscribed mode", command_name);
            if connection_state.should_reply() {
                send_reply(stream, vec![redis_error.as_protocol_error()])?;
            }
            return Ok(());
        }
    }

    // Inside a transaction a command is collected rather than run, so it must
    // not reach storage, the replicas, or - for PSYNC - the replica registry.
    // Queueing after `build_command` keeps an unrecognised command ignored the
    // same way it is outside a transaction, instead of filling the queue with
    // something EXEC could never run.
    if connection_state.transaction.queue(&command_name, received_message)? {
        debug!("Queued {} in the open transaction", command_name);
        if connection_state.should_reply() {
            send_reply(stream, vec![protocol::simple_string("QUEUED")])?;
        }
        return Ok(());
    }

    if command_name == "PSYNC" {
        connection_state.replica.fill(server_state.register_replica(stream)?)?;
    }

    let reply = match command.execute(server_state.storage()) {
        Ok(reply) => reply,
        // A RedisError is a client-facing error reply, not a connection failure:
        // surface it as a RESP simple error and keep serving the client. The
        // failed write is never propagated to replicas.
        Err(error) => match error.downcast::<RedisError>() {
            Ok(redis_error) => {
                if connection_state.should_reply() || command.should_always_reply() {
                    send_reply(stream, vec![protocol::simple_error(&redis_error.message)])?;
                }
                return Ok(());
            }
            Err(other) => return Err(other),
        },
    };

    if connection_state.should_reply() || command.should_always_reply() {
        send_reply(stream, reply)?;
    }

    if server_state.is_master() && command.is_propagated_to_replicas() {
        server_state.propagate_to_replicas(&*command)?;
    }

    Ok(())
}

fn send_reply(stream: &mut TcpStream, reply: Vec<DataType>) -> Result<(), anyhow::Error> {
    for message in reply.into_iter() {
        trace!("Sending: {:?}", message);
        let message_bytes = message.serialize();
        trace!("which serializes to {:?}", message_bytes);
        stream.write_all(&message_bytes)?;
    }
    Ok(())
}

fn handle_rdb_snapshot(
    value: &[u8],
    storage: &Mutex<Storage>,
) -> Result<(), anyhow::Error> {
    let maybe_received_storage = Storage::from_rdb_bytes(value).ok();
    debug!("Received storage {:?}", &maybe_received_storage);
    if let Some(received_storage) = maybe_received_storage {
        let mut storage = storage
            .lock()
            .map_err(|e| anyhow!("Failed to lock storage: {}", e))?;
        for (key, value) in received_storage.data.into_iter() {
            storage.data.insert(key, value);
        }
    }
    Ok(())
}

fn handle_simple_string(received_message: &DataType) -> Result<(), anyhow::Error> {
    let string_content = received_message.as_string()?;
    if string_content.starts_with("FULLRESYNC") {
        let reply_parts: Vec<&str> = string_content.split(' ').collect();
        let replication_id = reply_parts.get(1).ok_or_else(|| {
            anyhow!(
                "Could not read replication_id from FULLRESYNC reply {:?}",
                string_content
            )
        })?;
        info!("Received replication_id {} from the master", replication_id);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_handle_connection_requires_active_client() {
        // This function requires an active TCP stream
        // Real integration tests needed in integration_tests/
        assert!(true);
    }
}
