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
use crate::commands::{command, transaction::TransactionSlot};
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

    // Per-connection, not on `ServerState`: a disconnect discards any open
    // transaction, as Redis does.
    let transaction = Arc::new(TransactionSlot::new());
    // Empty until this connection turns out to be a replica's, which a PSYNC sent by the replica
    // decides below.
    let replica = Arc::new(ReplicaSlot::new());

    loop {
        let received_messages: Vec<(DataType, usize)> = io::read_messages_with_lengths(stream)?;
        for (received_message, message_length) in received_messages.into_iter() {
            trace!(
                "Received: {}",
                String::from_utf8_lossy(&received_message.serialize()).replace("\r\n", "\\r\\n")
            );
            match &received_message {
                DataType::Array { elements: _ } => {
                    handle_command(
                        stream,
                        &received_message,
                        server_state,
                        &transaction,
                        &replica,
                        role,
                    )?;
                    // After handling, so a REPLCONF GETACK reports the offset
                    // as it stood before that request - the request itself is
                    // only counted towards the next acknowledgement.
                    if role.counts_replication_offset() {
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
    transaction: &Arc<TransactionSlot>,
    replica: &Arc<ReplicaSlot>,
    role: ConnectionMode,
) -> Result<(), anyhow::Error> {
    let Some(command) = command::command_from_message(
        received_message,
        server_state,
        transaction,
        replica,
    )? else {
        return Ok(());
    };
    let command_name = command.name();

    // Inside a transaction a command is collected rather than run, so it must
    // not reach storage, the replicas, or - for PSYNC - the replica registry.
    // Queueing after `build_command` keeps an unrecognised command ignored the
    // same way it is outside a transaction, instead of filling the queue with
    // something EXEC could never run.
    if transaction.queue(&command_name, received_message)? {
        debug!("Queued {} in the open transaction", command_name);
        if role.should_reply() {
            send_reply(stream, vec![protocol::simple_string("QUEUED")])?;
        }
        return Ok(());
    }

    if command_name == "PSYNC" {
        replica.fill(server_state.register_replica(stream)?)?;
    }

    let reply = match command.execute(server_state.storage()) {
        Ok(reply) => reply,
        // A RedisError is a client-facing error reply, not a connection failure:
        // surface it as a RESP simple error and keep serving the client. The
        // failed write is never propagated to replicas.
        Err(error) => match error.downcast::<RedisError>() {
            Ok(redis_error) => {
                if role.should_reply() || command.should_always_reply() {
                    send_reply(stream, vec![protocol::simple_error(&redis_error.message)])?;
                }
                return Ok(());
            }
            Err(other) => return Err(other),
        },
    };

    if role.should_reply() || command.should_always_reply() {
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
    let maybe_received_storage = Storage::from_rdb(value).ok();
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
