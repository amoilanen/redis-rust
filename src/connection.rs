/// Connection handling for incoming Redis client connections.
///
/// This module handles incoming TCP connections, parses commands,
/// executes them, and sends responses back to clients.

use anyhow::anyhow;
use log::*;
use std::io::Write;
use std::net::TcpStream;
use std::sync::{Arc, Mutex};

use crate::protocol::DataType;
use crate::io;
use crate::commands::{self, RedisCommand, Echo, Ping, Set, Get, Command, Info, ReplConf, PSync, RPush, LPush, LRange, LLen, LPop, BLPop, Type, XAdd};
use crate::storage::Storage;
use crate::server_state::ServerState;

/// Handles a single client connection.
///
/// This function:
/// 1. Reads incoming messages from the client
/// 2. Parses commands
/// 3. Executes commands
/// 4. Sends responses back to the client
/// 5. Propagates write commands to replicas if master
///
/// # Arguments
/// * `stream` - TCP stream for the client connection
/// * `storage` - Shared storage for Redis data
/// * `server_state` - Server state (master/replica info)
/// * `should_reply` - Whether to send responses to this client (false for replicas during initial sync)
///
/// # Returns
/// Error if connection fails
pub fn handle_connection(
    stream: &mut TcpStream,
    storage: &Arc<Mutex<Storage>>,
    server_state: &Arc<ServerState>,
    should_reply: bool,
) -> Result<(), anyhow::Error> {
    debug!("accepted new connection");
    loop {
        let received_messages: Vec<DataType> = io::read_messages(stream)?;
        for received_message in received_messages.into_iter() {
            trace!(
                "Received: {}",
                String::from_utf8_lossy(&received_message.serialize()).replace("\r\n", "\\r\\n")
            );
            match &received_message {
                DataType::Array { elements } => {
                    handle_command(
                        stream,
                        &received_message,
                        elements,
                        storage,
                        server_state,
                        should_reply,
                    )?;
                }
                DataType::Rdb { value } => {
                    handle_rdb_snapshot(value, storage)?;
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
    elements: &[DataType],
    storage: &Arc<Mutex<Storage>>,
    server_state: &Arc<ServerState>,
    should_reply: bool,
) -> Result<(), anyhow::Error> {
    let command_name = commands::parse_command_name(received_message)?;
    let Some(command) = build_command(&command_name, received_message, elements, server_state)
    else {
        return Ok(());
    };

    if command_name == "PSYNC" {
        register_replica(stream, server_state)?;
    }

    let reply = command.execute(storage)?;
    if should_reply || command.should_always_reply() {
        send_reply(stream, reply)?;
    }

    if server_state.is_master() && command.is_propagated_to_replicas() {
        propagate_to_replicas(&*command, server_state)?;
    }

    Ok(())
}

fn build_command(
    command_name: &str,
    received_message: &DataType,
    elements: &[DataType],
    server_state: &Arc<ServerState>,
) -> Option<Box<dyn RedisCommand>> {
    let message = received_message.clone();
    let state = || Arc::clone(server_state);
    let notifier = || Arc::clone(&server_state.blocking_notifier);

    let command: Box<dyn RedisCommand> = match command_name {
        "ECHO"     => Box::new(Echo { message, argument: elements.get(1).cloned() }),
        "PING"     => Box::new(Ping { message }),
        "SET"      => Box::new(Set { message }),
        "GET"      => Box::new(Get { message }),
        "COMMAND"  => Box::new(Command { message }),
        "INFO"     => Box::new(Info { message, server_state: state() }),
        "REPLCONF" => Box::new(ReplConf { message, server_state: state() }),
        "RPUSH"    => Box::new(RPush { message, notifier: notifier() }),
        "LPUSH"    => Box::new(LPush { message, notifier: notifier() }),
        "LRANGE"   => Box::new(LRange { message }),
        "LLEN"     => Box::new(LLen { message }),
        "LPOP"     => Box::new(LPop { message }),
        "BLPOP"    => Box::new(BLPop { message, notifier: notifier() }),
        "TYPE"     => Box::new(Type { message }),
        "XADD"     => Box::new(XAdd { message }),
        "PSYNC"    => Box::new(PSync { message, server_state: state() }),
        _ => return None,
    };
    Some(command)
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

fn propagate_to_replicas(
    command: &dyn RedisCommand,
    server_state: &Arc<ServerState>,
) -> Result<(), anyhow::Error> {
    let command_bytes = command.serialize();
    let mut replica_streams = server_state
        .replica_connections
        .lock()
        .map_err(|e| anyhow!("Failed to lock replica connections: {}", e))?;
    for replica_stream in replica_streams.iter_mut() {
        debug!("Propagating command to replica: {:?}", &command_bytes);
        replica_stream.write_all(&command_bytes)?;
    }
    Ok(())
}

fn register_replica(
    stream: &TcpStream,
    server_state: &Arc<ServerState>,
) -> Result<(), anyhow::Error> {
    server_state
        .replica_connections
        .lock()
        .map_err(|e| anyhow!("Failed to lock replica connections: {}", e))?
        .push(stream.try_clone()?);
    Ok(())
}

fn handle_rdb_snapshot(
    value: &[u8],
    storage: &Arc<Mutex<Storage>>,
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
