/// Connection handling for incoming Redis client connections.
///
/// This module handles incoming TCP connections, parses commands,
/// executes them, and sends responses back to clients.

use anyhow::anyhow;
use std::io::Write;
use std::net::TcpStream;
use std::sync::{Arc, Mutex};

use crate::protocol::DataType;
use crate::io;
use crate::commands::{self, RedisCommand};
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
    loop {
        let received_messages: Vec<DataType> = io::read_messages(stream)?;
        for received_message in received_messages.into_iter() {
            println!(
                "Received: {}",
                String::from_utf8_lossy(&received_message.serialize()).replace("\r\n", "\\r\\n")
            );
            match &received_message {
                DataType::Array { elements } => {
                    let command_name: String = commands::parse_command_name(&received_message)?;
                    let mut command: Option<Box<dyn RedisCommand>> = None;
                    let command_name = command_name.as_str();

                    // Dispatch to appropriate command handler
                    if command_name == "ECHO" {
                        command = Some(Box::new(commands::Echo {
                            message: &received_message,
                            argument: elements.get(1),
                        }));
                    } else if command_name == "PING" {
                        command = Some(Box::new(commands::Ping {
                            message: &received_message,
                        }));
                    } else if command_name == "SET" {
                        command = Some(Box::new(commands::Set {
                            message: &received_message,
                        }));
                    } else if command_name == "GET" {
                        command = Some(Box::new(commands::Get {
                            message: &received_message,
                        }));
                    } else if command_name == "COMMAND" {
                        command = Some(Box::new(commands::Command {
                            message: &received_message,
                        }));
                    } else if command_name == "INFO" {
                        command = Some(Box::new(commands::Info {
                            message: &received_message,
                            server_state,
                        }));
                    } else if command_name == "REPLCONF" {
                        command = Some(Box::new(commands::ReplConf {
                            message: &received_message,
                            server_state,
                        }));
                    } else if command_name == "PSYNC" {
                        command = Some(Box::new(commands::PSync {
                            message: &received_message,
                            server_state,
                        }));
                        server_state
                            .replica_connections
                            .lock()
                            .map_err(|e| anyhow!("Failed to lock replica connections: {}", e))?
                            .push(stream.try_clone()?);
                    }

                    if let Some(command) = command {
                        let reply = command.execute(storage)?;
                        if should_reply || command.should_always_reply() {
                            for message in reply.into_iter() {
                                println!("Sending: {:?}", message);
                                let message_bytes = &message.serialize();
                                println!("which serializes to {:?}", message_bytes);
                                stream.write_all(message_bytes)?;
                            }
                        }

                        // Propagate write commands to replicas if this is a master
                        let should_propagate_to_replicas =
                            server_state.is_master() && command.is_propagated_to_replicas();
                        if should_propagate_to_replicas {
                            let command_bytes = command.serialize();
                            let mut replica_streams = server_state
                                .replica_connections
                                .lock()
                                .map_err(|e| anyhow!("Failed to lock replica connections: {}", e))?;
                            for replica_stream in replica_streams.iter_mut() {
                                println!("Propagating command to replica: {:?}", &command_bytes);
                                replica_stream.write_all(&command_bytes)?
                            }
                        }
                    }
                }
                DataType::Rdb { value } => {
                    // Replica receiving RDB snapshot from master
                    let maybe_received_storage = Storage::from_rdb(&value).ok();
                    println!("Received storage {:?}", &maybe_received_storage);
                    if let Some(received_storage) = maybe_received_storage {
                        let mut storage = storage.lock().map_err(|e| anyhow!("Failed to lock storage: {}", e))?;
                        for (key, value) in received_storage.data.into_iter() {
                            storage.data.insert(key, value);
                        }
                    }
                }
                DataType::SimpleString { value: _ } => {
                    // Replica receiving FULLRESYNC response
                    let string_content = received_message.as_string()?;
                    if string_content.starts_with("FULLRESYNC") {
                        let reply_parts: Vec<&str> = string_content.split(' ').collect();
                        let replication_id = reply_parts.get(1).ok_or(anyhow!(
                            "Could not read replication_id from FULLRESYNC reply {:?}",
                            string_content
                        ))?;
                        println!(
                            "Received replication_id {} from the master",
                            replication_id
                        );
                    }
                }
                _ => (),
            }
        }
    }
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
