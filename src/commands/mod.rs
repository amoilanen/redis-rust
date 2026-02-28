/// Redis command trait and utilities.
///
/// This module defines the interface for Redis commands and exports
/// all available command implementations.

use std::sync::{Arc, Mutex};
use crate::protocol;
use crate::storage;

pub mod echo;
pub mod ping;
pub mod command;
pub mod set;
pub mod get;
pub mod info;
pub mod replconf;
pub mod psync;

// Re-export all command types for convenience
pub use echo::Echo;
pub use ping::Ping;
pub use command::Command;
pub use set::Set;
pub use get::Get;
pub use info::Info;
pub use replconf::ReplConf;
pub use psync::PSync;

/// Trait for implementing Redis commands.
///
/// All Redis commands must implement this trait to be handled by the server.
pub trait RedisCommand {
    /// Execute the command and return response(s) to send to the client.
    fn execute(&self, storage: &Arc<Mutex<storage::Storage>>) -> Result<Vec<protocol::DataType>, anyhow::Error>;
    
    /// Whether this command should be propagated to replica servers.
    fn is_propagated_to_replicas(&self) -> bool;
    
    /// Whether to send a response even if this is a replica receiving replicated commands.
    fn should_always_reply(&self) -> bool;
    
    /// Serialize this command to its RESP protocol representation.
    fn serialize(&self) -> Vec<u8>;
}

/// Parses the command name from a received message.
///
/// # Arguments
/// * `received_message` - The parsed RESP message (should be an array)
///
/// # Returns
/// The command name (first element of the array) or empty string if not an array
///
/// # Errors
/// Returns error if message cannot be converted to array
pub fn parse_command_name(received_message: &protocol::DataType) -> Result<String, anyhow::Error> {
    let received_message_parts: Vec<String> = received_message.as_vec()?;
    let command_parts: Vec<&str> = received_message_parts.iter().map(|x| x.as_str()).collect();
    let command_name = command_parts.get(0).unwrap_or(&"").to_string();
    Ok(command_name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_command_name_valid() {
        let msg = protocol::array(vec![
            protocol::bulk_string("SET"),
            protocol::bulk_string("key"),
            protocol::bulk_string("value"),
        ]);
        
        let name = parse_command_name(&msg).unwrap();
        assert_eq!(name, "SET");
    }

    #[test]
    fn test_parse_command_name_single() {
        let msg = protocol::array(vec![
            protocol::bulk_string("PING"),
        ]);
        
        let name = parse_command_name(&msg).unwrap();
        assert_eq!(name, "PING");
    }
}
