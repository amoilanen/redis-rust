/// Redis command trait and utilities.
///
/// This module defines the interface for Redis commands and exports
/// all available command implementations.

use std::sync::{Arc, Mutex};
use crate::protocol::DataType;
use crate::storage::Storage;

pub mod echo;
pub mod ping;
pub mod command;
pub mod set;
pub mod get;
pub mod incr;
pub mod info;
pub mod replconf;
pub mod psync;
pub mod list;
pub mod stream;
pub mod r#type;

// Re-export all command types for convenience
pub use echo::Echo;
pub use ping::Ping;
pub use command::Command;
pub use set::Set;
pub use get::Get;
pub use incr::Incr;
pub use info::Info;
pub use replconf::ReplConf;
pub use psync::PSync;
pub use list::{RPush, LPush, LRange, LLen, LPop, BLPop};
pub use stream::{XAdd, XRange, XRead};
pub use r#type::Type;

/// Trait for implementing Redis commands.
///
/// All Redis commands must implement this trait to be handled by the server.
pub trait RedisCommand {
    /// Execute the command and return response(s) to send to the client.
    fn execute(&self, storage: &Arc<Mutex<Storage>>) -> Result<Vec<DataType>, anyhow::Error>;

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
pub fn parse_command_name(received_message: &DataType) -> Result<String, anyhow::Error> {
    let received_message_parts: Vec<String> = received_message.as_vec()?;
    let command_parts: Vec<&str> = received_message_parts.iter().map(|x| x.as_str()).collect();
    let command_name = command_parts.get(0).unwrap_or(&"").to_string();
    Ok(command_name)
}

// ---------------------------------------------------------------------------
// Test-only helpers shared across the command unit tests.
//
// Private by design: Rust's visibility rules still make them reachable from
// every descendant module (each command and its `#[cfg(test)] mod tests`),
// while `#[cfg(test)]` strips them from release builds entirely.
// ---------------------------------------------------------------------------

/// Build a fresh, empty `Storage` wrapped in an `Arc<Mutex<...>>` for unit tests.
#[cfg(test)]
fn create_test_storage() -> Arc<Mutex<Storage>> {
    use std::collections::HashMap;
    Arc::new(Mutex::new(Storage::new(HashMap::new())))
}

/// Build a fresh `BlockingNotifier` for the commands that take one (RPUSH,
/// LPUSH, BLPOP, XADD, XREAD). Tests that never block still need one to
/// construct the command.
#[cfg(test)]
fn create_test_notifier() -> Arc<crate::blocking::BlockingNotifier> {
    Arc::new(crate::blocking::BlockingNotifier::new())
}

/// Build a RESP command message - an Array of bulk strings - from its parts.
///
/// `command_message(&["SET", "foo", "41"])` is the wire form a client sends,
/// which is exactly what every command struct takes as its `message`.
#[cfg(test)]
fn command_message(parts: &[&str]) -> DataType {
    crate::protocol::array(parts.iter().map(|p| crate::protocol::bulk_string(p)).collect())
}

/// Build a `SET` command from its parts, e.g. `set(&["SET", "foo", "41"])` or
/// `set(&["SET", "foo", "41", "px", "100"])`.
///
/// Seeding a string key is a fixture for many other commands' tests (GET,
/// TYPE, INCR, ...), so the builder lives here rather than in one of them.
#[cfg(test)]
fn set(parts: &[&str]) -> Set {
    Set { message: command_message(parts) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_command_name_valid() {
        let msg = command_message(&["SET", "key", "value"]);

        let name = parse_command_name(&msg).unwrap();
        assert_eq!(name, "SET");
    }

    #[test]
    fn test_parse_command_name_single() {
        let msg = command_message(&["PING"]);

        let name = parse_command_name(&msg).unwrap();
        assert_eq!(name, "PING");
    }
}
