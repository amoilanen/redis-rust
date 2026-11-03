/// Redis command trait and utilities.
///
/// This module defines the interface for Redis commands and exports
/// all available command implementations.

use std::str::FromStr;
use std::sync::Mutex;
use crate::error::RedisError;
use crate::protocol::DataType;
use crate::storage::Storage;

pub mod echo;
pub mod ping;
pub mod command;
pub mod config;
pub mod set;
pub mod get;
pub mod incr;
pub mod keys;
pub mod info;
pub mod replconf;
pub mod psync;
pub mod wait;
pub mod list;
pub mod pubsub;
pub mod sorted_set;
pub mod stream;
pub mod transaction;
pub mod r#type;

// Re-export all command types for convenience
pub use echo::Echo;
pub use ping::Ping;
pub use command::Command;
pub use config::Config;
pub use set::Set;
pub use get::Get;
pub use incr::Incr;
pub use keys::Keys;
pub use info::Info;
pub use replconf::ReplConf;
pub use psync::PSync;
pub use wait::Wait;
pub use list::{RPush, LPush, LRange, LLen, LPop, BLPop};
pub use pubsub::{Publish, Subscribe, Unsubscribe};
pub use sorted_set::ZAdd;
pub use stream::{XAdd, XRange, XRead};
pub use transaction::{Multi, Exec, Discard};
pub use r#type::Type;

/// Trait for implementing Redis commands.
///
/// All Redis commands must implement this trait to be handled by the server.
pub trait RedisCommand {
    /// Execute the command and return response(s) to send to the client.
    fn execute(&self, storage: &Mutex<Storage>) -> Result<Vec<DataType>, anyhow::Error>;

    /// Whether this command should be propagated to replica servers.
    fn is_propagated_to_replicas(&self) -> bool;

    /// Whether to send a response even if this is a replica receiving replicated commands.
    fn should_always_reply(&self) -> bool;

    /// Serialize this command to its RESP protocol representation.
    fn serialize(&self) -> Vec<u8>;

    /// Name of the commmand
    fn name(&self) -> &str;
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
    let received_message_parts: Vec<String> = received_message.as_string_vec()?;
    let command_parts: Vec<&str> = received_message_parts.iter().map(|x| x.as_str()).collect();
    let command_name = command_parts.get(0).unwrap_or(&"").to_string();
    Ok(command_name)
}

/// Parses `text` as a `T`, failing with the caller's `error` instead of the
/// parser's own.
///
/// `str::parse` fails with errors like `ParseIntError`, which would travel as
/// an internal failure and drop the connection. A client that mistyped an
/// argument should just be told so, in the wording the caller picks - Redis
/// words a bad count differently from a bad timeout.
pub(crate) fn try_parsing<T: FromStr, E: Into<anyhow::Error>>(
    text: &str,
    error: E,
) -> Result<T, anyhow::Error> {
    text.parse().map_err(|_| error.into())
}

/// Reads a sorted set score the way Redis does: any float, including `inf`,
/// `+inf` and `-inf`, but never NaN, which has no place in an ordering.
pub(crate) fn parse_score(argument: &str) -> Result<f64, anyhow::Error> {
    let not_a_float = || RedisError::new("ERR value is not a valid float");
    let score: f64 = try_parsing(argument, not_a_float())?;
    if score.is_nan() {
        return Err(not_a_float().into());
    }
    Ok(score)
}

// ---------------------------------------------------------------------------
// Test-only helpers shared across the command unit tests.
//
// Private by design: Rust's visibility rules still make them reachable from
// every descendant module (each command and its `#[cfg(test)] mod tests`),
// while `#[cfg(test)]` strips them from release builds entirely.
// ---------------------------------------------------------------------------

/// Build a fresh, empty `Storage` for unit tests.
///
/// Still an `Arc` even though `execute` now takes a bare `&Mutex<Storage>`:
/// the BLPOP tests hand the keyspace to a second thread, and every other call
/// site passes `&storage`, which deref-coerces to `&Mutex<Storage>`.
#[cfg(test)]
fn create_test_storage() -> std::sync::Arc<Mutex<Storage>> {
    use std::collections::HashMap;
    std::sync::Arc::new(Mutex::new(Storage::new(HashMap::new())))
}

/// Build a fresh `BlockingNotifier` for the commands that take one (RPUSH,
/// LPUSH, BLPOP, XADD, XREAD). Tests that never block still need one to
/// construct the command.
#[cfg(test)]
fn create_test_notifier() -> std::sync::Arc<crate::blocking::BlockingNotifier> {
    std::sync::Arc::new(crate::blocking::BlockingNotifier::new())
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

/// The text of the `-...\r\n` simple error `error` sends back. Panics if it is
/// an internal error instead, since those close the connection rather than
/// reaching the client.
#[cfg(test)]
fn client_error_message(error: anyhow::Error) -> String {
    error
        .downcast::<crate::error::RedisError>()
        .expect("failure should be a client-facing RedisError")
        .message
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

    #[test]
    fn test_try_parsing_reads_any_number_type() {
        let error = || RedisError::new("ERR nope");

        assert_eq!(try_parsing::<i64, _>("-2", error()).unwrap(), -2);
        assert_eq!(try_parsing::<usize, _>("60000", error()).unwrap(), 60000);
        assert_eq!(try_parsing::<f64, _>("0.5", error()).unwrap(), 0.5);
    }

    #[test]
    fn test_try_parsing_reports_the_callers_error_to_the_client() {
        // A client-facing RedisError, not the ParseIntError that would travel
        // as an internal failure and take the connection down with it.
        let error = try_parsing::<i64, _>("soon", RedisError::new("ERR bad timeout")).unwrap_err();

        assert_eq!(client_error_message(error), "ERR bad timeout");
    }

    #[test]
    fn test_try_parsing_accepts_any_error_convertible_to_anyhow() {
        let error = try_parsing::<i64, _>("soon", anyhow::anyhow!("internal")).unwrap_err();

        assert_eq!(error.to_string(), "internal");
    }

    #[test]
    fn test_parse_score_accepts_floats_and_infinities() {
        assert_eq!(parse_score("10.0").unwrap(), 10.0);
        assert_eq!(parse_score("-1e3").unwrap(), -1000.0);
        assert_eq!(parse_score("inf").unwrap(), f64::INFINITY);
        assert_eq!(parse_score("+inf").unwrap(), f64::INFINITY);
        assert_eq!(parse_score("-inf").unwrap(), f64::NEG_INFINITY);
    }

    #[test]
    fn test_parse_score_rejects_nan_and_text() {
        for bad in ["nan", "NaN", "abc", ""] {
            let error = parse_score(bad).unwrap_err();
            assert_eq!(client_error_message(error), "ERR value is not a valid float");
        }
    }
}
