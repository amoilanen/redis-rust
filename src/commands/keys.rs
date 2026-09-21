//! KEYS command - the keys in the database whose name matches a pattern.
//!
//! Syntax: KEYS <pattern>
//! Returns: a RESP array of bulk strings, one per matching key, in no
//! particular order - `*1\r\n$3\r\nfoo\r\n` for a database holding only
//! `foo`. A database with nothing in it, or a pattern nothing matches, is an
//! empty array rather than a null one.
//!
//! The pattern is a glob; see [`crate::glob`] for the syntax.
//!
//! Expired keys are left out, but not deleted on the way past: a read that
//! rewrote the keyspace would be a write replicas never hear about.

use std::sync::Mutex;

use log::*;

use crate::error::RedisError;
use crate::glob;
use crate::protocol;
use crate::protocol::DataType;
use crate::storage::Storage;

use super::RedisCommand;

/// KEYS command implementation.
pub struct Keys {
    pub message: DataType,
}

impl RedisCommand for Keys {
    fn execute(&self, storage: &Mutex<Storage>) -> Result<Vec<DataType>, anyhow::Error> {
        let instructions: Vec<String> = self.message.as_string_vec()?;

        // Exactly one argument, as Redis has it: no pattern is an error, not `*`.
        if instructions.len() != 2 {
            return Err(
                RedisError::new("ERR wrong number of arguments for 'keys' command").into(),
            );
        }
        let pattern = &instructions[1];

        debug!("KEYS {}", pattern);

        let pattern = glob::Pattern::new(pattern.as_bytes());

        let storage = storage
            .lock()
            .map_err(|e| anyhow::anyhow!("Failed to lock storage: {}", e))?;

        let matching: Vec<DataType> = storage
            .data
            .iter()
            .filter(|(_, stored)| !stored.is_expired())
            .map(|(key, _)| key)
            .filter(|key| pattern.matches(key.as_bytes()))
            .map(|key| protocol::bulk_string(key))
            .collect();

        Ok(vec![protocol::array(matching)])
    }

    fn is_propagated_to_replicas(&self) -> bool {
        false
    }

    fn should_always_reply(&self) -> bool {
        false
    }

    fn serialize(&self) -> Vec<u8> {
        self.message.serialize()
    }

    fn name(&self) -> &str {
        "KEYS"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::{client_error_message, command_message, create_test_storage, set, stream::XAdd};

    fn keys(pattern: &str) -> Keys {
        Keys { message: command_message(&["KEYS", pattern]) }
    }

    /// The key names a `KEYS <pattern>` reply holds, sorted: the keyspace is a
    /// hash map, so the order they come back in is not a property worth
    /// asserting on.
    fn matched(storage: &Mutex<Storage>, pattern: &str) -> anyhow::Result<Vec<String>> {
        let reply = keys(pattern).execute(storage)?;
        assert_eq!(reply.len(), 1, "KEYS replies with exactly one array");

        let mut names: Vec<String> = reply[0]
            .as_vec()?
            .iter()
            .map(|element| element.as_string())
            .collect::<Result<_, _>>()?;
        names.sort();
        Ok(names)
    }

    // ----------------------------------------------------------------- KEYS

    #[test]
    fn should_answer_an_empty_database_with_an_empty_array() -> anyhow::Result<()> {
        let storage = create_test_storage();

        assert_eq!(matched(&storage, "*")?, Vec::<String>::new());
        Ok(())
    }

    #[test]
    fn should_list_every_key_for_the_star_pattern() -> anyhow::Result<()> {
        let storage = create_test_storage();
        set(&["SET", "foo", "1"]).execute(&storage)?;
        set(&["SET", "bar", "2"]).execute(&storage)?;
        set(&["SET", "baz", "3"]).execute(&storage)?;

        assert_eq!(matched(&storage, "*")?, vec!["bar", "baz", "foo"]);
        Ok(())
    }

    #[test]
    fn should_list_only_the_keys_the_pattern_matches() -> anyhow::Result<()> {
        let storage = create_test_storage();
        set(&["SET", "foo", "1"]).execute(&storage)?;
        set(&["SET", "bar", "2"]).execute(&storage)?;
        set(&["SET", "baz", "3"]).execute(&storage)?;

        assert_eq!(matched(&storage, "ba*")?, vec!["bar", "baz"]);
        assert_eq!(matched(&storage, "ba?")?, vec!["bar", "baz"]);
        assert_eq!(matched(&storage, "ba[r]")?, vec!["bar"]);
        assert_eq!(matched(&storage, "nothing*")?, Vec::<String>::new());
        Ok(())
    }

    #[test]
    fn should_leave_out_a_key_that_has_expired() -> anyhow::Result<()> {
        let storage = create_test_storage();
        set(&["SET", "lasting", "1"]).execute(&storage)?;
        set(&["SET", "fleeting", "2", "px", "1"]).execute(&storage)?;
        std::thread::sleep(std::time::Duration::from_millis(20));

        assert_eq!(matched(&storage, "*")?, vec!["lasting"]);
        Ok(())
    }

    #[test]
    fn should_list_a_key_whose_expiry_has_not_come_yet() -> anyhow::Result<()> {
        let storage = create_test_storage();
        set(&["SET", "later", "1", "px", "100000"]).execute(&storage)?;

        assert_eq!(matched(&storage, "*")?, vec!["later"]);
        Ok(())
    }

    #[test]
    fn should_list_keys_of_every_type() -> anyhow::Result<()> {
        // A stream is a key like any other: KEYS names keys, whatever they
        // hold.
        let storage = create_test_storage();
        set(&["SET", "a_string", "1"]).execute(&storage)?;
        XAdd {
            message: command_message(&["XADD", "a_stream", "0-1", "field", "value"]),
            notifier: crate::commands::create_test_notifier(),
        }
        .execute(&storage)?;

        assert_eq!(matched(&storage, "*")?, vec!["a_stream", "a_string"]);
        Ok(())
    }

    #[test]
    fn should_reject_a_call_that_names_no_pattern_or_too_many() {
        let storage = create_test_storage();

        let no_pattern = Keys { message: command_message(&["KEYS"]) };
        assert_eq!(
            client_error_message(no_pattern.execute(&storage).unwrap_err()),
            "ERR wrong number of arguments for 'keys' command"
        );

        let two_patterns = Keys { message: command_message(&["KEYS", "a*", "b*"]) };
        assert_eq!(
            client_error_message(two_patterns.execute(&storage).unwrap_err()),
            "ERR wrong number of arguments for 'keys' command"
        );
    }

    #[test]
    fn should_not_propagate_a_read_to_the_replicas() {
        assert!(!keys("*").is_propagated_to_replicas());
        assert_eq!(keys("*").name(), "KEYS");
    }

}
