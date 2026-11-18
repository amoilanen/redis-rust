//! ZCARD <key>
//!
//! Replies with the number of members in the sorted set at `key`, or 0 when
//! the key does not exist.

use std::sync::Mutex;

use anyhow::anyhow;
use log::*;

use crate::commands::RedisCommand;
use crate::error::RedisError;
use crate::protocol;
use crate::protocol::DataType;
use crate::storage::Storage;

/// ZCARD command implementation.
pub struct ZCard {
    pub message: DataType,
}

impl RedisCommand for ZCard {
    fn execute(&self, storage: &Mutex<Storage>) -> Result<Vec<DataType>, anyhow::Error> {
        let instructions: Vec<String> = self.message.as_string_vec()?;

        let [_, key] = instructions.as_slice() else {
            return Err(
                RedisError::new("ERR wrong number of arguments for 'zcard' command").into(),
            );
        };

        debug!("ZCARD {}", key);

        let cardinality = storage
            .lock()
            .map_err(|e| anyhow!("Failed to lock storage: {}", e))?
            .zcard(key)?;

        // A set can never hold anywhere near i64::MAX members.
        Ok(vec![protocol::integer(cardinality as i64)])
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
        "ZCARD"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::sorted_set::zadd;
    use crate::commands::{client_error_message, command_message, create_test_storage, set};

    fn zcard(key: &str) -> ZCard {
        ZCard { message: command_message(&["ZCARD", key]) }
    }

    #[test]
    fn test_zcard_counts_members() -> anyhow::Result<()> {
        let storage = create_test_storage();
        zadd(&["ZADD", "zset_key", "20.0", "zset_member1"]).execute(&storage)?;
        zadd(&["ZADD", "zset_key", "30.1", "zset_member2"]).execute(&storage)?;
        zadd(&["ZADD", "zset_key", "40.2", "zset_member3"]).execute(&storage)?;
        zadd(&["ZADD", "zset_key", "50.3", "zset_member4"]).execute(&storage)?;

        assert_eq!(zcard("zset_key").execute(&storage)?, vec![protocol::integer(4)]);
        Ok(())
    }

    #[test]
    fn test_zcard_unchanged_by_score_update() -> anyhow::Result<()> {
        let storage = create_test_storage();
        zadd(&["ZADD", "zset_key", "20.0", "a"]).execute(&storage)?;
        zadd(&["ZADD", "zset_key", "30.0", "b"]).execute(&storage)?;
        zadd(&["ZADD", "zset_key", "100.0", "a"]).execute(&storage)?;

        assert_eq!(zcard("zset_key").execute(&storage)?, vec![protocol::integer(2)]);
        Ok(())
    }

    #[test]
    fn test_zcard_missing_key_is_zero() -> anyhow::Result<()> {
        let storage = create_test_storage();
        let reply = zcard("missing_key").execute(&storage)?;

        assert_eq!(reply, vec![protocol::integer(0)]);
        assert_eq!(reply[0].serialize(), b":0\r\n");
        Ok(())
    }

    #[test]
    fn test_zcard_wrong_number_of_arguments() {
        let storage = create_test_storage();
        for parts in [&["ZCARD"][..], &["ZCARD", "a", "b"]] {
            let error = ZCard { message: command_message(parts) }.execute(&storage).unwrap_err();
            assert_eq!(client_error_message(error), "ERR wrong number of arguments for 'zcard' command");
        }
    }

    #[test]
    fn test_zcard_on_string_key_is_wrong_type() -> anyhow::Result<()> {
        let storage = create_test_storage();
        set(&["SET", "k", "v"]).execute(&storage)?;

        let error = zcard("k").execute(&storage).unwrap_err();
        assert_eq!(
            client_error_message(error),
            "WRONGTYPE Operation against a key holding the wrong kind of value"
        );
        Ok(())
    }

    #[test]
    fn test_zcard_is_not_propagated_to_replicas() {
        assert!(!zcard("z").is_propagated_to_replicas());
    }
}
