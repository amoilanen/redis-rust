//! ZSCORE <key> <member>
//!
//! Replies with `member`'s score in the sorted set at `key` as a bulk string
//! (formatted by [`format_score`]), or a null bulk string when the key or the
//! member does not exist.

use std::sync::Mutex;

use anyhow::anyhow;
use log::*;

use crate::commands::RedisCommand;
use crate::commands::sorted_set::score::format_score;
use crate::error::RedisError;
use crate::protocol;
use crate::protocol::DataType;
use crate::storage::Storage;

/// ZSCORE command implementation.
pub struct ZScore {
    pub message: DataType,
}

impl RedisCommand for ZScore {
    fn execute(&self, storage: &Mutex<Storage>) -> Result<Vec<DataType>, anyhow::Error> {
        let instructions: Vec<String> = self.message.as_string_vec()?;

        let [_, key, member] = instructions.as_slice() else {
            return Err(
                RedisError::new("ERR wrong number of arguments for 'zscore' command").into(),
            );
        };

        debug!("ZSCORE {} {}", key, member);

        let score = storage
            .lock()
            .map_err(|e| anyhow!("Failed to lock storage: {}", e))?
            .zscore(key, member)?;

        Ok(vec![match score {
            Some(score) => protocol::bulk_string(&format_score(score)),
            None => protocol::bulk_string_empty(),
        }])
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
        "ZSCORE"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::sorted_set::zadd;
    use crate::commands::{client_error_message, command_message, create_test_storage, set};

    fn zscore(key: &str, member: &str) -> ZScore {
        ZScore { message: command_message(&["ZSCORE", key, member]) }
    }

    #[test]
    fn test_zscore_returns_score_as_bulk_string() -> anyhow::Result<()> {
        let storage = create_test_storage();
        zadd(&["ZADD", "zset_key", "20.0", "zset_member1"]).execute(&storage)?;
        zadd(&["ZADD", "zset_key", "30.1", "zset_member2"]).execute(&storage)?;
        zadd(&["ZADD", "zset_key", "40.2", "zset_member3"]).execute(&storage)?;
        zadd(&["ZADD", "zset_key", "50.3", "zset_member4"]).execute(&storage)?;

        let reply = zscore("zset_key", "zset_member2").execute(&storage)?;
        assert_eq!(reply, vec![protocol::bulk_string("30.1")]);
        assert_eq!(reply[0].serialize(), b"$4\r\n30.1\r\n");

        // A whole-number score loses its trailing ".0", as in Redis.
        assert_eq!(
            zscore("zset_key", "zset_member1").execute(&storage)?,
            vec![protocol::bulk_string("20")]
        );
        Ok(())
    }

    #[test]
    fn test_zscore_follows_score_update() -> anyhow::Result<()> {
        let storage = create_test_storage();
        zadd(&["ZADD", "zset_key", "30.1", "zset_member2"]).execute(&storage)?;
        zadd(&["ZADD", "zset_key", "100.99", "zset_member2"]).execute(&storage)?;

        assert_eq!(
            zscore("zset_key", "zset_member2").execute(&storage)?,
            vec![protocol::bulk_string("100.99")]
        );
        Ok(())
    }

    #[test]
    fn test_zscore_infinite_scores() -> anyhow::Result<()> {
        let storage = create_test_storage();
        zadd(&["ZADD", "z", "+inf", "top", "-inf", "bottom"]).execute(&storage)?;

        assert_eq!(zscore("z", "top").execute(&storage)?, vec![protocol::bulk_string("inf")]);
        assert_eq!(zscore("z", "bottom").execute(&storage)?, vec![protocol::bulk_string("-inf")]);
        Ok(())
    }

    #[test]
    fn test_zscore_missing_member_or_key_is_null_bulk_string() -> anyhow::Result<()> {
        let storage = create_test_storage();
        zadd(&["ZADD", "zset_key", "1", "member"]).execute(&storage)?;

        let reply = zscore("zset_key", "zset_member100").execute(&storage)?;
        assert_eq!(reply, vec![protocol::bulk_string_empty()]);
        assert_eq!(reply[0].serialize(), b"$-1\r\n");

        assert_eq!(
            zscore("missing_key", "member").execute(&storage)?,
            vec![protocol::bulk_string_empty()]
        );
        Ok(())
    }

    #[test]
    fn test_zscore_wrong_number_of_arguments() {
        let storage = create_test_storage();
        for parts in [&["ZSCORE"][..], &["ZSCORE", "z"], &["ZSCORE", "z", "m", "extra"]] {
            let error = ZScore { message: command_message(parts) }.execute(&storage).unwrap_err();
            assert_eq!(client_error_message(error), "ERR wrong number of arguments for 'zscore' command");
        }
    }

    #[test]
    fn test_zscore_on_string_key_is_wrong_type() -> anyhow::Result<()> {
        let storage = create_test_storage();
        set(&["SET", "k", "v"]).execute(&storage)?;

        let error = zscore("k", "m").execute(&storage).unwrap_err();
        assert_eq!(
            client_error_message(error),
            "WRONGTYPE Operation against a key holding the wrong kind of value"
        );
        Ok(())
    }

    #[test]
    fn test_zscore_is_not_propagated_to_replicas() {
        assert!(!zscore("z", "m").is_propagated_to_replicas());
    }
}
