/// ZRANK <key> <member>
///
/// Replies with `member`'s 0-based rank in the sorted set at `key`, ordered
/// by ascending score and, for equal scores, lexicographically by member.
/// Replies with a null bulk string when the key or the member does not exist.
///
/// The WITHSCORE option is not supported yet.

use std::sync::Mutex;

use anyhow::anyhow;
use log::*;

use crate::commands::RedisCommand;
use crate::error::RedisError;
use crate::protocol;
use crate::protocol::DataType;
use crate::storage::Storage;

/// ZRANK command implementation.
pub struct ZRank {
    pub message: DataType,
}

impl RedisCommand for ZRank {
    fn execute(&self, storage: &Mutex<Storage>) -> Result<Vec<DataType>, anyhow::Error> {
        let instructions: Vec<String> = self.message.as_string_vec()?;

        let [_, key, member] = instructions.as_slice() else {
            return Err(
                RedisError::new("ERR wrong number of arguments for 'zrank' command").into(),
            );
        };

        debug!("ZRANK {} {}", key, member);

        let rank = storage
            .lock()
            .map_err(|e| anyhow!("Failed to lock storage: {}", e))?
            .zrank(key, member)?;

        Ok(vec![match rank {
            Some(rank) => protocol::integer(rank as i64),
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
        "ZRANK"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::sorted_set::zadd;
    use crate::commands::{client_error_message, command_message, create_test_storage, set};

    fn zrank(key: &str, member: &str) -> ZRank {
        ZRank { message: command_message(&["ZRANK", key, member]) }
    }

    #[test]
    fn test_zrank_orders_by_score_then_member() -> anyhow::Result<()> {
        let storage = create_test_storage();
        zadd(&["ZADD", "zset_key", "100.0", "foo"]).execute(&storage)?;
        zadd(&["ZADD", "zset_key", "100.0", "bar"]).execute(&storage)?;
        zadd(&["ZADD", "zset_key", "20.0", "baz"]).execute(&storage)?;
        zadd(&["ZADD", "zset_key", "30.1", "caz"]).execute(&storage)?;
        zadd(&["ZADD", "zset_key", "40.2", "paz"]).execute(&storage)?;

        for (member, rank) in [("baz", 0), ("caz", 1), ("paz", 2), ("bar", 3), ("foo", 4)] {
            assert_eq!(zrank("zset_key", member).execute(&storage)?, vec![protocol::integer(rank)]);
        }
        Ok(())
    }

    #[test]
    fn test_zrank_missing_member_or_key_is_null_bulk_string() -> anyhow::Result<()> {
        let storage = create_test_storage();
        zadd(&["ZADD", "zset_key", "1", "member"]).execute(&storage)?;

        assert_eq!(
            zrank("zset_key", "missing_member").execute(&storage)?,
            vec![protocol::bulk_string_empty()]
        );
        assert_eq!(
            zrank("missing_key", "member").execute(&storage)?,
            vec![protocol::bulk_string_empty()]
        );
        Ok(())
    }

    #[test]
    fn test_zrank_null_reply_serializes_as_null_bulk_string() -> anyhow::Result<()> {
        let storage = create_test_storage();
        let reply = zrank("missing_key", "member").execute(&storage)?;

        assert_eq!(reply[0].serialize(), b"$-1\r\n");
        Ok(())
    }

    #[test]
    fn test_zrank_wrong_number_of_arguments() {
        let storage = create_test_storage();
        for parts in [&["ZRANK", "z"][..], &["ZRANK"], &["ZRANK", "z", "m", "WITHSCORE"]] {
            let error = ZRank { message: command_message(parts) }.execute(&storage).unwrap_err();
            assert_eq!(client_error_message(error), "ERR wrong number of arguments for 'zrank' command");
        }
    }

    #[test]
    fn test_zrank_on_string_key_is_wrong_type() -> anyhow::Result<()> {
        let storage = create_test_storage();
        set(&["SET", "k", "v"]).execute(&storage)?;

        let error = zrank("k", "m").execute(&storage).unwrap_err();
        assert_eq!(
            client_error_message(error),
            "WRONGTYPE Operation against a key holding the wrong kind of value"
        );
        Ok(())
    }

    #[test]
    fn test_zrank_is_not_propagated_to_replicas() {
        assert!(!zrank("z", "m").is_propagated_to_replicas());
    }
}
