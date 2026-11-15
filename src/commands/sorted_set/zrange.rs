//! ZRANGE <key> <start> <stop>
//!
//! Replies with the members of the sorted set at `key` at positions `start`
//! through `stop`, both inclusive, ordered by ascending score and, for equal
//! scores, lexicographically by member. A negative index counts from the end,
//! `-1` being the last member. Out-of-range indexes are clamped to the set,
//! and a missing key or an empty range replies with an empty array.
//!
//! The BYSCORE/BYLEX/REV/LIMIT/WITHSCORES options are not supported yet.

use std::sync::Mutex;

use anyhow::anyhow;
use log::*;

use crate::commands::{RedisCommand, try_parsing};
use crate::error::RedisError;
use crate::protocol;
use crate::protocol::DataType;
use crate::storage::Storage;

/// ZRANGE command implementation.
pub struct ZRange {
    pub message: DataType,
}

impl RedisCommand for ZRange {
    fn execute(&self, storage: &Mutex<Storage>) -> Result<Vec<DataType>, anyhow::Error> {
        let instructions: Vec<String> = self.message.as_string_vec()?;

        let [_, key, start, stop] = instructions.as_slice() else {
            return Err(
                RedisError::new("ERR wrong number of arguments for 'zrange' command").into(),
            );
        };
        let start: i64 = try_parsing(start, RedisError::not_an_integer())?;
        let stop: i64 = try_parsing(stop, RedisError::not_an_integer())?;

        debug!("ZRANGE {} {} {}", key, start, stop);

        let members = storage
            .lock()
            .map_err(|e| anyhow!("Failed to lock storage: {}", e))?
            .zrange(key, start, stop)?;

        Ok(vec![protocol::array(
            members.iter().map(|member| protocol::bulk_string(member)).collect(),
        )])
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
        "ZRANGE"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::sorted_set::zadd;
    use crate::commands::{client_error_message, command_message, create_test_storage, set};

    fn zrange(key: &str, start: &str, stop: &str) -> ZRange {
        ZRange { message: command_message(&["ZRANGE", key, start, stop]) }
    }

    /// The reply ZRANGE sends for `members`: an array of bulk strings.
    fn members_reply(members: &[&str]) -> Vec<DataType> {
        vec![protocol::array(members.iter().map(|member| protocol::bulk_string(member)).collect())]
    }

    /// Seeds the sorted set the CodeCrafters tester uses, which orders as
    /// baz (20.0), caz (30.1), paz (40.2), bar (100.0), foo (100.0).
    fn seed_tester_set(storage: &Mutex<Storage>) -> anyhow::Result<()> {
        zadd(&["ZADD", "zset_key", "100.0", "foo"]).execute(storage)?;
        zadd(&["ZADD", "zset_key", "100.0", "bar"]).execute(storage)?;
        zadd(&["ZADD", "zset_key", "20.0", "baz"]).execute(storage)?;
        zadd(&["ZADD", "zset_key", "30.1", "caz"]).execute(storage)?;
        zadd(&["ZADD", "zset_key", "40.2", "paz"]).execute(storage)?;
        Ok(())
    }

    #[test]
    fn test_zrange_orders_by_score_then_member() -> anyhow::Result<()> {
        let storage = create_test_storage();
        seed_tester_set(&storage)?;

        assert_eq!(zrange("zset_key", "2", "4").execute(&storage)?, members_reply(&["paz", "bar", "foo"]));
        Ok(())
    }

    #[test]
    fn test_zrange_with_equal_start_and_stop_returns_one_member() -> anyhow::Result<()> {
        let storage = create_test_storage();
        seed_tester_set(&storage)?;

        assert_eq!(zrange("zset_key", "2", "2").execute(&storage)?, members_reply(&["paz"]));
        Ok(())
    }

    #[test]
    fn test_zrange_clamps_stop_past_the_end() -> anyhow::Result<()> {
        let storage = create_test_storage();
        zadd(&["ZADD", "zset_key", "30.0", "x"]).execute(&storage)?;
        zadd(&["ZADD", "zset_key", "20.0", "y"]).execute(&storage)?;
        zadd(&["ZADD", "zset_key", "10.0", "z"]).execute(&storage)?;

        assert_eq!(zrange("zset_key", "1", "5").execute(&storage)?, members_reply(&["y", "x"]));
        Ok(())
    }

    #[test]
    fn test_zrange_with_start_past_the_end_is_empty() -> anyhow::Result<()> {
        let storage = create_test_storage();
        zadd(&["ZADD", "zset_key", "10.0", "x"]).execute(&storage)?;

        assert_eq!(zrange("zset_key", "2", "4").execute(&storage)?, members_reply(&[]));
        Ok(())
    }

    #[test]
    fn test_zrange_with_start_after_stop_is_empty() -> anyhow::Result<()> {
        let storage = create_test_storage();
        seed_tester_set(&storage)?;

        assert_eq!(zrange("zset_key", "3", "1").execute(&storage)?, members_reply(&[]));
        Ok(())
    }

    #[test]
    fn test_zrange_negative_indexes_count_from_the_end() -> anyhow::Result<()> {
        let storage = create_test_storage();
        seed_tester_set(&storage)?;

        assert_eq!(zrange("zset_key", "-2", "-1").execute(&storage)?, members_reply(&["bar", "foo"]));
        assert_eq!(
            zrange("zset_key", "0", "-1").execute(&storage)?,
            members_reply(&["baz", "caz", "paz", "bar", "foo"])
        );
        assert_eq!(zrange("zset_key", "-100", "0").execute(&storage)?, members_reply(&["baz"]));
        Ok(())
    }

    #[test]
    fn test_zrange_missing_key_is_empty_array() -> anyhow::Result<()> {
        let storage = create_test_storage();
        let reply = zrange("missing_key", "0", "1").execute(&storage)?;

        assert_eq!(reply, members_reply(&[]));
        Ok(())
    }

    #[test]
    fn test_zrange_wrong_number_of_arguments() {
        let storage = create_test_storage();
        for parts in [&["ZRANGE", "z"][..], &["ZRANGE"], &["ZRANGE", "z", "1"], &["ZRANGE", "z", "0", "1", "WITHSCORES"]] {
            let error = ZRange { message: command_message(parts) }.execute(&storage).unwrap_err();
            assert_eq!(client_error_message(error), "ERR wrong number of arguments for 'zrange' command");
        }
    }

    #[test]
    fn test_zrange_non_integer_index_is_rejected() {
        let storage = create_test_storage();
        for (start, stop) in [("abc", "1"), ("0", "abc"), ("1.5", "2"), ("", "1")] {
            let error = zrange("z", start, stop).execute(&storage).unwrap_err();
            assert_eq!(client_error_message(error), "ERR value is not an integer or out of range");
        }
    }

    #[test]
    fn test_zrange_on_string_key_is_wrong_type() -> anyhow::Result<()> {
        let storage = create_test_storage();
        set(&["SET", "k", "v"]).execute(&storage)?;

        let error = zrange("k", "0", "1").execute(&storage).unwrap_err();
        assert_eq!(
            client_error_message(error),
            "WRONGTYPE Operation against a key holding the wrong kind of value"
        );
        Ok(())
    }

    #[test]
    fn test_zrange_is_not_propagated_to_replicas() {
        assert!(!zrange("k", "1", "2").is_propagated_to_replicas());
    }
}
