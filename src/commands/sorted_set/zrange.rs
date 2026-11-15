/// ZRANGE <key> <from> <to>
///
/// Replies with the array of stored members starting with the index from (inclusive)
/// and ending with the index to (inclusive)
///

use std::sync::Mutex;

use anyhow::anyhow;
use log::*;

use crate::commands::RedisCommand;
use crate::error::RedisError;
use crate::protocol;
use crate::protocol::DataType;
use crate::storage::Storage;

/// ZRANK command implementation.
pub struct ZRange {
    pub message: DataType,
}

impl RedisCommand for ZRange {
    fn execute(&self, storage: &Mutex<Storage>) -> Result<Vec<DataType>, anyhow::Error> {
        let instructions: Vec<String> = self.message.as_string_vec()?;

        if instructions.len() != 4 {
            return Err(
                RedisError::new("ERR wrong number of arguments for 'zrange' command").into(),
            );
        };
        let key =  &instructions[1];
        let ranges: Vec<usize> = instructions.as_slice().iter()
            .skip(2).take(2)
            .map(|arg| arg.parse::<usize>().map_err(anyhow::Error::from)).flatten().collect();
        let [from, to] = ranges[..] else {
            return Err(
                RedisError::new("ERR wrong arguments for 'zrange' command").into(),
            );
        };

        debug!("ZRANGE {} {}", from, to);

        let response = storage
            .lock()
            .map_err(|e| anyhow!("Failed to lock storage: {}", e))?
            .zrange(key, from, to);

        match response {
            Ok(elements) =>
                Ok(vec![
                    protocol::array(
                        elements.iter().map(|elem| {
                            protocol::bulk_string(elem)
                        }).collect()
                    )
                ]),
            Err(e) =>  Err(RedisError::new(&format!("ERR failed to execute 'zrange' command {:?}", e)).into()),
        }
        
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
    use crate::commands::{client_error_message, command_message, create_test_storage};

    fn zrange(key: &str, from: usize, to: usize) -> ZRange {
        ZRange { message: command_message(&["ZRANGE", key, from.to_string().as_str(), to.to_string().as_str()]) }
    }

    #[test]
    fn test_zrank_orders_by_score_then_member() -> anyhow::Result<()> {
        let storage = create_test_storage();
        zadd(&["ZADD", "zset_key", "100.0", "foo"]).execute(&storage)?;
        zadd(&["ZADD", "zset_key", "100.0", "bar"]).execute(&storage)?;
        zadd(&["ZADD", "zset_key", "20.0", "baz"]).execute(&storage)?;
        zadd(&["ZADD", "zset_key", "30.1", "caz"]).execute(&storage)?;
        zadd(&["ZADD", "zset_key", "40.2", "paz"]).execute(&storage)?;

        assert_eq!(zrange("zset_key", 2, 4).execute(&storage)?, vec![protocol::array(vec![
            protocol::bulk_string("paz"),
            protocol::bulk_string("bar"),
            protocol::bulk_string("foo")
        ])]);
        Ok(())
    }

    #[test]
    fn test_zrange_index_out_of_bounds() -> anyhow::Result<()> {
        let storage = create_test_storage();
        zadd(&["ZADD", "zset_key", "30.0", "x"]).execute(&storage)?;
        zadd(&["ZADD", "zset_key", "20.0", "y"]).execute(&storage)?;
        zadd(&["ZADD", "zset_key", "10.0", "z"]).execute(&storage)?;

        assert_eq!(zrange("zset_key", 1, 5).execute(&storage)?, vec![protocol::array(vec![
            protocol::bulk_string("y"),
            protocol::bulk_string("x")
        ])]);
        Ok(())
    }

    #[test]
    fn test_zrange_returns_empty_range() -> anyhow::Result<()> {
        let storage = create_test_storage();
        zadd(&["ZADD", "zset_key", "10.0", "x"]).execute(&storage)?;

        assert_eq!(zrange("zset_key", 3, 4).execute(&storage)?, vec![protocol::array(Vec::new())]);
        Ok(())
    }

    #[test]
    fn test_zrange_wrong_number_of_arguments() {
        let storage = create_test_storage();
        for parts in [&["ZRANGE", "z"][..], &["ZRANGE"], &["ZRANGE", "z", "1"]] {
            let error = ZRange { message: command_message(parts) }.execute(&storage).unwrap_err();
            assert_eq!(client_error_message(error), "ERR wrong number of arguments for 'zrange' command");
        }
    }

    #[test]
    fn test_zrange_is_not_propagated_to_replicas() {
        assert!(!zrange("k", 1, 2).is_propagated_to_replicas());
    }
}
