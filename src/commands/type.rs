/// TYPE command - returns the type of the value stored at a key.
///
/// Syntax: TYPE <key>
/// Returns: a simple string naming the type (`string`, `list`, ...), or
/// `none` if the key does not exist.

use std::sync::{Arc, Mutex};
use log::*;
use crate::protocol;
use crate::protocol::DataType;
use crate::storage::Storage;
use crate::error::RedisError;
use super::RedisCommand;

/// TYPE command implementation.
pub struct Type {
    pub message: DataType,
}

impl RedisCommand for Type {
    fn execute(&self, storage: &Arc<Mutex<Storage>>) -> Result<Vec<DataType>, anyhow::Error> {
        let instructions: Vec<String> = self.message.as_vec()?;
        let error = RedisError {
            message: "Invalid TYPE command syntax".to_string(),
        };

        let key = instructions.get(1).ok_or::<anyhow::Error>(error.clone().into())?;
        if instructions.len() != 2 {
            return Err(error.clone().into());
        }

        debug!("TYPE {}", key);

        let mut storage = storage
            .lock()
            .map_err(|e| anyhow::anyhow!("Failed to lock storage: {}", e))?;

        // Lists are stored as a RESP Array; anything else is a string.
        let type_name = if storage.contains_stream(key) {
            "stream"
        } else if let Some(value) = storage.get(key)? {
            match protocol::read_message_from_bytes(&value) {
                Ok(DataType::Array { .. }) => "list",
                _ => "string",
            }
        } else {
            "none"
        };

        Ok(vec![protocol::simple_string(type_name)])
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::create_test_storage;
    use crate::commands::set::Set;
    use crate::commands::list::RPush;
    use crate::blocking::BlockingNotifier;

    fn type_cmd(key: &str) -> Type {
        let msg = protocol::array(vec![
            protocol::bulk_string("TYPE"),
            protocol::bulk_string(key),
        ]);
        Type { message: msg }
    }

    #[test]
    fn test_type_of_string_value() -> anyhow::Result<()> {
        let storage = create_test_storage();
        let set_msg = protocol::array(vec![
            protocol::bulk_string("SET"),
            protocol::bulk_string("some_key"),
            protocol::bulk_string("foo"),
        ]);
        Set { message: set_msg }.execute(&storage)?;

        let result = type_cmd("some_key").execute(&storage)?;
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], protocol::simple_string("string"));
        Ok(())
    }

    #[test]
    fn test_type_of_missing_key_is_none() -> anyhow::Result<()> {
        let storage = create_test_storage();
        let result = type_cmd("missing_key").execute(&storage)?;
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], protocol::simple_string("none"));
        Ok(())
    }

    #[test]
    fn test_type_of_list_value() -> anyhow::Result<()> {
        let storage = create_test_storage();
        let push_msg = protocol::array(vec![
            protocol::bulk_string("RPUSH"),
            protocol::bulk_string("mylist"),
            protocol::bulk_string("a"),
            protocol::bulk_string("b"),
        ]);
        RPush {
            message: push_msg,
            notifier: Arc::new(BlockingNotifier::new()),
        }
        .execute(&storage)?;

        let result = type_cmd("mylist").execute(&storage)?;
        assert_eq!(result[0], protocol::simple_string("list"));
        Ok(())
    }

    #[test]
    fn test_type_of_stream_value() -> anyhow::Result<()> {
        let storage = create_test_storage();
        let xadd_msg = protocol::array(vec![
            protocol::bulk_string("XADD"),
            protocol::bulk_string("stream_key"),
            protocol::bulk_string("0-1"),
            protocol::bulk_string("foo"),
            protocol::bulk_string("bar"),
        ]);
        crate::commands::stream::XAdd { message: xadd_msg }.execute(&storage)?;

        let result = type_cmd("stream_key").execute(&storage)?;
        assert_eq!(result[0], protocol::simple_string("stream"));
        Ok(())
    }

    #[test]
    fn test_type_after_overwriting_stream_with_string() -> anyhow::Result<()> {
        let storage = create_test_storage();
        let xadd_msg = protocol::array(vec![
            protocol::bulk_string("XADD"),
            protocol::bulk_string("k"),
            protocol::bulk_string("0-1"),
            protocol::bulk_string("foo"),
            protocol::bulk_string("bar"),
        ]);
        crate::commands::stream::XAdd { message: xadd_msg }.execute(&storage)?;
        let set_msg = protocol::array(vec![
            protocol::bulk_string("SET"),
            protocol::bulk_string("k"),
            protocol::bulk_string("now_a_string"),
        ]);
        Set { message: set_msg }.execute(&storage)?;
        assert_eq!(type_cmd("k").execute(&storage)?[0], protocol::simple_string("string"));
        Ok(())
    }

    #[test]
    fn test_type_invalid_syntax() {
        let storage = create_test_storage();

        let msg1 = protocol::array(vec![protocol::bulk_string("TYPE")]);
        assert!(Type { message: msg1 }.execute(&storage).is_err());

        let msg2 = protocol::array(vec![
            protocol::bulk_string("TYPE"),
            protocol::bulk_string("key"),
            protocol::bulk_string("extra"),
        ]);
        assert!(Type { message: msg2 }.execute(&storage).is_err());
    }
}
