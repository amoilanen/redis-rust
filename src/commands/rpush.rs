/// RPUSH command - appends a value to the end of a list stored at key.
///
/// Syntax: RPUSH <key> <value>
///
/// If the key does not exist, a new list is created before appending the value.
/// If the key exists and holds a list, the value is appended to the end.
///
/// Returns: Integer reply - the length of the list after the push operation
///
/// Errors:
///   Returns an error if the value stored at key is not a list.

use std::sync::{Arc, Mutex};
use anyhow::anyhow;
use log::*;
use crate::protocol;
use crate::protocol::DataType;
use crate::storage::Storage;
use crate::error::RedisError;
use super::RedisCommand;

/// RPUSH command implementation.
pub struct RPush {
    pub message: DataType,
}

impl RedisCommand for RPush {
    fn execute(&self, storage: &Arc<Mutex<Storage>>) -> Result<Vec<DataType>, anyhow::Error> {
        let instructions: Vec<String> = self.message.as_vec()?;
        let error = RedisError {
            message: "Invalid RPUSH command syntax".to_string(),
        };

        let key = instructions.get(1).ok_or::<anyhow::Error>(error.clone().into())?;
        let value = instructions.get(2).ok_or::<anyhow::Error>(error.clone().into())?;

        debug!("RPUSH {} {}", key, value);

        let mut data = storage
            .lock()
            .map_err(|e| anyhow!("Failed to lock storage: {}", e))?;

        let stored_raw_value = data.get(key)?;
        let stored_value = stored_raw_value.map(|value| protocol::read_message_from_bytes(&value)).transpose()?;
        let mut stored_elements = match stored_value {
            Some(DataType::Array { elements }) => {
                Ok(elements)
            },
            None => {
                Ok(Vec::new())
            },
            Some(_) => Err(anyhow!("Not an Array is stored in storage")),
        }?;
        stored_elements.push(protocol::simple_string(value));
        data.set(key, protocol::array(stored_elements.clone()).serialize(), None)?;
        Ok(vec![protocol::integer(stored_elements.len() as i64)])
    }

    fn is_propagated_to_replicas(&self) -> bool {
        true
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
    use crate::commands::set::Set;
    use std::collections::HashMap;

    fn create_test_storage() -> Arc<Mutex<Storage>> {
        Arc::new(Mutex::new(Storage::new(HashMap::new())))
    }

    #[test]
    fn test_rpush_creates_and_appends() -> Result<(), Box<dyn std::error::Error>> {
        let storage = create_test_storage();

        let values = vec!["one", "two", "three"];
        for (i, value) in values.iter().enumerate() {
            let msg = protocol::array(vec![
                protocol::bulk_string("RPUSH"),
                protocol::bulk_string("mylist"),
                protocol::bulk_string(value),
            ]);
            let cmd = RPush { message: msg };
            let result = cmd.execute(&storage)?;
            assert_eq!(result.len(), 1);
            assert_eq!(result[0].as_string()?, (i + 1).to_string());
            assert!(cmd.is_propagated_to_replicas());
        }
        Ok(())
    }

    #[test]
    fn test_rpush_invalid_syntax() -> Result<(), Box<dyn std::error::Error>> {
        let storage = create_test_storage();

        // Missing both key and value
        let msg1 = protocol::array(vec![protocol::bulk_string("RPUSH")]);
        assert!(RPush { message: msg1 }.execute(&storage).is_err());

        // Missing value
        let msg2 = protocol::array(vec![
            protocol::bulk_string("RPUSH"),
            protocol::bulk_string("mylist"),
        ]);
        assert!(RPush { message: msg2 }.execute(&storage).is_err());
        Ok(())
    }

    #[test]
    fn test_rpush_wrong_type_fails() -> Result<(), Box<dyn std::error::Error>> {
        let storage = create_test_storage();

        // Store a plain string value using SET
        let set_msg = protocol::array(vec![
            protocol::bulk_string("SET"),
            protocol::bulk_string("mykey"),
            protocol::bulk_string("not_a_list"),
        ]);
        Set { message: set_msg }.execute(&storage)?;

        // RPUSH to the same key should fail since it's not a list
        let rpush_msg = protocol::array(vec![
            protocol::bulk_string("RPUSH"),
            protocol::bulk_string("mykey"),
            protocol::bulk_string("value"),
        ]);
        assert!(RPush { message: rpush_msg }.execute(&storage).is_err());
        Ok(())
    }
}
