/// GET command - returns the value of a key.
///
/// Syntax: GET <key>
/// Returns: The value at the key, or $-1\r\n if the key doesn't exist

use std::sync::{Arc, Mutex};
use crate::protocol;
use crate::storage;
use crate::error::RedisError;
use super::RedisCommand;

/// GET command implementation.
pub struct Get<'a> {
    pub message: &'a protocol::DataType,
}

impl RedisCommand for Get<'_> {
    fn execute(&self, storage: &Arc<Mutex<storage::Storage>>) -> Result<Vec<protocol::DataType>, anyhow::Error> {
        let instructions: Vec<String> = self.message.as_array()?;
        let error = RedisError {
            message: "GET command should have one argument".to_string(),
        };

        let key = instructions.get(1).ok_or::<anyhow::Error>(error.clone().into())?;

        println!("GET {}", key);

        let mut data = storage
            .lock()
            .map_err(|e| anyhow::anyhow!("Failed to lock storage: {}", e))?;

        let reply = match data.get(key)? {
            Some(value) => vec![protocol::bulk_string_from_bytes(value.clone())],
            None => vec![protocol::bulk_string_empty()],
        };

        Ok(reply)
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
    use std::collections::HashMap;

    fn create_test_storage() -> Arc<Mutex<storage::Storage>> {
        Arc::new(Mutex::new(storage::Storage::new(HashMap::new())))
    }

    fn insert_test_data(storage: &Arc<Mutex<storage::Storage>>, key: &str, value: &str) {
        let mut data = storage.lock().unwrap();
        let _ = data.set(key, value.as_bytes().to_vec(), None);
    }

    #[test]
    fn test_get_command_found() {
        let storage = create_test_storage();
        insert_test_data(&storage, "mykey", "myvalue");

        let message = protocol::array(vec![
            protocol::bulk_string("GET"),
            protocol::bulk_string("mykey"),
        ]);
        let cmd = Get { message: &message };

        let result = cmd.execute(&storage).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].as_string().unwrap(), "myvalue");
        assert!(!cmd.is_propagated_to_replicas());
    }

    #[test]
    fn test_get_command_not_found() {
        let message = protocol::array(vec![
            protocol::bulk_string("GET"),
            protocol::bulk_string("nonexistent"),
        ]);
        let cmd = Get { message: &message };

        let storage = create_test_storage();
        let result = cmd.execute(&storage).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].as_string().unwrap(), "");
    }

    #[test]
    fn test_get_command_invalid_syntax() {
        let message = protocol::array(vec![protocol::bulk_string("GET")]);
        let cmd = Get { message: &message };

        let storage = create_test_storage();
        let result = cmd.execute(&storage);

        assert!(result.is_err());
    }

    #[test]
    fn test_set_and_get_roundtrip() {
        let storage = create_test_storage();

        // Set a value
        let set_message = protocol::array(vec![
            protocol::bulk_string("SET"),
            protocol::bulk_string("test_key"),
            protocol::bulk_string("test_value"),
        ]);
        let set_cmd = super::super::set::Set {
            message: &set_message,
        };
        let set_result = set_cmd.execute(&storage).unwrap();
        assert_eq!(set_result[0].as_string().unwrap(), "OK");

        // Get the value
        let get_message = protocol::array(vec![
            protocol::bulk_string("GET"),
            protocol::bulk_string("test_key"),
        ]);
        let get_cmd = Get {
            message: &get_message,
        };
        let get_result = get_cmd.execute(&storage).unwrap();
        assert_eq!(get_result[0].as_string().unwrap(), "test_value");
    }

    #[test]
    fn test_get_with_binary_data() {
        let storage = create_test_storage();

        // Store binary data
        let mut data = storage.lock().unwrap();
        let binary_data = vec![0u8, 1, 2, 3, 255, 254];
        let _ = data.set("binary_key", binary_data.clone(), None);
        drop(data);

        // Retrieve binary data
        let get_message = protocol::array(vec![
            protocol::bulk_string("GET"),
            protocol::bulk_string("binary_key"),
        ]);
        let get_cmd = Get {
            message: &get_message,
        };
        let result = get_cmd.execute(&storage).unwrap();

        // Verify binary data is preserved
        match &result[0] {
            protocol::DataType::BulkString { value: Some(v) } => {
                assert_eq!(v, &binary_data);
            }
            _ => panic!("Expected bulk string with binary data"),
        }
    }

    #[test]
    fn test_multiple_keys() {
        let storage = create_test_storage();

        // Set multiple values
        for i in 0..5 {
            insert_test_data(&storage, &format!("key{}", i), &format!("value{}", i));
        }

        // Get each value
        for i in 0..5 {
            let get_message = protocol::array(vec![
                protocol::bulk_string("GET"),
                protocol::bulk_string(&format!("key{}", i)),
            ]);
            let get_cmd = Get {
                message: &get_message,
            };
            let result = get_cmd.execute(&storage).unwrap();
            assert_eq!(result[0].as_string().unwrap(), format!("value{}", i));
        }
    }
}
