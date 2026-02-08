/// SET command - sets a key to hold a value.
///
/// Syntax: SET <key> <value> [PX <milliseconds>]
/// Options:
///   PX: Set the specified expire time, in milliseconds
///
/// Returns: +OK on success

use std::sync::{Arc, Mutex};
use anyhow::anyhow;
use log::*;
use crate::protocol;
use crate::storage;
use crate::error::RedisError;
use super::RedisCommand;

/// SET command implementation.
pub struct Set<'a> {
    pub message: &'a protocol::DataType,
}

impl RedisCommand for Set<'_> {
    fn execute(&self, storage: &Arc<Mutex<storage::Storage>>) -> Result<Vec<protocol::DataType>, anyhow::Error> {
        let instructions: Vec<String> = self.message.as_array()?;
        let error = RedisError {
            message: "Invalid SET command syntax".to_string(),
        };

        let key = instructions.get(1).ok_or::<anyhow::Error>(error.clone().into())?;
        let value = instructions.get(2).ok_or::<anyhow::Error>(error.clone().into())?;

        // Parse expiration time if provided
        let expires_in_ms = if let Some(modifier) = instructions.get(3) {
            if modifier.to_lowercase() == "px" {
                let expiration_time: u64 = instructions
                    .get(4)
                    .ok_or::<anyhow::Error>(error.clone().into())?
                    .parse()?;
                Some(expiration_time)
            } else {
                None
            }
        } else {
            None
        };

        debug!("SET {} {}", key, value);
        debug!("expiration_after = {:?}", expires_in_ms);

        let mut data = storage
            .lock()
            .map_err(|e| anyhow!("Failed to lock storage: {}", e))?;
        data.set(key, value.as_bytes().to_vec(), expires_in_ms)?;

        Ok(vec![protocol::simple_string("OK")])
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
    use std::collections::HashMap;
    use std::thread;
    use std::time::Duration;

    fn create_test_storage() -> Arc<Mutex<storage::Storage>> {
        Arc::new(Mutex::new(storage::Storage::new(HashMap::new())))
    }

    #[test]
    fn test_set_command_basic() {
        let message = protocol::array(vec![
            protocol::bulk_string("SET"),
            protocol::bulk_string("key1"),
            protocol::bulk_string("value1"),
        ]);
        let cmd = Set { message: &message };

        let storage = create_test_storage();
        let result = cmd.execute(&storage).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].as_string().unwrap(), "OK");
        assert!(cmd.is_propagated_to_replicas());

        // Verify data was stored
        let mut data = storage.lock().unwrap();
        let retrieved = data.get("key1").unwrap();
        assert_eq!(retrieved, Some(b"value1".to_vec()));
    }

    #[test]
    fn test_set_command_with_expiration() {
        let message = protocol::array(vec![
            protocol::bulk_string("SET"),
            protocol::bulk_string("expiring_key"),
            protocol::bulk_string("expiring_value"),
            protocol::bulk_string("px"),
            protocol::bulk_string("100"),
        ]);
        let cmd = Set { message: &message };

        let storage = create_test_storage();
        let result = cmd.execute(&storage).unwrap();

        assert_eq!(result[0].as_string().unwrap(), "OK");

        // Immediately after set, key should exist
        let mut data = storage.lock().unwrap();
        assert_eq!(
            data.get("expiring_key").unwrap(),
            Some(b"expiring_value".to_vec())
        );

        drop(data);
        thread::sleep(Duration::from_millis(150));

        // After expiration, key should be gone
        let mut data = storage.lock().unwrap();
        assert_eq!(data.get("expiring_key").unwrap(), None);
    }

    #[test]
    fn test_set_command_invalid_syntax() {
        let message = protocol::array(vec![
            protocol::bulk_string("SET"),
            protocol::bulk_string("key_only"),
        ]);
        let cmd = Set { message: &message };

        let storage = create_test_storage();
        let result = cmd.execute(&storage);

        assert!(result.is_err());
    }
}
