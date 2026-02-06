/// Redis data storage with expiration support.
///
/// This module provides the in-memory storage backend for Redis commands.
/// It supports:
/// - String key-value pairs
/// - Per-key expiration times
/// - RDB serialization/deserialization
///
/// # Examples
/// ```ignore
/// let storage = Storage::new(HashMap::new());
/// storage.set("mykey", b"myvalue".to_vec(), None)?;
/// let value = storage.get("mykey")?;
/// assert_eq!(value, Some(b"myvalue".to_vec()));
/// ```

use anyhow::Error;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use std::io::Cursor;
use crate::rdb;

/// A single value stored in Redis with optional expiration.
#[derive(Debug, PartialEq)]
pub struct StoredValue {
    /// Time until expiration in milliseconds, None for no expiration
    expires_in_ms: Option<u64>,
    /// Timestamp when the value was stored (in milliseconds since UNIX_EPOCH)
    last_modified_timestamp: u128,
    /// The actual value stored
    pub value: Vec<u8>,
}

impl StoredValue {
    /// Creates a new stored value with optional expiration time.
    ///
    /// # Arguments
    /// * `value` - The data to store
    /// * `expires_in_ms` - Expiration time in milliseconds, None for no expiration
    ///
    /// # Returns
    /// A new StoredValue or error if system time is unavailable
    pub fn from(value: Vec<u8>, expires_in_ms: Option<u64>) -> Result<StoredValue, anyhow::Error> {
        Ok(StoredValue {
            expires_in_ms,
            last_modified_timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)?
                .as_millis(),
            value,
        })
    }
}

/// The main in-memory Redis storage.
///
/// Stores key-value pairs with support for per-key expiration.
#[derive(Debug, PartialEq)]
pub struct Storage {
    /// The actual data storage
    pub data: HashMap<String, StoredValue>,
}

impl Storage {
    /// Creates a new storage instance.
    ///
    /// # Arguments
    /// * `data` - Initial HashMap of stored values
    pub fn new(data: HashMap<String, StoredValue>) -> Storage {
        Storage { data }
    }

    /// Serializes storage to RDB format.
    ///
    /// # Returns
    /// RDB binary data
    pub fn to_rdb(&self) -> Result<Vec<u8>, Error> {
        let mut buffer: Vec<u8> = Vec::new();
        let mut writer = Cursor::new(&mut buffer);
        rdb::to_rdb(&self, &mut writer)?;
        Ok(buffer)
    }

    /// Deserializes storage from RDB format.
    ///
    /// # Arguments
    /// * `rdb` - RDB binary data
    ///
    /// # Returns
    /// Deserialized storage or error if RDB is invalid
    pub fn from_rdb(rdb: &[u8]) -> Result<Storage, Error> {
        let mut reader = Cursor::new(&rdb);
        rdb::from_rdb(&mut reader)
    }

    /// Returns all stored key-value pairs as a HashMap (without expiration info).
    ///
    /// # Returns
    /// HashMap of keys to values
    pub fn to_pairs(&self) -> HashMap<String, Vec<u8>> {
        let mut result = HashMap::new();
        for (key, value) in self.data.iter() {
            result.insert(key.clone(), value.value.clone());
        }
        result
    }

    /// Stores a value with optional expiration.
    ///
    /// # Arguments
    /// * `key` - The key to store
    /// * `value` - The value to store
    /// * `expires_in_ms` - Expiration time in milliseconds, None for no expiration
    ///
    /// # Returns
    /// The previous value if key existed, or None
    pub fn set(
        &mut self,
        key: &str,
        value: Vec<u8>,
        expires_in_ms: Option<u64>,
    ) -> Result<Option<StoredValue>, anyhow::Error> {
        Ok(self.data.insert(
            key.to_owned(),
            StoredValue::from(value, expires_in_ms)?,
        ))
    }

    /// Retrieves a value by key, checking for expiration.
    ///
    /// # Arguments
    /// * `key` - The key to retrieve
    ///
    /// # Returns
    /// * `Ok(Some(value))` - Key exists and has not expired
    /// * `Ok(None)` - Key does not exist or has expired
    /// * `Err(e)` - Error checking system time
    pub fn get(&mut self, key: &str) -> Result<Option<Vec<u8>>, anyhow::Error> {
        let value = match self.data.get(&key.to_owned()) {
            Some(stored_value) => {
                let current_time_ms = SystemTime::now()
                    .duration_since(UNIX_EPOCH)?
                    .as_millis();
                let has_value_expired = if let Some(expires_in_ms) = stored_value.expires_in_ms {
                    current_time_ms >= stored_value.last_modified_timestamp + expires_in_ms as u128
                } else {
                    false
                };
                if has_value_expired {
                    None
                } else {
                    Some(stored_value.value.clone())
                }
            }
            None => None,
        };
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_storage_new_empty() {
        let data: HashMap<String, StoredValue> = HashMap::new();
        let storage = Storage::new(data);
        assert_eq!(storage.data.len(), 0);
    }

    #[test]
    fn test_storage_set_and_get() {
        let mut storage = Storage::new(HashMap::new());
        storage.set("key1", b"value1".to_vec(), None).unwrap();

        let result = storage.get("key1").unwrap();
        assert_eq!(result, Some(b"value1".to_vec()));
    }

    #[test]
    fn test_storage_get_nonexistent_key() {
        let mut storage = Storage::new(HashMap::new());
        let result = storage.get("nonexistent").unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_storage_overwrite_key() {
        let mut storage = Storage::new(HashMap::new());
        storage.set("key", b"value1".to_vec(), None).unwrap();
        storage.set("key", b"value2".to_vec(), None).unwrap();

        let result = storage.get("key").unwrap();
        assert_eq!(result, Some(b"value2".to_vec()));
    }

    #[test]
    fn test_storage_set_returns_old_value() {
        let mut storage = Storage::new(HashMap::new());
        storage.set("key", b"old".to_vec(), None).unwrap();

        let old = storage.set("key", b"new".to_vec(), None).unwrap();
        assert!(old.is_some());
        assert_eq!(old.unwrap().value, b"old".to_vec());
    }

    #[test]
    fn test_storage_expiration_not_expired() {
        let mut storage = Storage::new(HashMap::new());
        storage
            .set("key", b"value".to_vec(), Some(5000))
            .unwrap();

        // Should still be there before expiration
        let result = storage.get("key").unwrap();
        assert_eq!(result, Some(b"value".to_vec()));
    }

    #[test]
    fn test_storage_expiration_expired() {
        let mut storage = Storage::new(HashMap::new());
        storage
            .set("key", b"value".to_vec(), Some(100))
            .unwrap();

        // Wait for expiration
        thread::sleep(Duration::from_millis(150));

        // Should be gone after expiration
        let result = storage.get("key").unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_storage_no_expiration() {
        let mut storage = Storage::new(HashMap::new());
        storage.set("key", b"value".to_vec(), None).unwrap();

        // Wait (but value should not expire)
        thread::sleep(Duration::from_millis(100));

        let result = storage.get("key").unwrap();
        assert_eq!(result, Some(b"value".to_vec()));
    }

    #[test]
    fn test_storage_multiple_keys() {
        let mut storage = Storage::new(HashMap::new());

        for i in 0..10 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            storage
                .set(&key, value.as_bytes().to_vec(), None)
                .unwrap();
        }

        assert_eq!(storage.data.len(), 10);

        for i in 0..10 {
            let key = format!("key{}", i);
            let result = storage.get(&key).unwrap();
            assert_eq!(
                result,
                Some(format!("value{}", i).as_bytes().to_vec())
            );
        }
    }

    #[test]
    fn test_storage_to_pairs() {
        let mut storage = Storage::new(HashMap::new());
        storage.set("key1", b"value1".to_vec(), None).unwrap();
        storage.set("key2", b"value2".to_vec(), None).unwrap();

        let pairs = storage.to_pairs();
        assert_eq!(pairs.len(), 2);
        assert_eq!(pairs.get("key1"), Some(&b"value1".to_vec()));
        assert_eq!(pairs.get("key2"), Some(&b"value2".to_vec()));
    }

    #[test]
    fn test_stored_value_from() {
        let value = StoredValue::from(b"test".to_vec(), Some(1000)).unwrap();
        assert_eq!(value.value, b"test".to_vec());
        // Just verify it was created successfully
        // (expires_in_ms is private)
    }

    #[test]
    fn test_stored_value_no_expiration() {
        let value = StoredValue::from(b"test".to_vec(), None).unwrap();
        assert_eq!(value.value, b"test".to_vec());
        // Just verify it was created successfully
        // (expires_in_ms is private)
    }

    #[test]
    fn test_storage_binary_data() {
        let mut storage = Storage::new(HashMap::new());
        let binary = vec![0u8, 1, 2, 255, 254, 127];

        storage.set("binary_key", binary.clone(), None).unwrap();
        let result = storage.get("binary_key").unwrap();
        assert_eq!(result, Some(binary));
    }

    #[test]
    fn test_storage_empty_value() {
        let mut storage = Storage::new(HashMap::new());
        storage.set("empty", b"".to_vec(), None).unwrap();

        let result = storage.get("empty").unwrap();
        assert_eq!(result, Some(b"".to_vec()));
    }
}