use anyhow::Error;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use std::io::Cursor;
use crate::rdb;

#[derive(Debug, PartialEq)]
pub struct StoredValue {
    expires_in_ms: Option<u64>,
    last_modified_timestamp: u128,
    pub value: Vec<u8>,
}

impl StoredValue {
    pub fn from(value: Vec<u8>, expires_in_ms: Option<u64>) -> Result<StoredValue, anyhow::Error> {
        Ok(StoredValue {
            expires_in_ms,
            last_modified_timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)?
                .as_millis(),
            value,
        })
    }

    /// Creates a StoredValue from an absolute expiry timestamp (ms since Unix epoch).
    /// Used when loading from RDB where expiry is stored as absolute time.
    pub fn with_absolute_expiry(value: Vec<u8>, expires_at_ms: Option<u64>) -> Result<StoredValue, anyhow::Error> {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)?
            .as_millis();
        let expires_in_ms = expires_at_ms.map(|abs_ms| {
            let abs = abs_ms as u128;
            if abs > now_ms { (abs - now_ms) as u64 } else { 0 }
        });
        Ok(StoredValue {
            expires_in_ms,
            last_modified_timestamp: now_ms,
            value,
        })
    }

    /// Returns the absolute expiry timestamp in ms since Unix epoch.
    pub fn expires_at_ms(&self) -> Option<u64> {
        self.expires_in_ms.map(|dur| self.last_modified_timestamp as u64 + dur)
    }

    /// Returns true if this value has already expired.
    pub fn is_expired(&self) -> bool {
        if let Some(expires_in_ms) = self.expires_in_ms {
            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis())
                .unwrap_or(0);
            now_ms >= self.last_modified_timestamp + expires_in_ms as u128
        } else {
            false
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct Storage {
    pub data: HashMap<String, StoredValue>,
}

impl Storage {
    pub fn new(data: HashMap<String, StoredValue>) -> Storage {
        Storage { data }
    }

    pub fn to_rdb(&self) -> Result<Vec<u8>, Error> {
        let mut buffer: Vec<u8> = Vec::new();
        let mut writer = Cursor::new(&mut buffer);
        rdb::to_rdb(&self, &mut writer)?;
        Ok(buffer)
    }
    pub fn from_rdb(rdb: &[u8]) -> Result<Storage, Error> {
        let mut reader = Cursor::new(&rdb);
        rdb::from_rdb(&mut reader)
    }

    pub fn to_pairs(&self) -> HashMap<String, Vec<u8>> {
        let mut result = HashMap::new();
        for (key, value) in self.data.iter() {
            result.insert(key.clone(), value.value.clone());
        }
        result
    }

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
    fn test_storage_new_empty() -> Result<(), anyhow::Error> {
        let data: HashMap<String, StoredValue> = HashMap::new();
        let storage = Storage::new(data);
        assert_eq!(storage.data.len(), 0);
        Ok(())
    }

    #[test]
    fn test_storage_set_and_get() -> Result<(), anyhow::Error> {
        let mut storage = Storage::new(HashMap::new());
        storage.set("key1", b"value1".to_vec(), None)?;

        let result = storage.get("key1")?;
        assert_eq!(result, Some(b"value1".to_vec()));
        Ok(())
    }

    #[test]
    fn test_storage_get_nonexistent_key() -> Result<(), anyhow::Error> {
        let mut storage = Storage::new(HashMap::new());
        let result = storage.get("nonexistent")?;
        assert_eq!(result, None);
        Ok(())
    }

    #[test]
    fn test_storage_overwrite_key() -> Result<(), anyhow::Error> {
        let mut storage = Storage::new(HashMap::new());
        storage.set("key", b"value1".to_vec(), None)?;
        storage.set("key", b"value2".to_vec(), None)?;

        let result = storage.get("key")?;
        assert_eq!(result, Some(b"value2".to_vec()));
        Ok(())
    }

    #[test]
    fn test_storage_set_returns_old_value() -> Result<(), anyhow::Error> {
        let mut storage = Storage::new(HashMap::new());
        storage.set("key", b"old".to_vec(), None)?;

        let old = storage.set("key", b"new".to_vec(), None)?;
        assert!(old.is_some());
        assert_eq!(old.unwrap().value, b"old".to_vec());
        Ok(())
    }

    #[test]
    fn test_storage_expiration_not_expired() -> Result<(), anyhow::Error> {
        let mut storage = Storage::new(HashMap::new());
        storage
            .set("key", b"value".to_vec(), Some(5000))?;

        let result = storage.get("key")?;
        assert_eq!(result, Some(b"value".to_vec()));
        Ok(())
    }

    #[test]
    fn test_storage_expiration_expired() -> Result<(), anyhow::Error> {
        let mut storage = Storage::new(HashMap::new());
        storage
            .set("key", b"value".to_vec(), Some(100))?;

        thread::sleep(Duration::from_millis(150));

        let result = storage.get("key")?;
        assert_eq!(result, None);
        Ok(())
    }

    #[test]
    fn test_storage_no_expiration() -> Result<(), anyhow::Error> {
        let mut storage = Storage::new(HashMap::new());
        storage.set("key", b"value".to_vec(), None)?;

        thread::sleep(Duration::from_millis(100));

        let result = storage.get("key")?;
        assert_eq!(result, Some(b"value".to_vec()));
        Ok(())
    }

    #[test]
    fn test_storage_multiple_keys() -> Result<(), anyhow::Error> {
        let mut storage = Storage::new(HashMap::new());

        for i in 0..10 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            storage
                .set(&key, value.as_bytes().to_vec(), None)?;
        }

        assert_eq!(storage.data.len(), 10);

        for i in 0..10 {
            let key = format!("key{}", i);
            let result = storage.get(&key)?;
            assert_eq!(
                result,
                Some(format!("value{}", i).as_bytes().to_vec())
            );
        }
        Ok(())
    }

    #[test]
    fn test_storage_to_pairs() -> Result<(), anyhow::Error> {
        let mut storage = Storage::new(HashMap::new());
        storage.set("key1", b"value1".to_vec(), None)?;
        storage.set("key2", b"value2".to_vec(), None)?;

        let pairs = storage.to_pairs();
        assert_eq!(pairs.len(), 2);
        assert_eq!(pairs.get("key1"), Some(&b"value1".to_vec()));
        assert_eq!(pairs.get("key2"), Some(&b"value2".to_vec()));
        Ok(())
    }

    #[test]
    fn test_storage_binary_data() -> Result<(), anyhow::Error> {
        let mut storage = Storage::new(HashMap::new());
        let binary = vec![0u8, 1, 2, 255, 254, 127];

        storage.set("binary_key", binary.clone(), None)?;
        let result = storage.get("binary_key")?;
        assert_eq!(result, Some(binary));
        Ok(())
    }

    #[test]
    fn test_storage_empty_value() -> Result<(), anyhow::Error> {
        let mut storage = Storage::new(HashMap::new());
        storage.set("empty", b"".to_vec(), None)?;

        let result = storage.get("empty")?;
        assert_eq!(result, Some(b"".to_vec()));
        Ok(())
    }

    #[test]
    fn test_with_absolute_expiry_future() -> Result<(), anyhow::Error> {
        let future_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)?
            .as_millis() as u64 + 60_000;
        let sv = StoredValue::with_absolute_expiry(b"val".to_vec(), Some(future_ms))?;
        assert!(!sv.is_expired());
        assert!(sv.expires_at_ms().is_some());
        Ok(())
    }

    #[test]
    fn test_with_absolute_expiry_past() -> Result<(), anyhow::Error> {
        let sv = StoredValue::with_absolute_expiry(b"val".to_vec(), Some(1000))?;
        assert!(sv.is_expired());
        Ok(())
    }

    #[test]
    fn test_with_absolute_expiry_none() -> Result<(), anyhow::Error> {
        let sv = StoredValue::with_absolute_expiry(b"val".to_vec(), None)?;
        assert!(!sv.is_expired());
        assert_eq!(sv.expires_at_ms(), None);
        Ok(())
    }

    #[test]
    fn test_expires_at_ms_roundtrip() -> Result<(), anyhow::Error> {
        let sv = StoredValue::from(b"val".to_vec(), Some(5000))?;
        let abs = sv.expires_at_ms().expect("should have expiry");
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64;
        assert!(abs >= now && abs <= now + 5100);
        Ok(())
    }

    #[test]
    fn test_is_expired_no_expiry() -> Result<(), anyhow::Error> {
        let sv = StoredValue::from(b"val".to_vec(), None)?;
        assert!(!sv.is_expired());
        Ok(())
    }
}