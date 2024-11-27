use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, PartialEq)]
pub(crate) struct Storage {
    data: HashMap<String, StoredValue>
}

impl Storage {
    pub(crate) fn new(data: HashMap<String, StoredValue>) -> Storage {
        Storage {
            data
        }
    }

    pub(crate) fn set(&mut self, key: &str, value: Vec<u8>, expires_in_ms: Option<u64>) -> Result<Option<StoredValue>, anyhow::Error> {
        Ok(self.data.insert(key.to_owned(), StoredValue::from(value, expires_in_ms)?))
    }

    pub(crate) fn get(&mut self, key: &str) -> Result<Option<Vec<u8>>, anyhow::Error> {
        let value = match self.data.get(&key.to_owned()) {
            Some(stored_value) => {
              let current_time_ms = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
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
            },
            None => None
        };
        Ok(value)
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct StoredValue {
    expires_in_ms: Option<u64>,
    last_modified_timestamp: u128,
    value: Vec<u8>
}

impl StoredValue {
    fn from(value: Vec<u8>, expires_in_ms: Option<u64>) -> Result<StoredValue, anyhow::Error> {
        Ok(StoredValue {
            expires_in_ms,
            last_modified_timestamp: SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
            value
        })
    }
}