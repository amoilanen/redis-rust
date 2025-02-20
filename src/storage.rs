use anyhow::Error;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use std::io::Cursor;
use crate::rdb;

#[derive(Debug, PartialEq)]
pub struct Storage {
    pub data: HashMap<String, StoredValue>
}

impl Storage {

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

    pub fn new(data: HashMap<String, StoredValue>) -> Storage {
        Storage {
            data
        }
    }

    pub fn to_pairs(&self) -> HashMap<String, Vec<u8>> {
        let mut result = HashMap::new();
        for (key, value) in self.data.iter() {
            result.insert(key.clone(), value.value.clone());
        }
        result
    }

    pub fn set(&mut self, key: &str, value: Vec<u8>, expires_in_ms: Option<u64>) -> Result<Option<StoredValue>, anyhow::Error> {
        Ok(self.data.insert(key.to_owned(), StoredValue::from(value, expires_in_ms)?))
    }

    pub fn get(&mut self, key: &str) -> Result<Option<Vec<u8>>, anyhow::Error> {
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
pub struct StoredValue {
    expires_in_ms: Option<u64>,
    last_modified_timestamp: u128,
    pub value: Vec<u8>
}

impl StoredValue {
    pub fn from(value: Vec<u8>, expires_in_ms: Option<u64>) -> Result<StoredValue, anyhow::Error> {
        Ok(StoredValue {
            expires_in_ms,
            last_modified_timestamp: SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
            value
        })
    }
}