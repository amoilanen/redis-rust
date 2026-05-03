/// LRANGE command - retrieve elements from a list using a start index and a stop index stored at key
///
/// Syntax: LRANGE start_index end_index
///
/// end_index is inclusive
///
/// Returns: array of values from the list stored by the given key
///
/// Errors:
///   Returns an error if the value stored at key is not a list

use std::sync::{Arc, Mutex};
use anyhow::anyhow;
use log::*;
use crate::protocol;
use crate::protocol::DataType;
use crate::storage::Storage;
use crate::error::RedisError;
use super::RedisCommand;

/// LRANGE command implementation.
pub struct LRange {
    pub message: DataType,
}

impl RedisCommand for LRange {
    //TODO: Add tests
    //TODO: Extract commonalities with rpush
    //TODO: Add handling of this command to the main connection handling loop
    fn execute(&self, storage: &Arc<Mutex<Storage>>) -> Result<Vec<DataType>, anyhow::Error> {
        let instructions: Vec<String> = self.message.as_vec()?;
        let error = RedisError {
            message: "Invalid LRANGE command syntax".to_string(),
        };

        let key = instructions.get(1).ok_or::<anyhow::Error>(error.clone().into())?;
        if instructions.len() < 4 {
            return Err(error.clone().into());
        }
        let start_index: usize = instructions.get(2).ok_or::<anyhow::Error>(error.clone().into())?.parse()?;
        let end_index: usize = instructions.get(3).ok_or::<anyhow::Error>(error.clone().into())?.parse()?;

        debug!("LRANGE {} {} {}", key, start_index, end_index);

        let mut data = storage
            .lock()
            .map_err(|e| anyhow!("Failed to lock storage: {}", e))?;

        let stored_raw_value = data.get(key)?;
        let stored_value = stored_raw_value.map(|value| protocol::read_message_from_bytes(&value)).transpose()?;
        let stored_elements = match stored_value {
            Some(DataType::Array { elements }) => {
                Ok(elements)
            },
            None => {
                Ok(Vec::new())
            },
            Some(_) => Err(anyhow!("Not an Array is stored in storage")),
        }?;
        let selected_elements = stored_elements.get(start_index..end_index).map(|s| s.to_vec()).unwrap_or(Vec::new());
      Ok(selected_elements)
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

    fn create_test_storage() -> Arc<Mutex<Storage>> {
        Arc::new(Mutex::new(Storage::new(HashMap::new())))
    }

    #[test]
    fn test_lpush_selects_elements() -> anyhow::Result<()> {
        let storage = create_test_storage();
        let key = "mylist";
        let values = vec!["value1", "value2", "value3", "value4", "value5"];
        let elements: Vec<DataType> = values.iter().map(|s| protocol::simple_string(s)).collect();
        storage.lock().unwrap().set(key, protocol::array(elements.clone()).serialize(), None)?;

        let start_index = 1;
        let end_index = 3;
        let msg = protocol::array(vec![
            protocol::bulk_string("LRANGE"),
            protocol::bulk_string("mylist"),
            protocol::integer(start_index),
            protocol::integer(end_index),
        ]);
        let cmd = LRange { message: msg };

        let result = cmd.execute(&storage)?;

        assert_eq!(&result, &elements[1..3]);
        Ok(())
    }
}
