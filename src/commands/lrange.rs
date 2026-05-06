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
use log::*;
use crate::protocol;
use crate::protocol::DataType;
use crate::storage::Storage;
use crate::error::RedisError;
use super::{RedisCommand, ListCommand};

/// LRANGE command implementation.
pub struct LRange {
    pub message: DataType,
}

impl RedisCommand for LRange {

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
        let mut end_index: usize = instructions.get(3).ok_or::<anyhow::Error>(error.clone().into())?.parse()?;
        end_index = end_index + 1;

        debug!("LRANGE {} {} {}", key, start_index, end_index);

        let stored_elements = self.get_stored_elements(key, storage)?;
        let selected_elements = stored_elements.get(start_index.max(0)..end_index.min(stored_elements.len())).map(|s| s.to_vec()).unwrap_or(Vec::new());
      Ok(vec![protocol::array(selected_elements)])
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

impl ListCommand for LRange {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn create_test_storage() -> Arc<Mutex<Storage>> {
        Arc::new(Mutex::new(Storage::new(HashMap::new())))
    }

    fn set_list_values(storage: &Arc<Mutex<Storage>>, key: &str, elements: &[DataType]) -> anyhow::Result<()> {
        storage.lock().unwrap().set(key, protocol::array(elements.to_vec().clone()).serialize(), None)?;
        Ok(())
    }

    fn lrange(key: &str, start_index: usize, end_index: usize) -> LRange {
        let msg = protocol::array(vec![
            protocol::bulk_string("LRANGE"),
            protocol::bulk_string(key),
            protocol::integer(start_index as i64),
            protocol::integer(end_index as i64),
        ]);
        LRange { message: msg }
    }

    #[test]
    fn test_lrange_selects_elements() -> anyhow::Result<()> {
        let storage = create_test_storage();
        let key = "mylist";
        let values = vec!["value1", "value2", "value3", "value4", "value5"];
        let elements: Vec<DataType> = values.iter().map(|s| protocol::simple_string(s)).collect();
        set_list_values(&storage, key, &elements)?;

        let start_index = 1;
        let end_index = 3;
        let result = lrange(key, start_index, end_index).execute(&storage)?;

        assert_eq!(&result[0], &protocol::array(elements[start_index..end_index + 1].to_vec()));
        Ok(())
    }


    #[test]
    fn test_lrange_with_start_index_larger_than_length() -> anyhow::Result<()> {
        let storage = create_test_storage();
        let key = "mylist";
        let values = vec!["value1", "value2", "value3", "value4", "value5"];
        let elements: Vec<DataType> = values.iter().map(|s| protocol::simple_string(s)).collect();
        set_list_values(&storage, key, &elements)?;

        let start_index = values.len() + 1;
        let end_index = start_index + 1;
        let result = lrange(key, start_index, end_index).execute(&storage)?;

        assert_eq!(&result[0], &protocol::array(Vec::new()));
        Ok(())
    }

    #[test]
    fn test_lrange_stop_index_larger_than_length() -> anyhow::Result<()> {
        let storage = create_test_storage();
        let key = "mylist";
        let values = vec!["value1", "value2", "value3", "value4", "value5"];
        let elements: Vec<DataType> = values.iter().map(|s| protocol::simple_string(s)).collect();
        set_list_values(&storage, key, &elements)?;

        let start_index = 2;
        let end_index = values.len() + 1;
        let result = lrange(key, start_index, end_index).execute(&storage)?;

        assert_eq!(&result[0], &protocol::array(elements[start_index..].to_vec()));
        Ok(())
    }

    #[test]
    fn test_lrange_start_greater_than_stop() -> anyhow::Result<()> {
        let storage = create_test_storage();
        let key = "mylist";
        let values = vec!["value1", "value2", "value3", "value4", "value5"];
        let elements: Vec<DataType> = values.iter().map(|s| protocol::simple_string(s)).collect();
        set_list_values(&storage, key, &elements)?;

        let start_index = 3;
        let end_index = 2;
        let result = lrange(key, start_index, end_index).execute(&storage)?;

        assert_eq!(&result[0], &protocol::array(Vec::new()));
        Ok(())
    }

    #[test]
    fn test_lrange_empty_list() -> anyhow::Result<()> {
        let storage = create_test_storage();
        let key = "mylist";
        set_list_values(&storage, key, &Vec::new())?;

        let start_index = 1;
        let end_index = 2;
        let result = lrange(key, start_index, end_index).execute(&storage)?;

        assert_eq!(&result[0], &protocol::array(Vec::new()));
        Ok(())
    }

    #[test]
    fn test_lrange_non_existing_list() -> anyhow::Result<()> {
        let storage = create_test_storage();
        let key = "mylist";
        let start_index = 1;
        let end_index = 2;
        let result = lrange(key, start_index, end_index).execute(&storage)?;

        assert_eq!(&result[0], &protocol::array(Vec::new()));
        Ok(())
    }
}
