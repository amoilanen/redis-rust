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
use crate::commands::RedisCommand;
use super::get_list_elements;

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
        let mut start_index: i64 = instructions.get(2).ok_or::<anyhow::Error>(error.clone().into())?.parse()?;
        let mut end_index: i64 = instructions.get(3).ok_or::<anyhow::Error>(error.clone().into())?.parse()?;

        debug!("LRANGE {} {} {}", key, start_index, end_index);
        let stored_elements = get_list_elements(key, storage)?;
        if end_index < 0 {
            end_index = (stored_elements.len() as i64 + end_index) as i64;
        }
        if start_index < 0 {
            start_index = (stored_elements.len() as i64 + start_index) as i64;
        }
        let final_start_index = start_index.max(0).min(stored_elements.len() as i64) as usize;
        let final_end_index = (end_index + 1).max(0).min(stored_elements.len() as i64) as usize;
        let selected_elements = stored_elements.get(final_start_index..final_end_index)
            .map(|s| s.to_vec()).unwrap_or(Vec::new());
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

    fn lrange(key: &str, start_index: i64, end_index: i64) -> LRange {
        let msg = protocol::array(vec![
            protocol::bulk_string("LRANGE"),
            protocol::bulk_string(key),
            protocol::integer(start_index),
            protocol::integer(end_index),
        ]);
        LRange { message: msg }
    }

    #[test]
    fn test_lrange_selects_elements() -> anyhow::Result<()> {
        let storage = create_test_storage();
        let key = "mylist";
        let values = vec!["value1", "value2", "value3", "value4", "value5"];
        let elements: Vec<DataType> = values.iter().map(|s| protocol::bulk_string(s)).collect();
        set_list_values(&storage, key, &elements)?;

        let start_index: usize = 1;
        let end_index: usize = 3;
        let result = lrange(key, start_index as i64, end_index as i64).execute(&storage)?;

        assert_eq!(&result[0], &protocol::array(elements[start_index..end_index + 1].to_vec()));
        Ok(())
    }


    #[test]
    fn test_lrange_with_start_index_larger_than_length() -> anyhow::Result<()> {
        let storage = create_test_storage();
        let key = "mylist";
        let values = vec!["value1", "value2", "value3", "value4", "value5"];
        let elements: Vec<DataType> = values.iter().map(|s| protocol::bulk_string(s)).collect();
        set_list_values(&storage, key, &elements)?;

        let start_index = values.len() + 1;
        let end_index = start_index + 1;
        let result = lrange(key, start_index as i64, end_index as i64).execute(&storage)?;

        assert_eq!(&result[0], &protocol::array(Vec::new()));
        Ok(())
    }

    #[test]
    fn test_lrange_with_negative_indices() -> anyhow::Result<()> {
        let storage = create_test_storage();
        let key = "mylist";
        let values = vec!["value1", "value2", "value3", "value4", "value5"];
        let elements: Vec<DataType> = values.iter().map(|s| protocol::bulk_string(s)).collect();
        set_list_values(&storage, key, &elements)?;

        {
            let start_index = 1;
            let end_index =  -1;
            let result = lrange(key, start_index as i64, end_index).execute(&storage)?;

            assert_eq!(&result[0], &protocol::array(elements[start_index..].to_vec()));
        }

        {
            let start_index = 1;
            let end_index =  -1;
            let result = lrange(key, start_index as i64, end_index).execute(&storage)?;

            assert_eq!(&result[0], &protocol::array(elements[start_index..].to_vec()));
        }

        {
            let start_index = 0;
            let end_index =  -1;
            let result = lrange(key, start_index as i64, end_index).execute(&storage)?;

            assert_eq!(&result[0], &protocol::array(elements.clone()));
        }

        {
            let start_index = -3;
            let end_index =  -1;
            let result = lrange(key, start_index as i64, end_index).execute(&storage)?;

            assert_eq!(&result[0], &protocol::array(elements[2..].to_vec()));
        }

        {
            let start_index = -100;
            let end_index =  100;
            let result = lrange(key, start_index as i64, end_index).execute(&storage)?;

            assert_eq!(&result[0], &protocol::array(elements.clone()));
        }
        Ok(())
    }

    #[test]
    fn test_lrange_stop_index_larger_than_length() -> anyhow::Result<()> {
        let storage = create_test_storage();
        let key = "mylist";
        let values = vec!["value1", "value2", "value3", "value4", "value5"];
        let elements: Vec<DataType> = values.iter().map(|s| protocol::bulk_string(s)).collect();
        set_list_values(&storage, key, &elements)?;

        let start_index = 2;
        let end_index = values.len() + 1;
        let result = lrange(key, start_index as i64, end_index as i64).execute(&storage)?;

        assert_eq!(&result[0], &protocol::array(elements[start_index..].to_vec()));
        Ok(())
    }

    #[test]
    fn test_lrange_start_greater_than_stop() -> anyhow::Result<()> {
        let storage = create_test_storage();
        let key = "mylist";
        let values = vec!["value1", "value2", "value3", "value4", "value5"];
        let elements: Vec<DataType> = values.iter().map(|s| protocol::bulk_string(s)).collect();
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
