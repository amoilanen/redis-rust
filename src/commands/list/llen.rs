/// LLEN command - returns the length of the list stored at key.
///
/// Syntax: LLEN <key>
///
/// If the key does not exist, it is treated as an empty list and 0 is
/// returned.
///
/// Returns: Integer reply - the length of the list at `key`.
///
/// Errors:
///   Returns an error if the value stored at key is not a list.

use std::sync::{Arc, Mutex};
use log::*;
use crate::protocol;
use crate::protocol::DataType;
use crate::storage::Storage;
use crate::error::RedisError;
use crate::commands::RedisCommand;
use super::get_list_elements;

/// LLEN command implementation.
pub struct LLen {
    pub message: DataType,
}

impl RedisCommand for LLen {
    fn execute(&self, storage: &Arc<Mutex<Storage>>) -> Result<Vec<DataType>, anyhow::Error> {
        let instructions: Vec<String> = self.message.as_vec()?;
        let error = RedisError {
            message: "Invalid LLEN command syntax".to_string(),
        };

        let key = instructions.get(1).ok_or::<anyhow::Error>(error.clone().into())?;
        if instructions.len() != 2 {
            return Err(error.clone().into());
        }

        debug!("LLEN {}", key);
        let stored_elements = get_list_elements(key, storage)?;
        Ok(vec![protocol::integer(stored_elements.len() as i64)])
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
    use super::super::{create_test_storage, set_list_values};
    use crate::commands::set::Set;

    fn llen(key: &str) -> LLen {
        let msg = protocol::array(vec![
            protocol::bulk_string("LLEN"),
            protocol::bulk_string(key),
        ]);
        LLen { message: msg }
    }

    #[test]
    fn test_llen_returns_length_of_existing_list() -> anyhow::Result<()> {
        let storage = create_test_storage();
        let key = "mylist";
        let values = vec!["value1", "value2", "value3", "value4"];
        let elements: Vec<DataType> = values.iter().map(|s| protocol::bulk_string(s)).collect();
        set_list_values(&storage, key, &elements)?;

        let result = llen(key).execute(&storage)?;
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], protocol::integer(4));
        Ok(())
    }

    #[test]
    fn test_llen_returns_zero_for_nonexistent_key() -> anyhow::Result<()> {
        let storage = create_test_storage();
        let result = llen("missing_list_key").execute(&storage)?;
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], protocol::integer(0));
        Ok(())
    }

    #[test]
    fn test_llen_returns_zero_for_empty_list() -> anyhow::Result<()> {
        let storage = create_test_storage();
        let key = "mylist";
        set_list_values(&storage, key, &Vec::new())?;

        let result = llen(key).execute(&storage)?;
        assert_eq!(result[0], protocol::integer(0));
        Ok(())
    }

    #[test]
    fn test_llen_invalid_syntax() -> anyhow::Result<()> {
        let storage = create_test_storage();

        // Missing key
        let msg1 = protocol::array(vec![protocol::bulk_string("LLEN")]);
        assert!(LLen { message: msg1 }.execute(&storage).is_err());

        // Too many arguments
        let msg2 = protocol::array(vec![
            protocol::bulk_string("LLEN"),
            protocol::bulk_string("mylist"),
            protocol::bulk_string("extra"),
        ]);
        assert!(LLen { message: msg2 }.execute(&storage).is_err());
        Ok(())
    }

    #[test]
    fn test_llen_wrong_type_fails() -> anyhow::Result<()> {
        let storage = create_test_storage();

        // Store a plain string value using SET
        let set_msg = protocol::array(vec![
            protocol::bulk_string("SET"),
            protocol::bulk_string("mykey"),
            protocol::bulk_string("not_a_list"),
        ]);
        Set { message: set_msg }.execute(&storage)?;

        // LLEN on the same key should fail since it's not a list
        assert!(llen("mykey").execute(&storage).is_err());
        Ok(())
    }
}
