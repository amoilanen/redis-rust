/// LPOP command - removes and returns the first element of a list stored at key.
///
/// Syntax: LPOP <key>
///
/// If the key does not exist or the list is empty, a null bulk string is
/// returned.
///
/// Returns: Bulk string reply - the removed element, or a null bulk string if
/// the key does not exist or the list is empty.
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
use super::update_list_elements;

/// LPOP command implementation.
pub struct LPop {
    pub message: DataType,
}

impl RedisCommand for LPop {
    fn execute(&self, storage: &Arc<Mutex<Storage>>) -> Result<Vec<DataType>, anyhow::Error> {
        let instructions: Vec<String> = self.message.as_vec()?;
        let error = RedisError {
            message: "Invalid LPOP command syntax".to_string(),
        };

        let key = instructions.get(1).ok_or::<anyhow::Error>(error.clone().into())?;
        if instructions.len() != 2 {
            return Err(error.clone().into());
        }

        debug!("LPOP {}", key);

        // Capture the popped element so we can return it after
        // update_list_elements writes the shortened list back. The closure
        // runs synchronously on this thread and only borrows `popped` for the
        // duration of the call, so a plain mutable local is sufficient.
        let mut popped: Option<DataType> = None;
        update_list_elements(key, storage, |elements| {
            if !elements.is_empty() {
                popped = Some(elements.remove(0));
            }
            Ok(())
        })?;

        match popped {
            Some(value) => Ok(vec![value]),
            None => Ok(vec![protocol::bulk_string_empty()]),
        }
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
    use super::super::{create_test_storage, read_list, set_list_values};
    use crate::commands::set::Set;

    fn lpop(key: &str) -> LPop {
        let msg = protocol::array(vec![
            protocol::bulk_string("LPOP"),
            protocol::bulk_string(key),
        ]);
        LPop { message: msg }
    }

    #[test]
    fn test_lpop_removes_and_returns_first_element() -> anyhow::Result<()> {
        let storage = create_test_storage();
        let key = "mylist";
        let values = vec!["one", "two", "three", "four", "five"];
        let elements: Vec<DataType> = values.iter().map(|s| protocol::bulk_string(s)).collect();
        set_list_values(&storage, key, &elements)?;

        let result = lpop(key).execute(&storage)?;
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], protocol::bulk_string("one"));

        // The remaining elements should be present and in the original order
        assert_eq!(
            read_list(&storage, key)?,
            vec!["two", "three", "four", "five"],
        );
        Ok(())
    }

    #[test]
    fn test_lpop_returns_null_for_nonexistent_key() -> anyhow::Result<()> {
        let storage = create_test_storage();
        let result = lpop("missing_list_key").execute(&storage)?;
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], protocol::bulk_string_empty());
        Ok(())
    }

    #[test]
    fn test_lpop_returns_null_for_empty_list() -> anyhow::Result<()> {
        let storage = create_test_storage();
        let key = "mylist";
        set_list_values(&storage, key, &Vec::new())?;

        let result = lpop(key).execute(&storage)?;
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], protocol::bulk_string_empty());
        Ok(())
    }

    #[test]
    fn test_lpop_repeated_until_empty() -> anyhow::Result<()> {
        let storage = create_test_storage();
        let key = "mylist";
        let values = vec!["a", "b"];
        let elements: Vec<DataType> = values.iter().map(|s| protocol::bulk_string(s)).collect();
        set_list_values(&storage, key, &elements)?;

        assert_eq!(lpop(key).execute(&storage)?[0], protocol::bulk_string("a"));
        assert_eq!(lpop(key).execute(&storage)?[0], protocol::bulk_string("b"));
        assert_eq!(lpop(key).execute(&storage)?[0], protocol::bulk_string_empty());
        Ok(())
    }

    #[test]
    fn test_lpop_invalid_syntax() -> anyhow::Result<()> {
        let storage = create_test_storage();

        // Missing key
        let msg1 = protocol::array(vec![protocol::bulk_string("LPOP")]);
        assert!(LPop { message: msg1 }.execute(&storage).is_err());

        // Too many arguments (count argument is not yet supported)
        let msg2 = protocol::array(vec![
            protocol::bulk_string("LPOP"),
            protocol::bulk_string("mylist"),
            protocol::bulk_string("2"),
        ]);
        assert!(LPop { message: msg2 }.execute(&storage).is_err());
        Ok(())
    }

    #[test]
    fn test_lpop_wrong_type_fails() -> anyhow::Result<()> {
        let storage = create_test_storage();

        // Store a plain string value using SET
        let set_msg = protocol::array(vec![
            protocol::bulk_string("SET"),
            protocol::bulk_string("mykey"),
            protocol::bulk_string("not_a_list"),
        ]);
        Set { message: set_msg }.execute(&storage)?;

        // LPOP on the same key should fail since it's not a list
        assert!(lpop("mykey").execute(&storage).is_err());
        Ok(())
    }
}
