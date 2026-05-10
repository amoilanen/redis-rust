/// RPUSH command - appends one or more values to the end of a list stored at key.
///
/// Syntax: RPUSH <key> <value> [value ...]
///
/// If the key does not exist, a new list is created before appending the values.
/// If the key exists and holds a list, the values are appended to the end in order.
///
/// Returns: Integer reply - the length of the list after the push operation
///
/// Errors:
///   Returns an error if the value stored at key is not a list.

use std::sync::{Arc, Mutex};
use crate::protocol;
use crate::protocol::DataType;
use crate::storage::Storage;
use crate::commands::RedisCommand;
use super::push_to_list;

/// RPUSH command implementation.
pub struct RPush {
    pub message: DataType,
}

impl RedisCommand for RPush {
    fn execute(&self, storage: &Arc<Mutex<Storage>>) -> Result<Vec<DataType>, anyhow::Error> {
        push_to_list(&self.message, storage, "RPUSH", |elements, value| {
            elements.push(protocol::bulk_string(value));
        })
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
    use super::super::read_list;
    use crate::commands::set::Set;
    use std::collections::HashMap;

    fn create_test_storage() -> Arc<Mutex<Storage>> {
        Arc::new(Mutex::new(Storage::new(HashMap::new())))
    }

    #[test]
    fn test_rpush_creates_and_appends() -> anyhow::Result<()> {
        let storage = create_test_storage();

        let values = vec!["one", "two", "three"];
        for (i, value) in values.iter().enumerate() {
            let msg = protocol::array(vec![
                protocol::bulk_string("RPUSH"),
                protocol::bulk_string("mylist"),
                protocol::bulk_string(value),
            ]);
            let cmd = RPush { message: msg };
            let result = cmd.execute(&storage)?;
            assert_eq!(result.len(), 1);
            assert_eq!(result[0].as_string()?, (i + 1).to_string());
            assert!(cmd.is_propagated_to_replicas());
        }
        Ok(())
    }

    #[test]
    fn test_rpush_multiple_elements() -> anyhow::Result<()> {
        let storage = create_test_storage();

        // Create new list with multiple elements
        let msg1 = protocol::array(vec![
            protocol::bulk_string("RPUSH"),
            protocol::bulk_string("mylist"),
            protocol::bulk_string("element1"),
            protocol::bulk_string("element2"),
            protocol::bulk_string("element3"),
        ]);
        let result1 = RPush { message: msg1 }.execute(&storage)?;
        assert_eq!(result1.len(), 1);
        assert_eq!(result1[0].as_string()?, "3");

        // Verify the stored list contains exactly the three elements in order
        assert_eq!(
            read_list(&storage, "mylist")?,
            vec!["element1", "element2", "element3"],
        );

        // Append more elements to existing list
        let msg2 = protocol::array(vec![
            protocol::bulk_string("RPUSH"),
            protocol::bulk_string("mylist"),
            protocol::bulk_string("element4"),
            protocol::bulk_string("element5"),
        ]);
        let result2 = RPush { message: msg2 }.execute(&storage)?;
        assert_eq!(result2.len(), 1);
        assert_eq!(result2[0].as_string()?, "5");

        // Verify the stored list now contains all five elements in order
        assert_eq!(
            read_list(&storage, "mylist")?,
            vec!["element1", "element2", "element3", "element4", "element5"],
        );
        Ok(())
    }

    #[test]
    fn test_rpush_invalid_syntax() -> anyhow::Result<()> {
        let storage = create_test_storage();

        // Missing both key and value
        let msg1 = protocol::array(vec![protocol::bulk_string("RPUSH")]);
        assert!(RPush { message: msg1 }.execute(&storage).is_err());

        // Missing value
        let msg2 = protocol::array(vec![
            protocol::bulk_string("RPUSH"),
            protocol::bulk_string("mylist"),
        ]);
        assert!(RPush { message: msg2 }.execute(&storage).is_err());
        Ok(())
    }

    #[test]
    fn test_rpush_wrong_type_fails() -> anyhow::Result<()> {
        let storage = create_test_storage();

        // Store a plain string value using SET
        let set_msg = protocol::array(vec![
            protocol::bulk_string("SET"),
            protocol::bulk_string("mykey"),
            protocol::bulk_string("not_a_list"),
        ]);
        Set { message: set_msg }.execute(&storage)?;

        // RPUSH to the same key should fail since it's not a list
        let rpush_msg = protocol::array(vec![
            protocol::bulk_string("RPUSH"),
            protocol::bulk_string("mykey"),
            protocol::bulk_string("value"),
        ]);
        assert!(RPush { message: rpush_msg }.execute(&storage).is_err());
        Ok(())
    }
}
