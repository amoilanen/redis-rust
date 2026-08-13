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

use super::push_to_list;
use crate::blocking::BlockingNotifier;
use crate::commands::RedisCommand;
use crate::protocol;
use crate::protocol::DataType;
use crate::storage::Storage;

/// RPUSH command implementation.
pub struct RPush {
    pub message: DataType,
    pub notifier: Arc<BlockingNotifier>,
}

impl RedisCommand for RPush {
    fn execute(&self, storage: &Arc<Mutex<Storage>>) -> Result<Vec<DataType>, anyhow::Error> {
        push_to_list(&self.message, storage, &self.notifier, "RPUSH", |elements, value| {
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

    fn name(&self) -> &str {
        "RPUSH"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::read_list;
    use crate::commands::{command_message, create_test_notifier, create_test_storage, set};

    #[test]
    fn test_rpush_creates_and_appends() -> anyhow::Result<()> {
        let storage = create_test_storage();
        let notifier = create_test_notifier();

        let values = vec!["one", "two", "three"];
        for (i, value) in values.iter().enumerate() {
            let msg = command_message(&["RPUSH", "mylist", value]);
            let cmd = RPush { message: msg, notifier: Arc::clone(&notifier) };
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
        let notifier = create_test_notifier();

        // Create new list with multiple elements
        let msg1 = command_message(&["RPUSH", "mylist", "element1", "element2", "element3"]);
        let result1 = RPush { message: msg1, notifier: Arc::clone(&notifier) }.execute(&storage)?;
        assert_eq!(result1.len(), 1);
        assert_eq!(result1[0].as_string()?, "3");

        // Verify the stored list contains exactly the three elements in order
        assert_eq!(
            read_list(&storage, "mylist")?,
            vec!["element1", "element2", "element3"],
        );

        // Append more elements to existing list
        let msg2 = command_message(&["RPUSH", "mylist", "element4", "element5"]);
        let result2 = RPush { message: msg2, notifier: Arc::clone(&notifier) }.execute(&storage)?;
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
        let notifier = create_test_notifier();

        // Missing both key and value
        let msg1 = command_message(&["RPUSH"]);
        assert!(RPush { message: msg1, notifier: Arc::clone(&notifier) }.execute(&storage).is_err());

        // Missing value
        let msg2 = command_message(&["RPUSH", "mylist"]);
        assert!(RPush { message: msg2, notifier: Arc::clone(&notifier) }.execute(&storage).is_err());
        Ok(())
    }

    #[test]
    fn test_rpush_wrong_type_fails() -> anyhow::Result<()> {
        let storage = create_test_storage();
        let notifier = create_test_notifier();

        // Store a plain string value using SET
        set(&["SET", "mykey", "not_a_list"]).execute(&storage)?;

        // RPUSH to the same key should fail since it's not a list
        let rpush_msg = command_message(&["RPUSH", "mykey", "value"]);
        assert!(RPush { message: rpush_msg, notifier: Arc::clone(&notifier) }.execute(&storage).is_err());
        Ok(())
    }
}
