/// LPUSH command - prepends one or more values to the start of a list stored at key.
///
/// Syntax: LPUSH <key> <value> [value ...]
///
/// If the key does not exist, a new list is created before prepending the values.
/// Each value is inserted at the head of the list one at a time, so the values
/// end up in reverse order relative to how they were supplied. For example,
/// `LPUSH list a b c` results in the list `["c", "b", "a"]`.
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

/// LPUSH command implementation.
pub struct LPush {
    pub message: DataType,
}

impl RedisCommand for LPush {
    fn execute(&self, storage: &Arc<Mutex<Storage>>) -> Result<Vec<DataType>, anyhow::Error> {
        push_to_list(&self.message, storage, "LPUSH", |elements, value| {
            elements.insert(0, protocol::bulk_string(value));
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

    fn lpush_msg(key: &str, values: &[&str]) -> DataType {
        let mut parts = vec![
            protocol::bulk_string("LPUSH"),
            protocol::bulk_string(key),
        ];
        for v in values {
            parts.push(protocol::bulk_string(v));
        }
        protocol::array(parts)
    }

    #[test]
    fn test_lpush_creates_and_prepends() -> anyhow::Result<()> {
        let storage = create_test_storage();

        // Single-value pushes accumulate at the head: pushing "one", "two", "three"
        // one at a time should result in ["three", "two", "one"].
        let values = vec!["one", "two", "three"];
        for (i, value) in values.iter().enumerate() {
            let cmd = LPush { message: lpush_msg("mylist", &[value]) };
            let result = cmd.execute(&storage)?;
            assert_eq!(result.len(), 1);
            assert_eq!(result[0].as_string()?, (i + 1).to_string());
            assert!(cmd.is_propagated_to_replicas());
        }

        assert_eq!(
            read_list(&storage, "mylist")?,
            vec!["three", "two", "one"],
        );
        Ok(())
    }

    #[test]
    fn test_lpush_multiple_elements_reverse_order() -> anyhow::Result<()> {
        let storage = create_test_storage();

        // Create new list with multiple elements - they should be inserted in
        // reverse order, so "a", "b", "c" becomes ["c", "b", "a"].
        let result1 = LPush { message: lpush_msg("mylist", &["a", "b", "c"]) }.execute(&storage)?;
        assert_eq!(result1.len(), 1);
        assert_eq!(result1[0].as_string()?, "3");

        assert_eq!(
            read_list(&storage, "mylist")?,
            vec!["c", "b", "a"],
        );

        // Prepend more elements to existing list. "d", "e" -> head becomes ["e", "d", ...].
        let result2 = LPush { message: lpush_msg("mylist", &["d", "e"]) }.execute(&storage)?;
        assert_eq!(result2.len(), 1);
        assert_eq!(result2[0].as_string()?, "5");

        assert_eq!(
            read_list(&storage, "mylist")?,
            vec!["e", "d", "c", "b", "a"],
        );
        Ok(())
    }

    #[test]
    fn test_lpush_invalid_syntax() -> anyhow::Result<()> {
        let storage = create_test_storage();

        // Missing both key and value
        let msg1 = protocol::array(vec![protocol::bulk_string("LPUSH")]);
        assert!(LPush { message: msg1 }.execute(&storage).is_err());

        // Missing value
        let msg2 = protocol::array(vec![
            protocol::bulk_string("LPUSH"),
            protocol::bulk_string("mylist"),
        ]);
        assert!(LPush { message: msg2 }.execute(&storage).is_err());
        Ok(())
    }

    #[test]
    fn test_lpush_wrong_type_fails() -> anyhow::Result<()> {
        let storage = create_test_storage();

        // Store a plain string value using SET
        let set_msg = protocol::array(vec![
            protocol::bulk_string("SET"),
            protocol::bulk_string("mykey"),
            protocol::bulk_string("not_a_list"),
        ]);
        Set { message: set_msg }.execute(&storage)?;

        // LPUSH to the same key should fail since it's not a list
        assert!(LPush { message: lpush_msg("mykey", &["value"]) }.execute(&storage).is_err());
        Ok(())
    }

    #[test]
    fn test_lpush_single_then_multi() -> anyhow::Result<()> {
        let storage = create_test_storage();

        // Single push to a fresh key
        let r1 = LPush { message: lpush_msg("k", &["c"]) }.execute(&storage)?;
        assert_eq!(r1[0].as_string()?, "1");
        assert_eq!(read_list(&storage, "k")?, vec!["c"]);

        // Multi push - "b", "a" should be inserted as head ["a", "b", ...]
        let r2 = LPush { message: lpush_msg("k", &["b", "a"]) }.execute(&storage)?;
        assert_eq!(r2[0].as_string()?, "3");
        assert_eq!(read_list(&storage, "k")?, vec!["a", "b", "c"]);
        Ok(())
    }
}
