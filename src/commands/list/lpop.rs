/// LPOP command - removes and returns one or more first elements of a list
/// stored at key.
///
/// Syntax: LPOP <key> [count]
///
/// Without `count` the command behaves as a single-element pop:
/// returns a bulk string with the removed value, or a null bulk string if the
/// key does not exist (or the list is empty).
///
/// With `count` (a non-negative integer) the command removes up to `count`
/// elements from the head of the list and returns them as a RESP array, in the
/// order they were removed. If `count` exceeds the list length, all elements
/// are removed and returned. If the key does not exist (or the list is empty)
/// an empty RESP array is returned.
///
/// Errors:
///   Returns an error if the value stored at key is not a list, if the
///   `count` argument cannot be parsed as an integer, or if `count` is
///   negative.

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

        if instructions.len() < 2 || instructions.len() > 3 {
            return Err(error.clone().into());
        }
        let key = instructions.get(1).ok_or::<anyhow::Error>(error.clone().into())?;

        // Parse the optional count argument. `None` keeps the legacy
        // single-element bulk-string return shape; `Some(n)` switches to the
        // array reply shape with up to `n` elements.
        let count: Option<usize> = instructions
            .get(2)
            .map(|value_str| {
                let parsed: i64 = value_str.parse().map_err(|_| error.clone())?;
                usize::try_from(parsed).map_err(|_| error.clone())
            })
            .transpose()?;

        debug!("LPOP {} {:?}", key, count);

        // `update_list_elements` writes the shortened list back to storage
        // and propagates the closure's return value — here, the elements we
        // drained from the front.
        let total_count_to_pop = count.unwrap_or(1);
        let popped: Vec<DataType> = update_list_elements(key, storage, |elements| {
            let n = total_count_to_pop.min(elements.len());
            Ok(elements.drain(..n).collect())
        })?;

        // Legacy single-element form returns a bulk string (or null bulk string
        // when nothing was popped); the multi-element form always replies with
        // a RESP array, even when empty (`*0\r\n`).
        let reply = if count.is_some() {
            protocol::array(popped)
        } else {
            popped.into_iter().next().unwrap_or_else(protocol::bulk_string_empty)
        };
        Ok(vec![reply])
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

    fn lpop_n(key: &str, count: i64) -> LPop {
        let msg = protocol::array(vec![
            protocol::bulk_string("LPOP"),
            protocol::bulk_string(key),
            protocol::bulk_string(&count.to_string()),
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

        // Too many arguments (only one optional count is supported)
        let msg2 = protocol::array(vec![
            protocol::bulk_string("LPOP"),
            protocol::bulk_string("mylist"),
            protocol::bulk_string("2"),
            protocol::bulk_string("extra"),
        ]);
        assert!(LPop { message: msg2 }.execute(&storage).is_err());

        // Non-integer count
        let msg3 = protocol::array(vec![
            protocol::bulk_string("LPOP"),
            protocol::bulk_string("mylist"),
            protocol::bulk_string("notanumber"),
        ]);
        assert!(LPop { message: msg3 }.execute(&storage).is_err());

        // Negative count is rejected
        let msg4 = protocol::array(vec![
            protocol::bulk_string("LPOP"),
            protocol::bulk_string("mylist"),
            protocol::bulk_string("-1"),
        ]);
        assert!(LPop { message: msg4 }.execute(&storage).is_err());
        Ok(())
    }

    #[test]
    fn test_lpop_with_count_removes_and_returns_array() -> anyhow::Result<()> {
        let storage = create_test_storage();
        let key = "mylist";
        let values = vec!["one", "two", "three", "four", "five"];
        let elements: Vec<DataType> = values.iter().map(|s| protocol::bulk_string(s)).collect();
        set_list_values(&storage, key, &elements)?;

        // LPOP key 2 -> array ["one", "two"], remainder ["three", "four", "five"]
        let result = lpop_n(key, 2).execute(&storage)?;
        assert_eq!(result.len(), 1);
        assert_eq!(
            result[0],
            protocol::array(vec![
                protocol::bulk_string("one"),
                protocol::bulk_string("two"),
            ])
        );

        assert_eq!(
            read_list(&storage, key)?,
            vec!["three", "four", "five"],
        );
        Ok(())
    }

    #[test]
    fn test_lpop_with_count_greater_than_length_drains_list() -> anyhow::Result<()> {
        let storage = create_test_storage();
        let key = "mylist";
        let values = vec!["a", "b", "c"];
        let elements: Vec<DataType> = values.iter().map(|s| protocol::bulk_string(s)).collect();
        set_list_values(&storage, key, &elements)?;

        // LPOP key 10 on a 3-element list returns all 3 elements, list is empty after.
        let result = lpop_n(key, 10).execute(&storage)?;
        assert_eq!(
            result[0],
            protocol::array(vec![
                protocol::bulk_string("a"),
                protocol::bulk_string("b"),
                protocol::bulk_string("c"),
            ])
        );
        assert!(read_list(&storage, key)?.is_empty());
        Ok(())
    }

    #[test]
    fn test_lpop_with_count_zero_returns_empty_array() -> anyhow::Result<()> {
        let storage = create_test_storage();
        let key = "mylist";
        let values = vec!["a", "b"];
        let elements: Vec<DataType> = values.iter().map(|s| protocol::bulk_string(s)).collect();
        set_list_values(&storage, key, &elements)?;

        // LPOP key 0 removes nothing and returns an empty array.
        let result = lpop_n(key, 0).execute(&storage)?;
        assert_eq!(result[0], protocol::array(Vec::new()));
        assert_eq!(read_list(&storage, key)?, vec!["a", "b"]);
        Ok(())
    }

    #[test]
    fn test_lpop_with_count_on_empty_list_returns_empty_array() -> anyhow::Result<()> {
        let storage = create_test_storage();
        let key = "mylist";
        set_list_values(&storage, key, &Vec::new())?;

        let result = lpop_n(key, 3).execute(&storage)?;
        assert_eq!(result[0], protocol::array(Vec::new()));
        Ok(())
    }

    #[test]
    fn test_lpop_with_count_on_missing_key_returns_empty_array() -> anyhow::Result<()> {
        let storage = create_test_storage();

        let result = lpop_n("missing_list_key", 4).execute(&storage)?;
        assert_eq!(result[0], protocol::array(Vec::new()));
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
