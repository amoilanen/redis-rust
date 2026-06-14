/// XADD <key> <id> <field> <value> [<field> <value> ...]
///
/// Appends an entry with an explicit ID to a stream, creating it if needed,
/// and replies with the entry ID as a bulk string.

use std::sync::{Arc, Mutex};

use anyhow::anyhow;
use log::*;

use crate::commands::RedisCommand;
use crate::error::RedisError;
use crate::protocol;
use crate::protocol::DataType;
use crate::storage::Storage;

/// XADD command implementation.
pub struct XAdd {
    pub message: DataType,
}

impl RedisCommand for XAdd {
    fn execute(&self, storage: &Arc<Mutex<Storage>>) -> Result<Vec<DataType>, anyhow::Error> {
        let instructions: Vec<String> = self.message.as_vec()?;
        let error = RedisError {
            message: "Invalid XADD command syntax".to_string(),
        };

        // Need key, id, and an even number of field-value arguments.
        if instructions.len() < 5 || (instructions.len() - 3) % 2 != 0 {
            return Err(error.clone().into());
        }

        let key = &instructions[1];
        let id = &instructions[2];
        let fields: Vec<(String, String)> = instructions[3..]
            .chunks(2)
            .map(|pair| (pair[0].clone(), pair[1].clone()))
            .collect();

        debug!("XADD {} {} {:?}", key, id, fields);

        let stored_id = storage
            .lock()
            .map_err(|e| anyhow!("Failed to lock storage: {}", e))?
            .xadd(key, id, fields)?;

        Ok(vec![protocol::bulk_string(&stored_id)])
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
    use crate::commands::create_test_storage;

    fn xadd_cmd(parts: &[&str]) -> XAdd {
        let elements = parts.iter().map(|p| protocol::bulk_string(p)).collect();
        XAdd { message: protocol::array(elements) }
    }

    #[test]
    fn test_xadd_creates_stream_and_returns_id() -> anyhow::Result<()> {
        let storage = create_test_storage();
        let cmd = xadd_cmd(&["XADD", "stream_key", "0-1", "foo", "bar"]);

        let result = cmd.execute(&storage)?;
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], protocol::bulk_string("0-1"));
        assert!(cmd.is_propagated_to_replicas());

        let guard = storage.lock().unwrap();
        assert!(guard.contains_stream("stream_key"));
        let stream = guard.get_stream("stream_key").unwrap();
        assert_eq!(stream.entries.len(), 1);
        assert_eq!(stream.entries[0].id, "0-1");
        assert_eq!(
            stream.entries[0].fields,
            vec![("foo".to_string(), "bar".to_string())],
        );
        Ok(())
    }

    #[test]
    fn test_xadd_appends_to_existing_stream() -> anyhow::Result<()> {
        let storage = create_test_storage();
        xadd_cmd(&["XADD", "stream_key", "0-1", "foo", "bar"]).execute(&storage)?;
        let result = xadd_cmd(&["XADD", "stream_key", "0-2", "baz", "qux"]).execute(&storage)?;

        assert_eq!(result[0], protocol::bulk_string("0-2"));

        let guard = storage.lock().unwrap();
        let stream = guard.get_stream("stream_key").unwrap();
        let ids: Vec<&str> = stream.entries.iter().map(|e| e.id.as_str()).collect();
        assert_eq!(ids, vec!["0-1", "0-2"]);
        Ok(())
    }

    #[test]
    fn test_xadd_multiple_field_value_pairs() -> anyhow::Result<()> {
        let storage = create_test_storage();
        let result = xadd_cmd(&[
            "XADD", "stream_key", "1-1", "temperature", "36", "humidity", "95",
        ])
        .execute(&storage)?;

        assert_eq!(result[0], protocol::bulk_string("1-1"));

        let guard = storage.lock().unwrap();
        let stream = guard.get_stream("stream_key").unwrap();
        assert_eq!(
            stream.entries[0].fields,
            vec![
                ("temperature".to_string(), "36".to_string()),
                ("humidity".to_string(), "95".to_string()),
            ],
        );
        Ok(())
    }

    #[test]
    fn test_xadd_on_existing_string_key_fails() -> anyhow::Result<()> {
        use crate::commands::set::Set;
        let storage = create_test_storage();

        let set_msg = protocol::array(vec![
            protocol::bulk_string("SET"),
            protocol::bulk_string("mykey"),
            protocol::bulk_string("not_a_stream"),
        ]);
        Set { message: set_msg }.execute(&storage)?;

        assert!(xadd_cmd(&["XADD", "mykey", "0-1", "foo", "bar"]).execute(&storage).is_err());
        assert!(!storage.lock().unwrap().contains_stream("mykey"));
        Ok(())
    }

    #[test]
    fn test_xadd_invalid_syntax() {
        let storage = create_test_storage();

        // Missing id and fields
        assert!(xadd_cmd(&["XADD", "stream_key"]).execute(&storage).is_err());
        // Missing fields
        assert!(xadd_cmd(&["XADD", "stream_key", "0-1"]).execute(&storage).is_err());
        // Odd number of field-value arguments
        assert!(xadd_cmd(&["XADD", "stream_key", "0-1", "foo"]).execute(&storage).is_err());
        assert!(
            xadd_cmd(&["XADD", "stream_key", "0-1", "foo", "bar", "baz"])
                .execute(&storage)
                .is_err()
        );
    }
}
