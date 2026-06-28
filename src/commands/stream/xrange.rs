/// XRANGE <key> <start> <end>
///
/// Returns the entries of the stream at `key` whose IDs fall within the
/// inclusive range `[start, end]`. Boundary IDs may omit the sequence number:
/// it defaults to `0` for `start` and to the maximum sequence for `end`.
///
/// The reply is a RESP array of entries. Each entry is a two-element array of
/// the entry ID (bulk string) and an array of its field/value pairs (bulk
/// strings) in insertion order.

use std::sync::{Arc, Mutex};

use anyhow::anyhow;
use log::*;

use crate::commands::RedisCommand;
use crate::error::RedisError;
use crate::protocol;
use crate::protocol::DataType;
use crate::storage::Storage;
use crate::stream::{StreamEntry, StreamId};

/// XRANGE command implementation.
pub struct XRange {
    pub message: DataType,
}

impl RedisCommand for XRange {
    fn execute(&self, storage: &Arc<Mutex<Storage>>) -> Result<Vec<DataType>, anyhow::Error> {
        let instructions: Vec<String> = self.message.as_vec()?;
        let error = RedisError {
            message: "ERR wrong number of arguments for 'xrange' command".to_string(),
        };

        if instructions.len() != 4 {
            return Err(error.into());
        }

        let key = &instructions[1];
        // A missing sequence defaults to 0 for the start and the max for the end,
        // so a bare `<ms>` start/end spans the whole millisecond.
        let start = StreamId::parse_range(&instructions[2], 0)?;
        let end = StreamId::parse_range(&instructions[3], u64::MAX)?;

        debug!("XRANGE {} {} {}", key, start, end);

        let entries = storage
            .lock()
            .map_err(|e| anyhow!("Failed to lock storage: {}", e))?
            .xrange(key, start, end);

        Ok(vec![encode_entries(&entries)])
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

/// Encodes stream entries as a RESP array of `[id, [field, value, ...]]` pairs.
fn encode_entries(entries: &[StreamEntry]) -> DataType {
    let encoded = entries.iter().map(encode_entry).collect();
    protocol::array(encoded)
}

fn encode_entry(entry: &StreamEntry) -> DataType {
    let mut fields: Vec<DataType> = Vec::with_capacity(entry.fields.len() * 2);
    for (field, value) in &entry.fields {
        fields.push(protocol::bulk_string(field));
        fields.push(protocol::bulk_string(value));
    }
    protocol::array(vec![
        protocol::bulk_string(&entry.id.to_string()),
        protocol::array(fields),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::create_test_storage;
    use crate::commands::stream::XAdd;

    fn xadd(parts: &[&str]) -> XAdd {
        let elements = parts.iter().map(|p| protocol::bulk_string(p)).collect();
        XAdd { message: protocol::array(elements) }
    }

    fn xrange_cmd(parts: &[&str]) -> XRange {
        let elements = parts.iter().map(|p| protocol::bulk_string(p)).collect();
        XRange { message: protocol::array(elements) }
    }

    #[test]
    fn test_xrange_returns_inclusive_range() -> anyhow::Result<()> {
        let storage = create_test_storage();
        xadd(&["XADD", "stream_key", "0-1", "foo", "bar"]).execute(&storage)?;
        xadd(&["XADD", "stream_key", "0-2", "bar", "baz"]).execute(&storage)?;
        xadd(&["XADD", "stream_key", "0-3", "baz", "foo"]).execute(&storage)?;

        let result = xrange_cmd(&["XRANGE", "stream_key", "0-2", "0-3"]).execute(&storage)?;

        let expected = protocol::array(vec![
            protocol::array(vec![
                protocol::bulk_string("0-2"),
                protocol::array(vec![protocol::bulk_string("bar"), protocol::bulk_string("baz")]),
            ]),
            protocol::array(vec![
                protocol::bulk_string("0-3"),
                protocol::array(vec![protocol::bulk_string("baz"), protocol::bulk_string("foo")]),
            ]),
        ]);
        assert_eq!(result, vec![expected]);
        Ok(())
    }

    #[test]
    fn test_xrange_omitted_sequence_numbers() -> anyhow::Result<()> {
        let storage = create_test_storage();
        xadd(&["XADD", "s", "5-0", "a", "1"]).execute(&storage)?;
        xadd(&["XADD", "s", "5-9", "b", "2"]).execute(&storage)?;
        xadd(&["XADD", "s", "6-0", "c", "3"]).execute(&storage)?;

        // start "5" -> 5-0, end "5" -> 5-MAX captures both 5-* entries but not 6-0.
        let result = xrange_cmd(&["XRANGE", "s", "5", "5"]).execute(&storage)?;
        let expected = protocol::array(vec![
            protocol::array(vec![
                protocol::bulk_string("5-0"),
                protocol::array(vec![protocol::bulk_string("a"), protocol::bulk_string("1")]),
            ]),
            protocol::array(vec![
                protocol::bulk_string("5-9"),
                protocol::array(vec![protocol::bulk_string("b"), protocol::bulk_string("2")]),
            ]),
        ]);
        assert_eq!(result, vec![expected]);
        Ok(())
    }

    #[test]
    fn test_xrange_start_dash_reads_from_beginning() -> anyhow::Result<()> {
        let storage = create_test_storage();
        xadd(&["XADD", "stream_key", "0-1", "foo", "bar"]).execute(&storage)?;
        xadd(&["XADD", "stream_key", "0-2", "bar", "baz"]).execute(&storage)?;
        xadd(&["XADD", "stream_key", "0-3", "baz", "foo"]).execute(&storage)?;

        let result = xrange_cmd(&["XRANGE", "stream_key", "-", "0-2"]).execute(&storage)?;

        let expected = protocol::array(vec![
            protocol::array(vec![
                protocol::bulk_string("0-1"),
                protocol::array(vec![protocol::bulk_string("foo"), protocol::bulk_string("bar")]),
            ]),
            protocol::array(vec![
                protocol::bulk_string("0-2"),
                protocol::array(vec![protocol::bulk_string("bar"), protocol::bulk_string("baz")]),
            ]),
        ]);
        assert_eq!(result, vec![expected]);
        Ok(())
    }

    #[test]
    fn test_xrange_end_plus_reads_until_end() -> anyhow::Result<()> {
        let storage = create_test_storage();
        xadd(&["XADD", "stream_key", "0-1", "foo", "bar"]).execute(&storage)?;
        xadd(&["XADD", "stream_key", "0-2", "bar", "baz"]).execute(&storage)?;
        xadd(&["XADD", "stream_key", "0-3", "baz", "foo"]).execute(&storage)?;

        let result = xrange_cmd(&["XRANGE", "stream_key", "0-2", "+"]).execute(&storage)?;

        let expected = protocol::array(vec![
            protocol::array(vec![
                protocol::bulk_string("0-2"),
                protocol::array(vec![protocol::bulk_string("bar"), protocol::bulk_string("baz")]),
            ]),
            protocol::array(vec![
                protocol::bulk_string("0-3"),
                protocol::array(vec![protocol::bulk_string("baz"), protocol::bulk_string("foo")]),
            ]),
        ]);
        assert_eq!(result, vec![expected]);
        Ok(())
    }

    #[test]
    fn test_xrange_multiple_field_value_pairs() -> anyhow::Result<()> {
        let storage = create_test_storage();
        xadd(&["XADD", "s", "1-1", "temperature", "36", "humidity", "95"]).execute(&storage)?;

        let result = xrange_cmd(&["XRANGE", "s", "1-1", "1-1"]).execute(&storage)?;
        let expected = protocol::array(vec![protocol::array(vec![
            protocol::bulk_string("1-1"),
            protocol::array(vec![
                protocol::bulk_string("temperature"),
                protocol::bulk_string("36"),
                protocol::bulk_string("humidity"),
                protocol::bulk_string("95"),
            ]),
        ])]);
        assert_eq!(result, vec![expected]);
        Ok(())
    }

    #[test]
    fn test_xrange_missing_key_returns_empty_array() -> anyhow::Result<()> {
        let storage = create_test_storage();
        let result = xrange_cmd(&["XRANGE", "missing", "0", "9"]).execute(&storage)?;
        assert_eq!(result, vec![protocol::array(Vec::new())]);
        Ok(())
    }

    #[test]
    fn test_xrange_invalid_syntax() {
        let storage = create_test_storage();
        assert!(xrange_cmd(&["XRANGE", "s", "0"]).execute(&storage).is_err());
        assert!(xrange_cmd(&["XRANGE", "s", "0", "9", "extra"]).execute(&storage).is_err());
    }

    #[test]
    fn test_xrange_invalid_id() {
        let storage = create_test_storage();
        assert!(xrange_cmd(&["XRANGE", "s", "abc", "9"]).execute(&storage).is_err());
    }
}
