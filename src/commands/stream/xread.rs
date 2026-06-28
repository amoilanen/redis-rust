/// XREAD STREAMS <key> <id>
///
/// Returns the entries of the stream at `key` whose IDs are strictly greater
/// than `id`. Unlike XRANGE, XREAD is exclusive and takes only a single ID.
///
/// The reply is a RESP array of streams. Each stream is a two-element array of
/// the stream key (bulk string) and an array of its matching entries, where
/// each entry is encoded as `[id, [field, value, ...]]` (see
/// [`super::encode_entries`]).

use std::sync::{Arc, Mutex};

use anyhow::anyhow;
use log::*;

use crate::commands::RedisCommand;
use crate::commands::stream::encode_entries;
use crate::error::RedisError;
use crate::protocol;
use crate::protocol::DataType;
use crate::storage::Storage;
use crate::stream::StreamId;

/// XREAD command implementation.
pub struct XRead {
    pub message: DataType,
}

impl RedisCommand for XRead {
    fn execute(&self, storage: &Arc<Mutex<Storage>>) -> Result<Vec<DataType>, anyhow::Error> {
        let instructions: Vec<String> = self.message.as_vec()?;
        let error = RedisError {
            message: format!("ERR cannot parse 'xread' command: {}", self.message.as_string()?),
        };

        // `XREAD STREAMS <key1> <key2> ... <id1> <id2> ...`: the STREAMS keyword
        // is followed by N keys and then their N corresponding IDs. Optional
        // arguments (COUNT, BLOCK) come later.
        if !instructions[1].eq_ignore_ascii_case("STREAMS") {
            return Err(error.into());
        }
        let args = &instructions[2..];
        if args.is_empty() || args.len() % 2 != 0 {
            return Err(error.into());
        }
        let (keys, ids) = args.split_at(args.len() / 2);

        let storage = storage
            .lock()
            .map_err(|e| anyhow!("Failed to lock storage: {}", e))?;

        let mut streams = Vec::with_capacity(keys.len());
        for (key, id) in keys.iter().zip(ids) {
            let after = StreamId::parse_range(id, 0)?;
            debug!("XREAD STREAMS {} {}", key, after);

            let entries = storage.xread(key, after);
            streams.push(protocol::array(vec![
                protocol::bulk_string(key),
                encode_entries(&entries),
            ]));
        }

        Ok(vec![protocol::array(streams)])
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
    use crate::commands::create_test_storage;
    use crate::commands::stream::xadd;
    use crate::protocol;

    fn xread_cmd(parts: &[&str]) -> XRead {
        let elements = parts.iter().map(|p| protocol::bulk_string(p)).collect();
        XRead { message: protocol::array(elements) }
    }

    #[test]
    fn test_xread_is_exclusive() -> anyhow::Result<()> {
        let storage = create_test_storage();
        xadd(&["XADD", "some_key", "1-0", "temperature", "36"]).execute(&storage)?;
        xadd(&["XADD", "some_key", "2-0", "temperature", "37"]).execute(&storage)?;
        xadd(&["XADD", "some_key", "3-0", "temperature", "38"]).execute(&storage)?;

        // Reading after 1-0 must skip 1-0 itself and return only 2-0.
        let result = xread_cmd(&["XREAD", "STREAMS", "some_key", "1-0"]).execute(&storage)?;

        let expected = protocol::array(vec![
            protocol::array(vec![
                protocol::bulk_string("some_key"),
                protocol::array(vec![
                    protocol::array(vec![
                        protocol::bulk_string("2-0"),
                        protocol::array(vec![
                            protocol::bulk_string("temperature"),
                            protocol::bulk_string("37"),
                        ]),
                    ]),
                    protocol::array(vec![
                        protocol::bulk_string("3-0"),
                        protocol::array(vec![
                            protocol::bulk_string("temperature"),
                            protocol::bulk_string("38"),
                        ]),
                ])])])]);
        assert_eq!(result, vec![expected]);
        Ok(())
    }

    #[test]
    fn test_xread_multiple_streams() -> anyhow::Result<()> {
        let storage = create_test_storage();
        xadd(&["XADD", "stream_key", "0-1", "temperature", "95"]).execute(&storage)?;
        xadd(&["XADD", "other_stream_key", "0-2", "humidity", "97"]).execute(&storage)?;

        let result = xread_cmd(&[
            "XREAD", "STREAMS", "stream_key", "other_stream_key", "0-0", "0-1",
        ])
        .execute(&storage)?;

        let expected = protocol::array(vec![
            protocol::array(vec![
                protocol::bulk_string("stream_key"),
                protocol::array(vec![protocol::array(vec![
                    protocol::bulk_string("0-1"),
                    protocol::array(vec![
                        protocol::bulk_string("temperature"),
                        protocol::bulk_string("95"),
                    ]),
                ])]),
            ]),
            protocol::array(vec![
                protocol::bulk_string("other_stream_key"),
                protocol::array(vec![protocol::array(vec![
                    protocol::bulk_string("0-2"),
                    protocol::array(vec![
                        protocol::bulk_string("humidity"),
                        protocol::bulk_string("97"),
                    ]),
                ])]),
            ]),
        ]);
        assert_eq!(result, vec![expected]);
        Ok(())
    }

    #[test]
    fn test_xread_missing_key_yields_empty_entries() -> anyhow::Result<()> {
        let storage = create_test_storage();

        let result = xread_cmd(&["XREAD", "STREAMS", "missing", "0-0"]).execute(&storage)?;

        let expected = protocol::array(vec![protocol::array(vec![
            protocol::bulk_string("missing"),
            protocol::array(vec![]),
        ])]);
        assert_eq!(result, vec![expected]);
        Ok(())
    }

    #[test]
    fn test_xread_wrong_arity_is_error() {
        let storage = create_test_storage();
        assert!(xread_cmd(&["XREAD", "STREAMS", "k"]).execute(&storage).is_err());
        assert!(xread_cmd(&["XREAD", "k", "0-0"]).execute(&storage).is_err());
    }
}
