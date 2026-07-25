/// XREAD [BLOCK <ms>] STREAMS <key>... <id>...
///
/// Returns, per stream, the entries whose IDs are strictly greater than the
/// matching `id`. Unlike XRANGE, XREAD is exclusive and takes a single ID per
/// stream.
///
/// With the optional `BLOCK <ms>` argument the command waits for new data when
/// none is available: it parks until an XADD on one of the requested streams
/// wakes it, then re-reads and replies. `BLOCK 0` waits indefinitely; a
/// non-zero timeout replies with a RESP null array (`*-1\r\n`) once it elapses.
///
/// The reply is a RESP array of streams. Each stream is a two-element array of
/// the stream key (bulk string) and an array of its matching entries, where
/// each entry is encoded as `[id, [field, value, ...]]` (see
/// [`super::encode_entries`]).

use std::sync::mpsc::RecvTimeoutError;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use anyhow::anyhow;
use log::*;

use crate::blocking::BlockingNotifier;
use crate::commands::RedisCommand;
use crate::commands::stream::encode_entries;
use crate::error::RedisError;
use crate::protocol;
use crate::protocol::DataType;
use crate::storage::Storage;
use crate::stream::{StreamEntry, StreamId};

/// XREAD command implementation.
pub struct XRead {
    pub message: DataType,
    pub notifier: Arc<BlockingNotifier>,
}

impl RedisCommand for XRead {
    fn execute(&self, storage: &Arc<Mutex<Storage>>) -> Result<Vec<DataType>, anyhow::Error> {
        let instructions: Vec<String> = self.message.as_vec()?;
        let error = RedisError {
            message: format!("ERR cannot parse 'xread' command: {}", self.message.as_string()?),
        };

        // Consume optional leading options (only BLOCK is supported) until the
        // STREAMS keyword, after which come N keys followed by their N IDs.
        let mut index = 1;
        let mut block: Option<Duration> = None;
        loop {
            match instructions.get(index) {
                Some(token) if token.eq_ignore_ascii_case("BLOCK") => {
                    let millis: u64 = instructions
                        .get(index + 1)
                        .ok_or_else(|| error.clone())?
                        .parse()
                        .map_err(|_| error.clone())?;
                    block = Some(Duration::from_millis(millis));
                    index += 2;
                }
                Some(token) if token.eq_ignore_ascii_case("STREAMS") => {
                    index += 1;
                    break;
                }
                _ => return Err(error.into()),
            }
        }

        let args = &instructions[index..];
        if args.is_empty() || args.len() % 2 != 0 {
            return Err(error.into());
        }
        let (keys, ids) = args.split_at(args.len() / 2);
        let keys = keys.to_vec();

        // Resolve each exclusive lower-bound ID once up front, before any read,
        // so that `$` means "entries added after this command was issued".
        let afters = resolve_afters(storage, &keys, ids)?;

        // Non-blocking XREAD echoes every requested stream, including those with
        // no new entries (as an empty array).
        let Some(block) = block else {
            let (_guard, results) = read_locked(storage, &keys, &afters)?;
            return Ok(vec![encode_streams(&keys, &results, false)]);
        };

        // Blocking XREAD: `BLOCK 0` waits forever, otherwise honour the deadline.
        let deadline = if block.is_zero() {
            None
        } else {
            Some(Instant::now() + block)
        };

        loop {
            // Read and, if empty, register as a waiter — both under the storage
            // lock so a concurrent XADD can't append between the read and the
            // registration (which would otherwise be a lost wake-up).
            let receiver = {
                let (_guard, results) = read_locked(storage, &keys, &afters)?;
                if results.iter().any(|entries| !entries.is_empty()) {
                    return Ok(vec![encode_streams(&keys, &results, true)]);
                }
                // `_guard` is still held here, so the registration is atomic with
                // the read above — a concurrent XADD can't slip in between.
                self.notifier.register_streams(&keys)?
            };

            match deadline {
                None => receiver
                    .recv()
                    .map_err(|e| anyhow!("XREAD wait failed: {}", e))?,
                Some(deadline) => {
                    let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                        return Ok(vec![protocol::null_array()]);
                    };
                    match receiver.recv_timeout(remaining) {
                        Ok(()) => {}
                        Err(RecvTimeoutError::Timeout) => return Ok(vec![protocol::null_array()]),
                        Err(RecvTimeoutError::Disconnected) => {
                            return Err(anyhow!("XREAD wait failed: disconnected"));
                        }
                    }
                }
            }
        }
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

/// Resolves the requested IDs into exclusive lower bounds, one per key.
///
/// `$` stands for the stream's current last ID, so the read only yields entries
/// added afterwards; an unknown or empty stream resolves to `0-0`. All `$`s are
/// resolved under a single storage lock, giving every stream in the request the
/// same point-in-time snapshot.
fn resolve_afters(
    storage: &Arc<Mutex<Storage>>,
    keys: &[String],
    ids: &[String],
) -> Result<Vec<StreamId>, anyhow::Error> {
    if !ids.iter().any(|id| id == "$") {
        return Ok(ids
            .iter()
            .map(|id| StreamId::parse_range(id, 0))
            .collect::<Result<Vec<StreamId>, RedisError>>()?);
    }

    let guard = storage
        .lock()
        .map_err(|e| anyhow!("Failed to lock storage: {}", e))?;
    keys.iter()
        .zip(ids)
        .map(|(key, id)| {
            if id == "$" {
                Ok(guard.stream_last_id(key).unwrap_or(StreamId::ZERO))
            } else {
                Ok(StreamId::parse_range(id, 0)?)
            }
        })
        .collect()
}

/// Locks storage and reads every requested stream after its lower-bound ID.
///
/// Returns the guard alongside the results so a blocking caller can keep the
/// lock held while registering as a waiter — making the read-and-register
/// atomic against a concurrent XADD. A non-blocking caller simply drops it.
fn read_locked<'a>(
    storage: &'a Arc<Mutex<Storage>>,
    keys: &[String],
    afters: &[StreamId],
) -> Result<(MutexGuard<'a, Storage>, Vec<Vec<StreamEntry>>), anyhow::Error> {
    let guard = storage
        .lock()
        .map_err(|e| anyhow!("Failed to lock storage: {}", e))?;
    let results = keys
        .iter()
        .zip(afters)
        .map(|(key, after)| {
            debug!("XREAD STREAMS {} {}", key, after);
            guard.xread(key, *after)
        })
        .collect();
    Ok((guard, results))
}

/// Encodes per-stream results as a RESP array of `[key, [entry, ...]]` pairs.
///
/// When `skip_empty` is set, streams with no matching entries are omitted —
/// the shape used for a blocking reply, which only carries streams that woke
/// it. Otherwise every stream is included (possibly with an empty entry list),
/// matching the synchronous XREAD reply.
fn encode_streams(keys: &[String], results: &[Vec<StreamEntry>], skip_empty: bool) -> DataType {
    let mut streams = Vec::with_capacity(keys.len());
    for (key, entries) in keys.iter().zip(results) {
        if skip_empty && entries.is_empty() {
            continue;
        }
        streams.push(protocol::array(vec![
            protocol::bulk_string(key),
            encode_entries(entries),
        ]));
    }
    protocol::array(streams)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::{command_message, create_test_notifier, create_test_storage};
    use crate::commands::stream::{XAdd, xadd};
    use crate::protocol;
    use std::thread;

    fn xread_cmd(parts: &[&str]) -> XRead {
        xread_with(parts, &create_test_notifier())
    }

    fn xread_with(parts: &[&str], notifier: &Arc<BlockingNotifier>) -> XRead {
        XRead {
            message: command_message(parts),
            notifier: Arc::clone(notifier),
        }
    }

    /// Build an XADD that shares `notifier`, so it wakes blocked readers.
    fn xadd_with(parts: &[&str], notifier: &Arc<BlockingNotifier>) -> XAdd {
        XAdd {
            message: command_message(parts),
            notifier: Arc::clone(notifier),
        }
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

    #[test]
    fn test_xread_block_returns_immediately_when_data_available() -> anyhow::Result<()> {
        let storage = create_test_storage();
        xadd(&["XADD", "stream_key", "0-1", "temperature", "96"]).execute(&storage)?;

        // Data already present: a generous BLOCK must not actually block.
        let result =
            xread_cmd(&["XREAD", "BLOCK", "10000", "STREAMS", "stream_key", "0-0"]).execute(&storage)?;

        let expected = protocol::array(vec![protocol::array(vec![
            protocol::bulk_string("stream_key"),
            protocol::array(vec![protocol::array(vec![
                protocol::bulk_string("0-1"),
                protocol::array(vec![
                    protocol::bulk_string("temperature"),
                    protocol::bulk_string("96"),
                ]),
            ])]),
        ])]);
        assert_eq!(result, vec![expected]);
        Ok(())
    }

    #[test]
    fn test_xread_block_times_out_to_null_array() -> anyhow::Result<()> {
        let storage = create_test_storage();
        xadd(&["XADD", "stream_key", "0-1", "temperature", "96"]).execute(&storage)?;

        // Nothing newer than 0-1 arrives before the timeout elapses.
        let result =
            xread_cmd(&["XREAD", "BLOCK", "50", "STREAMS", "stream_key", "0-1"]).execute(&storage)?;
        assert_eq!(result, vec![protocol::null_array()]);
        Ok(())
    }

    #[test]
    fn test_xread_block_wakes_on_concurrent_xadd() -> anyhow::Result<()> {
        let storage = create_test_storage();
        let notifier = create_test_notifier();
        xadd(&["XADD", "stream_key", "0-1", "temperature", "96"]).execute(&storage)?;

        let storage_for_waiter = Arc::clone(&storage);
        let notifier_for_waiter = Arc::clone(&notifier);
        // Generous timeout so the XADD always wins under scheduling jitter.
        let waiter = thread::spawn(move || {
            xread_with(
                &["XREAD", "BLOCK", "10000", "STREAMS", "stream_key", "0-1"],
                &notifier_for_waiter,
            )
            .execute(&storage_for_waiter)
            .expect("XREAD failed")
        });

        // Give the reader time to park before appending the entry that wakes it.
        thread::sleep(Duration::from_millis(100));
        xadd_with(&["XADD", "stream_key", "0-2", "temperature", "95"], &notifier)
            .execute(&storage)?;

        let result = waiter.join().expect("waiter panicked");
        let expected = protocol::array(vec![protocol::array(vec![
            protocol::bulk_string("stream_key"),
            protocol::array(vec![protocol::array(vec![
                protocol::bulk_string("0-2"),
                protocol::array(vec![
                    protocol::bulk_string("temperature"),
                    protocol::bulk_string("95"),
                ]),
            ])]),
        ])]);
        assert_eq!(result, vec![expected]);
        Ok(())
    }

    #[test]
    fn test_xread_block_dollar_wakes_on_concurrent_xadd() -> anyhow::Result<()> {
        let storage = create_test_storage();
        let notifier = create_test_notifier();
        xadd(&["XADD", "stream_key", "0-1", "temperature", "96"]).execute(&storage)?;

        let storage_for_waiter = Arc::clone(&storage);
        let notifier_for_waiter = Arc::clone(&notifier);
        // `$` resolves to 0-1, the last ID at the time the command is issued.
        let waiter = thread::spawn(move || {
            xread_with(
                &["XREAD", "BLOCK", "0", "STREAMS", "stream_key", "$"],
                &notifier_for_waiter,
            )
            .execute(&storage_for_waiter)
            .expect("XREAD failed")
        });

        // Give the reader time to park before appending the entry that wakes it.
        thread::sleep(Duration::from_millis(100));
        xadd_with(&["XADD", "stream_key", "0-2", "temperature", "95"], &notifier)
            .execute(&storage)?;

        // Only the entry added after the command was issued comes back — not 0-1.
        let result = waiter.join().expect("waiter panicked");
        let expected = protocol::array(vec![protocol::array(vec![
            protocol::bulk_string("stream_key"),
            protocol::array(vec![protocol::array(vec![
                protocol::bulk_string("0-2"),
                protocol::array(vec![
                    protocol::bulk_string("temperature"),
                    protocol::bulk_string("95"),
                ]),
            ])]),
        ])]);
        assert_eq!(result, vec![expected]);
        Ok(())
    }

    #[test]
    fn test_xread_block_dollar_times_out_to_null_array() -> anyhow::Result<()> {
        let storage = create_test_storage();
        xadd(&["XADD", "stream_key", "0-1", "temperature", "96"]).execute(&storage)?;

        // Nothing is added after the command, so the existing 0-1 must not match.
        let result =
            xread_cmd(&["XREAD", "BLOCK", "50", "STREAMS", "stream_key", "$"]).execute(&storage)?;
        assert_eq!(result, vec![protocol::null_array()]);
        Ok(())
    }

    #[test]
    fn test_resolve_afters_maps_dollar_to_current_last_id() -> anyhow::Result<()> {
        let storage = create_test_storage();
        xadd(&["XADD", "stream_key", "0-1", "temperature", "96"]).execute(&storage)?;
        xadd(&["XADD", "stream_key", "5-7", "temperature", "97"]).execute(&storage)?;

        let keys = vec![
            "stream_key".to_string(),
            "missing".to_string(),
            "stream_key".to_string(),
        ];
        let ids = vec!["$".to_string(), "$".to_string(), "0-1".to_string()];

        let afters = resolve_afters(&storage, &keys, &ids)?;

        // `$` is the stream's newest ID (not its first, and not a maximal
        // sentinel); an unknown stream has no last ID, so it falls back to 0-0;
        // an explicit ID alongside a `$` is still parsed literally.
        assert_eq!(
            afters,
            vec![StreamId::new(5, 7), StreamId::ZERO, StreamId::new(0, 1)]
        );
        Ok(())
    }

    #[test]
    fn test_xread_block_dollar_on_missing_stream_wakes_on_first_entry() -> anyhow::Result<()> {
        let storage = create_test_storage();
        let notifier = create_test_notifier();

        let storage_for_waiter = Arc::clone(&storage);
        let notifier_for_waiter = Arc::clone(&notifier);
        // The stream does not exist yet, so `$` must fall back to 0-0. A maximal
        // fallback would park this reader forever and time the test out instead.
        let waiter = thread::spawn(move || {
            xread_with(
                &["XREAD", "BLOCK", "10000", "STREAMS", "brand_new", "$"],
                &notifier_for_waiter,
            )
            .execute(&storage_for_waiter)
            .expect("XREAD failed")
        });

        // Give the reader time to park before creating the stream that wakes it.
        thread::sleep(Duration::from_millis(100));
        xadd_with(&["XADD", "brand_new", "0-1", "temperature", "96"], &notifier)
            .execute(&storage)?;

        let result = waiter.join().expect("waiter panicked");
        let expected = protocol::array(vec![protocol::array(vec![
            protocol::bulk_string("brand_new"),
            protocol::array(vec![protocol::array(vec![
                protocol::bulk_string("0-1"),
                protocol::array(vec![
                    protocol::bulk_string("temperature"),
                    protocol::bulk_string("96"),
                ]),
            ])]),
        ])]);
        assert_eq!(result, vec![expected]);
        Ok(())
    }

    #[test]
    fn test_xread_dollar_mixed_with_explicit_ids() -> anyhow::Result<()> {
        let storage = create_test_storage();
        xadd(&["XADD", "stream_key", "0-1", "temperature", "95"]).execute(&storage)?;
        xadd(&["XADD", "other_stream_key", "0-1", "humidity", "97"]).execute(&storage)?;

        // `$` skips everything in other_stream_key; 0-0 still yields stream_key's entry.
        let result = xread_cmd(&[
            "XREAD", "STREAMS", "stream_key", "other_stream_key", "0-0", "$",
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
                protocol::array(vec![]),
            ]),
        ]);
        assert_eq!(result, vec![expected]);
        Ok(())
    }
}
