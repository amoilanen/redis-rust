/// BLPOP command - blocking variant of LPOP.
///
/// Syntax: `BLPOP <key> <timeout>`. Returns `[key, element]` as a RESP
/// array once an element is available. When `timeout` is `0` the call
/// blocks forever; a non-zero `timeout` (in seconds, may be fractional)
/// bounds the wait and replies with a RESP null array (`*-1\r\n`) on
/// expiry. Multiple waiters on the same key are served in FIFO order.

use std::sync::{Arc, Mutex};
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::time::Duration;

use anyhow::{Result, anyhow};
use log::*;

use super::update_list_elements;
use crate::blocking::BlockingNotifier;
use crate::commands::RedisCommand;
use crate::error::RedisError;
use crate::protocol;
use crate::protocol::DataType;
use crate::storage::Storage;

pub struct BLPop {
    pub message: DataType,
    pub notifier: Arc<BlockingNotifier>,
}

/// Outcome of the head-pop attempt performed under the storage lock.
///
/// Returning this from the `update_list_elements` closure lets BLPOP share
/// the read-modify-write plumbing with LPOP while still deciding — atomically
/// with the read — whether to reply immediately or park as a waiter.
enum PopOutcome {
    Popped(DataType),
    Waiting(Receiver<DataType>),
}

impl RedisCommand for BLPop {
    fn execute(&self, storage: &Arc<Mutex<Storage>>) -> Result<Vec<DataType>, anyhow::Error> {
        let instructions: Vec<String> = self.message.as_vec()?;
        let error = RedisError {
            message: "Invalid BLPOP command syntax".to_string(),
        };

        if instructions.len() != 3 {
            return Err(error.clone().into());
        }
        let key = instructions[1].clone();
        let timeout_secs: f64 = instructions[2].parse().map_err(|_| error.clone())?;
        if timeout_secs < 0.0 || !timeout_secs.is_finite() {
            return Err(error.clone().into());
        }

        debug!("BLPOP {} {}", key, timeout_secs);

        let outcome = update_list_elements(&key, storage, |elements| {
            if !elements.is_empty() {
                Ok(PopOutcome::Popped(elements.remove(0)))
            } else {
                Ok(PopOutcome::Waiting(self.notifier.register(&key)?))
            }
        })?;

        let element = match outcome {
            PopOutcome::Popped(value) => Some(value),
            PopOutcome::Waiting(rx) => {
                if timeout_secs == 0.0 {
                    Some(rx.recv().map_err(|e| anyhow!("BLPOP wait failed: {}", e))?)
                } else {
                    match rx.recv_timeout(Duration::from_secs_f64(timeout_secs)) {
                        Ok(value) => Some(value),
                        // Dropping `rx` here leaves a stale `Sender` in the
                        // notifier queue; the next `handoff` will discover the
                        // dead receiver and skip it (see `BlockingNotifier`).
                        Err(RecvTimeoutError::Timeout) => None,
                        Err(RecvTimeoutError::Disconnected) => {
                            return Err(anyhow!("BLPOP wait failed: disconnected"));
                        }
                    }
                }
            }
        };

        let reply = match element {
            Some(value) => protocol::array(vec![protocol::bulk_string(&key), value]),
            None => protocol::null_array(),
        };
        Ok(vec![reply])
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
    use super::super::{create_test_notifier, create_test_storage, set_list_values};
    use crate::commands::list::RPush;
    use crate::commands::set::Set;
    use std::thread;
    use std::time::Duration;

    fn blpop(key: &str, timeout: &str, notifier: &Arc<BlockingNotifier>) -> BLPop {
        BLPop {
            message: protocol::array(vec![
                protocol::bulk_string("BLPOP"),
                protocol::bulk_string(key),
                protocol::bulk_string(timeout),
            ]),
            notifier: Arc::clone(notifier),
        }
    }

    #[test]
    fn test_blpop_immediate_when_list_non_empty() -> Result<()> {
        let storage = create_test_storage();
        let notifier = create_test_notifier();
        let key = "list_key";

        set_list_values(
            &storage,
            key,
            &[protocol::bulk_string("foo"), protocol::bulk_string("bar")],
        )?;

        let result = blpop(key, "0", &notifier).execute(&storage)?;
        assert_eq!(
            result[0],
            protocol::array(vec![
                protocol::bulk_string(key),
                protocol::bulk_string("foo"),
            ]),
        );
        Ok(())
    }

    #[test]
    fn test_blpop_blocks_until_rpush_wakes_it() -> Result<()> {
        let storage = create_test_storage();
        let notifier = create_test_notifier();
        let key = "list_key";

        let storage_for_waiter = Arc::clone(&storage);
        let notifier_for_waiter = Arc::clone(&notifier);
        let key_for_waiter = key.to_string();

        let waiter = thread::spawn(move || {
            blpop(&key_for_waiter, "0", &notifier_for_waiter)
                .execute(&storage_for_waiter)
                .expect("BLPOP failed")
        });

        // Give the waiter time to register so we exercise the blocking path.
        thread::sleep(Duration::from_millis(100));

        let push_result = RPush {
            message: protocol::array(vec![
                protocol::bulk_string("RPUSH"),
                protocol::bulk_string(key),
                protocol::bulk_string("foo"),
            ]),
            notifier: Arc::clone(&notifier),
        }
        .execute(&storage)?;
        assert_eq!(push_result[0], protocol::integer(1));

        let blpop_result = waiter.join().expect("waiter panicked");
        assert_eq!(
            blpop_result[0],
            protocol::array(vec![
                protocol::bulk_string(key),
                protocol::bulk_string("foo"),
            ]),
        );
        Ok(())
    }

    #[test]
    fn test_blpop_fifo_across_multiple_waiters() -> Result<()> {
        let storage = create_test_storage();
        let notifier = create_test_notifier();
        let key = "another_list_key";

        let s1 = Arc::clone(&storage);
        let n1 = Arc::clone(&notifier);
        let k1 = key.to_string();
        let w1 = thread::spawn(move || blpop(&k1, "0", &n1).execute(&s1).unwrap());
        thread::sleep(Duration::from_millis(100));

        let s2 = Arc::clone(&storage);
        let n2 = Arc::clone(&notifier);
        let k2 = key.to_string();
        let w2 = thread::spawn(move || blpop(&k2, "0", &n2).execute(&s2).unwrap());
        thread::sleep(Duration::from_millis(100));

        RPush {
            message: protocol::array(vec![
                protocol::bulk_string("RPUSH"),
                protocol::bulk_string(key),
                protocol::bulk_string("foo"),
            ]),
            notifier: Arc::clone(&notifier),
        }
        .execute(&storage)?;

        let r1 = w1.join().expect("waiter 1 panicked");
        assert_eq!(
            r1[0],
            protocol::array(vec![
                protocol::bulk_string(key),
                protocol::bulk_string("foo"),
            ]),
        );

        RPush {
            message: protocol::array(vec![
                protocol::bulk_string("RPUSH"),
                protocol::bulk_string(key),
                protocol::bulk_string("bar"),
            ]),
            notifier: Arc::clone(&notifier),
        }
        .execute(&storage)?;
        let r2 = w2.join().expect("waiter 2 panicked");
        assert_eq!(
            r2[0],
            protocol::array(vec![
                protocol::bulk_string(key),
                protocol::bulk_string("bar"),
            ]),
        );
        Ok(())
    }

    #[test]
    fn test_blpop_returns_null_array_on_timeout() -> Result<()> {
        let storage = create_test_storage();
        let notifier = create_test_notifier();

        let result = blpop("missing_key", "0.1", &notifier).execute(&storage)?;
        assert_eq!(result[0], protocol::null_array());
        Ok(())
    }

    #[test]
    fn test_blpop_with_nonzero_timeout_returns_value_when_pushed_in_time() -> Result<()> {
        let storage = create_test_storage();
        let notifier = create_test_notifier();
        let key = "list_key";

        let storage_for_waiter = Arc::clone(&storage);
        let notifier_for_waiter = Arc::clone(&notifier);
        let key_for_waiter = key.to_string();

        // Generous timeout so the push always wins under CI scheduling jitter.
        let waiter = thread::spawn(move || {
            blpop(&key_for_waiter, "5", &notifier_for_waiter)
                .execute(&storage_for_waiter)
                .expect("BLPOP failed")
        });

        thread::sleep(Duration::from_millis(100));

        RPush {
            message: protocol::array(vec![
                protocol::bulk_string("RPUSH"),
                protocol::bulk_string(key),
                protocol::bulk_string("foo"),
            ]),
            notifier: Arc::clone(&notifier),
        }
        .execute(&storage)?;

        let result = waiter.join().expect("waiter panicked");
        assert_eq!(
            result[0],
            protocol::array(vec![
                protocol::bulk_string(key),
                protocol::bulk_string("foo"),
            ]),
        );
        Ok(())
    }

    #[test]
    fn test_blpop_invalid_syntax() {
        let storage = create_test_storage();
        let notifier = create_test_notifier();

        let msg = protocol::array(vec![
            protocol::bulk_string("BLPOP"),
            protocol::bulk_string("k"),
        ]);
        assert!(BLPop { message: msg, notifier: Arc::clone(&notifier) }
            .execute(&storage)
            .is_err());

        assert!(blpop("k", "notanumber", &notifier).execute(&storage).is_err());
        assert!(blpop("k", "-1", &notifier).execute(&storage).is_err());
    }

    #[test]
    fn test_blpop_wrong_type_fails() -> Result<()> {
        let storage = create_test_storage();
        let notifier = create_test_notifier();

        Set {
            message: protocol::array(vec![
                protocol::bulk_string("SET"),
                protocol::bulk_string("k"),
                protocol::bulk_string("not_a_list"),
            ]),
        }
        .execute(&storage)?;

        assert!(blpop("k", "0", &notifier).execute(&storage).is_err());
        Ok(())
    }
}
