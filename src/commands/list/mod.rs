/// List command family.
///
/// This module groups every Redis command that operates on the list data type
/// (RPUSH, LPUSH, LRANGE, LLEN, ...) together with the helpers they share.
///
/// Helpers are private to this module - by Rust's default visibility rules
/// they are still accessible from descendant modules (rpush, lpush, lrange,
/// and their `#[cfg(test)] mod tests` children), but invisible to anything
/// outside the list family.

use std::sync::{Arc, Mutex};

use anyhow::{Result, anyhow};
use log::*;

use crate::blocking::BlockingNotifier;
use crate::error::RedisError;
use crate::protocol;
use crate::protocol::DataType;
use crate::storage::Storage;

pub mod rpush;
pub mod lpush;
pub mod lrange;
pub mod llen;
pub mod lpop;
pub mod blpop;

pub use rpush::RPush;
pub use lpush::LPush;
pub use lrange::LRange;
pub use llen::LLen;
pub use lpop::LPop;
pub use blpop::BLPop;

/// Read the list stored at `key` from `storage`.
///
/// Returns an empty vector if the key does not exist, and an error if the
/// stored value is not an Array (i.e. a wrong-type collision with another
/// data type).
fn get_list_elements(
    key: &str,
    storage: &Arc<Mutex<Storage>>,
) -> Result<Vec<DataType>> {
    let mut data = storage
        .lock()
        .map_err(|e| anyhow!("Failed to lock storage: {}", e))?;
    let stored_raw_value = data.get(key)?;
    let stored_value = stored_raw_value
        .map(|value| protocol::read_message_from_bytes(&value))
        .transpose()?;
    let stored_elements = match stored_value {
        Some(DataType::Array { elements }) => Ok(elements),
        None => Ok(Vec::new()),
        Some(_) => Err(anyhow!("Not an Array is stored in storage")),
    }?;
    Ok(stored_elements)
}

/// Read-modify-write a list stored at `key`.
///
/// Loads the list (creating an empty one if missing), hands a mutable
/// reference to the closure `f`, and writes the resulting list back to
/// storage. The closure's return value is propagated to the caller so the
/// closure can hand back arbitrary derived data (e.g. the elements that
/// were popped, the post-mutation length, or — in BLPOP's case — a
/// `Receiver` for a freshly registered waiter).
///
/// The storage `Mutex` is held for the entire span: read, mutate, and
/// write-back. This is what lets BLPOP register a waiter inside `f` while
/// guaranteeing a concurrent pusher either sees the unmodified list (and
/// won't try to hand off yet) or sees the registered waiter under its own
/// lock acquisition.
fn update_list_elements<F, T>(
    key: &str,
    storage: &Arc<Mutex<Storage>>,
    f: F,
) -> Result<T>
where
    F: FnOnce(&mut Vec<DataType>) -> Result<T>,
{
    let mut data = storage
        .lock()
        .map_err(|e| anyhow!("Failed to lock storage: {}", e))?;
    let stored_raw_value = data.get(key)?;
    let stored_value = stored_raw_value
        .map(|value| protocol::read_message_from_bytes(&value))
        .transpose()?;
    let mut stored_elements = match stored_value {
        Some(DataType::Array { elements }) => Ok(elements),
        None => Ok(Vec::new()),
        Some(_) => Err(anyhow!("Not an Array is stored in storage")),
    }?;
    let result = f(&mut stored_elements)?;
    data.set(key, protocol::array(stored_elements).serialize(), None)?;
    Ok(result)
}

/// Shared implementation for list-push commands (RPUSH / LPUSH).
///
/// Parses `<COMMAND> <key> <value> [value ...]`, applies `push_fn` for each
/// supplied value, then hands off head elements to any BLPOP waiters while
/// still holding the storage lock so push + wake-up are atomic. Returns
/// the post-push, pre-handoff length — `RPUSH k foo` against one waiter
/// still reports `1` even though the handoff immediately drains the list.
fn push_to_list<F>(
    message: &DataType,
    storage: &Arc<Mutex<Storage>>,
    notifier: &Arc<BlockingNotifier>,
    command_name: &str,
    push_fn: F,
) -> Result<Vec<DataType>>
where
    F: Fn(&mut Vec<DataType>, &str),
{
    let instructions: Vec<String> = message.as_vec()?;
    let error = RedisError {
        message: format!("Invalid {} command syntax", command_name),
    };

    let key = instructions.get(1).ok_or::<anyhow::Error>(error.clone().into())?;
    if instructions.len() < 3 {
        return Err(error.clone().into());
    }
    let values = &instructions[2..];

    debug!("{} {} {:?}", command_name, key, values);

    let len_after_push = update_list_elements(key, storage, |elements| {
        for value in values {
            push_fn(elements, value);
        }
        let len = elements.len() as i64;
        notifier.handoff(key, elements)?;
        Ok(len)
    })?;
    Ok(vec![protocol::integer(len_after_push)])
}

// ---------------------------------------------------------------------------
// Test-only helpers shared across the list-family unit tests.
//
// Gated with `#[cfg(test)]` so they are stripped from release builds entirely.
// ---------------------------------------------------------------------------

/// Build a fresh, empty `Storage` wrapped in an `Arc<Mutex<...>>` for unit tests.
#[cfg(test)]
fn create_test_storage() -> Arc<Mutex<Storage>> {
    use std::collections::HashMap;
    Arc::new(Mutex::new(Storage::new(HashMap::new())))
}

#[cfg(test)]
fn create_test_notifier() -> Arc<BlockingNotifier> {
    Arc::new(BlockingNotifier::new())
}

/// Seed `storage` with a list at `key` containing the given `elements`.
///
/// Writes the values as a RESP-serialized Array so that `get_list_elements`
/// and the public command implementations can read it back.
#[cfg(test)]
fn set_list_values(
    storage: &Arc<Mutex<Storage>>,
    key: &str,
    elements: &[DataType],
) -> Result<()> {
    storage
        .lock()
        .map_err(|e| anyhow!("Failed to lock storage: {}", e))?
        .set(key, protocol::array(elements.to_vec()).serialize(), None)?;
    Ok(())
}

/// Read the list stored at `key` back as a `Vec<String>`.
///
/// Returns an error if the key is missing or the stored value isn't an Array.
#[cfg(test)]
fn read_list(
    storage: &Arc<Mutex<Storage>>,
    key: &str,
) -> Result<Vec<String>> {
    let raw = storage
        .lock()
        .map_err(|e| anyhow!("Failed to lock storage: {}", e))?
        .get(key)?
        .ok_or_else(|| anyhow!("key '{}' not found in storage", key))?;
    match protocol::read_message_from_bytes(&raw)? {
        DataType::Array { elements } => elements
            .iter()
            .map(|e| e.as_string())
            .collect(),
        other => Err(anyhow!("Expected stored value to be an Array, got {:?}", other)),
    }
}
