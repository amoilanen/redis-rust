/// List command family.
///
/// This module groups every Redis command that operates on the list data type
/// (RPUSH, LPUSH, LRANGE, ...) together with the helpers they share.
///
/// Helpers are private to this module - by Rust's default visibility rules
/// they are still accessible from descendant modules (rpush, lpush, lrange,
/// and their `#[cfg(test)] mod tests` children), but invisible to anything
/// outside the list family.

use std::sync::{Arc, Mutex};
use log::*;
use anyhow::anyhow;

use crate::protocol;
use crate::protocol::DataType;
use crate::storage::Storage;
use crate::error::RedisError;

pub mod rpush;
pub mod lpush;
pub mod lrange;

pub use rpush::RPush;
pub use lpush::LPush;
pub use lrange::LRange;

/// Read the list stored at `key` from `storage`.
///
/// Returns an empty vector if the key does not exist, and an error if the
/// stored value is not an Array (i.e. a wrong-type collision with another
/// data type).
fn get_list_elements(
    key: &str,
    storage: &Arc<Mutex<Storage>>,
) -> Result<Vec<DataType>, anyhow::Error> {
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
/// storage. Returns the updated elements so callers can compute a length
/// or any other derived value.
fn update_list_elements<F>(
    key: &str,
    storage: &Arc<Mutex<Storage>>,
    f: F,
) -> Result<Vec<DataType>, anyhow::Error>
where
    F: FnOnce(&mut Vec<DataType>) -> Result<(), anyhow::Error>,
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
    f(&mut stored_elements)?;
    data.set(key, protocol::array(stored_elements.clone()).serialize(), None)?;
    Ok(stored_elements)
}

/// Shared implementation for list-push commands (RPUSH / LPUSH).
///
/// Parses `<COMMAND> <key> <value> [value ...]` from `message`, then for each
/// supplied value invokes `push_fn(elements, value)` to mutate the stored list.
/// Returns the new length of the list as a single RESP integer.
///
/// `command_name` is only used for error messages and debug logs.
fn push_to_list<F>(
    message: &DataType,
    storage: &Arc<Mutex<Storage>>,
    command_name: &str,
    push_fn: F,
) -> Result<Vec<DataType>, anyhow::Error>
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

    let updated_elements = update_list_elements(key, storage, |elements| {
        for value in values {
            push_fn(elements, value);
        }
        Ok(())
    })?;
    Ok(vec![protocol::integer(updated_elements.len() as i64)])
}

// ---------------------------------------------------------------------------
// Test-only helpers shared across the list-family unit tests.
//
// Gated with `#[cfg(test)]` so they are stripped from release builds entirely.
// ---------------------------------------------------------------------------

/// Read the list stored at `key` back as a `Vec<String>`.
///
/// Returns an error if the key is missing or the stored value isn't an Array.
#[cfg(test)]
fn read_list(
    storage: &Arc<Mutex<Storage>>,
    key: &str,
) -> anyhow::Result<Vec<String>> {
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
