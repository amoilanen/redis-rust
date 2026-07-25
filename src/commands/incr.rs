/// INCR command - increments the integer value stored at a key by one.
///
/// Syntax: INCR <key>
/// Returns: the value after the increment, as a RESP integer (`:N\r\n`)
///
/// Only keys that already hold a numeric string are supported so far; creating
/// a missing key and rejecting non-numeric values come in later stages.

use std::sync::{Arc, Mutex};
use log::*;
use crate::protocol;
use crate::protocol::DataType;
use crate::storage::Storage;
use crate::error::RedisError;
use super::RedisCommand;

/// INCR command implementation.
pub struct Incr {
    pub message: DataType,
}

impl RedisCommand for Incr {
    fn execute(&self, storage: &Arc<Mutex<Storage>>) -> Result<Vec<DataType>, anyhow::Error> {
        let instructions: Vec<String> = self.message.as_vec()?;
        let error = RedisError {
            message: format!("Invalid INCR command syntax: '{}'", instructions.join(" ")).to_string(),
        };

        if instructions.len() != 2 {
            return Err(error.clone().into());
        }
        let key = instructions.get(1).ok_or::<anyhow::Error>(error.clone().into())?;

        debug!("INCR {}", key);

        let mut data = storage
            .lock()
            .map_err(|e| anyhow::anyhow!("Failed to lock storage: {}", e))?;

        let incremented = data.incr(key)?;

        Ok(vec![protocol::integer(incremented)])
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
    use std::thread;
    use std::time::Duration;
    use crate::commands::{command_message, create_test_storage, set};

    fn incr(key: &str) -> Incr {
        Incr { message: command_message(&["INCR", key]) }
    }

    /// Absolute expiry deadline (ms since epoch) currently recorded for `key`,
    /// or `None` when the key has no TTL. Panics if the key is missing.
    fn expires_at_ms(storage: &Arc<Mutex<Storage>>, key: &str) -> Option<u64> {
        storage
            .lock()
            .unwrap()
            .data
            .get(key)
            .unwrap_or_else(|| panic!("key '{}' not found in storage", key))
            .expires_at_ms()
    }

    #[test]
    fn test_incr_existing_numeric_value() -> anyhow::Result<()> {
        let storage = create_test_storage();
        set(&["SET", "foo", "41"]).execute(&storage)?;

        let result = incr("foo").execute(&storage)?;

        assert_eq!(result, vec![protocol::integer(42)]);
        Ok(())
    }

    #[test]
    fn test_incr_is_repeatable_and_persists() -> anyhow::Result<()> {
        let storage = create_test_storage();
        set(&["SET", "counter", "5"]).execute(&storage)?;

        assert_eq!(incr("counter").execute(&storage)?, vec![protocol::integer(6)]);
        assert_eq!(incr("counter").execute(&storage)?, vec![protocol::integer(7)]);

        // The stored value is rewritten, so GET observes the new number.
        let stored = storage.lock().unwrap().get("counter")?;
        assert_eq!(stored, Some(b"7".to_vec()));
        Ok(())
    }

    #[test]
    fn test_incr_of_negative_value() -> anyhow::Result<()> {
        let storage = create_test_storage();
        set(&["SET", "neg", "-3"]).execute(&storage)?;

        assert_eq!(incr("neg").execute(&storage)?, vec![protocol::integer(-2)]);
        Ok(())
    }

    #[test]
    fn test_incr_keeps_the_original_expiry_deadline() -> anyhow::Result<()> {
        // The rewritten value must inherit the *exact* deadline of the value it
        // replaces. Two ways to get this wrong, both caught here: dropping the
        // TTL (deadline becomes None) and restarting it (deadline shifts later
        // by however long elapsed since the SET - hence the sleep, which makes
        // a restart observably different from a preserved deadline).
        let storage = create_test_storage();
        set(&["SET", "volatile", "1", "px", "10000"]).execute(&storage)?;
        let deadline_before = expires_at_ms(&storage, "volatile");
        assert!(deadline_before.is_some(), "SET PX should record a deadline");

        thread::sleep(Duration::from_millis(20));
        assert_eq!(incr("volatile").execute(&storage)?, vec![protocol::integer(2)]);

        assert_eq!(expires_at_ms(&storage, "volatile"), deadline_before);
        assert_eq!(storage.lock().unwrap().get("volatile")?, Some(b"2".to_vec()));
        Ok(())
    }

    #[test]
    fn test_incremented_key_still_expires_on_the_original_schedule() -> anyhow::Result<()> {
        // Behavioural counterpart to the deadline assertion above: a key
        // incremented halfway through its TTL must still disappear at the
        // original deadline, not 100ms after the INCR.
        let storage = create_test_storage();
        set(&["SET", "volatile", "1", "px", "100"]).execute(&storage)?;

        thread::sleep(Duration::from_millis(60));
        assert_eq!(incr("volatile").execute(&storage)?, vec![protocol::integer(2)]);
        // Still inside the original window: the key is alive with its new value.
        assert_eq!(storage.lock().unwrap().get("volatile")?, Some(b"2".to_vec()));

        // Past the original deadline (~120ms) but well before a restarted one
        // would fire (~160ms), so a reset TTL fails here.
        thread::sleep(Duration::from_millis(60));
        assert_eq!(storage.lock().unwrap().get("volatile")?, None);
        Ok(())
    }

    #[test]
    fn test_incr_of_expired_key_does_not_resurrect_it() -> anyhow::Result<()> {
        // An expired key is logically absent, so it must not be incremented in
        // place - creating it fresh is the (not yet implemented) missing-key path.
        let storage = create_test_storage();
        set(&["SET", "gone", "41", "px", "10"]).execute(&storage)?;
        thread::sleep(Duration::from_millis(30));

        let result = incr("gone").execute(&storage);

        assert!(result.unwrap_err().downcast::<RedisError>().is_ok());
        assert_eq!(storage.lock().unwrap().get("gone")?, None);
        Ok(())
    }

    #[test]
    fn test_incr_overflow_is_client_error() -> anyhow::Result<()> {
        let storage = create_test_storage();
        set(&["SET", "big", &i64::MAX.to_string()]).execute(&storage)?;

        let result = incr("big").execute(&storage);

        assert!(result.unwrap_err().downcast::<RedisError>().is_ok());
        // The value is left untouched by the rejected increment.
        assert_eq!(
            storage.lock().unwrap().get("big")?,
            Some(i64::MAX.to_string().into_bytes())
        );
        Ok(())
    }

    #[test]
    fn test_incr_invalid_syntax() {
        let storage = create_test_storage();

        let no_key = Incr { message: command_message(&["INCR"]) };
        assert!(no_key.execute(&storage).is_err());

        let extra_argument = Incr { message: command_message(&["INCR", "key", "extra"]) };
        assert!(extra_argument.execute(&storage).is_err());
    }
}
