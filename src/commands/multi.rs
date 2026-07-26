/// MULTI command - starts a transaction.
///
/// Syntax: MULTI
/// Returns: +OK
///
/// In real Redis, MULTI switches the connection into a queueing mode where
/// subsequent commands reply `+QUEUED` until `EXEC`/`DISCARD`. This stage only
/// acknowledges the command; queueing and `EXEC` come in later stages.

use std::sync::{Arc, Mutex};
use log::*;
use crate::protocol;
use crate::protocol::DataType;
use crate::storage::Storage;
use crate::error::RedisError;
use super::RedisCommand;

/// MULTI command implementation.
pub struct Multi {
    pub message: DataType,
}

impl RedisCommand for Multi {
    fn execute(&self, _: &Arc<Mutex<Storage>>) -> Result<Vec<DataType>, anyhow::Error> {
        let instructions: Vec<String> = self.message.as_vec()?;

        // MULTI takes no arguments; Redis rejects extras rather than ignoring them.
        if instructions.len() != 1 {
            return Err(RedisError {
                message: "ERR wrong number of arguments for 'multi' command".to_string(),
            }
            .into());
        }

        debug!("MULTI");

        Ok(vec![protocol::simple_string("OK")])
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
    use crate::commands::{command_message, create_test_storage};

    fn multi(parts: &[&str]) -> Multi {
        Multi { message: command_message(parts) }
    }

    #[test]
    fn test_multi_replies_ok() -> anyhow::Result<()> {
        let storage = create_test_storage();

        let result = multi(&["MULTI"]).execute(&storage)?;

        assert_eq!(result, vec![protocol::simple_string("OK")]);
        Ok(())
    }

    #[test]
    fn test_multi_is_not_propagated_to_replicas() {
        // Starting a transaction is connection-local state, so nothing about it
        // belongs on the replication stream.
        let cmd = multi(&["MULTI"]);

        assert!(!cmd.is_propagated_to_replicas());
        assert!(!cmd.should_always_reply());
    }

    #[test]
    fn test_multi_rejects_arguments() -> anyhow::Result<()> {
        let storage = create_test_storage();

        let error = multi(&["MULTI", "extra"]).execute(&storage).unwrap_err();

        assert_eq!(
            error
                .downcast::<RedisError>()
                .expect("failure should be a client-facing RedisError")
                .message,
            "ERR wrong number of arguments for 'multi' command"
        );
        Ok(())
    }
}
