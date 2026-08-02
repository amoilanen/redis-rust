/// EXEC command - runs the transaction opened by MULTI and ends it.
///
/// Syntax: EXEC
/// Returns: an array of the queued commands' replies, or
///          `-ERR EXEC without MULTI` when no transaction is open.
///
/// Queueing is a later stage, so the transaction is always empty and the array
/// is always `*0\r\n` - which Redis treats as a successful run of a transaction
/// that had nothing in it, not as an error.

use std::sync::{Arc, Mutex};

use log::*;

use super::{expect_no_arguments, TransactionSlot};
use crate::commands::RedisCommand;
use crate::error::RedisError;
use crate::protocol::{self, DataType};
use crate::storage::Storage;

/// EXEC command implementation.
pub struct Exec {
    pub message: DataType,
    /// Transaction slot of the connection this EXEC arrived on.
    pub transaction: Arc<TransactionSlot>,
}

impl RedisCommand for Exec {
    fn execute(&self, _: &Arc<Mutex<Storage>>) -> Result<Vec<DataType>, anyhow::Error> {
        expect_no_arguments(&self.message, "exec")?;

        let Some(_transaction) = self.transaction.take()? else {
            debug!("EXEC without MULTI");
            return Err(RedisError {
                message: "ERR EXEC without MULTI".to_string(),
            }
            .into());
        };

        debug!("EXEC: ran an empty transaction");

        Ok(vec![protocol::array(vec![])])
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
    use crate::commands::{client_error_message, command_message, create_test_storage};

    fn exec(parts: &[&str], transaction: &Arc<TransactionSlot>) -> Exec {
        Exec {
            message: command_message(parts),
            transaction: Arc::clone(transaction),
        }
    }

    /// A connection that has not sent MULTI.
    fn no_transaction() -> Arc<TransactionSlot> {
        Arc::new(TransactionSlot::new())
    }

    /// A connection that has sent MULTI.
    fn open_transaction() -> anyhow::Result<Arc<TransactionSlot>> {
        let slot = no_transaction();
        slot.open()?;
        Ok(slot)
    }

    #[test]
    fn test_exec_without_multi_is_a_client_error() {
        let storage = create_test_storage();

        let error = exec(&["EXEC"], &no_transaction()).execute(&storage).unwrap_err();

        assert_eq!(client_error_message(error), "ERR EXEC without MULTI");
    }

    #[test]
    fn test_exec_of_an_empty_transaction_replies_with_an_empty_array() -> anyhow::Result<()> {
        let storage = create_test_storage();

        let result = exec(&["EXEC"], &open_transaction()?).execute(&storage)?;

        assert_eq!(result, vec![protocol::array(vec![])]);
        assert_eq!(result[0].serialize(), b"*0\r\n");
        Ok(())
    }

    #[test]
    fn test_exec_ends_the_transaction() -> anyhow::Result<()> {
        let storage = create_test_storage();
        let transaction = open_transaction()?;

        exec(&["EXEC"], &transaction).execute(&storage)?;
        let error = exec(&["EXEC"], &transaction).execute(&storage).unwrap_err();

        assert_eq!(client_error_message(error), "ERR EXEC without MULTI");
        Ok(())
    }

    #[test]
    fn test_exec_only_sees_its_own_connection() -> anyhow::Result<()> {
        let storage = create_test_storage();
        let elsewhere = open_transaction()?;

        let error = exec(&["EXEC"], &no_transaction()).execute(&storage).unwrap_err();

        assert_eq!(client_error_message(error), "ERR EXEC without MULTI");
        assert!(elsewhere.take()?.is_some(), "the other connection lost its transaction");
        Ok(())
    }

    #[test]
    fn test_exec_rejects_arguments() {
        let storage = create_test_storage();

        let error = exec(&["EXEC", "extra"], &no_transaction()).execute(&storage).unwrap_err();

        assert_eq!(
            client_error_message(error),
            "ERR wrong number of arguments for 'exec' command"
        );
    }

    #[test]
    fn test_rejected_exec_leaves_an_open_transaction_alone() -> anyhow::Result<()> {
        // Arity is checked before the slot is touched, so a malformed EXEC must
        // not consume a transaction a valid one could still run.
        let storage = create_test_storage();
        let transaction = open_transaction()?;

        assert!(exec(&["EXEC", "extra"], &transaction).execute(&storage).is_err());

        let result = exec(&["EXEC"], &transaction).execute(&storage)?;
        assert_eq!(result, vec![protocol::array(vec![])]);
        Ok(())
    }

    #[test]
    fn test_exec_is_not_propagated_to_replicas() {
        let cmd = exec(&["EXEC"], &no_transaction());

        assert!(!cmd.is_propagated_to_replicas());
        assert!(!cmd.should_always_reply());
    }
}
