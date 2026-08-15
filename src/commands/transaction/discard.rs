/// DISCARD command - discards the active transaction opened by MULTI and ends it.
///
/// Syntax: DISCARD
/// Returns: OK, if there is an active transaction, error if there is no active transaction
///          `-ERR EXEC without MULTI` when no transaction is open.
///
/// Running the queued commands is a later stage: for now EXEC ends the
/// transaction and discards them, always replying `*0\r\n` - which Redis treats
/// as a successful run of a transaction that had nothing in it, not as an error.

use std::sync::{Arc, Mutex};

use log::*;

use super::{expect_no_arguments, TransactionSlot};
use crate::commands::RedisCommand;
use crate::error::RedisError;
use crate::protocol::{self, DataType};
use crate::storage::Storage;

/// DISCARD command implementation.
pub struct Discard {
    pub message: DataType,
    pub transaction: Arc<TransactionSlot>
}

impl RedisCommand for Discard {
    fn execute(&self, _: &Arc<Mutex<Storage>>) -> Result<Vec<DataType>, anyhow::Error> {
        expect_no_arguments(&self.message, "discard")?;

        // Transaction is taken out - there is no more transaction left if there was one
        let Some(_) = self.transaction.take()? else {
            debug!("DISCARD without MULTI");
            return Err(RedisError {
                message: "ERR DISCARD without MULTI".to_string(),
            }
            .into());
        };

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

    fn name(&self) -> &str {
        "DISCARD"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::{client_error_message, command_message, create_test_storage};

    fn discard(parts: &[&str], transaction: &Arc<TransactionSlot>) -> Discard {
        Discard {
            message: command_message(parts),
            transaction: Arc::clone(transaction)
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
    fn test_discard_without_multi_is_a_client_error() {
        let storage = create_test_storage();

        let error = discard(&["DISCARD"], &no_transaction()).execute(&storage).unwrap_err();

        assert_eq!(client_error_message(error), "ERR DISCARD without MULTI");
    }

    #[test]
    fn test_discard_of_empty_transaction() -> anyhow::Result<()> {
        let storage = create_test_storage();

        let result = discard(&["DISCARD"], &open_transaction()?).execute(&storage)?;
        assert_eq!(result, vec![protocol::simple_string("OK")]);
        Ok(())
    }

    #[test]
    fn test_discard_of_transaction_with_operations() -> anyhow::Result<()> {
        let storage = create_test_storage();

        let transaction = open_transaction()?;
        transaction.queue("SET", &command_message(&["SET", "x", "1"]))?;
        transaction.queue("SET", &command_message(&["SET", "y", "2"]))?;
        transaction.queue("SET", &command_message(&["SET", "z", "3"]))?;

        let result = discard(&["DISCARD"], &transaction).execute(&storage)?;
        assert_eq!(result, vec![protocol::simple_string("OK")]);
        Ok(())
    }

    #[test]
    fn test_discard_rejects_arguments() {
        let storage = create_test_storage();

        let error = discard(&["DISCARD", "extra"], &no_transaction()).execute(&storage).unwrap_err();

        assert_eq!(
            client_error_message(error),
            "ERR wrong number of arguments for 'discard' command"
        );
    }

    #[test]
    fn test_rejected_discard_leaves_an_open_transaction_alone() -> anyhow::Result<()> {
        let storage = create_test_storage();
        let transaction = open_transaction()?;

        assert!(discard(&["DISCARD", "extra"], &transaction).execute(&storage).is_err());

        let result = discard(&["DISCARD"], &transaction).execute(&storage)?;
        assert_eq!(result, vec![protocol::simple_string("OK")]);
        Ok(())
    }

    #[test]
    fn test_discard_is_not_propagated_to_replicas() {
        let cmd = discard(&["DISCARD"], &no_transaction());

        assert!(!cmd.is_propagated_to_replicas());
        assert!(!cmd.should_always_reply());
    }
}
