/// MULTI command - opens a transaction on the connection it arrives on.
///
/// Syntax: MULTI
/// Returns: +OK
///
/// In real Redis it also switches the connection into a queueing mode where
/// subsequent commands reply `+QUEUED` instead of executing. That comes in a
/// later stage, so for now the transaction is only ever empty.

use std::sync::{Arc, Mutex};

use log::*;

use super::{expect_no_arguments, TransactionSlot};
use crate::commands::RedisCommand;
use crate::protocol;
use crate::protocol::DataType;
use crate::storage::Storage;

/// MULTI command implementation.
pub struct Multi {
    pub message: DataType,
    /// Transaction slot of the connection this MULTI arrived on.
    pub transaction: Arc<TransactionSlot>,
}

impl RedisCommand for Multi {
    fn execute(&self, _: &Arc<Mutex<Storage>>) -> Result<Vec<DataType>, anyhow::Error> {
        expect_no_arguments(&self.message, "multi")?;

        debug!("MULTI");

        self.transaction.open()?;

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
    use crate::commands::{client_error_message, command_message, create_test_storage};

    fn multi(parts: &[&str], transaction: &Arc<TransactionSlot>) -> Multi {
        Multi {
            message: command_message(parts),
            transaction: Arc::clone(transaction),
        }
    }

    /// A connection that has not sent MULTI.
    fn no_transaction() -> Arc<TransactionSlot> {
        Arc::new(TransactionSlot::new())
    }

    #[test]
    fn test_multi_replies_ok() -> anyhow::Result<()> {
        let storage = create_test_storage();

        let result = multi(&["MULTI"], &no_transaction()).execute(&storage)?;

        assert_eq!(result, vec![protocol::simple_string("OK")]);
        Ok(())
    }

    #[test]
    fn test_multi_opens_a_transaction() -> anyhow::Result<()> {
        let storage = create_test_storage();
        let transaction = no_transaction();

        multi(&["MULTI"], &transaction).execute(&storage)?;

        assert!(transaction.take()?.is_some());
        Ok(())
    }

    #[test]
    fn test_multi_is_not_propagated_to_replicas() {
        let cmd = multi(&["MULTI"], &no_transaction());

        assert!(!cmd.is_propagated_to_replicas());
        assert!(!cmd.should_always_reply());
    }

    #[test]
    fn test_multi_rejects_arguments() {
        let storage = create_test_storage();

        let error = multi(&["MULTI", "extra"], &no_transaction())
            .execute(&storage)
            .unwrap_err();

        assert_eq!(
            client_error_message(error),
            "ERR wrong number of arguments for 'multi' command"
        );
    }

    #[test]
    fn test_rejected_multi_does_not_open_a_transaction() -> anyhow::Result<()> {
        let storage = create_test_storage();
        let transaction = no_transaction();

        assert!(multi(&["MULTI", "extra"], &transaction).execute(&storage).is_err());

        assert!(transaction.take()?.is_none());
        Ok(())
    }
}
