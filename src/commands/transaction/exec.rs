/// EXEC command - runs the transaction opened by MULTI and ends it.
///
/// Syntax: EXEC
/// Returns: an array of the queued commands' replies, or
///          `-ERR EXEC without MULTI` when no transaction is open.
///
/// Running the queued commands is a later stage: for now EXEC ends the
/// transaction and discards them, always replying `*0\r\n` - which Redis treats
/// as a successful run of a transaction that had nothing in it, not as an error.

use std::sync::{Arc, Mutex};

use log::*;

use super::{expect_no_arguments, TransactionSlot};
use crate::commands::{RedisCommand, command};
use crate::error::RedisError;
use crate::protocol::{self, DataType};
use crate::server_state::ServerState;
use crate::storage::Storage;

/// EXEC command implementation.
pub struct Exec {
    pub message: DataType,
    pub transaction: Arc<TransactionSlot>,
    pub server_state: Arc<ServerState>
}

impl RedisCommand for Exec {
    fn execute(&self, storage: &Arc<Mutex<Storage>>) -> Result<Vec<DataType>, anyhow::Error> {
        expect_no_arguments(&self.message, "exec")?;

        let Some(transaction) = self.transaction.take()? else {
            debug!("EXEC without MULTI");
            return Err(RedisError {
                message: "ERR EXEC without MULTI".to_string(),
            }
            .into());
        };

        let mut commands: Vec<Box<dyn RedisCommand>> = Vec::new();
        for received_message in transaction.queued().iter() {
            // Transaction is empty at this point (it was taken from), but it is OK to start a nested transaction on this connection if required
            if let Some(command) = command::command_from_message(received_message, &self.server_state, &self.transaction)? {
                commands.push(command);
            }
        }

        let mut command_results: Vec<DataType> = Vec::new();
        for command in commands.iter() {
            let mut command_result = command.execute(storage)?;
            command_results.append(&mut command_result);
        }

        debug!(
            "EXEC: ended a transaction, executing {} queued command(s)",
            transaction.queued().len()
        );

        Ok(vec![protocol::array(command_results)])
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
        "EXEC"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::{client_error_message, command_message, create_test_storage};

    fn exec(parts: &[&str], transaction: &Arc<TransactionSlot>, server_state: &Arc<ServerState>) -> Exec {
        Exec {
            message: command_message(parts),
            transaction: Arc::clone(transaction),
            server_state: Arc::clone(server_state)
        }
    }

    /// A connection that has not sent MULTI.
    fn no_transaction() -> Arc<TransactionSlot> {
        Arc::new(TransactionSlot::new())
    }

    fn server_state() -> Arc<ServerState> {
      Arc::new(ServerState::new(None, 6379))
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
        let state = server_state();

        let error = exec(&["EXEC"], &no_transaction(), &state).execute(&storage).unwrap_err();

        assert_eq!(client_error_message(error), "ERR EXEC without MULTI");
    }

    #[test]
    fn test_exec_of_an_empty_transaction_replies_with_an_empty_array() -> anyhow::Result<()> {
        let storage = create_test_storage();
        let state = server_state();

        let result = exec(&["EXEC"], &open_transaction()?, &state).execute(&storage)?;

        assert_eq!(result, vec![protocol::array(vec![])]);
        assert_eq!(result[0].serialize(), b"*0\r\n");
        Ok(())
    }

    #[test]
    fn test_exec_executes_single_queued_command() -> anyhow::Result<()> {
        // Running them is the next stage; ending the transaction is this one.
        let storage = create_test_storage();
        let state = server_state();
        let transaction = open_transaction()?;
        transaction.queue("SET", &command_message(&["SET", "foo", "41"]))?;

        let result = exec(&["EXEC"], &transaction, &state).execute(&storage)?;

        assert_eq!(result.len(), 1);
        assert_eq!(result[0], protocol::array(vec![protocol::simple_string("OK")]));
        assert!(transaction.take()?.is_none());
        Ok(())
    }

    #[test]
    fn test_exec_executes_multiple_commands() -> anyhow::Result<()> {
        // Running them is the next stage; ending the transaction is this one.
        let storage = create_test_storage();
        let state = server_state();
        let transaction = open_transaction()?;
        transaction.queue("SET", &command_message(&["SET", "x", "1"]))?;
        transaction.queue("GET", &command_message(&["GET", "x"]))?;
        let result = exec(&["EXEC"], &transaction, &state).execute(&storage)?;

        assert_eq!(result.len(), 1);
        assert_eq!(result[0], protocol::array(vec![protocol::simple_string("OK"), protocol::bulk_string("1")]));
        assert!(transaction.take()?.is_none());
        Ok(())
    }

    #[test]
    fn test_exec_ends_the_transaction() -> anyhow::Result<()> {
        let storage = create_test_storage();
        let state = server_state();
        let transaction = open_transaction()?;

        exec(&["EXEC"], &transaction, &state).execute(&storage)?;
        let error = exec(&["EXEC"], &transaction, &state).execute(&storage).unwrap_err();

        assert_eq!(client_error_message(error), "ERR EXEC without MULTI");
        Ok(())
    }

    #[test]
    fn test_exec_only_sees_its_own_connection() -> anyhow::Result<()> {
        let storage = create_test_storage();
        let state = server_state();
        let elsewhere = open_transaction()?;

        let error = exec(&["EXEC"], &no_transaction(), &state).execute(&storage).unwrap_err();

        assert_eq!(client_error_message(error), "ERR EXEC without MULTI");
        assert!(elsewhere.take()?.is_some(), "the other connection lost its transaction");
        Ok(())
    }

    #[test]
    fn test_exec_rejects_arguments() {
        let storage = create_test_storage();
        let state = server_state();

        let error = exec(&["EXEC", "extra"], &no_transaction(), &state).execute(&storage).unwrap_err();

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
        let state = server_state();
        let transaction = open_transaction()?;

        assert!(exec(&["EXEC", "extra"], &transaction, &state).execute(&storage).is_err());

        let result = exec(&["EXEC"], &transaction, &state).execute(&storage)?;
        assert_eq!(result, vec![protocol::array(vec![])]);
        Ok(())
    }

    #[test]
    fn test_exec_is_not_propagated_to_replicas() {
        let state = server_state();
        let cmd = exec(&["EXEC"], &no_transaction(), &state);

        assert!(!cmd.is_propagated_to_replicas());
        assert!(!cmd.should_always_reply());
    }
}
