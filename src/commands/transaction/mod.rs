/// MULTI/EXEC commands and the per-connection state they share.
///
/// The state is per-connection, not per-server: MULTI on one client must not
/// open a transaction for any other.

use std::sync::{Mutex, MutexGuard};

use anyhow::{anyhow, Result};

use crate::error::RedisError;
use crate::protocol::DataType;

pub mod multi;
pub mod exec;

pub use multi::Multi;
pub use exec::Exec;

/// A transaction, opened by MULTI and consumed by EXEC.
///
/// There is no way to reopen one: EXEC takes it by value, and a later MULTI
/// starts a new transaction. Holds the queued commands once queueing lands.
pub struct Transaction;

/// The at-most-one transaction open on a connection.
///
/// Owned by `connection::handle_connection`, so an open transaction dies with
/// the client that opened it. The `Mutex` is internal because commands only get
/// `&self` in `RedisCommand::execute`.
pub struct TransactionSlot {
    open: Mutex<Option<Transaction>>,
}

impl TransactionSlot {
    pub fn new() -> Self {
        TransactionSlot { open: Mutex::new(None) }
    }

    /// Discards any transaction already open. Real Redis instead rejects a
    /// nested MULTI, which is not part of this stage.
    pub fn open(&self) -> Result<()> {
        *self.lock()? = Some(Transaction);
        Ok(())
    }

    pub fn take(&self) -> Result<Option<Transaction>> {
        Ok(self.lock()?.take())
    }

    fn lock(&self) -> Result<MutexGuard<'_, Option<Transaction>>> {
        self.open
            .lock()
            .map_err(|e| anyhow!("Failed to lock transaction slot: {}", e))
    }
}

impl Default for TransactionSlot {
    fn default() -> Self {
        Self::new()
    }
}

/// Rejects anything beyond the command name, which both MULTI and EXEC do
/// before looking at the transaction slot.
///
/// `command_name` is the lowercase name Redis quotes back, e.g. `exec` gives
/// `ERR wrong number of arguments for 'exec' command`.
fn expect_no_arguments(message: &DataType, command_name: &str) -> Result<()> {
    let instructions: Vec<String> = message.as_vec()?;
    if instructions.len() != 1 {
        return Err(RedisError {
            message: format!(
                "ERR wrong number of arguments for '{}' command",
                command_name
            ),
        }
        .into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::{client_error_message, command_message};

    #[test]
    fn test_expect_no_arguments_accepts_a_bare_command() {
        assert!(expect_no_arguments(&command_message(&["MULTI"]), "multi").is_ok());
    }

    #[test]
    fn test_expect_no_arguments_rejects_any_number_of_extras() {
        for extras in 1..4 {
            let mut parts = vec!["EXEC"];
            parts.extend(std::iter::repeat("x").take(extras));

            let error = expect_no_arguments(&command_message(&parts), "exec").unwrap_err();

            assert_eq!(
                client_error_message(error),
                "ERR wrong number of arguments for 'exec' command",
                "{} extra argument(s) should be rejected",
                extras
            );
        }
    }

    #[test]
    fn test_slot_starts_empty() -> Result<()> {
        assert!(TransactionSlot::new().take()?.is_none());
        Ok(())
    }

    #[test]
    fn test_open_puts_a_transaction_in_the_slot() -> Result<()> {
        let slot = TransactionSlot::new();

        slot.open()?;

        assert!(slot.take()?.is_some());
        Ok(())
    }

    #[test]
    fn test_take_consumes_the_transaction() -> Result<()> {
        // Why the second EXEC of `MULTI, EXEC, EXEC` errors.
        let slot = TransactionSlot::new();
        slot.open()?;

        assert!(slot.take()?.is_some());
        assert!(slot.take()?.is_none());
        Ok(())
    }

    #[test]
    fn test_a_new_transaction_can_be_opened_after_the_previous_one_ends() -> Result<()> {
        let slot = TransactionSlot::new();

        for round in 1..4 {
            slot.open()?;
            assert!(slot.take()?.is_some(), "transaction {}", round);
        }
        Ok(())
    }

    #[test]
    fn test_repeated_open_leaves_a_single_transaction() -> Result<()> {
        let slot = TransactionSlot::new();

        slot.open()?;
        slot.open()?;

        assert!(slot.take()?.is_some());
        assert!(slot.take()?.is_none());
        Ok(())
    }

    #[test]
    fn test_slots_are_independent() -> Result<()> {
        let one = TransactionSlot::new();
        let other = TransactionSlot::new();

        one.open()?;

        assert!(other.take()?.is_none());
        assert!(one.take()?.is_some());
        Ok(())
    }
}
