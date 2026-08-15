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
pub mod discard;

pub use multi::Multi;
pub use exec::Exec;
pub use discard::Discard;

/// A transaction, opened by MULTI and consumed by EXEC.
///
/// There is no way to reopen one: EXEC takes it by value, and a later MULTI
/// starts a new transaction. Holds the commands queued since MULTI as the
/// messages they arrived as, so EXEC can run them unchanged.
pub struct Transaction {
    queued: Vec<DataType>,
}

impl Transaction {
    fn new() -> Self {
        Transaction { queued: Vec::new() }
    }

    /// The queued commands, oldest first - the order EXEC has to run them in.
    pub fn queued(&self) -> &[DataType] {
        &self.queued
    }
}

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

    /// Discards any transaction already open, queued commands included. Real
    /// Redis instead rejects a nested MULTI, which is not part of this stage.
    pub fn open(&self) -> Result<()> {
        *self.lock()? = Some(Transaction::new());
        Ok(())
    }

    /// Appends `command` to the open transaction, reporting whether it was
    /// queued - that is, whether the connection is inside a transaction at all.
    ///
    /// `command_name` is the name as it arrived: MULTI and EXEC act on the
    /// transaction itself, so they run rather than being queued into it.
    ///
    /// The check and the append share one lock so that a command can never be
    /// queued into a transaction EXEC has already taken.
    pub fn queue(&self, command_name: &str, command: &DataType) -> Result<bool> {
        if is_transaction_control(command_name) {
            return Ok(false);
        }

        let mut slot = self.lock()?;
        let Some(transaction) = slot.as_mut() else {
            return Ok(false);
        };
        transaction.queued.push(command.clone());
        Ok(true)
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

/// Whether `command_name` drives the transaction rather than belonging in it.
///
/// These are the commands that keep running while a transaction is open; every
/// other command is queued.
fn is_transaction_control(command_name: &str) -> bool {
    matches!(command_name, "MULTI" | "EXEC" | "DISCARD")
}

/// Rejects anything beyond the command name, which both MULTI and EXEC do
/// before looking at the transaction slot.
///
/// `command_name` is the lowercase name Redis quotes back, e.g. `exec` gives
/// `ERR wrong number of arguments for 'exec' command`.
fn expect_no_arguments(message: &DataType, command_name: &str) -> Result<()> {
    let instructions: Vec<String> = message.as_string_vec()?;
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
    fn test_queueing_without_a_transaction_reports_nothing_was_queued() -> Result<()> {
        let slot = TransactionSlot::new();

        assert!(!slot.queue("SET", &command_message(&["SET", "foo", "41"]))?);
        Ok(())
    }

    #[test]
    fn test_a_new_transaction_has_nothing_queued() -> Result<()> {
        let slot = TransactionSlot::new();

        slot.open()?;

        assert!(slot.take()?.unwrap().queued().is_empty());
        Ok(())
    }

    #[test]
    fn test_queued_commands_are_kept_in_arrival_order() -> Result<()> {
        let slot = TransactionSlot::new();
        slot.open()?;
        let set = command_message(&["SET", "foo", "41"]);
        let incr = command_message(&["INCR", "foo"]);

        assert!(slot.queue("SET", &set)?);
        assert!(slot.queue("INCR", &incr)?);

        assert_eq!(slot.take()?.unwrap().queued(), &[set, incr]);
        Ok(())
    }

    #[test]
    fn test_the_same_command_can_be_queued_more_than_once() -> Result<()> {
        let slot = TransactionSlot::new();
        slot.open()?;
        let incr = command_message(&["INCR", "foo"]);

        for _ in 0..3 {
            slot.queue("INCR", &incr)?;
        }

        assert_eq!(slot.take()?.unwrap().queued().len(), 3);
        Ok(())
    }

    #[test]
    fn test_transaction_control_commands_are_not_queued() -> Result<()> {
        // MULTI and EXEC have to reach their own implementations to nest or end
        // the transaction, so they must run instead of being queued into it.
        let slot = TransactionSlot::new();
        slot.open()?;

        assert!(!slot.queue("MULTI", &command_message(&["MULTI"]))?);
        assert!(!slot.queue("EXEC", &command_message(&["EXEC"]))?);

        assert!(slot.take()?.unwrap().queued().is_empty());
        Ok(())
    }

    #[test]
    fn test_taking_a_transaction_discards_its_queue() -> Result<()> {
        let slot = TransactionSlot::new();
        slot.open()?;
        slot.queue("SET", &command_message(&["SET", "foo", "41"]))?;

        slot.take()?;

        assert!(!slot.queue("INCR", &command_message(&["INCR", "foo"]))?);
        Ok(())
    }

    #[test]
    fn test_reopening_starts_from_an_empty_queue() -> Result<()> {
        let slot = TransactionSlot::new();
        slot.open()?;
        slot.queue("SET", &command_message(&["SET", "foo", "41"]))?;

        slot.open()?;

        assert!(slot.take()?.unwrap().queued().is_empty());
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
