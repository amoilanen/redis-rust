/// COMMAND command - returns information about available commands.
///
/// Syntax: COMMAND
/// Returns: +OK (simplified version, not full command metadata)

use std::sync::{Arc, Mutex};
use crate::protocol;
use crate::protocol::DataType;
use crate::storage::Storage;
use super::RedisCommand;
use crate::commands::{Echo, Ping, Set, Get, Incr, Multi, Exec, Info, ReplConf, PSync, RPush, LPush, LRange, LLen, LPop, BLPop, Type, XAdd, XRange, XRead};
use crate::commands::transaction::TransactionSlot;
use crate::server_state::ServerState;

/// COMMAND command implementation.
pub struct Command {
    pub message: DataType,
}

impl RedisCommand for Command {
    fn execute(&self, _: &Arc<Mutex<Storage>>) -> Result<Vec<DataType>, anyhow::Error> {
        // TODO: Should return the list of all the available commands and their documentation instead
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

pub(crate) fn build_command(
    command_name: &str,
    received_message: &DataType,
    elements: &[DataType],
    server_state: &Arc<ServerState>,
    transaction: &Arc<TransactionSlot>,
) -> Option<Box<dyn RedisCommand>> {
    let message = received_message.clone();
    let state = || Arc::clone(server_state);
    let notifier = || Arc::clone(&server_state.blocking_notifier);
    let transaction = || Arc::clone(transaction);

    let command: Box<dyn RedisCommand> = match command_name {
        "ECHO"     => Box::new(Echo { message, argument: elements.get(1).cloned() }),
        "PING"     => Box::new(Ping { message }),
        "SET"      => Box::new(Set { message }),
        "GET"      => Box::new(Get { message }),
        "INCR"     => Box::new(Incr { message }),
        "MULTI"    => Box::new(Multi { message, transaction: transaction() }),
        "EXEC"     => Box::new(Exec { message, transaction: transaction() }),
        "COMMAND"  => Box::new(Command { message }),
        "INFO"     => Box::new(Info { message, server_state: state() }),
        "REPLCONF" => Box::new(ReplConf { message, server_state: state() }),
        "RPUSH"    => Box::new(RPush { message, notifier: notifier() }),
        "LPUSH"    => Box::new(LPush { message, notifier: notifier() }),
        "LRANGE"   => Box::new(LRange { message }),
        "LLEN"     => Box::new(LLen { message }),
        "LPOP"     => Box::new(LPop { message }),
        "BLPOP"    => Box::new(BLPop { message, notifier: notifier() }),
        "TYPE"     => Box::new(Type { message }),
        "XADD"     => Box::new(XAdd { message, notifier: notifier() }),
        "XRANGE"   => Box::new(XRange { message }),
        "XREAD"    => Box::new(XRead { message, notifier: notifier() }),
        "PSYNC"    => Box::new(PSync { message, server_state: state() }),
        _ => return None,
    };
    Some(command)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::command_message;

    #[test]
    fn test_command_command() {
        let message = command_message(&["COMMAND"]);
        let cmd = Command { message };

        let storage = Arc::new(std::sync::Mutex::new(Storage::new(
            std::collections::HashMap::new(),
        )));
        let result = cmd.execute(&storage).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].as_string().unwrap(), "OK");
        assert!(!cmd.is_propagated_to_replicas());
    }
}
