/// COMMAND command - returns information about available commands.
///
/// Syntax: COMMAND
/// Returns: +OK (simplified version, not full command metadata)

use std::sync::{Arc, Mutex};
use crate::protocol;
use crate::storage;
use super::RedisCommand;

/// COMMAND command implementation.
pub struct Command<'a> {
    pub message: &'a protocol::DataType,
}

impl RedisCommand for Command<'_> {
    fn execute(&self, _: &Arc<Mutex<storage::Storage>>) -> Result<Vec<protocol::DataType>, anyhow::Error> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_command_command() {
        let message = protocol::array(vec![protocol::bulk_string("COMMAND")]);
        let cmd = Command { message: &message };

        let storage = Arc::new(std::sync::Mutex::new(storage::Storage::new(
            std::collections::HashMap::new(),
        )));
        let result = cmd.execute(&storage).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].as_string().unwrap(), "OK");
        assert!(!cmd.is_propagated_to_replicas());
    }
}
