/// ECHO command - echoes the argument back to the client.
///
/// Syntax: ECHO <message>
/// Returns: The message back to the client

use std::sync::{Arc, Mutex};
use crate::protocol::DataType;
use crate::storage::Storage;
use super::RedisCommand;

/// ECHO command implementation.
pub struct Echo {
    pub message: DataType
}

impl RedisCommand for Echo {
    fn execute(&self, _: &Arc<Mutex<Storage>>) -> Result<Vec<DataType>, anyhow::Error> {
        let elements = self.message.as_vec()?;
        let argument = elements.get(1);
        let mut reply: Vec<DataType> = Vec::new();
        if let Some(echo_argument) = argument {
            reply = vec![echo_argument.to_owned()];
        }
        Ok(reply)
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
        "ECHO"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::command_message;
    use crate::protocol;

    #[test]
    fn test_echo_command_with_message() {
        let echo_msg = protocol::bulk_string("Hello World");
        let message = protocol::array(vec![
            protocol::bulk_string("ECHO"),
            echo_msg.clone(),
        ]);

        let cmd = Echo {
            message
        };

        let storage = Arc::new(std::sync::Mutex::new(Storage::new(
            std::collections::HashMap::new(),
        )));
        let result = cmd.execute(&storage).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].as_string().unwrap(), "Hello World");
    }

    #[test]
    fn test_echo_command_without_message() {
        let message = command_message(&["ECHO"]);
        let cmd = Echo {
            message
        };

        let storage = Arc::new(std::sync::Mutex::new(Storage::new(
            std::collections::HashMap::new(),
        )));
        let result = cmd.execute(&storage).unwrap();

        assert_eq!(result.len(), 0);
    }
}
