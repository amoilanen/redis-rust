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
    pub message: DataType,
    pub argument: Option<DataType>,
}

impl RedisCommand for Echo {
    fn execute(&self, _: &Arc<Mutex<Storage>>) -> Result<Vec<DataType>, anyhow::Error> {
        let mut reply: Vec<DataType> = Vec::new();
        if let Some(echo_argument) = &self.argument {
            reply = vec![echo_argument.clone()];
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol;

    #[test]
    fn test_echo_command_with_message() {
        let echo_msg = protocol::bulk_string("Hello World");
        let message = protocol::array(vec![
            protocol::bulk_string("ECHO"),
            echo_msg.clone(),
        ]);
        let elements: Vec<DataType> = message.as_vec()
            .unwrap()
            .iter()
            .map(|s| protocol::bulk_string(s))
            .collect();

        let cmd = Echo {
            message,
            argument: Some(elements[1].clone()),
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
        let message = protocol::array(vec![protocol::bulk_string("ECHO")]);
        let cmd = Echo {
            message,
            argument: None,
        };

        let storage = Arc::new(std::sync::Mutex::new(Storage::new(
            std::collections::HashMap::new(),
        )));
        let result = cmd.execute(&storage).unwrap();

        assert_eq!(result.len(), 0);
    }
}
