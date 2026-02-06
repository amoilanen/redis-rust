/// PING command - tests server connectivity.
///
/// Syntax: PING
/// Returns: +PONG

use std::sync::{Arc, Mutex};
use crate::protocol;
use crate::storage;
use super::RedisCommand;

/// PING command implementation.
pub struct Ping<'a> {
    pub message: &'a protocol::DataType,
}

impl RedisCommand for Ping<'_> {
    fn execute(&self, _: &Arc<Mutex<storage::Storage>>) -> Result<Vec<protocol::DataType>, anyhow::Error> {
        Ok(vec![protocol::simple_string("PONG")])
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
    fn test_ping_command() {
        let message = protocol::array(vec![protocol::bulk_string("PING")]);
        let cmd = Ping { message: &message };

        let storage = Arc::new(std::sync::Mutex::new(storage::Storage::new(
            std::collections::HashMap::new(),
        )));
        let result = cmd.execute(&storage).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].as_string().unwrap(), "PONG");
        assert!(!cmd.is_propagated_to_replicas());
        assert!(!cmd.should_always_reply());
    }
}
