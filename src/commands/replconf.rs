/// REPLCONF command - replication configuration during handshake.
///
/// Syntax: REPLCONF <subcommand> [arguments]
/// Subcommands:
///   listening-port <port>
///   capa <capability>
///   getack <offset>
/// Returns: +OK or response depending on subcommand

use std::sync::{Arc, Mutex};
use anyhow::anyhow;
use crate::protocol;
use crate::protocol::DataType;
use crate::storage::Storage;
use crate::server_state::ServerState;
use super::RedisCommand;

/// REPLCONF command implementation.
pub struct ReplConf {
    pub message: DataType,
    pub server_state: Arc<ServerState>,
}

impl RedisCommand for ReplConf {
    fn execute(&self, _: &Mutex<Storage>) -> Result<Vec<DataType>, anyhow::Error> {
        let mut reply = Vec::new();
        let instructions: Vec<String> = self.message.as_string_vec()?;
        let sub_command = instructions
            .get(1)
            .ok_or(anyhow!("replication_id not defined in {:?}", instructions))?;

        if sub_command.to_lowercase() == "getack" {
            // The bytes of this GETACK are not part of the answer: the
            // connection to the master counts a command towards the offset
            // only once it has been handled.
            let offset = self.server_state.replication_offset();
            reply.push(protocol::array(vec![
                protocol::bulk_string("REPLCONF"),
                protocol::bulk_string("ACK"),
                protocol::bulk_string(&offset.to_string()),
            ]));
        } else {
            reply.push(protocol::simple_string("OK"));
        }

        Ok(reply)
    }

    fn is_propagated_to_replicas(&self) -> bool {
        false
    }

    fn should_always_reply(&self) -> bool {
        true
    }

    fn serialize(&self) -> Vec<u8> {
        self.message.serialize()
    }

    fn name(&self) -> &str {
        "REPLCONF"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::command_message;
    use std::collections::HashMap;

    #[test]
    fn test_replconf_listening_port() {
        let server_state = Arc::new(ServerState::new(None, 6380));
        let message = command_message(&["REPLCONF", "listening-port", "6380"]);
        let cmd = ReplConf {
            message,
            server_state,
        };

        let storage = Arc::new(Mutex::new(Storage::new(HashMap::new())));
        let result = cmd.execute(&storage).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].as_string().unwrap(), "OK");
        assert!(cmd.should_always_reply());
    }

    #[test]
    fn test_replconf_getack() {
        let server_state = Arc::new(ServerState::new(None, 6379));
        let message = command_message(&["REPLCONF", "getack", "*"]);
        let cmd = ReplConf {
            message,
            server_state,
        };

        let storage = Arc::new(Mutex::new(Storage::new(HashMap::new())));
        let result = cmd.execute(&storage).unwrap();

        assert_eq!(result.len(), 1);
        let response = result[0].as_string_vec().unwrap();
        assert_eq!(response.len(), 3);
        assert_eq!(response[0], "REPLCONF");
        assert_eq!(response[1], "ACK");
        assert_eq!(response[2], "0");
    }

    #[test]
    fn test_replconf_getack_reports_the_processed_offset() {
        let server_state = Arc::new(ServerState::new(Some("localhost 6379".to_owned()), 6380));
        // A REPLCONF GETACK * (37 bytes) and a PING (14 bytes) processed
        // before this request.
        let first_offset = 37;
        let second_offset = 14;
        server_state.advance_replication_offset(first_offset);
        server_state.advance_replication_offset(second_offset);
        let message = command_message(&["REPLCONF", "getack", "*"]);
        let cmd = ReplConf {
            message,
            server_state,
        };

        let storage = Arc::new(Mutex::new(Storage::new(HashMap::new())));
        let result = cmd.execute(&storage).unwrap();

        let response = result[0].as_string_vec().unwrap();
        assert_eq!(response, vec!["REPLCONF", "ACK", &(first_offset + second_offset).to_string()]);
    }
}
