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
use crate::storage;
use crate::server_state;
use super::RedisCommand;

/// REPLCONF command implementation.
pub struct ReplConf<'a> {
    pub message: &'a protocol::DataType,
    pub server_state: &'a server_state::ServerState,
}

impl RedisCommand for ReplConf<'_> {
    fn execute(&self, _: &Arc<Mutex<storage::Storage>>) -> Result<Vec<protocol::DataType>, anyhow::Error> {
        let mut reply = Vec::new();
        let instructions: Vec<String> = self.message.as_vec()?;
        let sub_command = instructions
            .get(1)
            .ok_or(anyhow!("replication_id not defined in {:?}", instructions))?;

        if sub_command.to_lowercase() == "getack" {
            // TODO: Implement proper offset tracking later, for now hardcoding as 0
            reply.push(protocol::array(vec![
                protocol::bulk_string("REPLCONF"),
                protocol::bulk_string("ACK"),
                protocol::bulk_string("0"),
            ]));
        } else {
            reply.push(protocol::bulk_string("OK"));
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_replconf_listening_port() {
        let server_state = server_state::ServerState::new(None, 6380);
        let message = protocol::array(vec![
            protocol::bulk_string("REPLCONF"),
            protocol::bulk_string("listening-port"),
            protocol::bulk_string("6380"),
        ]);
        let cmd = ReplConf {
            message: &message,
            server_state: &server_state,
        };

        let storage = Arc::new(Mutex::new(storage::Storage::new(HashMap::new())));
        let result = cmd.execute(&storage).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].as_string().unwrap(), "OK");
        assert!(cmd.should_always_reply());
    }

    #[test]
    fn test_replconf_getack() {
        let server_state = server_state::ServerState::new(None, 6379);
        let message = protocol::array(vec![
            protocol::bulk_string("REPLCONF"),
            protocol::bulk_string("getack"),
            protocol::bulk_string("*"),
        ]);
        let cmd = ReplConf {
            message: &message,
            server_state: &server_state,
        };

        let storage = Arc::new(Mutex::new(storage::Storage::new(HashMap::new())));
        let result = cmd.execute(&storage).unwrap();

        assert_eq!(result.len(), 1);
        let response = result[0].as_vec().unwrap();
        assert_eq!(response.len(), 3);
        assert_eq!(response[0], "REPLCONF");
        assert_eq!(response[1], "ACK");
        assert_eq!(response[2], "0");
    }
}
