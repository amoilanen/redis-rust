/// INFO command - returns server information.
///
/// Syntax: INFO [section]
/// Currently supports: replication
/// Returns: Information about the specified section

use std::sync::{Arc, Mutex};
use crate::protocol;
use crate::storage;
use crate::server_state;
use crate::error::RedisError;
use super::RedisCommand;

/// INFO command implementation.
pub struct Info<'a> {
    pub message: &'a protocol::DataType,
    pub server_state: &'a server_state::ServerState,
}

impl RedisCommand for Info<'_> {
    fn execute(&self, _: &Arc<Mutex<storage::Storage>>) -> Result<Vec<protocol::DataType>, anyhow::Error> {
        let instructions: Vec<String> = self.message.as_vec()?;
        let error = RedisError {
            message: "INFO command should have one argument".to_string(),
        };

        let argument = instructions.get(1).ok_or::<anyhow::Error>(error.clone().into())?;

        let reply = if argument == "replication" {
            let role = match &self.server_state.replica_of {
                Some(_) => "slave",
                None => "master",
            };

            let additional_info = match role {
                "slave" => "".to_owned(),
                "master" => format!(
                    "master_replid:{}\r\nmaster_repl_offset:{}\r\n",
                    &self
                        .server_state
                        .master_replication_id
                        .clone()
                        .unwrap_or_else(|| "".to_owned()),
                    &self.server_state.master_replication_offset.unwrap_or(0)
                ),
                _ => "".to_owned(),
            };

            vec![protocol::bulk_string(&format!(
                "# Replication\r\nrole:{}\r\n{}",
                role, additional_info
            ))]
        } else {
            vec![protocol::bulk_string_empty()]
        };

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
    use std::collections::HashMap;

    #[test]
    fn test_info_replication_master() {
        let server_state = server_state::ServerState::new(None, 6379);
        let message = protocol::array(vec![
            protocol::bulk_string("INFO"),
            protocol::bulk_string("replication"),
        ]);
        let cmd = Info {
            message: &message,
            server_state: &server_state,
        };

        let storage = Arc::new(Mutex::new(storage::Storage::new(HashMap::new())));
        let result = cmd.execute(&storage).unwrap();

        assert_eq!(result.len(), 1);
        let info = result[0].as_string().unwrap();
        assert!(info.contains("role:master"));
        assert!(info.contains("master_replid"));
    }

    #[test]
    fn test_info_replication_slave() {
        let server_state = server_state::ServerState::new(Some("localhost 6379".to_owned()), 6380);
        let message = protocol::array(vec![
            protocol::bulk_string("INFO"),
            protocol::bulk_string("replication"),
        ]);
        let cmd = Info {
            message: &message,
            server_state: &server_state,
        };

        let storage = Arc::new(Mutex::new(storage::Storage::new(HashMap::new())));
        let result = cmd.execute(&storage).unwrap();

        assert_eq!(result.len(), 1);
        let info = result[0].as_string().unwrap();
        assert!(info.contains("role:slave"));
    }
}
