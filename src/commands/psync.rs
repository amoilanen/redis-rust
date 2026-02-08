/// PSYNC command - partial resynchronization for replication.
///
/// Syntax: PSYNC <replication_id> <offset>
/// Returns: +FULLRESYNC <replication_id> <offset> followed by RDB

use std::sync::{Arc, Mutex};
use anyhow::anyhow;
use log::*;
use crate::protocol;
use crate::storage;
use crate::server_state;
use super::RedisCommand;

/// PSYNC command implementation.
pub struct PSync<'a> {
    pub message: &'a protocol::DataType,
    pub server_state: &'a server_state::ServerState,
}

impl RedisCommand for PSync<'_> {
    fn execute(&self, storage: &Arc<Mutex<storage::Storage>>) -> Result<Vec<protocol::DataType>, anyhow::Error> {
        let mut reply = Vec::new();
        let instructions: Vec<String> = self.message.as_array()?;

        let replication_id = instructions
            .get(1)
            .ok_or(anyhow!("replication_id not defined in {:?}", instructions))?;
        let offset: i64 = instructions
            .get(2)
            .ok_or(anyhow!("offset is not defined in {:?}", instructions))?
            .parse()?;

        info!(
            "Master handling PSYNC: replication_id = {}, offset = {}",
            replication_id, offset
        );

        let replication_id = self
            .server_state
            .master_replication_id
            .clone()
            .ok_or(anyhow!("replication_id is not defined on the master node"))?;

        reply.push(protocol::simple_string(
            format!("FULLRESYNC {} 0", replication_id).as_str(),
        ));

        let rdb_bytes = storage
            .lock()
            .map_err(|e| anyhow!("Failed to lock storage: {}", e))?
            .to_rdb()?;
        reply.push(protocol::DataType::Rdb { value: rdb_bytes });

        //TODO: In practice it would be OK to send this command, but it fails some test expectations on Codecrafters, commenting out temporarily
        //reply.push(protocol::array(vec![protocol::bulk_string("REPLCONF"), protocol::bulk_string("GETACK"), protocol::bulk_string("*")]));

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
    fn test_psync_returns_fullresync() {
        let server_state = server_state::ServerState::new(None, 6379);
        let message = protocol::array(vec![
            protocol::bulk_string("PSYNC"),
            protocol::bulk_string("?"),
            protocol::bulk_string("-1"),
        ]);
        let cmd = PSync {
            message: &message,
            server_state: &server_state,
        };

        let storage = Arc::new(Mutex::new(storage::Storage::new(HashMap::new())));
        let result = cmd.execute(&storage).unwrap();

        assert_eq!(result.len(), 2);
        let fullresync = result[0].as_string().unwrap();
        assert!(fullresync.starts_with("FULLRESYNC"));

        // Verify we got RDB
        match &result[1] {
            protocol::DataType::Rdb { value: _ } => {
                // Expected
            }
            _ => panic!("Expected RDB data type"),
        }
    }
}
