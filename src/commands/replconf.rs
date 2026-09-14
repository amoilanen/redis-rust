/// REPLCONF command - replication configuration during handshake, and the
/// acknowledgements that keep master and replica in step afterwards.
///
/// Syntax: REPLCONF <subcommand> [arguments]
/// Subcommands:
///   listening-port <port>
///   capa <capability>
///   getack *          - asks a replica how far it has got
///   ack <offset>      - a replica's answer, travelling back up to the master
/// Returns: +OK, the acknowledgement GETACK asks for, or - for an ACK -
///          nothing at all

use std::sync::{Arc, Mutex};
use anyhow::anyhow;
use log::*;
use crate::protocol;
use crate::protocol::DataType;
use crate::storage::Storage;
use crate::server_state::{ReplicaSlot, ServerState};
use super::RedisCommand;

/// Asks a replica how far into the replication stream it has got.
const GETACK: &str = "getack";
/// A replica's answer to that, carrying the offset it has reached.
const ACK: &str = "ack";

/// REPLCONF command implementation.
pub struct ReplConf {
    pub message: DataType,
    pub server_state: Arc<ServerState>,
    /// The replica this connection belongs to, once it has one: whose
    /// acknowledgement an `ACK` arriving here is.
    pub replica: Arc<ReplicaSlot>,
}

impl RedisCommand for ReplConf {
    fn execute(&self, _: &Mutex<Storage>) -> Result<Vec<DataType>, anyhow::Error> {
        let instructions: Vec<String> = self.message.as_string_vec()?;
        let sub_command = instructions
            .get(1)
            .ok_or(anyhow!("Subcommand not defined in {:?}", instructions))?;

        // Handled by the replica, self.replica is None
        if sub_command.eq_ignore_ascii_case(GETACK) {
            Ok(vec![self.acknowledgement()])
        // Handled by the server, self.replica contains connected replica from which this command was received
        } else if sub_command.eq_ignore_ascii_case(ACK) {
            self.record_acknowledgement(instructions.get(2))?;
            Ok(Vec::new())
        } else {
            Ok(vec![protocol::simple_string("OK")])
        }
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

impl ReplConf {

    fn acknowledgement(&self) -> DataType {
        let offset = self.server_state.replication_offset();
        protocol::array(vec![
            protocol::bulk_string("REPLCONF"),
            protocol::bulk_string("ACK"),
            protocol::bulk_string(&offset.to_string()),
        ])
    }

    fn record_acknowledgement(&self, offset: Option<&String>) -> Result<(), anyhow::Error> {
        let Some(link) = self.replica.link()? else {
            debug!("Ignoring a REPLCONF ACK from a connection that is not a replica's");
            return Ok(());
        };
        let Some(acknowledged) = offset.and_then(|offset| offset.parse::<usize>().ok()) else {
            warn!("Ignoring a REPLCONF ACK with an unreadable offset: {:?}", offset);
            return Ok(());
        };

        debug!("Replica acknowledged offset {}", acknowledged);
        self.server_state.record_acknowledgement(&link, acknowledged)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::{command_message, create_test_storage};
    use crate::server_state::ReplicaLink;
    use std::net::{TcpListener, TcpStream};

    fn replconf_from_master(parts: &[&str], server_state: &Arc<ServerState>) -> ReplConf {
        ReplConf {
            message: command_message(parts),
            server_state: Arc::clone(server_state),
            replica: Arc::new(ReplicaSlot::new()),
        }
    }

    fn replconf_from_replica(
        parts: &[&str],
    ) -> anyhow::Result<(ReplConf, TcpListener, Arc<ReplicaLink>)> {
        let server_state = Arc::new(ServerState::new(None, 6379));
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let replica = TcpStream::connect(listener.local_addr()?)?;
        let link = server_state.register_replica(&replica)?;
        let slot = Arc::new(ReplicaSlot::new());
        slot.fill(Arc::clone(&link))?;

        let command = ReplConf {
            message: command_message(parts),
            server_state,
            replica: slot,
        };
        Ok((command, listener, link))
    }

    #[test]
    fn test_replconf_listening_port() {
        let server_state = Arc::new(ServerState::new(None, 6380));
        let cmd = replconf_from_master(&["REPLCONF", "listening-port", "6380"], &server_state);

        let result = cmd.execute(&create_test_storage()).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].as_string().unwrap(), "OK");
        assert!(cmd.should_always_reply());
    }

    #[test]
    fn test_replconf_getack() {
        let server_state = Arc::new(ServerState::new(None, 6379));
        let cmd = replconf_from_master(&["REPLCONF", "getack", "*"], &server_state);

        let result = cmd.execute(&create_test_storage()).unwrap();

        assert_eq!(result.len(), 1);
        let response = result[0].as_string_vec().unwrap();
        assert_eq!(response, vec!["REPLCONF", "ACK", "0"]);
    }

    #[test]
    fn test_replconf_getack_reports_the_processed_offset() {
        let server_state = Arc::new(ServerState::new(Some("localhost 6379".to_owned()), 6380));
        let first_offset = 37;
        let second_offset = 14;
        server_state.advance_replication_offset(first_offset);
        server_state.advance_replication_offset(second_offset);
        let cmd = replconf_from_master(&["REPLCONF", "getack", "*"], &server_state);

        let result = cmd.execute(&create_test_storage()).unwrap();

        let response = result[0].as_string_vec().unwrap();
        assert_eq!(response, vec!["REPLCONF", "ACK", &(first_offset + second_offset).to_string()]);
    }

    #[test]
    fn test_replconf_ack_records_the_offset_against_the_replica() -> anyhow::Result<()> {
        let (cmd, _listener, link) = replconf_from_replica(&["REPLCONF", "ACK", "146"])?;

        let result = cmd.execute(&create_test_storage())?;

        assert_eq!(link.acknowledged_offset(), 146);
        // Never answered: a reply would travel back down to the replica and be
        // read there as one more command.
        assert_eq!(result, Vec::new());
        Ok(())
    }

    #[test]
    fn test_replconf_ack_is_matched_whatever_its_case() -> anyhow::Result<()> {
        let (cmd, _listener, link) = replconf_from_replica(&["replconf", "ack", "146"])?;

        cmd.execute(&create_test_storage())?;

        assert_eq!(link.acknowledged_offset(), 146);
        Ok(())
    }

    #[test]
    fn test_replconf_ack_ignores_an_unreadable_offset() -> anyhow::Result<()> {
        for parts in [vec!["REPLCONF", "ACK"], vec!["REPLCONF", "ACK", "soon"]] {
            let (cmd, _listener, link) = replconf_from_replica(&parts)?;

            let result = cmd.execute(&create_test_storage())?;

            assert_eq!(link.acknowledged_offset(), 0, "{:?}", parts);
            assert_eq!(result, Vec::new(), "{:?}", parts);
        }
        Ok(())
    }

    #[test]
    fn test_replconf_ack_from_a_client_is_ignored_rather_than_answered() {
        let server_state = Arc::new(ServerState::new(None, 6379));
        let cmd = replconf_from_master(&["REPLCONF", "ACK", "146"], &server_state);

        let result = cmd.execute(&create_test_storage()).unwrap();

        assert_eq!(result, Vec::new());
    }
}
