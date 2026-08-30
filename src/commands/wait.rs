/// WAIT command - how many replicas have acknowledged the writes so far.
///
/// Syntax: WAIT <numreplicas> <timeout>
/// Returns: the number of replicas, as a RESP integer (`:N\r\n`)
///
/// At this stage the answer is simply how many replicas are connected, sent
/// back immediately: neither `numreplicas` nor `timeout` changes it yet, since
/// waiting for acknowledgements comes in a later stage. Both arguments are
/// still parsed, so a malformed WAIT is rejected the way Redis rejects it
/// rather than being silently answered.

use std::sync::{Arc, Mutex};
use log::*;
use crate::protocol;
use crate::protocol::DataType;
use crate::error::RedisError;
use crate::server_state::ServerState;
use crate::storage::Storage;
use super::{parse_argument, RedisCommand};

/// WAIT command implementation.
pub struct Wait {
    pub message: DataType,
    pub server_state: Arc<ServerState>,
}

impl RedisCommand for Wait {
    fn execute(&self, _: &Mutex<Storage>) -> Result<Vec<DataType>, anyhow::Error> {
        let instructions: Vec<String> = self.message.as_string_vec()?;

        if instructions.len() != 3 {
            return Err(
                RedisError::new("ERR wrong number of arguments for 'wait' command").into()
            );
        }
        let requested_replicas: i64 = parse_argument(&instructions[1], RedisError::not_an_integer())?;
        let timeout_ms: i64 = parse_argument(&instructions[2], RedisError::not_an_integer())?;

        let connected_replicas = self.server_state.replica_count()?;
        debug!(
            "WAIT {} {}: {} replica(s) connected",
            requested_replicas, timeout_ms, connected_replicas
        );

        Ok(vec![protocol::integer(connected_replicas as i64)])
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
        "WAIT"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{TcpListener, TcpStream};
    use crate::commands::{client_error_message, command_message, create_test_storage};

    fn wait(parts: &[&str], server_state: &Arc<ServerState>) -> Wait {
        Wait {
            message: command_message(parts),
            server_state: Arc::clone(server_state),
        }
    }

    fn master() -> Arc<ServerState> {
        Arc::new(ServerState::new(None, 6379))
    }

    #[test]
    fn test_wait_without_replicas_answers_zero() -> anyhow::Result<()> {
        let server_state = master();
        let storage = create_test_storage();

        let result = wait(&["WAIT", "0", "60000"], &server_state).execute(&storage)?;

        assert_eq!(result, vec![protocol::integer(0)]);
        Ok(())
    }

    #[test]
    fn test_wait_reports_the_connected_replicas() -> anyhow::Result<()> {
        // The count is the one the master holds, not the number asked for: a
        // WAIT for more replicas than exist still reports what is there.
        let server_state = master();
        let storage = create_test_storage();
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let replica = TcpStream::connect(listener.local_addr()?)?;
        server_state.register_replica(&replica)?;

        let result = wait(&["WAIT", "3", "500"], &server_state).execute(&storage)?;

        assert_eq!(result, vec![protocol::integer(1)]);
        Ok(())
    }

    #[test]
    fn test_wait_rejects_a_wrong_number_of_arguments() {
        let server_state = master();
        let storage = create_test_storage();

        for parts in [
            vec!["WAIT"],
            vec!["WAIT", "0"],
            vec!["WAIT", "0", "60000", "extra"],
        ] {
            let error = wait(&parts, &server_state).execute(&storage).unwrap_err();

            assert_eq!(
                client_error_message(error),
                "ERR wrong number of arguments for 'wait' command",
                "{:?} should be rejected",
                parts
            );
        }
    }

    #[test]
    fn test_wait_rejects_non_numeric_arguments() {
        let server_state = master();
        let storage = create_test_storage();

        for parts in [
            vec!["WAIT", "some", "60000"],
            vec!["WAIT", "0", "soon"],
        ] {
            let error = wait(&parts, &server_state).execute(&storage).unwrap_err();

            assert_eq!(
                client_error_message(error),
                "ERR value is not an integer or out of range",
                "{:?} should be rejected",
                parts
            );
        }
    }

    #[test]
    fn test_wait_is_not_propagated_to_replicas() {
        let server_state = master();

        let command = wait(&["WAIT", "0", "60000"], &server_state);

        assert!(!command.is_propagated_to_replicas());
        assert!(!command.should_always_reply());
    }
}
