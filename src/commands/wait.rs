/// WAIT command - how many replicas have processed the writes so far.
///
/// Syntax: WAIT <numreplicas> <timeout>
/// Returns: the number of replicas, as a RESP integer (`:N\r\n`)
///
/// The answer is how many replicas have acknowledged the whole replication
/// stream as it stood when the WAIT was issued - not how many are connected.
/// A replica only says so when asked, so WAIT asks (`REPLCONF GETACK *`) and
/// then waits for the answers, giving up after `timeout` and reporting
/// whatever had come in by then. The number may fall short of `numreplicas`,
/// and may exceed it: more replicas than were asked for can be up to date.

use std::sync::{Arc, Mutex};
use std::time::Duration;
use log::*;
use crate::protocol;
use crate::protocol::DataType;
use crate::error::RedisError;
use crate::server_state::ServerState;
use crate::storage::Storage;
use super::{parse_argument, RedisCommand};

const NO_TIMEOUT_MS: i64 = 0;

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
        if timeout_ms < 0 {
            return Err(RedisError::new("ERR timeout is negative").into());
        }
        // A negative count asks for nothing, which any set of replicas - none
        // included - already satisfies.
        let requested_replicas = requested_replicas.max(0) as usize;
        let timeout = (timeout_ms != NO_TIMEOUT_MS).then(|| Duration::from_millis(timeout_ms as u64));

        let caught_up = self.server_state.await_replicas(requested_replicas, timeout)?;
        debug!(
            "WAIT {} {}: {} replica(s) have processed every write",
            requested_replicas, timeout_ms, caught_up
        );

        Ok(vec![protocol::integer(caught_up as i64)])
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
    use std::thread;
    use std::time::Instant;
    use crate::commands::{client_error_message, command_message, create_test_storage, set};
    use crate::server_state::ReplicaLink;

    fn wait(parts: &[&str], server_state: &Arc<ServerState>) -> Wait {
        Wait {
            message: command_message(parts),
            server_state: Arc::clone(server_state),
        }
    }

    fn master() -> Arc<ServerState> {
        Arc::new(ServerState::new(None, 6379))
    }

    fn master_with_replicas(
        replicas: usize,
    ) -> anyhow::Result<(Arc<ServerState>, TcpListener, Vec<Arc<ReplicaLink>>)> {
        let server_state = master();
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let address = listener.local_addr()?;
        let links = (0..replicas)
            .map(|_| {
                let replica = TcpStream::connect(address)?;
                server_state.register_replica(&replica)
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        Ok((server_state, listener, links))
    }

    fn propagate_a_write(server_state: &Arc<ServerState>) -> anyhow::Result<usize> {
        server_state.propagate_to_replicas(&set(&["SET", "foo", "41"]))?;
        Ok(server_state.write_offset())
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
    fn test_wait_reports_the_replicas_that_have_nothing_left_to_process() -> anyhow::Result<()> {
        // Nothing has been propagated, so every connected replica is by
        // definition up to date and the answer comes back at once.
        let (server_state, _listener, _links) = master_with_replicas(2)?;
        let storage = create_test_storage();

        let started_at = Instant::now();
        let result = wait(&["WAIT", "2", "60000"], &server_state).execute(&storage)?;

        assert_eq!(result, vec![protocol::integer(2)]);
        assert!(started_at.elapsed() < Duration::from_secs(1));
        Ok(())
    }

    #[test]
    fn test_wait_counts_the_replicas_that_acknowledged_the_writes() -> anyhow::Result<()> {
        // One replica has caught up with what was propagated and the other has
        // not, so only the first one counts.
        let (server_state, _listener, links) = master_with_replicas(2)?;
        let storage = create_test_storage();
        let propagated = propagate_a_write(&server_state)?;
        server_state.record_acknowledgement(&links[0], propagated)?;

        let result = wait(&["WAIT", "1", "60000"], &server_state).execute(&storage)?;

        assert_eq!(result, vec![protocol::integer(1)]);
        Ok(())
    }

    #[test]
    fn test_wait_waits_for_an_acknowledgement_that_is_still_on_its_way() -> anyhow::Result<()> {
        // The replica has not reported in yet, so WAIT has to ask and wait -
        // and answer as soon as the acknowledgement lands, well inside its
        // generous timeout.
        let (server_state, _listener, links) = master_with_replicas(1)?;
        let storage = create_test_storage();
        let propagated = propagate_a_write(&server_state)?;
        let acknowledging = {
            let server_state = Arc::clone(&server_state);
            let link = Arc::clone(&links[0]);
            thread::spawn(move || {
                thread::sleep(Duration::from_millis(20));
                server_state.record_acknowledgement(&link, propagated).unwrap();
            })
        };

        let started_at = Instant::now();
        let result = wait(&["WAIT", "1", "60000"], &server_state).execute(&storage)?;

        assert_eq!(result, vec![protocol::integer(1)]);
        assert!(started_at.elapsed() < Duration::from_secs(1));
        acknowledging.join().unwrap();
        Ok(())
    }

    #[test]
    fn test_wait_gives_up_on_replicas_that_never_answer() -> anyhow::Result<()> {
        // More replicas are asked for than will ever acknowledge, so the
        // timeout is spent and the ones that did are reported.
        let (server_state, _listener, _links) = master_with_replicas(2)?;
        let storage = create_test_storage();
        propagate_a_write(&server_state)?;

        let result = wait(&["WAIT", "2", "50"], &server_state).execute(&storage)?;

        assert_eq!(result, vec![protocol::integer(0)]);
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
    fn test_wait_rejects_a_negative_timeout() {
        // Rejected rather than read as "no timeout", which is what a timeout
        // of zero means: a WAIT that never returns is not what a client asking
        // to wait for -1 milliseconds meant.
        let server_state = master();
        let storage = create_test_storage();

        let error = wait(&["WAIT", "0", "-1"], &server_state).execute(&storage).unwrap_err();

        assert_eq!(client_error_message(error), "ERR timeout is negative");
    }

    #[test]
    fn test_wait_is_not_propagated_to_replicas() {
        let server_state = master();

        let command = wait(&["WAIT", "0", "60000"], &server_state);

        assert!(!command.is_propagated_to_replicas());
        assert!(!command.should_always_reply());
    }
}
