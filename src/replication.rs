/// Replication protocol handling for master-replica synchronization.
///
/// This module implements the Redis replication handshake protocol,
/// allowing replicas to connect to a master and receive commands.

use anyhow::{anyhow, bail};
use log::*;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::time::Duration;

use crate::protocol;
use crate::io;
use crate::server_state::ServerState;

/// How long to wait for a reply from the master before giving up.
const MASTER_READ_TIMEOUT: Duration = Duration::from_secs(5);

/// Connects this node to a master as its replica and keeps replicating from it.
///
/// # Arguments
/// * `master_address` - Address of master in format "host:port"
/// * `server_state` - Server state for storing replication info, and the
///   keyspace the replicated commands are applied to
///
/// # Returns
/// Error if the handshake fails or the connection is lost
pub fn join_as_replica(
    master_address: &str,
    server_state: &Arc<ServerState>,
) -> Result<(), anyhow::Error> {
    let mut stream = connect_to_master(master_address)?;
    perform_handshake(&mut stream, server_state.port)?;

    info!("Replica listening for commands from master...");
    crate::connection::handle_connection(&mut stream, server_state, false)
}

fn connect_to_master(master_address: &str) -> Result<TcpStream, anyhow::Error> {
    let stream = TcpStream::connect(master_address)?;
    stream.set_read_timeout(Some(MASTER_READ_TIMEOUT))?;
    Ok(stream)
}

/// Runs the replication handshake: PING, the two REPLCONFs, then PSYNC.
///
/// The PSYNC reply is deliberately left unread: the FULLRESYNC line and the RDB
/// snapshot that follows it are consumed by the replication loop.
fn perform_handshake(
    stream: &mut TcpStream,
    listening_port: usize,
) -> Result<(), anyhow::Error> {
    send_command(stream, &["PING"])?;
    expect_reply(stream, "PONG", "PING")?;

    send_command(
        stream,
        &["REPLCONF", "listening-port", &listening_port.to_string()],
    )?;
    expect_reply(stream, "OK", "REPLCONF listening-port")?;

    send_command(stream, &["REPLCONF", "capa", "psync2"])?;
    expect_reply(stream, "OK", "REPLCONF capa")?;

    send_command(stream, &["PSYNC", "?", "-1"])
}

fn send_command<W: Write>(writer: &mut W, parts: &[&str]) -> Result<(), anyhow::Error> {
    let command = protocol::array(parts.iter().map(|part| protocol::bulk_string(part)).collect());
    writer.write_all(&command.serialize())?;
    Ok(())
}

fn expect_reply<R: Read>(
    reader: &mut R,
    expected: &str,
    step: &str,
) -> Result<(), anyhow::Error> {
    let reply = io::read_single_message(reader)?
        .ok_or_else(|| anyhow!("No reply from the master node to {}, expected {}", step, expected))?;
    let reply = reply.as_string()?;
    if reply != expected {
        bail!(
            "Expected {} from the master node in reply to {}, got {:?}",
            expected,
            step,
            reply
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_send_command_serializes_as_resp_array() -> Result<(), Box<dyn std::error::Error>> {
        let mut written: Vec<u8> = Vec::new();

        send_command(&mut written, &["REPLCONF", "capa", "psync2"])?;

        assert_eq!(
            written,
            b"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"
        );
        Ok(())
    }

    #[test]
    fn test_send_command_single_part() -> Result<(), Box<dyn std::error::Error>> {
        let mut written: Vec<u8> = Vec::new();

        send_command(&mut written, &["PING"])?;

        assert_eq!(written, b"*1\r\n$4\r\nPING\r\n");
        Ok(())
    }

    #[test]
    fn test_expect_reply_accepts_expected_value() -> Result<(), Box<dyn std::error::Error>> {
        let mut reader = Cursor::new(b"+PONG\r\n".to_vec());

        assert!(expect_reply(&mut reader, "PONG", "PING").is_ok());
        Ok(())
    }

    #[test]
    fn test_expect_reply_rejects_unexpected_value() {
        let mut reader = Cursor::new(b"+WHAT\r\n".to_vec());

        let error = expect_reply(&mut reader, "PONG", "PING")
            .expect_err("an unexpected reply should be an error");

        let message = error.to_string();
        assert!(message.contains("PONG"), "{}", message);
        assert!(message.contains("PING"), "{}", message);
        assert!(message.contains("WHAT"), "{}", message);
    }

    #[test]
    fn test_expect_reply_rejects_missing_reply() {
        let mut reader = Cursor::new(Vec::new());

        let error = expect_reply(&mut reader, "OK", "REPLCONF capa")
            .expect_err("a missing reply should be an error");

        let message = error.to_string();
        assert!(message.contains("REPLCONF capa"), "{}", message);
    }
}
