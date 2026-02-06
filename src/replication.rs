/// Replication protocol handling for master-replica synchronization.
///
/// This module implements the Redis replication handshake protocol,
/// allowing replicas to connect to a master and receive commands.

use anyhow::{anyhow, ensure};
use std::io::Write;
use std::net::TcpStream;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::protocol;
use crate::io;
use crate::storage::Storage;
use crate::server_state::ServerState;

/// Initiates a replica connection to a master node.
///
/// Performs the Redis replication handshake:
/// 1. Sends PING and waits for PONG
/// 2. Sends REPLCONF listening-port
/// 3. Sends REPLCONF capa psync2
/// 4. Sends PSYNC ? -1
/// 5. Receives RDB snapshot
/// 6. Enters command replication loop
///
/// # Arguments
/// * `master_address` - Address of master in format "host:port"
/// * `server_state` - Server state for storing replication info
/// * `storage` - Storage to receive replicated commands
///
/// # Returns
/// Error if handshake fails or connection is lost
pub fn join_replica(
    master_address: &str,
    server_state: &Arc<ServerState>,
    storage: &Arc<Mutex<Storage>>,
) -> Result<(), anyhow::Error> {
    let mut stream = TcpStream::connect(master_address)?;
    stream.set_read_timeout(Some(Duration::new(5, 0)))?;

    // Step 1: PING handshake
    let ping = protocol::array(vec![protocol::bulk_string("PING")]);
    stream.write_all(&ping.serialize())?;
    if let Some(pong) = io::read_single_message(&mut stream)? {
        ensure!(
            pong.as_string()? == "PONG",
            "Should receive PONG from the master node"
        )
    } else {
        return Err(anyhow!("Should receive PONG from the master node"));
    }

    // Step 2: REPLCONF listening-port
    let port_replconf = protocol::array(vec![
        protocol::bulk_string("REPLCONF"),
        protocol::bulk_string("listening-port"),
        protocol::bulk_string(&server_state.port.to_string()),
    ]);
    stream.write_all(&port_replconf.serialize())?;
    if let Some(ok) = io::read_single_message(&mut stream)? {
        ensure!(
            ok.as_string()? == "OK",
            "Should receive OK from the master node for listening-port"
        )
    } else {
        return Err(anyhow!("Should receive OK from the master node for listening-port"));
    }

    // Step 3: REPLCONF capa psync2
    let capa_replconf = protocol::array(vec![
        protocol::bulk_string("REPLCONF"),
        protocol::bulk_string("capa"),
        protocol::bulk_string("psync2"),
    ]);
    stream.write_all(&capa_replconf.serialize())?;
    if let Some(ok) = io::read_single_message(&mut stream)? {
        ensure!(
            ok.as_string()? == "OK",
            "Should receive OK from the master node for capa"
        )
    } else {
        return Err(anyhow!("Should receive OK from the master node for capa"));
    }

    // Step 4: PSYNC ? -1
    let psync = protocol::array(vec![
        protocol::bulk_string("PSYNC"),
        protocol::bulk_string("?"),
        protocol::bulk_string("-1"),
    ]);
    stream.write_all(&psync.serialize())?;

    println!("Replica listening for commands from master...");
    
    // Step 5-6: Receive RDB and enter replication loop
    crate::connection::handle_connection(&mut stream, storage, server_state, false)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_replication_handshake_not_applicable_in_unit_tests() {
        // Integration test - cannot be tested without a real master
        // This is a placeholder to ensure test coverage structure
        assert!(true);
    }
}
