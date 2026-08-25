#![allow(dead_code)]
//! Shared test infrastructure for E2E tests.
//!
//! - [`find_free_port`] hands out a TCP port no other server will take
//! - [`ServerProcess`] spawns and supervises the redis-rust binary
//! - [`RespClient`] speaks just enough RESP2 to drive a server
//!
//! Each suite sees this module as a flat namespace: the split into `port`,
//! `server` and `client` is an internal one, and everything a test needs is
//! re-exported here.

mod client;
mod port;
mod server;

// Re-exported for the suites, not all of which name every type: most reach a
// `RespClient` through `ServerProcess::client` rather than by name.
#[allow(unused_imports)]
pub use client::RespClient;
pub use port::find_free_port;
pub use server::ServerProcess;

use std::time::Duration;

/// How long a replica is given to finish its handshake with the master.
const REPLICA_HANDSHAKE_WAIT: Duration = Duration::from_secs(2);

pub const REPLICATION_PROPAGATION_WAIT: Duration = Duration::from_millis(1000);

// ---------------------------------------------------------------------------
// Test setup helpers
// ---------------------------------------------------------------------------

/// Start a master and 3 replicas, returning (master, vec![replica1, replica2, replica3]).
///
/// Waits for the replica handshake to complete before returning.
pub fn start_master_and_replicas() -> (ServerProcess, Vec<ServerProcess>) {
    let master = ServerProcess::start_master(find_free_port());

    let mut replicas = Vec::new();
    for _ in 0..3 {
        // `master.port`, not the port it was asked for: a master that lost a
        // race for its port was restarted on another one, and a replica sent to
        // the original would silently never replicate anything.
        let replica = ServerProcess::start_replica(find_free_port(), master.port);
        replicas.push(replica);
    }

    // Give replicas time to complete the replication handshake
    std::thread::sleep(REPLICA_HANDSHAKE_WAIT);

    (master, replicas)
}
