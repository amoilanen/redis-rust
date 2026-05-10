#![allow(dead_code)]
/// Shared test infrastructure for E2E tests.
///
/// Provides:
/// - `ServerProcess`: spawn/stop the redis-rust binary
/// - `RespClient`: minimal RESP protocol client over raw TCP
/// - `free_port()`: find an available TCP port

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const SERVER_BINARY: &str = env!("CARGO_BIN_EXE_codecrafters-redis");
const SERVER_STARTUP_TIMEOUT: Duration = Duration::from_secs(10);
const REPLICA_HANDSHAKE_WAIT: Duration = Duration::from_secs(2);

pub const REPLICATION_PROPAGATION_WAIT: Duration = Duration::from_millis(1000);

// ---------------------------------------------------------------------------
// Port allocation
// ---------------------------------------------------------------------------

/// Find a free TCP port by binding to port 0 and reading the assigned port.
pub fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("failed to bind to port 0");
    listener.local_addr().unwrap().port()
}

// ---------------------------------------------------------------------------
// ServerProcess
// ---------------------------------------------------------------------------

/// Manages the lifecycle of a single redis-rust server process.
pub struct ServerProcess {
    pub port: u16,
    child: Option<Child>,
}

impl ServerProcess {
    /// Start a **master** server on the given port.
    pub fn start_master(port: u16) -> Self {
        let child = Command::new(SERVER_BINARY)
            .args(["--port", &port.to_string()])
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .expect("failed to start server");

        let mut server = Self {
            port,
            child: Some(child),
        };
        server.wait_for_ready();
        server
    }

    /// Start a **replica** server that connects to `master_port`.
    pub fn start_replica(port: u16, master_port: u16) -> Self {
        let replicaof = format!("127.0.0.1 {}", master_port);
        let child = Command::new(SERVER_BINARY)
            .args(["--port", &port.to_string(), "--replicaof", &replicaof])
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .expect("failed to start replica");

        let mut server = Self {
            port,
            child: Some(child),
        };
        server.wait_for_ready();
        server
    }

    /// Return a new `RespClient` connected to this server.
    pub fn client(&self) -> RespClient {
        RespClient::connect(self.port)
    }

    /// Poll the server with PING until it responds or the timeout expires.
    fn wait_for_ready(&mut self) {
        let deadline = Instant::now() + SERVER_STARTUP_TIMEOUT;
        while Instant::now() < deadline {
            // Check if the child exited prematurely
            if let Some(ref mut child) = self.child {
                if let Some(status) = child.try_wait().expect("failed to check child status") {
                    panic!(
                        "Server on port {} exited prematurely with status: {}",
                        self.port, status
                    );
                }
            }
            // Try to connect and PING
            if let Ok(mut client) = RespClient::try_connect(self.port) {
                if let Ok(resp) = client.send_command(&["PING"]) {
                    if resp == "PONG" {
                        return;
                    }
                }
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        panic!(
            "Server on port {} did not become ready within {:?}",
            self.port, SERVER_STARTUP_TIMEOUT
        );
    }

    /// Explicitly stop the server (also called by Drop).
    pub fn stop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

impl Drop for ServerProcess {
    fn drop(&mut self) {
        self.stop();
    }
}

// ---------------------------------------------------------------------------
// RespClient — minimal RESP2 client
// ---------------------------------------------------------------------------

/// A minimal Redis client that speaks just enough RESP2 to drive E2E tests.
///
/// Supports:
/// - Sending commands as RESP arrays of bulk strings
/// - Reading simple strings (`+OK\r\n`), bulk strings (`$N\r\n...\r\n`),
///   null bulk strings (`$-1\r\n`), and errors (`-ERR ...\r\n`).
pub struct RespClient {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
}

impl RespClient {
    /// Connect to a server, panicking on failure.
    pub fn connect(port: u16) -> Self {
        Self::try_connect(port).unwrap_or_else(|e| {
            panic!("failed to connect to 127.0.0.1:{}: {}", port, e);
        })
    }

    /// Try to connect; returns Err on failure.
    pub fn try_connect(port: u16) -> Result<Self, std::io::Error> {
        let stream = TcpStream::connect_timeout(
            &format!("127.0.0.1:{}", port).parse().unwrap(),
            Duration::from_secs(2),
        )?;
        stream.set_read_timeout(Some(Duration::from_secs(5)))?;
        stream.set_write_timeout(Some(Duration::from_secs(5)))?;
        let writer = stream.try_clone()?;
        let reader = BufReader::new(stream);
        Ok(Self { reader, writer })
    }

    /// Send a command (as an array of bulk strings) and read the response.
    ///
    /// Returns the response as a `String`.  For null bulk strings, returns
    /// the special value `"(nil)"`.
    pub fn send_command(&mut self, args: &[&str]) -> anyhow::Result<String> {
        // Serialize as RESP array of bulk strings
        let mut buf = format!("*{}\r\n", args.len());
        for arg in args {
            buf.push_str(&format!("${}\r\n{}\r\n", arg.len(), arg));
        }
        self.writer.write_all(buf.as_bytes())?;
        self.writer.flush()?;

        self.read_response()
    }

    /// Read a single RESP response from the stream.
    pub fn read_response(&mut self) -> anyhow::Result<String> {
        let mut line = String::new();
        self.reader.read_line(&mut line)?;
        let line = line.trim_end_matches("\r\n").trim_end_matches('\n');

        if line.is_empty() {
            anyhow::bail!("empty response");
        }

        let prefix = &line[..1];
        let payload = &line[1..];

        match prefix {
            // Simple string: +OK
            "+" => Ok(payload.to_string()),
            // Error: -ERR ...
            "-" => Err(anyhow::anyhow!("{}", payload)),
            // Integer: :42
            ":" => Ok(payload.to_string()),
            // Bulk string: $N\r\n<data>\r\n  or  $-1\r\n
            "$" => {
                let len: i64 = payload.parse()?;
                if len < 0 {
                    return Ok("(nil)".to_string());
                }
                let len = len as usize;
                let mut data = vec![0u8; len + 2]; // +2 for trailing \r\n
                self.reader.read_exact(&mut data)?;
                data.truncate(len); // strip \r\n
                Ok(String::from_utf8(data)?)
            }
            // Array: *N\r\n  (read N elements)
            "*" => {
                let count: i64 = payload.parse()?;
                if count < 0 {
                    return Ok("(nil)".to_string());
                }
                let mut parts = Vec::new();
                for _ in 0..count {
                    parts.push(self.read_response()?);
                }
                // Return as comma-separated for simple inspection
                Ok(parts.join(","))
            }
            _ => Err(anyhow::anyhow!("unknown RESP prefix: {}", prefix)),
        }
    }
}

// ---------------------------------------------------------------------------
// Test setup helpers
// ---------------------------------------------------------------------------

/// Start a master and 3 replicas, returning (master, vec![replica1, replica2, replica3]).
///
/// Waits for the replica handshake to complete before returning.
pub fn start_master_and_replicas() -> (ServerProcess, Vec<ServerProcess>) {
    let master_port = free_port();
    let master = ServerProcess::start_master(master_port);

    let mut replicas = Vec::new();
    for _ in 0..3 {
        let rp = free_port();
        let replica = ServerProcess::start_replica(rp, master_port);
        replicas.push(replica);
    }

    // Give replicas time to complete the replication handshake
    std::thread::sleep(REPLICA_HANDSHAKE_WAIT);

    (master, replicas)
}
