//! Spawning and supervising the redis-rust binary under test.

use std::fs::File;
use std::path::PathBuf;
use std::process::{Child, Command};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use super::client::RespClient;
use super::port::{find_free_port, release_port};

const SERVER_BINARY: &str = env!("CARGO_BIN_EXE_codecrafters-redis");
const SERVER_STARTUP_TIMEOUT: Duration = Duration::from_secs(10);

/// How many times to re-allocate a port and respawn when a server loses a race
/// for its port to another process (see [`ServerProcess::start_with_retry`]).
const START_ATTEMPTS: usize = 5;

/// Log level requested from a spawned server, so that it announces the port it
/// has bound (see [`ServerProcess::wait_for_ready`]).
const SERVER_LOG_LEVEL: &str = "info";

/// How often a starting server is checked on.
const POLL_INTERVAL: Duration = Duration::from_millis(20);


/// Manages the lifecycle of a single redis-rust server process.
pub struct ServerProcess {
    pub port: u16,
    child: Option<Child>,
    /// File the server's stderr is redirected to.  A file rather than a pipe so
    /// that the harness can re-read the server's output at any point without
    /// risking a blocking read, and so that a dead server's output is complete
    /// the moment it exits.
    log_path: PathBuf,
}

impl ServerProcess {
    /// Start a **master** server on the given port.
    pub fn start_master(port: u16) -> Self {
        Self::start_with_retry(port, None)
    }

    /// Start a **replica** server that connects to `master_port`.
    pub fn start_replica(port: u16, master_port: u16) -> Self {
        Self::start_with_retry(port, Some(master_port))
    }

    /// Start a server, re-allocating the port and respawning if the port turns
    /// out to be taken by another process.
    ///
    /// A free port can only ever be *observed* free: between the moment
    /// [`find_free_port`] releases it and the moment the server binds it, another
    /// process (notably a concurrently running e2e test binary) may grab it,
    /// leaving this server to die with `Address already in use`.  That is a
    /// lost race rather than a failure of the code under test, so it is simply
    /// retried on a fresh port.
    fn start_with_retry(mut port: u16, master_port: Option<u16>) -> Self {
        for attempt in 1..=START_ATTEMPTS {
            match Self::try_start(port, master_port) {
                Ok(server) => return server,
                Err(error) if is_address_in_use(&error) && attempt < START_ATTEMPTS => {
                    eprintln!(
                        "port {} was taken by another process, retrying on a new port \
                         (attempt {}/{})",
                        port, attempt, START_ATTEMPTS
                    );
                    port = find_free_port();
                }
                Err(error) => panic!("{}", error),
            }
        }
        unreachable!("the loop either returns a server or panics");
    }

    /// Spawn the server once and wait for it to be serving on its port.
    ///
    /// The spawned child is owned by the returned `ServerProcess`, so a failed
    /// attempt kills it via `Drop` before the caller retries.
    fn try_start(port: u16, master_port: Option<u16>) -> Result<Self, String> {
        let port_arg = port.to_string();
        let mut args = vec!["--port", &port_arg];
        let replicaof = master_port.map(|mp| format!("127.0.0.1 {}", mp));
        if let Some(ref replicaof) = replicaof {
            args.extend(["--replicaof", replicaof]);
        }

        let log_path = server_log_path(port);
        let log = File::create(&log_path).expect("failed to create server log");

        // Hand the port over to the server as late as possible.
        release_port(port);

        let child = Command::new(SERVER_BINARY)
            .args(&args)
            // Log to `log_path`, so that the server announces the port it binds.
            .env("RUST_LOG", SERVER_LOG_LEVEL)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::from(log))
            .spawn()
            .expect("failed to start server");

        let mut server = Self {
            port,
            child: Some(child),
            log_path,
        };
        server.wait_for_ready()?;
        Ok(server)
    }

    /// Return a new `RespClient` connected to this server.
    pub fn client(&self) -> RespClient {
        RespClient::connect(self.port)
    }

    /// Wait until the server has bound *our* port and answers PING.
    ///
    /// A PONG on its own proves nothing: a server that lost the race for this
    /// port is answered by whoever won it, while our own child is still starting
    /// up and about to die of `Address already in use`.  The server logging the
    /// port it bound is proof that the port belongs to the child we spawned.
    fn wait_for_ready(&mut self) -> Result<(), String> {
        let bound = format!("listening on 127.0.0.1:{}", self.port);
        let deadline = Instant::now() + SERVER_STARTUP_TIMEOUT;

        while !self.log().contains(&bound) {
            if let Some(status) = self.exit_status() {
                return Err(format!(
                    "Server on port {} exited prematurely with status: {}; stderr: {}",
                    self.port,
                    status,
                    self.log().trim()
                ));
            }
            if Instant::now() > deadline {
                return Err(format!(
                    "Server on port {} never logged {:?} within {:?} — if the server's \
                     startup log changed, update `wait_for_ready`. stderr: {}",
                    self.port,
                    bound,
                    SERVER_STARTUP_TIMEOUT,
                    self.log().trim()
                ));
            }
            std::thread::sleep(POLL_INTERVAL);
        }

        // The port is bound and ours; the accept loop answers straight away.
        while Instant::now() < deadline {
            if let Ok(mut client) = RespClient::try_connect(self.port) {
                if client.send_command(&["PING"]).is_ok_and(|resp| resp == "PONG") {
                    return Ok(());
                }
            }
            std::thread::sleep(POLL_INTERVAL);
        }
        Err(format!(
            "Server on port {} did not answer PING within {:?}",
            self.port, SERVER_STARTUP_TIMEOUT
        ))
    }

    /// `Some(status)` if the server has exited, `None` while it is running.
    fn exit_status(&mut self) -> Option<std::process::ExitStatus> {
        self.child
            .as_mut()?
            .try_wait()
            .expect("failed to check child status")
    }

    /// Everything the server has logged so far.  Complete once it has exited.
    fn log(&self) -> String {
        std::fs::read_to_string(&self.log_path).unwrap_or_default()
    }

    /// Explicitly stop the server (also called by Drop).
    pub fn stop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        let _ = std::fs::remove_file(&self.log_path);
    }
}

impl Drop for ServerProcess {
    fn drop(&mut self) {
        self.stop();
    }
}

/// A unique path for one server's log.
///
/// Unique per process *and* per start, so that a server retried onto another
/// port - and servers started by test binaries running side by side - never
/// share a log file.
fn server_log_path(port: u16) -> PathBuf {
    static NEXT_ID: AtomicUsize = AtomicUsize::new(0);
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "redis-rust-e2e-{}-{}-{}.log",
        std::process::id(),
        port,
        id
    ))
}

/// Whether a server's stderr says it died because the port was already bound.
fn is_address_in_use(stderr: &str) -> bool {
    // `std::io::Error` for `EADDRINUSE` renders as
    // "Address already in use (os error 98)" on Linux and
    // "Address already in use (os error 48)" on macOS.
    stderr.contains("Address already in use") || stderr.contains("AddrInUse")
}
