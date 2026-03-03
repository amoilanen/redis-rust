"""
Shared fixtures and utilities for the Redis server test suite.

This module provides:
- ServerProcess: manages starting/stopping the Redis server binary
- Pytest fixtures for spinning up master and replica servers
"""

import os
import signal
import socket
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import redis
import redis.connection
import pytest

# Path to the cargo project root (one level up from testing/)
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Path to the compiled binary (set after cargo build)
SERVER_BINARY = PROJECT_ROOT / "target" / "debug" / "codecrafters-redis"

# How long to wait for a server to start accepting connections
SERVER_STARTUP_TIMEOUT = 10.0
# How long to wait for replicas to complete the handshake
REPLICA_HANDSHAKE_WAIT = 2.0


class BareConnection(redis.connection.Connection):
    """
    A redis-py Connection subclass that skips the CLIENT SETINFO
    handshake on connect.  Our Redis implementation does not support
    the CLIENT command, so the default redis-py behaviour would hang
    waiting for a response that never comes.
    """

    def on_connect(self):
        self._parser.on_connect(self)

    def on_connect_check_health(self, check_health=True):
        self._parser.on_connect(self)


def make_client(port: int, **kwargs) -> redis.Redis:
    """Create a redis.Redis client that works with our server (no CLIENT handshake)."""
    pool_kwargs = dict(
        host="127.0.0.1",
        port=port,
        decode_responses=True,
        socket_timeout=5,
        connection_class=BareConnection,
    )
    pool_kwargs.update(kwargs)  # allow callers to override defaults
    pool = redis.ConnectionPool(**pool_kwargs)
    return redis.Redis(connection_pool=pool)


@dataclass
class ServerProcess:
    """Manages a single redis-rust server process."""

    port: int
    replicaof: Optional[str] = None  # e.g. "127.0.0.1 6379"
    process: Optional[subprocess.Popen] = field(default=None, init=False)
    env_vars: dict = field(default_factory=dict)

    def start(self) -> "ServerProcess":
        """Start the server process and wait until it accepts connections."""
        cmd = [str(SERVER_BINARY), "--port", str(self.port)]
        if self.replicaof:
            cmd.extend(["--replicaof", self.replicaof])

        env = os.environ.copy()
        env.update(self.env_vars)

        self.process = subprocess.Popen(
            cmd,
            cwd=str(PROJECT_ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
        )
        self._wait_for_ready()
        return self

    def _wait_for_ready(self) -> None:
        """Poll the server until it responds to PING or timeout is reached."""
        deadline = time.monotonic() + SERVER_STARTUP_TIMEOUT
        while time.monotonic() < deadline:
            # Check if process has crashed
            if self.process and self.process.poll() is not None:
                stdout = self.process.stdout.read().decode() if self.process.stdout else ""
                stderr = self.process.stderr.read().decode() if self.process.stderr else ""
                raise RuntimeError(
                    f"Server on port {self.port} exited prematurely "
                    f"(code={self.process.returncode}).\n"
                    f"stdout: {stdout}\nstderr: {stderr}"
                )
            try:
                client = make_client(self.port, socket_timeout=1)
                if client.ping():
                    client.close()
                    return
            except (redis.ConnectionError, redis.TimeoutError, ConnectionRefusedError, OSError):
                pass
            time.sleep(0.2)
        raise TimeoutError(
            f"Server on port {self.port} did not become ready "
            f"within {SERVER_STARTUP_TIMEOUT}s"
        )

    def stop(self) -> None:
        """Gracefully stop the server process."""
        if self.process is None:
            return
        try:
            self.process.send_signal(signal.SIGTERM)
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait(timeout=5)
        finally:
            self.process = None

    def client(self, **kwargs) -> redis.Redis:
        """Return a redis.Redis client connected to this server."""
        return make_client(self.port, **kwargs)


def _free_port(start: int = 16379) -> int:
    """Find a free port starting from `start` (simple incrementing strategy)."""
    port = start
    while port < start + 100:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind(("127.0.0.1", port))
                return port
            except OSError:
                port += 1
    raise RuntimeError("Cannot find a free port")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def cargo_build():
    """Build the project once before all tests."""
    result = subprocess.run(
        ["cargo", "build"],
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
        timeout=120,
    )
    if result.returncode != 0:
        pytest.fail(f"cargo build failed:\n{result.stderr}")


@pytest.fixture()
def master_port():
    """Return a free port for the master server."""
    return _free_port(16379)


@pytest.fixture()
def master_server(cargo_build, master_port):
    """Start a master server and yield it; stop on teardown."""
    server = ServerProcess(port=master_port)
    server.start()
    yield server
    server.stop()


@pytest.fixture()
def replica_ports(master_port):
    """Return three free ports for replica servers."""
    ports = []
    start = master_port + 1
    for _ in range(3):
        p = _free_port(start)
        ports.append(p)
        start = p + 1
    return ports


@pytest.fixture()
def replica_servers(cargo_build, master_server, replica_ports):
    """Start 3 replica servers connected to master_server; stop on teardown."""
    replicas = []
    for port in replica_ports:
        replicaof = f"127.0.0.1 {master_server.port}"
        srv = ServerProcess(port=port, replicaof=replicaof)
        srv.start()
        replicas.append(srv)
    # Give replicas a moment to complete the replication handshake
    time.sleep(REPLICA_HANDSHAKE_WAIT)
    yield replicas
    for srv in replicas:
        srv.stop()
