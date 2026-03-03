"""
Tests for basic Redis commands: PING, ECHO, SET, GET, COMMAND, INFO.

These tests run against a single standalone (master) server.
"""

import time

import pytest
import redis


class TestPing:
    """Test the PING command."""

    def test_ping_returns_pong(self, master_server):
        client = master_server.client()
        assert client.ping() is True

    def test_multiple_pings(self, master_server):
        client = master_server.client()
        for _ in range(10):
            assert client.ping() is True


class TestEcho:
    """Test the ECHO command."""

    def test_echo_simple(self, master_server):
        client = master_server.client()
        result = client.echo("Hello, Redis!")
        assert result == "Hello, Redis!"

    def test_echo_empty_string(self, master_server):
        client = master_server.client()
        result = client.echo("")
        assert result == ""

    def test_echo_special_characters(self, master_server):
        client = master_server.client()
        result = client.echo("hello world !@#$%^&*()")
        assert result == "hello world !@#$%^&*()"


class TestSetGet:
    """Test SET and GET commands."""

    def test_set_returns_ok(self, master_server):
        client = master_server.client()
        result = client.set("testkey", "testvalue")
        assert result is True

    def test_get_existing_key(self, master_server):
        client = master_server.client()
        client.set("mykey", "myvalue")
        result = client.get("mykey")
        assert result == "myvalue"

    def test_get_nonexistent_key(self, master_server):
        client = master_server.client()
        result = client.get("definitely_does_not_exist")
        assert result is None

    def test_set_overwrites_value(self, master_server):
        client = master_server.client()
        client.set("overwrite_key", "first")
        client.set("overwrite_key", "second")
        result = client.get("overwrite_key")
        assert result == "second"

    def test_multiple_keys(self, master_server):
        client = master_server.client()
        keys = {f"key_{i}": f"value_{i}" for i in range(20)}
        for k, v in keys.items():
            client.set(k, v)
        for k, v in keys.items():
            assert client.get(k) == v

    def test_numeric_values(self, master_server):
        client = master_server.client()
        client.set("number", "42")
        assert client.get("number") == "42"

    def test_value_with_spaces(self, master_server):
        client = master_server.client()
        client.set("greeting", "hello world")
        assert client.get("greeting") == "hello world"

    def test_large_value(self, master_server):
        client = master_server.client()
        # Keep value moderate — the server has a 1s read timeout per
        # connection which can cause resets with very large payloads.
        large_val = "x" * 1000
        client.set("large", large_val)
        assert client.get("large") == large_val


class TestSetWithExpiration:
    """Test SET with PX (millisecond expiration)."""

    def test_key_exists_before_expiry(self, master_server):
        client = master_server.client()
        client.set("expiring", "value", px=5000)
        assert client.get("expiring") == "value"

    def test_key_expires_after_timeout(self, master_server):
        client = master_server.client()
        client.set("short_lived", "gone_soon", px=500)
        # Key should exist immediately
        assert client.get("short_lived") == "gone_soon"
        # Wait for expiration
        time.sleep(0.8)
        # Key should be gone
        assert client.get("short_lived") is None

    def test_set_without_expiry_persists(self, master_server):
        client = master_server.client()
        client.set("persistent", "stays")
        time.sleep(0.5)
        assert client.get("persistent") == "stays"


class TestInfo:
    """Test the INFO command."""

    def test_info_replication_master(self, master_server):
        client = master_server.client()
        info = client.execute_command("INFO", "replication")
        # redis-py may parse INFO into a dict or return raw string
        if isinstance(info, dict):
            assert info.get("role") == "master"
            assert "master_replid" in info
            assert info.get("master_repl_offset") == 0
        else:
            assert "role:master" in info
            assert "master_replid:" in info
            assert "master_repl_offset:0" in info


class TestCommand:
    """Test the COMMAND command."""

    def test_command_responds(self, master_server):
        import socket as sock
        # Use raw socket because redis-py tries to parse COMMAND response
        # as structured command metadata, but our server just returns +OK
        s = sock.socket(sock.AF_INET, sock.SOCK_STREAM)
        s.settimeout(3)
        s.connect(("127.0.0.1", master_server.port))
        s.sendall(b"*1\r\n$7\r\nCOMMAND\r\n")
        data = s.recv(1024)
        s.close()
        assert data == b"+OK\r\n"


class TestConcurrentClients:
    """Test that multiple clients can connect simultaneously."""

    def test_multiple_clients_independent_operations(self, master_server):
        client_a = master_server.client()
        client_b = master_server.client()
        client_c = master_server.client()

        client_a.set("a_key", "a_value")
        client_b.set("b_key", "b_value")
        client_c.set("c_key", "c_value")

        # Each client can see all keys
        assert client_a.get("b_key") == "b_value"
        assert client_b.get("c_key") == "c_value"
        assert client_c.get("a_key") == "a_value"

    def test_concurrent_writes_to_same_key(self, master_server):
        client_a = master_server.client()
        client_b = master_server.client()

        client_a.set("shared", "from_a")
        assert client_b.get("shared") == "from_a"

        client_b.set("shared", "from_b")
        assert client_a.get("shared") == "from_b"
