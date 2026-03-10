/// E2E tests for basic Redis commands: PING, ECHO, SET, GET, SET PX, INFO, COMMAND.
///
/// Each test starts a fresh master server process, sends commands over TCP,
/// and asserts on the RESP responses.

mod common;

use common::{free_port, ServerProcess};
use std::thread;
use std::time::Duration;

// ========================= PING =========================

#[test]
fn test_ping_returns_pong() {
    let port = free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    let resp = client.send_command(&["PING"]).unwrap();
    assert_eq!(resp, "PONG");
}

#[test]
fn test_multiple_pings() {
    let port = free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    for _ in 0..10 {
        let resp = client.send_command(&["PING"]).unwrap();
        assert_eq!(resp, "PONG");
    }
}

// ========================= ECHO =========================

#[test]
fn test_echo_simple() {
    let port = free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    let resp = client.send_command(&["ECHO", "Hello, Redis!"]).unwrap();
    assert_eq!(resp, "Hello, Redis!");
}

#[test]
fn test_echo_empty_string() {
    let port = free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    let resp = client.send_command(&["ECHO", ""]).unwrap();
    assert_eq!(resp, "");
}

#[test]
fn test_echo_special_characters() {
    let port = free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    let resp = client
        .send_command(&["ECHO", "hello world !@#$%^&*()"])
        .unwrap();
    assert_eq!(resp, "hello world !@#$%^&*()");
}

// ========================= SET / GET =========================

#[test]
fn test_set_returns_ok() {
    let port = free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    let resp = client.send_command(&["SET", "testkey", "testvalue"]).unwrap();
    assert_eq!(resp, "OK");
}

#[test]
fn test_get_existing_key() {
    let port = free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    client.send_command(&["SET", "mykey", "myvalue"]).unwrap();
    let resp = client.send_command(&["GET", "mykey"]).unwrap();
    assert_eq!(resp, "myvalue");
}

#[test]
fn test_get_nonexistent_key() {
    let port = free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    let resp = client
        .send_command(&["GET", "definitely_does_not_exist"])
        .unwrap();
    assert_eq!(resp, "(nil)");
}

#[test]
fn test_set_overwrites_value() {
    let port = free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    client.send_command(&["SET", "ow_key", "first"]).unwrap();
    client.send_command(&["SET", "ow_key", "second"]).unwrap();
    let resp = client.send_command(&["GET", "ow_key"]).unwrap();
    assert_eq!(resp, "second");
}

#[test]
fn test_multiple_keys() {
    let port = free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    for i in 0..20 {
        let key = format!("key_{}", i);
        let val = format!("value_{}", i);
        client.send_command(&["SET", &key, &val]).unwrap();
    }
    for i in 0..20 {
        let key = format!("key_{}", i);
        let expected = format!("value_{}", i);
        let resp = client.send_command(&["GET", &key]).unwrap();
        assert_eq!(resp, expected, "key_{} mismatch", i);
    }
}

#[test]
fn test_numeric_values() {
    let port = free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    client.send_command(&["SET", "number", "42"]).unwrap();
    let resp = client.send_command(&["GET", "number"]).unwrap();
    assert_eq!(resp, "42");
}

#[test]
fn test_value_with_spaces() {
    let port = free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    client
        .send_command(&["SET", "greeting", "hello world"])
        .unwrap();
    let resp = client.send_command(&["GET", "greeting"]).unwrap();
    assert_eq!(resp, "hello world");
}

#[test]
fn test_large_value() {
    let port = free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    // Keep moderate — the server has a 1s read timeout per connection
    let large_val: String = "x".repeat(1000);
    client.send_command(&["SET", "large", &large_val]).unwrap();
    let resp = client.send_command(&["GET", "large"]).unwrap();
    assert_eq!(resp, large_val);
}

// ========================= SET with PX expiration =========================

#[test]
fn test_key_exists_before_expiry() {
    let port = free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    client
        .send_command(&["SET", "expiring", "value", "px", "5000"])
        .unwrap();
    let resp = client.send_command(&["GET", "expiring"]).unwrap();
    assert_eq!(resp, "value");
}

#[test]
fn test_key_expires_after_timeout() {
    let port = free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    client
        .send_command(&["SET", "short_lived", "gone_soon", "px", "500"])
        .unwrap();
    // Should exist immediately
    let resp = client.send_command(&["GET", "short_lived"]).unwrap();
    assert_eq!(resp, "gone_soon");

    // Wait for expiration
    thread::sleep(Duration::from_millis(800));

    // Should be gone
    let resp = client.send_command(&["GET", "short_lived"]).unwrap();
    assert_eq!(resp, "(nil)");
}

#[test]
fn test_set_without_expiry_persists() {
    let port = free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    client
        .send_command(&["SET", "persistent", "stays"])
        .unwrap();
    thread::sleep(Duration::from_millis(500));
    let resp = client.send_command(&["GET", "persistent"]).unwrap();
    assert_eq!(resp, "stays");
}

// ========================= INFO =========================

#[test]
fn test_info_replication_master() {
    let port = free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    let resp = client.send_command(&["INFO", "replication"]).unwrap();
    assert!(
        resp.contains("role:master"),
        "expected role:master in: {}",
        resp
    );
    assert!(
        resp.contains("master_replid:"),
        "expected master_replid in: {}",
        resp
    );
    assert!(
        resp.contains("master_repl_offset:0"),
        "expected master_repl_offset:0 in: {}",
        resp
    );
}

// ========================= COMMAND =========================

#[test]
fn test_command_responds() {
    let port = free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    let resp = client.send_command(&["COMMAND"]).unwrap();
    assert_eq!(resp, "OK");
}

// ========================= Concurrent clients =========================

#[test]
fn test_multiple_clients_independent_operations() {
    let port = free_port();
    let server = ServerProcess::start_master(port);

    let mut client_a = server.client();
    let mut client_b = server.client();
    let mut client_c = server.client();

    client_a.send_command(&["SET", "a_key", "a_value"]).unwrap();
    client_b.send_command(&["SET", "b_key", "b_value"]).unwrap();
    client_c.send_command(&["SET", "c_key", "c_value"]).unwrap();

    // Each client can see all keys
    assert_eq!(
        client_a.send_command(&["GET", "b_key"]).unwrap(),
        "b_value"
    );
    assert_eq!(
        client_b.send_command(&["GET", "c_key"]).unwrap(),
        "c_value"
    );
    assert_eq!(
        client_c.send_command(&["GET", "a_key"]).unwrap(),
        "a_value"
    );
}

#[test]
fn test_concurrent_writes_to_same_key() {
    let port = free_port();
    let server = ServerProcess::start_master(port);

    let mut client_a = server.client();
    let mut client_b = server.client();

    client_a.send_command(&["SET", "shared", "from_a"]).unwrap();
    assert_eq!(
        client_b.send_command(&["GET", "shared"]).unwrap(),
        "from_a"
    );

    client_b.send_command(&["SET", "shared", "from_b"]).unwrap();
    assert_eq!(
        client_a.send_command(&["GET", "shared"]).unwrap(),
        "from_b"
    );
}
