/// E2E tests for basic Redis commands: PING, ECHO, SET, GET, SET PX, INFO, COMMAND.
///
/// Each test starts a fresh master server process, sends commands over TCP,
/// and asserts on the RESP responses.

mod common;

use anyhow::Result;
use common::{free_port, ServerProcess};
use std::thread;
use std::time::Duration;

// ========================= PING =========================

#[test]
fn test_ping_returns_pong() -> Result<()> {
    let port = free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    let resp = client.send_command(&["PING"])?;
    assert_eq!(resp, "PONG");
    Ok(())
}

#[test]
fn test_multiple_pings() -> Result<()> {
    let port = free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    for _ in 0..10 {
        let resp = client.send_command(&["PING"])?;
        assert_eq!(resp, "PONG");
    }
    Ok(())
}

// ========================= ECHO =========================

#[test]
fn test_echo_simple() -> Result<()> {
    let port = free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    let resp = client.send_command(&["ECHO", "Hello, Redis!"])?;
    assert_eq!(resp, "Hello, Redis!");
    Ok(())
}

#[test]
fn test_echo_empty_string() -> Result<()> {
    let port = free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    let resp = client.send_command(&["ECHO", ""])?;
    assert_eq!(resp, "");
    Ok(())
}

#[test]
fn test_echo_special_characters() -> Result<()> {
    let port = free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    let resp = client.send_command(&["ECHO", "hello world !@#$%^&*()"])?;
    assert_eq!(resp, "hello world !@#$%^&*()");
    Ok(())
}

// ========================= SET / GET =========================

#[test]
fn test_set_returns_ok() -> Result<()> {
    let port = free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    let resp = client.send_command(&["SET", "testkey", "testvalue"])?;
    assert_eq!(resp, "OK");
    Ok(())
}

#[test]
fn test_get_existing_key() -> Result<()> {
    let port = free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    client.send_command(&["SET", "mykey", "myvalue"])?;
    let resp = client.send_command(&["GET", "mykey"])?;
    assert_eq!(resp, "myvalue");
    Ok(())
}

#[test]
fn test_get_nonexistent_key() -> Result<()> {
    let port = free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    let resp = client.send_command(&["GET", "definitely_does_not_exist"])?;
    assert_eq!(resp, "(nil)");
    Ok(())
}

#[test]
fn test_set_overwrites_value() -> Result<()> {
    let port = free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    client.send_command(&["SET", "ow_key", "first"])?;
    client.send_command(&["SET", "ow_key", "second"])?;
    let resp = client.send_command(&["GET", "ow_key"])?;
    assert_eq!(resp, "second");
    Ok(())
}

#[test]
fn test_multiple_keys() -> Result<()> {
    let port = free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    for i in 0..20 {
        let key = format!("key_{}", i);
        let val = format!("value_{}", i);
        client.send_command(&["SET", &key, &val])?;
    }
    for i in 0..20 {
        let key = format!("key_{}", i);
        let expected = format!("value_{}", i);
        let resp = client.send_command(&["GET", &key])?;
        assert_eq!(resp, expected, "key_{} mismatch", i);
    }
    Ok(())
}

#[test]
fn test_numeric_values() -> Result<()> {
    let port = free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    client.send_command(&["SET", "number", "42"])?;
    let resp = client.send_command(&["GET", "number"])?;
    assert_eq!(resp, "42");
    Ok(())
}

#[test]
fn test_value_with_spaces() -> Result<()> {
    let port = free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    client.send_command(&["SET", "greeting", "hello world"])?;
    let resp = client.send_command(&["GET", "greeting"])?;
    assert_eq!(resp, "hello world");
    Ok(())
}

#[test]
fn test_large_value() -> Result<()> {
    let port = free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    // Keep moderate — the server has a 1s read timeout per connection
    let large_val: String = "x".repeat(1000);
    client.send_command(&["SET", "large", &large_val])?;
    let resp = client.send_command(&["GET", "large"])?;
    assert_eq!(resp, large_val);
    Ok(())
}

// ========================= SET with PX expiration =========================

#[test]
fn test_key_exists_before_expiry() -> Result<()> {
    let port = free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    client.send_command(&["SET", "expiring", "value", "px", "5000"])?;
    let resp = client.send_command(&["GET", "expiring"])?;
    assert_eq!(resp, "value");
    Ok(())
}

#[test]
fn test_key_expires_after_timeout() -> Result<()> {
    let port = free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    client.send_command(&["SET", "short_lived", "gone_soon", "px", "500"])?;
    // Should exist immediately
    let resp = client.send_command(&["GET", "short_lived"])?;
    assert_eq!(resp, "gone_soon");

    // Wait for expiration
    thread::sleep(Duration::from_millis(800));

    // Should be gone
    let resp = client.send_command(&["GET", "short_lived"])?;
    assert_eq!(resp, "(nil)");
    Ok(())
}

#[test]
fn test_set_without_expiry_persists() -> Result<()> {
    let port = free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    client.send_command(&["SET", "persistent", "stays"])?;
    thread::sleep(Duration::from_millis(500));
    let resp = client.send_command(&["GET", "persistent"])?;
    assert_eq!(resp, "stays");
    Ok(())
}

// ========================= INCR =========================

#[test]
fn test_incr_on_existing_numeric_key() -> Result<()> {
    // Wire-level behaviour: INCR must reply with a RESP integer (`:42\r\n`),
    // which the test client surfaces as the bare number.
    let port = free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    assert_eq!(client.send_command(&["SET", "foo", "41"])?, "OK");
    assert_eq!(client.send_command(&["INCR", "foo"])?, "42");
    Ok(())
}

#[test]
fn test_incr_repeated_and_visible_to_get() -> Result<()> {
    let port = free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    client.send_command(&["SET", "counter", "5"])?;
    assert_eq!(client.send_command(&["INCR", "counter"])?, "6");
    assert_eq!(client.send_command(&["INCR", "counter"])?, "7");
    assert_eq!(client.send_command(&["GET", "counter"])?, "7");
    Ok(())
}

#[test]
fn test_incr_on_missing_key_starts_at_one() -> Result<()> {
    // Each never-before-seen key gets its own counter starting at 1, and the
    // created value is a normal string that GET can read back.
    let port = free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    assert_eq!(client.send_command(&["INCR", "foo"])?, "1");
    assert_eq!(client.send_command(&["INCR", "bar"])?, "1");
    assert_eq!(client.send_command(&["INCR", "foo"])?, "2");
    assert_eq!(client.send_command(&["GET", "foo"])?, "2");
    Ok(())
}

#[test]
fn test_incr_on_non_numeric_value_errors() -> Result<()> {
    // Wire-level behaviour: the client gets `-ERR value is not an integer or
    // out of range\r\n`, the value survives, and the connection stays usable.
    let port = free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    assert_eq!(client.send_command(&["SET", "foo", "bar"])?, "OK");

    let err = client.send_command(&["INCR", "foo"]).unwrap_err();
    assert_eq!(err.to_string(), "ERR value is not an integer or out of range");

    assert_eq!(client.send_command(&["GET", "foo"])?, "bar");
    assert_eq!(client.send_command(&["INCR", "other"])?, "1");
    Ok(())
}

// ========================= MULTI =========================

#[test]
fn test_multi_replies_ok() -> Result<()> {
    // Wire-level behaviour: `MULTI` is acknowledged with `+OK\r\n`.
    let port = free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    assert_eq!(client.send_command(&["MULTI"])?, "OK");
    Ok(())
}

#[test]
fn test_commands_after_multi_still_execute_for_now() -> Result<()> {
    // Queueing is a later stage: for now MULTI does not change how the
    // following commands behave, so they run and reply normally.
    let port = free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    assert_eq!(client.send_command(&["MULTI"])?, "OK");
    assert_eq!(client.send_command(&["SET", "foo", "41"])?, "OK");
    assert_eq!(client.send_command(&["INCR", "foo"])?, "42");
    Ok(())
}

#[test]
fn test_nested_multi_is_accepted_for_now() -> Result<()> {
    // Real Redis replies `ERR MULTI calls can not be nested`. Until that lands,
    // the second MULTI replaces the first transaction and one EXEC ends both.
    let port = free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    assert_eq!(client.send_command(&["MULTI"])?, "OK");
    assert_eq!(client.send_command(&["MULTI"])?, "OK");

    assert_eq!(client.send_command_json(&["EXEC"])?, "[]");
    let err = client.send_command(&["EXEC"]).unwrap_err();
    assert_eq!(err.to_string(), "ERR EXEC without MULTI");
    Ok(())
}

// ========================= EXEC =========================

#[test]
fn test_exec_without_multi_errors() -> Result<()> {
    let port = free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    let err = client.send_command(&["EXEC"]).unwrap_err();
    assert_eq!(err.to_string(), "ERR EXEC without MULTI");
    Ok(())
}

#[test]
fn test_exec_error_leaves_the_connection_usable() -> Result<()> {
    // A rejected EXEC is an error reply, not a connection failure.
    let port = free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    assert!(client.send_command(&["EXEC"]).is_err());
    assert_eq!(client.send_command(&["SET", "foo", "41"])?, "OK");
    assert_eq!(client.send_command(&["INCR", "foo"])?, "42");

    let err = client.send_command(&["EXEC"]).unwrap_err();
    assert_eq!(err.to_string(), "ERR EXEC without MULTI");
    Ok(())
}

#[test]
fn test_exec_after_multi_on_another_connection_still_errors() -> Result<()> {
    // B's failed EXEC must also leave A's transaction intact.
    let port = free_port();
    let server = ServerProcess::start_master(port);
    let mut client_a = server.client();
    let mut client_b = server.client();

    assert_eq!(client_a.send_command(&["MULTI"])?, "OK");

    let err = client_b.send_command(&["EXEC"]).unwrap_err();
    assert_eq!(err.to_string(), "ERR EXEC without MULTI");

    assert_eq!(client_a.send_command_json(&["EXEC"])?, "[]");
    Ok(())
}

#[test]
fn test_exec_after_multi_replies_with_an_empty_array() -> Result<()> {
    let port = free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    assert_eq!(client.send_command(&["MULTI"])?, "OK");
    assert_eq!(client.send_command_json(&["EXEC"])?, "[]");

    let err = client.send_command(&["EXEC"]).unwrap_err();
    assert_eq!(err.to_string(), "ERR EXEC without MULTI");
    Ok(())
}

#[test]
fn test_transactions_can_be_repeated_on_one_connection() -> Result<()> {
    let port = free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    for round in 1..4 {
        assert_eq!(client.send_command(&["MULTI"])?, "OK", "round {}", round);
        assert_eq!(client.send_command_json(&["EXEC"])?, "[]", "round {}", round);
    }
    Ok(())
}

#[test]
fn test_an_open_transaction_does_not_outlive_its_connection() -> Result<()> {
    let port = free_port();
    let server = ServerProcess::start_master(port);

    let mut abandoning_client = server.client();
    assert_eq!(abandoning_client.send_command(&["MULTI"])?, "OK");
    drop(abandoning_client);

    let mut fresh_client = server.client();
    let err = fresh_client.send_command(&["EXEC"]).unwrap_err();
    assert_eq!(err.to_string(), "ERR EXEC without MULTI");
    Ok(())
}

// ========================= INFO =========================

#[test]
fn test_info_replication_master() -> Result<()> {
    let port = free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    let resp = client.send_command(&["INFO", "replication"])?;
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
    Ok(())
}

// ========================= COMMAND =========================

#[test]
fn test_command_responds() -> Result<()> {
    let port = free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    let resp = client.send_command(&["COMMAND"])?;
    assert_eq!(resp, "OK");
    Ok(())
}

// ========================= Concurrent clients =========================

#[test]
fn test_multiple_clients_independent_operations() -> Result<()> {
    let port = free_port();
    let server = ServerProcess::start_master(port);

    let mut client_a = server.client();
    let mut client_b = server.client();
    let mut client_c = server.client();

    client_a.send_command(&["SET", "a_key", "a_value"])?;
    client_b.send_command(&["SET", "b_key", "b_value"])?;
    client_c.send_command(&["SET", "c_key", "c_value"])?;

    // Each client can see all keys
    assert_eq!(client_a.send_command(&["GET", "b_key"])?, "b_value");
    assert_eq!(client_b.send_command(&["GET", "c_key"])?, "c_value");
    assert_eq!(client_c.send_command(&["GET", "a_key"])?, "a_value");
    Ok(())
}

#[test]
fn test_concurrent_writes_to_same_key() -> Result<()> {
    let port = free_port();
    let server = ServerProcess::start_master(port);

    let mut client_a = server.client();
    let mut client_b = server.client();

    client_a.send_command(&["SET", "shared", "from_a"])?;
    assert_eq!(client_b.send_command(&["GET", "shared"])?, "from_a");

    client_b.send_command(&["SET", "shared", "from_b"])?;
    assert_eq!(client_a.send_command(&["GET", "shared"])?, "from_b");
    Ok(())
}
