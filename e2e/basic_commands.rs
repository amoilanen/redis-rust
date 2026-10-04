/// E2E tests for basic Redis commands: PING, ECHO, SET, GET, SET PX, INFO, COMMAND.
///
/// Each test starts a fresh master server process, sends commands over TCP,
/// and asserts on the RESP responses.

mod common;

use anyhow::Result;
use common::{
    array, bulk, find_free_port, int, server_loaded_from_rdb, temp_dir, write_file, ServerProcess,
    RDB_FILENAME,
};
use std::thread;
use std::time::Duration;

// ========================= PING =========================

#[test]
fn test_ping_returns_pong() -> Result<()> {
    let port = find_free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    let resp = client.send_command(&["PING"])?;
    assert_eq!(resp, "PONG");
    Ok(())
}

#[test]
fn test_multiple_pings() -> Result<()> {
    let port = find_free_port();
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
    let port = find_free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    let resp = client.send_command(&["ECHO", "Hello, Redis!"])?;
    assert_eq!(resp, "Hello, Redis!");
    Ok(())
}

#[test]
fn test_echo_empty_string() -> Result<()> {
    let port = find_free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    let resp = client.send_command(&["ECHO", ""])?;
    assert_eq!(resp, "");
    Ok(())
}

#[test]
fn test_echo_special_characters() -> Result<()> {
    let port = find_free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    let resp = client.send_command(&["ECHO", "hello world !@#$%^&*()"])?;
    assert_eq!(resp, "hello world !@#$%^&*()");
    Ok(())
}

// ========================= SET / GET =========================

#[test]
fn test_set_returns_ok() -> Result<()> {
    let port = find_free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    let resp = client.send_command(&["SET", "testkey", "testvalue"])?;
    assert_eq!(resp, "OK");
    Ok(())
}

#[test]
fn test_get_existing_key() -> Result<()> {
    let port = find_free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    client.send_command(&["SET", "mykey", "myvalue"])?;
    let resp = client.send_command(&["GET", "mykey"])?;
    assert_eq!(resp, "myvalue");
    Ok(())
}

#[test]
fn test_get_nonexistent_key() -> Result<()> {
    let port = find_free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    let resp = client.send_command(&["GET", "definitely_does_not_exist"])?;
    assert_eq!(resp, "(nil)");
    Ok(())
}

#[test]
fn test_set_overwrites_value() -> Result<()> {
    let port = find_free_port();
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
    let port = find_free_port();
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
    let port = find_free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    client.send_command(&["SET", "number", "42"])?;
    let resp = client.send_command(&["GET", "number"])?;
    assert_eq!(resp, "42");
    Ok(())
}

#[test]
fn test_value_with_spaces() -> Result<()> {
    let port = find_free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    client.send_command(&["SET", "greeting", "hello world"])?;
    let resp = client.send_command(&["GET", "greeting"])?;
    assert_eq!(resp, "hello world");
    Ok(())
}

#[test]
fn test_large_value() -> Result<()> {
    let port = find_free_port();
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
    let port = find_free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    client.send_command(&["SET", "expiring", "value", "px", "5000"])?;
    let resp = client.send_command(&["GET", "expiring"])?;
    assert_eq!(resp, "value");
    Ok(())
}

#[test]
fn test_key_expires_after_timeout() -> Result<()> {
    let port = find_free_port();
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
    let port = find_free_port();
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
    let port = find_free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    assert_eq!(client.send_command(&["SET", "foo", "41"])?, "OK");
    assert_eq!(client.send_command(&["INCR", "foo"])?, "42");
    Ok(())
}

#[test]
fn test_incr_repeated_and_visible_to_get() -> Result<()> {
    let port = find_free_port();
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
    let port = find_free_port();
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
    let port = find_free_port();
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
    let port = find_free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    assert_eq!(client.send_command(&["MULTI"])?, "OK");
    Ok(())
}

#[test]
fn test_commands_after_multi_are_queued() -> Result<()> {
    // The tester's sequence: every command after MULTI is acknowledged with
    // `+QUEUED\r\n` instead of its own reply.
    let port = find_free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    assert_eq!(client.send_command(&["MULTI"])?, "OK");

    assert_eq!(client.send_command(&["SET", "foo", "41"])?, "QUEUED");
    assert_eq!(client.send_command(&["INCR", "foo"])?, "QUEUED");
    Ok(())
}

#[test]
fn test_queued_commands_do_not_touch_the_database() -> Result<()> {
    // A second connection is how the tester checks that the queued SET never
    // ran: `foo` must still be missing.
    let port = find_free_port();
    let server = ServerProcess::start_master(port);
    let mut queueing_client = server.client();
    let mut observer = server.client();

    assert_eq!(queueing_client.send_command(&["MULTI"])?, "OK");
    assert_eq!(queueing_client.send_command(&["SET", "foo", "41"])?, "QUEUED");
    assert_eq!(queueing_client.send_command(&["INCR", "foo"])?, "QUEUED");

    assert_eq!(observer.send_command(&["GET", "foo"])?, "(nil)");
    Ok(())
}

#[test]
fn test_queueing_reads_does_not_run_them_either() -> Result<()> {
    // Even a read replies `+QUEUED`, so the value seeded before MULTI is not
    // reported back until the transaction runs.
    let port = find_free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    assert_eq!(client.send_command(&["SET", "foo", "41"])?, "OK");
    assert_eq!(client.send_command(&["MULTI"])?, "OK");

    assert_eq!(client.send_command(&["GET", "foo"])?, "QUEUED");
    Ok(())
}

#[test]
fn test_queueing_only_affects_the_connection_that_sent_multi() -> Result<()> {
    let port = find_free_port();
    let server = ServerProcess::start_master(port);
    let mut queueing_client = server.client();
    let mut other_client = server.client();

    assert_eq!(queueing_client.send_command(&["MULTI"])?, "OK");
    assert_eq!(queueing_client.send_command(&["SET", "foo", "41"])?, "QUEUED");

    assert_eq!(other_client.send_command(&["SET", "bar", "1"])?, "OK");
    assert_eq!(other_client.send_command(&["INCR", "bar"])?, "2");
    Ok(())
}

#[test]
fn test_a_queued_transaction_dies_with_its_connection() -> Result<()> {
    let port = find_free_port();
    let server = ServerProcess::start_master(port);

    let mut abandoning_client = server.client();
    assert_eq!(abandoning_client.send_command(&["MULTI"])?, "OK");
    assert_eq!(abandoning_client.send_command(&["SET", "foo", "41"])?, "QUEUED");
    drop(abandoning_client);

    let mut fresh_client = server.client();
    assert_eq!(fresh_client.send_command(&["GET", "foo"])?, "(nil)");
    assert_eq!(fresh_client.send_command(&["SET", "foo", "1"])?, "OK");
    Ok(())
}

#[test]
fn test_unknown_commands_are_still_ignored_while_queueing() -> Result<()> {
    // An unrecognised command gets no reply at all, in or out of a transaction,
    // so the connection stays usable for the commands that follow it.
    let port = find_free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    assert_eq!(client.send_command(&["MULTI"])?, "OK");
    client.write_command(&["NOSUCHCOMMAND", "foo"])?;

    assert_eq!(client.send_command(&["SET", "foo", "41"])?, "QUEUED");
    Ok(())
}

#[test]
fn test_nested_multi_is_accepted_for_now() -> Result<()> {
    // Real Redis replies `ERR MULTI calls can not be nested`. Until that lands,
    // the second MULTI replaces the first transaction and one EXEC ends both.
    let port = find_free_port();
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
    let port = find_free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    let err = client.send_command(&["EXEC"]).unwrap_err();
    assert_eq!(err.to_string(), "ERR EXEC without MULTI");
    Ok(())
}

#[test]
fn test_exec_error_leaves_the_connection_usable() -> Result<()> {
    // A rejected EXEC is an error reply, not a connection failure.
    let port = find_free_port();
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
    let port = find_free_port();
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
    let port = find_free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    assert_eq!(client.send_command(&["MULTI"])?, "OK");
    assert_eq!(client.send_command_json(&["EXEC"])?, "[]");

    let err = client.send_command(&["EXEC"]).unwrap_err();
    assert_eq!(err.to_string(), "ERR EXEC without MULTI");
    Ok(())
}

#[test]
fn test_exec_executes_single_queued_command() -> Result<()> {
    // Running the queue is the next stage: for now EXEC only ends the
    // transaction, so the queued SET never reaches the database.
    let port = find_free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    assert_eq!(client.send_command(&["MULTI"])?, "OK");
    assert_eq!(client.send_command(&["SET", "foo", "41"])?, "QUEUED");
    assert_eq!(client.send_command_json(&["EXEC"])?, "[\"OK\"]");

    assert_eq!(client.send_command(&["GET", "foo"])?, "41");
    Ok(())
}

#[test]
fn test_commands_run_again_once_exec_has_ended_the_transaction() -> Result<()> {
    let port = find_free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    assert_eq!(client.send_command(&["MULTI"])?, "OK");
    assert_eq!(client.send_command(&["SET", "foo", "41"])?, "QUEUED");
    assert_eq!(client.send_command_json(&["EXEC"])?, "[\"OK\"]");

    assert_eq!(client.send_command(&["SET", "foo", "42"])?, "OK");
    assert_eq!(client.send_command(&["INCR", "foo"])?, "43");
    Ok(())
}

#[test]
fn test_several_commands_in_transaction() -> Result<()> {
    let port = find_free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    assert_eq!(client.send_command(&["MULTI"])?, "OK");
    assert_eq!(client.send_command(&["SET", "x", "1"])?, "QUEUED");
    assert_eq!(client.send_command(&["SET", "y", "3"])?, "QUEUED");
    assert_eq!(client.send_command(&["INCR", "x"])?, "QUEUED");
    assert_eq!(client.send_command_json(&["EXEC"])?, "[\"OK\",\"OK\",2]");

    assert_eq!(client.send_command(&["GET", "x"])?, "2");
    assert_eq!(client.send_command(&["GET", "y"])?, "3");
    Ok(())
}

#[test]
fn test_transactions_can_be_repeated_on_one_connection() -> Result<()> {
    let port = find_free_port();
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
    let port = find_free_port();
    let server = ServerProcess::start_master(port);

    let mut abandoning_client = server.client();
    assert_eq!(abandoning_client.send_command(&["MULTI"])?, "OK");
    drop(abandoning_client);

    let mut fresh_client = server.client();
    let err = fresh_client.send_command(&["EXEC"]).unwrap_err();
    assert_eq!(err.to_string(), "ERR EXEC without MULTI");
    Ok(())
}

// ========================= DISCARD =========================

#[test]
fn test_discard_without_multi_errors() -> Result<()> {
    let port = find_free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    let err = client.send_command(&["DISCARD"]).unwrap_err();
    assert_eq!(err.to_string(), "ERR DISCARD without MULTI");
    Ok(())
}

#[test]
fn test_discard_error_leaves_the_connection_usable() -> Result<()> {
    // A rejected EXEC is an error reply, not a connection failure.
    let port = find_free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    assert!(client.send_command(&["DISCARD"]).is_err());
    assert_eq!(client.send_command(&["SET", "foo", "41"])?, "OK");
    assert_eq!(client.send_command(&["INCR", "foo"])?, "42");

    let err = client.send_command(&["DISCARD"]).unwrap_err();
    assert_eq!(err.to_string(), "ERR DISCARD without MULTI");
    Ok(())
}

#[test]
fn test_discard_after_multi_on_another_connection_still_errors() -> Result<()> {
    // B's failed EXEC must also leave A's transaction intact.
    let port = find_free_port();
    let server = ServerProcess::start_master(port);
    let mut client_a = server.client();
    let mut client_b = server.client();

    assert_eq!(client_a.send_command(&["MULTI"])?, "OK");

    let err = client_b.send_command(&["DISCARD"]).unwrap_err();
    assert_eq!(err.to_string(), "ERR DISCARD without MULTI");

    assert_eq!(client_a.send_command(&["DISCARD"])?, "OK");
    Ok(())
}

#[test]
fn test_discard_discards_active_transaction() -> Result<()> {
    let port = find_free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    assert_eq!(client.send_command(&["MULTI"])?, "OK");
    assert_eq!(client.send_command(&["SET", "foo", "41"])?, "QUEUED");
    assert_eq!(client.send_command(&["DISCARD"])?, "OK");

    let err = client.send_command(&["EXEC"]).unwrap_err();
    assert_eq!(err.to_string(), "ERR EXEC without MULTI");
    Ok(())
}

#[test]
fn test_transaction_can_be_restarted_after_discard() -> Result<()> {
    let port = find_free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    assert_eq!(client.send_command(&["MULTI"])?, "OK");
    assert_eq!(client.send_command(&["SET", "foo", "41"])?, "QUEUED");
    assert_eq!(client.send_command(&["DISCARD"])?, "OK");

    assert_eq!(client.send_command(&["MULTI"])?, "OK");
    assert_eq!(client.send_command(&["SET", "foo", "42"])?, "QUEUED");
    assert_eq!(client.send_command_json(&["EXEC"])?, "[\"OK\"]");

    Ok(())
}

// ========================= INFO =========================

#[test]
fn test_info_replication_master() -> Result<()> {
    let port = find_free_port();
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
    let port = find_free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    let resp = client.send_command(&["COMMAND"])?;
    assert_eq!(resp, "OK");
    Ok(())
}

// ========================= Concurrent clients =========================

#[test]
fn test_multiple_clients_independent_operations() -> Result<()> {
    let port = find_free_port();
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
    let port = find_free_port();
    let server = ServerProcess::start_master(port);

    let mut client_a = server.client();
    let mut client_b = server.client();

    client_a.send_command(&["SET", "shared", "from_a"])?;
    assert_eq!(client_b.send_command(&["GET", "shared"])?, "from_a");

    client_b.send_command(&["SET", "shared", "from_b"])?;
    assert_eq!(client_a.send_command(&["GET", "shared"])?, "from_b");
    Ok(())
}

// ========================= CONFIG GET =========================

/// The directory a server under test is told to keep its RDB file in; the file
/// name it is paired with is [`RDB_FILENAME`].
///
/// Neither has to exist for `CONFIG GET` to report them back, which is all the
/// tests just below ask of them. The `KEYS` tests further down want a file
/// there to read, and make their own directory rather than share this one.
const RDB_DIR: &str = "/tmp/redis-files";

/// A master started with `--dir /tmp/redis-files --dbfilename dump.rdb`, the
/// command line the tester runs.
fn server_with_rdb_file() -> ServerProcess {
    ServerProcess::start_master_with_rdb_file(find_free_port(), std::path::Path::new(RDB_DIR), RDB_FILENAME)
}

#[test]
fn test_config_get_dir_returns_the_configured_directory() -> Result<()> {
    // Compared as parsed RESP rather than JSON: the tester wants bulk strings,
    // and the JSON rendering would pass simple ones too.
    let server = server_with_rdb_file();
    let mut client = server.client();

    let resp = client.send_command_resp(&["CONFIG", "GET", "dir"])?;

    assert_eq!(resp, array(vec![bulk("dir"), bulk("/tmp/redis-files")]));
    Ok(())
}

#[test]
fn test_config_get_dbfilename_returns_the_configured_file_name() -> Result<()> {
    let server = server_with_rdb_file();
    let mut client = server.client();

    let resp = client.send_command_resp(&["CONFIG", "GET", "dbfilename"])?;

    assert_eq!(resp, array(vec![bulk("dbfilename"), bulk("dump.rdb")]));
    Ok(())
}

#[test]
fn test_config_get_both_parameters_on_one_connection() -> Result<()> {
    // The tester's sequence: both parameters asked for in turn, down the same
    // connection.
    let server = server_with_rdb_file();
    let mut client = server.client();

    assert_eq!(
        client.send_command_resp(&["CONFIG", "GET", "dir"])?,
        array(vec![bulk("dir"), bulk("/tmp/redis-files")])
    );
    assert_eq!(
        client.send_command_resp(&["CONFIG", "GET", "dbfilename"])?,
        array(vec![bulk("dbfilename"), bulk("dump.rdb")])
    );
    Ok(())
}

#[test]
fn test_config_get_leaves_out_a_parameter_the_server_has_no_value_for() -> Result<()> {
    // Started without --dir or --dbfilename: an empty array, not an error.
    let server = ServerProcess::start_master(find_free_port());
    let mut client = server.client();

    assert_eq!(client.send_command_resp(&["CONFIG", "GET", "dir"])?, array(vec![]));
    assert_eq!(
        client.send_command_resp(&["CONFIG", "GET", "maxmemory"])?,
        array(vec![])
    );
    Ok(())
}

#[test]
fn test_config_keeps_serving_after_an_unsupported_subcommand() -> Result<()> {
    // A client-facing error, so the connection survives it.
    let server = server_with_rdb_file();
    let mut client = server.client();

    let error = client
        .send_command(&["CONFIG", "SET", "dir", "/tmp"])
        .unwrap_err()
        .to_string();

    assert!(
        error.contains("Unknown CONFIG subcommand"),
        "unexpected error: {}",
        error
    );
    assert_eq!(
        client.send_command_resp(&["CONFIG", "GET", "dir"])?,
        array(vec![bulk("dir"), bulk("/tmp/redis-files")])
    );
    Ok(())
}

// ========================= KEYS over an RDB file =========================

#[test]
fn test_keys_returns_the_single_key_in_the_rdb_file() -> Result<()> {
    // Compared as parsed RESP rather than JSON: the tester wants an array of
    // bulk strings, and the JSON rendering would pass simple ones too.
    let (server, _dir) = server_loaded_from_rdb(&[("foo", "bar")]);
    let mut client = server.client();

    let resp = client.send_command_resp(&["KEYS", "*"])?;

    assert_eq!(resp, array(vec![bulk("foo")]));
    Ok(())
}

#[test]
fn test_keys_returns_every_key_in_the_rdb_file() -> Result<()> {
    let (server, _dir) = server_loaded_from_rdb(&[("foo", "1"), ("bar", "2"), ("baz", "3")]);
    let mut client = server.client();

    // The keyspace is a hash map, so the reply comes back in no particular
    // order: compare the set of names rather than the sequence.
    let mut names: Vec<String> = client
        .send_command(&["KEYS", "*"])?
        .split(',')
        .map(str::to_owned)
        .collect();
    names.sort();

    assert_eq!(names, vec!["bar", "baz", "foo"]);
    Ok(())
}

#[test]
fn test_keys_is_empty_when_the_rdb_file_does_not_exist() -> Result<()> {
    // The directory is real, the file inside it never written: an RDB file
    // that was never saved is an empty database, not a server that refuses to
    // start.
    let dir = temp_dir();
    let server = ServerProcess::start_master_with_rdb_file(find_free_port(), dir.path(), RDB_FILENAME);
    let mut client = server.client();

    assert_eq!(client.send_command_resp(&["KEYS", "*"])?, array(vec![]));
    // And it is a working server, not a half-started one.
    assert_eq!(client.send_command(&["PING"])?, "PONG");
    Ok(())
}

#[test]
fn test_keys_is_empty_on_a_server_started_without_an_rdb_file() -> Result<()> {
    let server = ServerProcess::start_master(find_free_port());
    let mut client = server.client();

    assert_eq!(client.send_command_resp(&["KEYS", "*"])?, array(vec![]));
    Ok(())
}

#[test]
fn test_keys_matches_a_glob_pattern_rather_than_a_prefix() -> Result<()> {
    let (server, _dir) = server_loaded_from_rdb(&[("foo", "1"), ("bar", "2"), ("baz", "3")]);
    let mut client = server.client();

    assert_eq!(client.send_command_resp(&["KEYS", "foo"])?, array(vec![bulk("foo")]));
    assert_eq!(client.send_command_resp(&["KEYS", "f?o"])?, array(vec![bulk("foo")]));
    assert_eq!(client.send_command_resp(&["KEYS", "ba[r]"])?, array(vec![bulk("bar")]));
    assert_eq!(client.send_command_resp(&["KEYS", "nothing*"])?, array(vec![]));
    Ok(())
}

#[test]
fn test_values_are_loaded_from_the_rdb_file_too() -> Result<()> {
    // KEYS names the keys; the values came along with them.
    let (server, _dir) = server_loaded_from_rdb(&[("foo", "bar")]);
    let mut client = server.client();

    assert_eq!(client.send_command(&["GET", "foo"])?, "bar");
    Ok(())
}

#[test]
fn test_keys_sees_what_was_written_after_the_rdb_file_was_loaded() -> Result<()> {
    // A loaded database is a starting point, not a frozen one.
    let (server, _dir) = server_loaded_from_rdb(&[("foo", "bar")]);
    let mut client = server.client();

    client.send_command(&["SET", "added", "later"])?;

    let mut names: Vec<String> = client
        .send_command(&["KEYS", "*"])?
        .split(',')
        .map(str::to_owned)
        .collect();
    names.sort();

    assert_eq!(names, vec!["added", "foo"]);
    Ok(())
}

#[test]
fn test_server_starts_empty_rather_than_failing_on_a_corrupt_rdb_file() -> Result<()> {
    // Real Redis refuses to start on a file it cannot read. Coming up with an
    // empty database is the friendlier answer, and the important half is that
    // the server comes up at all.
    let dir = temp_dir();
    write_file(&dir, RDB_FILENAME, b"this is not an RDB file at all");
    let server = ServerProcess::start_master_with_rdb_file(find_free_port(), dir.path(), RDB_FILENAME);
    let mut client = server.client();

    assert_eq!(client.send_command(&["PING"])?, "PONG");
    assert_eq!(client.send_command_resp(&["KEYS", "*"])?, array(vec![]));
    Ok(())
}

// ========================= SUBSCRIBE =========================

#[test]
fn test_subscribe_confirms_the_channel_and_the_count() -> Result<()> {
    // Compared as parsed RESP rather than JSON: the tester wants `subscribe`
    // and the channel as bulk strings, and the JSON rendering would pass
    // simple ones too.
    let port = find_free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    let resp = client.send_command_resp(&["SUBSCRIBE", "foo"])?;

    assert_eq!(resp, array(vec![bulk("subscribe"), bulk("foo"), int(1)]));
    Ok(())
}

#[test]
fn test_subscribe_counts_the_channels_of_the_connection_it_arrived_on() -> Result<()> {
    // Each client counts its own subscriptions: the second client's first
    // SUBSCRIBE reports 1, however many channels the first one has taken.
    let port = find_free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    assert_eq!(
        client.send_command_json(&["SUBSCRIBE", "foo"])?,
        r#"["subscribe","foo",1]"#
    );
    assert_eq!(
        client.send_command_json(&["SUBSCRIBE", "bar"])?,
        r#"["subscribe","bar",2]"#
    );

    let mut other_client = server.client();
    assert_eq!(
        other_client.send_command_json(&["SUBSCRIBE", "baz"])?,
        r#"["subscribe","baz",1]"#
    );
    Ok(())
}
