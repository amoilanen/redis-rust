/// E2E tests for sorted set commands: ZADD and its interaction with TYPE.
///
/// Each test starts a fresh master server process, sends commands over TCP,
/// and asserts on the RESP responses.

mod common;

use anyhow::Result;
use common::{find_free_port, ServerProcess};

#[test]
fn test_zadd_creates_sorted_set_with_one_member() -> Result<()> {
    let port = find_free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    assert_eq!(client.send_command(&["ZADD", "zset_key", "10.0", "zset_member"])?, "1");
    assert_eq!(client.send_command(&["TYPE", "zset_key"])?, "zset");
    Ok(())
}

#[test]
fn test_zadd_counts_only_new_members() -> Result<()> {
    let port = find_free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    assert_eq!(client.send_command(&["ZADD", "racer_scores", "8.0", "Sam"])?, "1");
    assert_eq!(client.send_command(&["ZADD", "racer_scores", "9.5", "Sam", "6.1", "Ford"])?, "1");
    Ok(())
}

#[test]
fn test_zadd_errors_keep_connection_usable() -> Result<()> {
    let port = find_free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    let err = client.send_command(&["ZADD", "z", "not_a_score", "m"]).unwrap_err();
    assert_eq!(err.to_string(), "ERR value is not a valid float");

    assert_eq!(client.send_command(&["SET", "s", "v"])?, "OK");
    let err = client.send_command(&["ZADD", "s", "1", "m"]).unwrap_err();
    assert_eq!(
        err.to_string(),
        "WRONGTYPE Operation against a key holding the wrong kind of value"
    );

    assert_eq!(client.send_command(&["ZADD", "z", "1", "m"])?, "1");
    Ok(())
}
