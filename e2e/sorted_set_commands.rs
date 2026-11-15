/// E2E tests for sorted set commands: ZADD, ZRANK, and their interaction
/// with TYPE.
///
/// Each test starts a fresh master server process, sends commands over TCP,
/// and asserts on the RESP responses.

mod common;

use anyhow::Result;
use common::{array, bulk, find_free_port, int, null_bulk, ServerProcess};

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

#[test]
fn test_zrank_orders_by_score_then_member() -> Result<()> {
    let port = find_free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    for (score, member) in [("100.0", "foo"), ("100.0", "bar"), ("20.0", "baz"), ("30.1", "caz"), ("40.2", "paz")] {
        assert_eq!(client.send_command(&["ZADD", "zset_key", score, member])?, "1");
    }

    // "bar" and "foo" tie on score, so they are ordered by name.
    for (member, rank) in [("caz", 1), ("baz", 0), ("foo", 4), ("bar", 3), ("paz", 2)] {
        assert_eq!(client.send_command_resp(&["ZRANK", "zset_key", member])?, int(rank));
    }
    Ok(())
}

#[test]
fn test_zrank_missing_member_or_key_is_null_bulk_string() -> Result<()> {
    let port = find_free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    assert_eq!(client.send_command(&["ZADD", "zset_key", "1.0", "member"])?, "1");

    assert_eq!(client.send_command_resp(&["ZRANK", "zset_key", "missing_member"])?, null_bulk());
    assert_eq!(client.send_command_resp(&["ZRANK", "missing_key", "member"])?, null_bulk());
    Ok(())
}

#[test]
fn test_zrank_errors_keep_connection_usable() -> Result<()> {
    let port = find_free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    let err = client.send_command(&["ZRANK", "z"]).unwrap_err();
    assert_eq!(err.to_string(), "ERR wrong number of arguments for 'zrank' command");

    assert_eq!(client.send_command(&["SET", "s", "v"])?, "OK");
    let err = client.send_command(&["ZRANK", "s", "m"]).unwrap_err();
    assert_eq!(
        err.to_string(),
        "WRONGTYPE Operation against a key holding the wrong kind of value"
    );

    assert_eq!(client.send_command(&["ZADD", "z", "1", "m"])?, "1");
    assert_eq!(client.send_command_resp(&["ZRANK", "z", "m"])?, int(0));
    Ok(())
}

#[test]
fn test_zrange_returns_values_in_index_range() -> Result<()> {
    let port = find_free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    assert_eq!(client.send_command(&["ZADD", "my_key", "1.0", "a"])?, "1");
    assert_eq!(client.send_command(&["ZADD", "my_key", "2.0", "b"])?, "1");
    assert_eq!(client.send_command(&["ZADD", "my_key", "3.0", "c"])?, "1");
    assert_eq!(client.send_command(&["ZADD", "my_key", "4.0", "d"])?, "1");
    assert_eq!(client.send_command(&["ZADD", "my_key", "5.0", "e"])?, "1");

    assert_eq!(
        client.send_command_resp(&["ZRANGE", "my_key", "1", "3"])?,
        array(vec![bulk("b"), bulk("c"), bulk("d")])
    );
    Ok(())
}