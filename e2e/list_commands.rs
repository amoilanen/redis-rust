/// E2E tests for list commands: RPUSH, LPUSH, LRANGE.
///
/// Each test starts a fresh master server process, sends commands over TCP,
/// and asserts on the RESP responses. Unlike the unit/integration tests,
/// these exercise the full stack: argument parsing, TCP listener, connection
/// dispatcher, and RESP wire encoding (`:N\r\n` integers and `*N\r\n...`
/// arrays).

mod common;

use anyhow::Result;
use common::{free_port, ServerProcess};

// ========================= RPUSH =========================

#[test]
fn test_rpush_returns_new_length_as_integer() -> Result<()> {
    // Validates the wire encoding for RPUSH: it must reply with a RESP
    // integer (`:N\r\n`), which the test client surfaces as the bare number.
    let port = free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    let resp = client.send_command(&["RPUSH", "fruits", "apple", "banana", "cherry"])?;
    assert_eq!(resp, "3");
    Ok(())
}

#[test]
fn test_rpush_repeated_calls_grow_the_list() -> Result<()> {
    let port = free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    assert_eq!(client.send_command(&["RPUSH", "k", "a"])?, "1");
    assert_eq!(client.send_command(&["RPUSH", "k", "b"])?, "2");
    assert_eq!(client.send_command(&["RPUSH", "k", "c", "d"])?, "4");

    // Read the full list back to confirm append order.
    let resp = client.send_command(&["LRANGE", "k", "0", "-1"])?;
    assert_eq!(resp, "a,b,c,d");
    Ok(())
}

// ========================= LPUSH =========================

#[test]
fn test_lpush_returns_new_length_as_integer() -> Result<()> {
    let port = free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    let resp = client.send_command(&["LPUSH", "letters", "a", "b", "c"])?;
    assert_eq!(resp, "3");
    Ok(())
}

#[test]
fn test_lpush_inserts_in_reverse_order() -> Result<()> {
    // The trickiest LPUSH semantic: each value is independently inserted at
    // the head, so "a", "b", "c" -> ["c", "b", "a"]. This is the most
    // common bug surface and is worth proving over the wire.
    let port = free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    client.send_command(&["LPUSH", "letters", "a", "b", "c"])?;
    let resp = client.send_command(&["LRANGE", "letters", "0", "-1"])?;
    assert_eq!(resp, "c,b,a");
    Ok(())
}

// ========================= RPUSH + LPUSH composed =========================

#[test]
fn test_rpush_and_lpush_combined_produce_expected_order() -> Result<()> {
    let port = free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    // Start with [x, y, z] via RPUSH ...
    assert_eq!(
        client.send_command(&["RPUSH", "queue", "x", "y", "z"])?,
        "3"
    );
    // ... then prepend two via LPUSH: "b" first ([b,x,y,z]), then "a" ([a,b,x,y,z]).
    assert_eq!(client.send_command(&["LPUSH", "queue", "b", "a"])?, "5");

    let resp = client.send_command(&["LRANGE", "queue", "0", "-1"])?;
    assert_eq!(resp, "a,b,x,y,z");
    Ok(())
}

// ========================= LRANGE =========================

#[test]
fn test_lrange_full_range_via_negative_one() -> Result<()> {
    let port = free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    client.send_command(&["RPUSH", "nums", "one", "two", "three", "four", "five"])?;

    let resp = client.send_command(&["LRANGE", "nums", "0", "-1"])?;
    assert_eq!(resp, "one,two,three,four,five");
    Ok(())
}

#[test]
fn test_lrange_negative_indices_count_from_end() -> Result<()> {
    let port = free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    client.send_command(&["RPUSH", "nums", "one", "two", "three", "four", "five"])?;

    // LRANGE 1 -2 -> ["two", "three", "four"]
    let resp = client.send_command(&["LRANGE", "nums", "1", "-2"])?;
    assert_eq!(resp, "two,three,four");
    Ok(())
}

#[test]
fn test_lrange_clamps_out_of_range_stop() -> Result<()> {
    let port = free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    client.send_command(&["RPUSH", "nums", "one", "two", "three"])?;

    // Stop index well past the end is clamped to the list length.
    let resp = client.send_command(&["LRANGE", "nums", "1", "100"])?;
    assert_eq!(resp, "two,three");
    Ok(())
}

#[test]
fn test_lrange_on_nonexistent_key_returns_empty_array() -> Result<()> {
    // Wire-level behaviour: LRANGE on a key that was never created must
    // reply with an empty RESP array (`*0\r\n`), not a nil or an error.
    // The test client surfaces an empty array as an empty string.
    let port = free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    let resp = client.send_command(&["LRANGE", "never_created", "0", "-1"])?;
    assert_eq!(resp, "");
    Ok(())
}

// ========================= Concurrent clients =========================

#[test]
fn test_two_clients_share_the_same_list() -> Result<()> {
    // One client writes, another reads — verifies that the list lives in
    // shared storage across separate TCP connections (not per-connection).
    let port = free_port();
    let server = ServerProcess::start_master(port);

    let mut writer = server.client();
    let mut reader = server.client();

    writer.send_command(&["RPUSH", "shared", "first", "second", "third"])?;

    let resp = reader.send_command(&["LRANGE", "shared", "0", "-1"])?;
    assert_eq!(resp, "first,second,third");

    // The reader can also push, and the writer sees it.
    reader.send_command(&["LPUSH", "shared", "zeroth"])?;
    let resp = writer.send_command(&["LRANGE", "shared", "0", "-1"])?;
    assert_eq!(resp, "zeroth,first,second,third");
    Ok(())
}
