/// E2E tests for stream commands: XADD and its interaction with TYPE.
///
/// Each test starts a fresh master server process, sends commands over TCP,
/// and asserts on the RESP responses.

mod common;

use anyhow::Result;
use common::{free_port, ServerProcess};

#[test]
fn test_xadd_returns_entry_id_as_bulk_string() -> Result<()> {
    let port = free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    let resp = client.send_command(&["XADD", "stream_key", "0-1", "foo", "bar"])?;
    assert_eq!(resp, "0-1");
    Ok(())
}

#[test]
fn test_xadd_with_multiple_field_value_pairs() -> Result<()> {
    let port = free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    let resp = client.send_command(&[
        "XADD", "stream_key", "1526919030474-0", "temperature", "36", "humidity", "95",
    ])?;
    assert_eq!(resp, "1526919030474-0");
    Ok(())
}

#[test]
fn test_xadd_appends_to_existing_stream() -> Result<()> {
    let port = free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    assert_eq!(client.send_command(&["XADD", "k", "0-1", "a", "1"])?, "0-1");
    assert_eq!(client.send_command(&["XADD", "k", "0-2", "b", "2"])?, "0-2");
    Ok(())
}

#[test]
fn test_xadd_auto_generated_sequence_id() -> Result<()> {
    let port = free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    assert_eq!(client.send_command(&["XADD", "k", "1526919030473-1", "a", "1"])?, "1526919030473-1");
    assert_eq!(client.send_command(&["XADD", "k", "1526919030473-*", "b", "2"])?, "1526919030473-2");
    Ok(())
}

#[test]
fn test_xadd_rejects_invalid_id() -> Result<()> {
    let port = free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    assert_eq!(client.send_command(&["XADD", "stream_key", "1-1", "foo", "bar"])?, "1-1");

    // An ID that is not strictly greater than the top item is rejected.
    let err = client
        .send_command(&["XADD", "stream_key", "1-1", "baz", "foo"])
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        "ERR The ID specified in XADD is equal or smaller than the target stream top item"
    );

    // The connection stays usable: a valid ID still succeeds afterwards.
    assert_eq!(client.send_command(&["XADD", "stream_key", "1-2", "baz", "foo"])?, "1-2");

    Ok(())
}

#[test]
fn test_xrange_returns_entries_in_range() -> Result<()> {
    let port = free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    client.send_command(&["XADD", "stream_key", "0-1", "foo", "bar"])?;
    client.send_command(&["XADD", "stream_key", "0-2", "bar", "baz"])?;
    client.send_command(&["XADD", "stream_key", "0-3", "baz", "foo"])?;

    // Read as JSON so the assert reflects the real nested array structure:
    // an array of [id, [field, value, ...]] entries.
    let resp = client.send_command_json(&["XRANGE", "stream_key", "0-2", "0-3"])?;
    assert_eq!(resp, r#"[["0-2",["bar","baz"]],["0-3",["baz","foo"]]]"#);
    Ok(())
}

#[test]
fn test_xrange_omitted_sequence_numbers() -> Result<()> {
    let port = free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    client.send_command(&["XADD", "stream_key", "5-0", "a", "1"])?;
    client.send_command(&["XADD", "stream_key", "5-9", "b", "2"])?;
    client.send_command(&["XADD", "stream_key", "6-0", "c", "3"])?;

    // start "5" -> 5-0, end "5" -> 5-MAX captures both 5-* entries but not 6-0.
    let resp = client.send_command_json(&["XRANGE", "stream_key", "5", "5"])?;
    assert_eq!(resp, r#"[["5-0",["a","1"]],["5-9",["b","2"]]]"#);
    Ok(())
}

#[test]
fn test_xread_returns_entries_after_id_exclusive() -> Result<()> {
    let port = free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    client.send_command(&["XADD", "stream_key", "1-0", "temperature", "36"])?;
    client.send_command(&["XADD", "stream_key", "2-0", "temperature", "37"])?;
    client.send_command(&["XADD", "stream_key", "3-0", "temperature", "38"])?;
    client.send_command(&["XADD", "stream_key", "4-0", "temperature", "39"])?;

    let resp = client.send_command_json(&["XREAD", "STREAMS", "stream_key", "2-0"])?;
    assert_eq!(resp, r#"[["stream_key",[["3-0",["temperature","38"]],["4-0",["temperature","39"]]]]]"#);
    Ok(())
}

#[test]
fn test_type_of_stream_key() -> Result<()> {
    let port = free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    client.send_command(&["XADD", "stream_key", "0-1", "foo", "bar"])?;
    assert_eq!(client.send_command(&["TYPE", "stream_key"])?, "stream");
    Ok(())
}

#[test]
fn test_type_distinguishes_stream_from_string_and_none() -> Result<()> {
    let port = free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    client.send_command(&["XADD", "stream_key", "0-1", "foo", "bar"])?;
    client.send_command(&["SET", "string_key", "value"])?;

    assert_eq!(client.send_command(&["TYPE", "stream_key"])?, "stream");
    assert_eq!(client.send_command(&["TYPE", "string_key"])?, "string");
    assert_eq!(client.send_command(&["TYPE", "missing_key"])?, "none");
    Ok(())
}

#[test]
fn test_type_reflects_overwrite_from_stream_to_string() -> Result<()> {
    let port = free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    client.send_command(&["XADD", "k", "0-1", "foo", "bar"])?;
    assert_eq!(client.send_command(&["TYPE", "k"])?, "stream");

    client.send_command(&["SET", "k", "now_a_string"])?;
    assert_eq!(client.send_command(&["TYPE", "k"])?, "string");
    Ok(())
}
