/// E2E tests for the TYPE command, which replies with a simple string
/// (`+string\r\n`, `+list\r\n`, `+none\r\n`).

mod common;

use anyhow::Result;
use common::{free_port, ServerProcess};

#[test]
fn test_type_of_string_key() -> Result<()> {
    let port = free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    assert_eq!(client.send_command(&["SET", "some_key", "foo"])?, "OK");
    let resp = client.send_command(&["TYPE", "some_key"])?;
    assert_eq!(resp, "string");
    Ok(())
}

#[test]
fn test_type_of_missing_key_is_none() -> Result<()> {
    let port = free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    let resp = client.send_command(&["TYPE", "missing_key"])?;
    assert_eq!(resp, "none");
    Ok(())
}

#[test]
fn test_type_of_list_key() -> Result<()> {
    let port = free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    assert_eq!(client.send_command(&["RPUSH", "mylist", "a", "b"])?, "2");
    let resp = client.send_command(&["TYPE", "mylist"])?;
    assert_eq!(resp, "list");
    Ok(())
}

#[test]
fn test_type_reflects_overwrite_from_list_to_string() -> Result<()> {
    let port = free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    client.send_command(&["RPUSH", "k", "a"])?;
    assert_eq!(client.send_command(&["TYPE", "k"])?, "list");

    client.send_command(&["SET", "k", "now_a_string"])?;
    assert_eq!(client.send_command(&["TYPE", "k"])?, "string");
    Ok(())
}

#[test]
fn test_type_of_expired_key_is_none() -> Result<()> {
    let port = free_port();
    let server = ServerProcess::start_master(port);
    let mut client = server.client();

    client.send_command(&["SET", "ephemeral", "value", "px", "100"])?;
    assert_eq!(client.send_command(&["TYPE", "ephemeral"])?, "string");

    std::thread::sleep(std::time::Duration::from_millis(200));

    assert_eq!(client.send_command(&["TYPE", "ephemeral"])?, "none");
    Ok(())
}
