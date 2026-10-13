/// E2E tests for Pub/Sub: PUBLISH replies with how many clients - across all
/// connections - are subscribed to the channel.

mod common;

use anyhow::Result;
use common::{array, bulk, find_free_port, int, RespClient, ServerProcess};

/// A client of `server` subscribed to `channel`, its confirmation read.
fn subscriber(server: &ServerProcess, channel: &str) -> Result<RespClient> {
    let mut client = server.client();
    assert_eq!(
        client.send_command_resp(&["SUBSCRIBE", channel])?,
        array(vec![bulk("subscribe"), bulk(channel), int(1)])
    );
    Ok(client)
}

#[test]
fn test_publish_replies_with_the_number_of_subscribers_of_the_channel() -> Result<()> {
    let server = ServerProcess::start_master(find_free_port());
    let _foo = subscriber(&server, "foo")?;
    let _bar_one = subscriber(&server, "bar")?;
    let _bar_two = subscriber(&server, "bar")?;
    let mut publisher = server.client();

    assert_eq!(publisher.send_command_resp(&["PUBLISH", "bar", "msg"])?, int(2));
    assert_eq!(publisher.send_command_resp(&["PUBLISH", "foo", "msg"])?, int(1));
    Ok(())
}

#[test]
fn test_publish_to_a_channel_without_subscribers_replies_zero() -> Result<()> {
    let server = ServerProcess::start_master(find_free_port());
    let mut publisher = server.client();

    assert_eq!(publisher.send_command_resp(&["PUBLISH", "foo", "msg"])?, int(0));
    Ok(())
}

#[test]
fn test_a_client_subscribed_twice_is_counted_once() -> Result<()> {
    let server = ServerProcess::start_master(find_free_port());
    let mut foo = subscriber(&server, "foo")?;
    assert_eq!(
        foo.send_command_resp(&["SUBSCRIBE", "foo"])?,
        array(vec![bulk("subscribe"), bulk("foo"), int(1)])
    );
    let mut publisher = server.client();

    assert_eq!(publisher.send_command_resp(&["PUBLISH", "foo", "msg"])?, int(1));
    Ok(())
}
