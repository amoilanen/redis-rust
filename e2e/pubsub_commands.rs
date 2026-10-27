/// E2E tests for Pub/Sub: PUBLISH delivers its message to every client -
/// across all connections - subscribed to the channel, and replies with how
/// many that was.

mod common;

use std::time::Duration;

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

/// The array a subscriber of `channel` is sent when `message` is published.
fn delivery(channel: &str, message: &str) -> common::RespValue {
    array(vec![bulk("message"), bulk(channel), bulk(message)])
}

/// How long a client is watched for a message that must not arrive.
const NOTHING_ARRIVES_WAIT: Duration = Duration::from_millis(300);

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

#[test]
fn test_published_messages_reach_the_subscribers_of_the_channel_only() -> Result<()> {
    let server = ServerProcess::start_master(find_free_port());
    let mut foo_one = subscriber(&server, "foo")?;
    let mut foo_two = subscriber(&server, "foo")?;
    let mut bar = subscriber(&server, "bar")?;
    let mut publisher = server.client();

    assert_eq!(publisher.send_command_resp(&["PUBLISH", "foo", "hello"])?, int(2));
    assert_eq!(foo_one.read_response_resp()?, delivery("foo", "hello"));
    assert_eq!(foo_two.read_response_resp()?, delivery("foo", "hello"));
    assert!(!bar.has_pending_response(NOTHING_ARRIVES_WAIT)?);

    assert_eq!(publisher.send_command_resp(&["PUBLISH", "bar", "world"])?, int(1));
    assert_eq!(bar.read_response_resp()?, delivery("bar", "world"));
    assert!(!foo_one.has_pending_response(NOTHING_ARRIVES_WAIT)?);
    assert!(!foo_two.has_pending_response(NOTHING_ARRIVES_WAIT)?);
    Ok(())
}

#[test]
fn test_a_client_on_several_channels_receives_each_message_once() -> Result<()> {
    let server = ServerProcess::start_master(find_free_port());
    let mut client = subscriber(&server, "foo")?;
    assert_eq!(
        client.send_command_resp(&["SUBSCRIBE", "bar"])?,
        array(vec![bulk("subscribe"), bulk("bar"), int(2)])
    );
    let mut publisher = server.client();

    assert_eq!(publisher.send_command_resp(&["PUBLISH", "foo", "one"])?, int(1));
    assert_eq!(publisher.send_command_resp(&["PUBLISH", "bar", "two"])?, int(1));

    assert_eq!(client.read_response_resp()?, delivery("foo", "one"));
    assert_eq!(client.read_response_resp()?, delivery("bar", "two"));
    assert!(!client.has_pending_response(NOTHING_ARRIVES_WAIT)?);
    Ok(())
}

#[test]
fn test_a_subscriber_still_gets_replies_to_its_own_commands() -> Result<()> {
    let server = ServerProcess::start_master(find_free_port());
    let mut client = subscriber(&server, "foo")?;
    let mut publisher = server.client();

    assert_eq!(publisher.send_command_resp(&["PUBLISH", "foo", "hello"])?, int(1));
    assert_eq!(client.read_response_resp()?, delivery("foo", "hello"));
    assert_eq!(
        client.send_command_resp(&["PING"])?,
        array(vec![bulk("pong"), bulk("")])
    );
    Ok(())
}
