//! PUBLISH command - posts a message to a channel.
//!
//! Syntax: PUBLISH <channel> <message>
//! Returns: the number of clients subscribed to the channel, as an integer:
//!
//! ```text
//! :2\r\n
//! ```
//!
//! The count is server-wide - every connection subscribed to the channel, not
//! just the publishing one - and counts each connection once, however often it
//! subscribed. The message itself is not delivered to the subscribers yet.

use std::sync::{Arc, Mutex};

use log::*;

use super::ChannelRegistry;
use crate::commands::RedisCommand;
use crate::error::RedisError;
use crate::protocol;
use crate::protocol::DataType;
use crate::storage::Storage;

/// PUBLISH command implementation.
pub struct Publish {
    pub message: DataType,
    /// The server's count of subscribers per channel.
    pub channel_registry: Arc<ChannelRegistry>,
}

impl RedisCommand for Publish {
    fn execute(&self, _: &Mutex<Storage>) -> Result<Vec<DataType>, anyhow::Error> {
        let instructions: Vec<String> = self.message.as_string_vec()?;

        let [_, channel, _message] = instructions.as_slice() else {
            return Err(
                RedisError::new("ERR wrong number of arguments for 'publish' command").into(),
            );
        };

        let subscriber_count = self.channel_registry.subscriber_count(channel)?;

        debug!("PUBLISH {}: {} subscriber(s)", channel, subscriber_count);

        Ok(vec![protocol::integer(subscriber_count as i64)])
    }

    fn is_propagated_to_replicas(&self) -> bool {
        false
    }

    fn should_always_reply(&self) -> bool {
        false
    }

    fn serialize(&self) -> Vec<u8> {
        self.message.serialize()
    }

    fn name(&self) -> &str {
        "PUBLISH"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::pubsub::Subscriptions;
    use crate::commands::{client_error_message, command_message, create_test_storage};

    /// A PUBLISH on the server whose channels `registry` keeps count of.
    fn publish(parts: &[&str], registry: &Arc<ChannelRegistry>) -> Publish {
        Publish {
            message: command_message(parts),
            channel_registry: Arc::clone(registry),
        }
    }

    /// What a PUBLISH to `channel` replies with.
    fn reply(channel: &str, registry: &Arc<ChannelRegistry>) -> anyhow::Result<Vec<DataType>> {
        publish(&["PUBLISH", channel, "msg"], registry).execute(&create_test_storage())
    }

    /// A connection on the server of `registry`, subscribed to `channels`.
    fn client(registry: &Arc<ChannelRegistry>, channels: &[&str]) -> anyhow::Result<Subscriptions> {
        let subscriptions = Subscriptions::new(Arc::clone(registry));
        for channel in channels {
            subscriptions.subscribe(channel)?;
        }
        Ok(subscriptions)
    }

    #[test]
    fn should_count_the_clients_subscribed_to_the_channel() -> anyhow::Result<()> {
        let registry = Arc::new(ChannelRegistry::new());
        let _foo = client(&registry, &["foo"])?;
        let _bar_one = client(&registry, &["bar"])?;
        let _bar_two = client(&registry, &["bar"])?;

        assert_eq!(reply("bar", &registry)?, vec![protocol::integer(2)]);
        assert_eq!(reply("foo", &registry)?, vec![protocol::integer(1)]);
        Ok(())
    }

    #[test]
    fn should_reply_zero_for_a_channel_nobody_is_subscribed_to() -> anyhow::Result<()> {
        let registry = Arc::new(ChannelRegistry::new());
        let _foo = client(&registry, &["foo"])?;

        assert_eq!(reply("baz", &registry)?, vec![protocol::integer(0)]);
        Ok(())
    }

    #[test]
    fn should_count_a_client_that_subscribed_twice_once() -> anyhow::Result<()> {
        let registry = Arc::new(ChannelRegistry::new());
        let _foo = client(&registry, &["foo", "foo"])?;

        assert_eq!(reply("foo", &registry)?, vec![protocol::integer(1)]);
        Ok(())
    }

    #[test]
    fn should_count_a_client_once_on_each_of_its_channels() -> anyhow::Result<()> {
        let registry = Arc::new(ChannelRegistry::new());
        let _both = client(&registry, &["foo", "bar"])?;

        assert_eq!(reply("foo", &registry)?, vec![protocol::integer(1)]);
        assert_eq!(reply("bar", &registry)?, vec![protocol::integer(1)]);
        Ok(())
    }

    #[test]
    fn should_stop_counting_a_client_once_its_connection_is_gone() -> anyhow::Result<()> {
        let registry = Arc::new(ChannelRegistry::new());
        let staying = client(&registry, &["foo"])?;
        let leaving = client(&registry, &["foo", "bar"])?;

        drop(leaving);

        assert_eq!(reply("foo", &registry)?, vec![protocol::integer(1)]);
        assert_eq!(reply("bar", &registry)?, vec![protocol::integer(0)]);
        drop(staying);
        Ok(())
    }

    #[test]
    fn should_not_count_subscribers_of_another_server() -> anyhow::Result<()> {
        let registry = Arc::new(ChannelRegistry::new());
        let _elsewhere = client(&Arc::new(ChannelRegistry::new()), &["foo"])?;

        assert_eq!(reply("foo", &registry)?, vec![protocol::integer(0)]);
        Ok(())
    }

    #[test]
    fn should_reject_a_call_without_exactly_a_channel_and_a_message() {
        let registry = Arc::new(ChannelRegistry::new());

        for parts in [
            &["PUBLISH"][..],
            &["PUBLISH", "foo"][..],
            &["PUBLISH", "foo", "msg", "extra"][..],
        ] {
            let error = publish(parts, &registry)
                .execute(&create_test_storage())
                .unwrap_err();

            assert_eq!(
                client_error_message(error),
                "ERR wrong number of arguments for 'publish' command",
                "{:?}",
                parts
            );
        }
    }

    #[test]
    fn should_not_propagate_a_publish_to_the_replicas() {
        let command = publish(&["PUBLISH", "foo", "msg"], &Arc::new(ChannelRegistry::new()));

        assert!(!command.is_propagated_to_replicas());
        assert_eq!(command.name(), "PUBLISH");
    }
}
