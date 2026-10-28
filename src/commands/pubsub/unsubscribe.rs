//! UNSUBSCRIBE command - unsubscribes the connection from one or more channels.
//!
//! Syntax: UNSUBSCRIBE [channel [channel ...]]
//! Returns: one confirmation per channel, in the order the channels were
//! named. Each is a RESP array of three elements - `unsubscribe` as a bulk
//! string, the channel name as a bulk string, and the number of channels the
//! connection is still subscribed to as an integer:
//!
//! ```text
//! *3\r\n$11\r\nunsubscribe\r\n$3\r\nfoo\r\n:1\r\n
//! ```
//!
//! A channel the connection is not subscribed to is confirmed all the same,
//! at an unchanged count. Naming no channel unsubscribes from every channel
//! the connection is on; when it is on none, the single confirmation carries
//! a null channel and a count of 0, as in Redis.

use std::sync::{Arc, Mutex};

use log::*;

use super::ChannelSubscriptions;
use crate::commands::RedisCommand;
use crate::protocol;
use crate::protocol::DataType;
use crate::storage::Storage;

/// UNSUBSCRIBE command implementation.
pub struct Unsubscribe {
    pub message: DataType,
    /// Channel subscriptions of the connection this UNSUBSCRIBE arrived on.
    pub channel_subscriptions: Arc<ChannelSubscriptions>,
}

/// The confirmation of leaving `channel` - null for a bare UNSUBSCRIBE on a
/// connection subscribed to nothing - with `remaining` channels left.
fn confirmation(channel: DataType, remaining: usize) -> DataType {
    protocol::array(vec![
        protocol::bulk_string("unsubscribe"),
        channel,
        protocol::integer(remaining as i64),
    ])
}

impl RedisCommand for Unsubscribe {
    fn execute(&self, _: &Mutex<Storage>) -> Result<Vec<DataType>, anyhow::Error> {
        let instructions: Vec<String> = self.message.as_string_vec()?;

        let channels: Vec<String> = match instructions.get(1..) {
            Some(named) if !named.is_empty() => named.to_vec(),
            _ => self.channel_subscriptions.channels()?,
        };

        if channels.is_empty() {
            return Ok(vec![confirmation(protocol::bulk_string_empty(), 0)]);
        }

        let mut confirmations: Vec<DataType> = Vec::with_capacity(channels.len());
        for channel in &channels {
            let remaining = self.channel_subscriptions.unsubscribe(channel)?;

            debug!(
                "UNSUBSCRIBE {}: the connection is still subscribed to {} channel(s)",
                channel, remaining
            );

            confirmations.push(confirmation(protocol::bulk_string(channel), remaining));
        }

        Ok(confirmations)
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
        "UNSUBSCRIBE"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::{command_message, create_test_storage};

    /// The confirmations an UNSUBSCRIBE replies with on a connection whose
    /// channel subscriptions are `channel_subscriptions`.
    fn confirmations(
        parts: &[&str],
        channel_subscriptions: &Arc<ChannelSubscriptions>,
    ) -> anyhow::Result<Vec<DataType>> {
        Unsubscribe {
            message: command_message(parts),
            channel_subscriptions: Arc::clone(channel_subscriptions),
        }
        .execute(&create_test_storage())
    }

    /// A connection subscribed to `channels`.
    fn subscribed_to(channels: &[&str]) -> anyhow::Result<Arc<ChannelSubscriptions>> {
        let subscriptions = Arc::new(ChannelSubscriptions::default());
        for channel in channels {
            subscriptions.subscribe(channel)?;
        }
        Ok(subscriptions)
    }

    /// The confirmation of leaving `channel` with `remaining` channels left -
    /// on the wire, `*3\r\n$11\r\nunsubscribe\r\n$3\r\nfoo\r\n:1\r\n`.
    fn left(channel: &str, remaining: usize) -> DataType {
        confirmation(protocol::bulk_string(channel), remaining)
    }

    #[test]
    fn should_count_down_as_the_connection_leaves_its_channels() -> anyhow::Result<()> {
        let subscriptions = subscribed_to(&["foo", "bar"])?;

        assert_eq!(confirmations(&["UNSUBSCRIBE", "foo"], &subscriptions)?, vec![left("foo", 1)]);
        assert_eq!(confirmations(&["UNSUBSCRIBE", "bar"], &subscriptions)?, vec![left("bar", 0)]);
        assert!(!subscriptions.is_subscribed_mode()?);
        Ok(())
    }

    #[test]
    fn should_confirm_a_channel_not_subscribed_to_at_the_same_count() -> anyhow::Result<()> {
        let subscriptions = subscribed_to(&["foo"])?;

        assert_eq!(confirmations(&["UNSUBSCRIBE", "bar"], &subscriptions)?, vec![left("bar", 1)]);
        assert_eq!(subscriptions.channels()?, vec!["foo".to_owned()]);
        Ok(())
    }

    #[test]
    fn should_confirm_every_channel_named_in_one_call() -> anyhow::Result<()> {
        let subscriptions = subscribed_to(&["foo", "bar", "baz"])?;

        assert_eq!(
            confirmations(&["UNSUBSCRIBE", "baz", "foo"], &subscriptions)?,
            vec![left("baz", 2), left("foo", 1)]
        );
        Ok(())
    }

    #[test]
    fn should_leave_every_channel_when_none_is_named() -> anyhow::Result<()> {
        let subscriptions = subscribed_to(&["foo", "bar"])?;

        assert_eq!(
            confirmations(&["UNSUBSCRIBE"], &subscriptions)?,
            vec![left("bar", 1), left("foo", 0)]
        );
        assert!(!subscriptions.is_subscribed_mode()?);
        Ok(())
    }

    #[test]
    fn should_confirm_with_a_null_channel_when_none_is_named_and_none_is_subscribed_to(
    ) -> anyhow::Result<()> {
        let subscriptions = subscribed_to(&[])?;

        assert_eq!(
            confirmations(&["UNSUBSCRIBE"], &subscriptions)?,
            vec![confirmation(protocol::bulk_string_empty(), 0)]
        );
        Ok(())
    }

    #[test]
    fn should_not_propagate_an_unsubscription_to_the_replicas() -> anyhow::Result<()> {
        let command = Unsubscribe {
            message: command_message(&["UNSUBSCRIBE", "foo"]),
            channel_subscriptions: subscribed_to(&["foo"])?,
        };

        assert!(!command.is_propagated_to_replicas());
        assert_eq!(command.name(), "UNSUBSCRIBE");
        Ok(())
    }
}
