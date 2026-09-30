//! SUBSCRIBE command - subscribes the connection to one or more channels.
//!
//! Syntax: SUBSCRIBE <channel> [channel ...]
//! Returns: one confirmation per channel, in the order the channels were
//! named. Each is a RESP array of three elements - `subscribe` as a bulk
//! string, the channel name as a bulk string, and the number of channels the
//! connection is subscribed to as an integer:
//!
//! ```text
//! *3\r\n$9\r\nsubscribe\r\n$3\r\nfoo\r\n:1\r\n
//! ```
//!
//! The count is per connection and counts distinct channels, so subscribing
//! twice to the same channel confirms it twice at the same count.

use std::sync::{Arc, Mutex};

use log::*;

use super::Subscriptions;
use crate::commands::RedisCommand;
use crate::error::RedisError;
use crate::protocol;
use crate::protocol::DataType;
use crate::storage::Storage;

/// SUBSCRIBE command implementation.
pub struct Subscribe {
    pub message: DataType,
    /// Subscriptions of the connection this SUBSCRIBE arrived on.
    pub subscriptions: Arc<Subscriptions>,
}

impl RedisCommand for Subscribe {
    fn execute(&self, _: &Mutex<Storage>) -> Result<Vec<DataType>, anyhow::Error> {
        let instructions: Vec<String> = self.message.as_string_vec()?;

        // At least one channel, as Redis has it: a bare SUBSCRIBE names
        // nothing to subscribe to.
        let Some(channels) = instructions.get(1..).filter(|instruction| !instruction.is_empty()) else {
            return Err(
                RedisError::new("ERR wrong number of arguments for 'subscribe' command").into(),
            );
        };

        let mut confirmations: Vec<DataType> = Vec::with_capacity(channels.len());
        for channel in channels {
            let subscribed_count = self.subscriptions.subscribe(channel)?;

            debug!(
                "SUBSCRIBE {}: the connection is now subscribed to {} channel(s)",
                channel, subscribed_count
            );

            confirmations.push(protocol::array(vec![
                protocol::bulk_string("subscribe"),
                protocol::bulk_string(channel),
                protocol::integer(subscribed_count as i64),
            ]));
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
        "SUBSCRIBE"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::{client_error_message, command_message, create_test_storage};

    /// A SUBSCRIBE on a connection whose subscriptions are `subscriptions`.
    fn subscribe(parts: &[&str], subscriptions: &Arc<Subscriptions>) -> Subscribe {
        Subscribe {
            message: command_message(parts),
            subscriptions: Arc::clone(subscriptions),
        }
    }

    /// The confirmations a SUBSCRIBE replies with, as the RESP values it
    /// sends - compared whole, so a simple string where a bulk one belongs, or
    /// a count sent as text, fails the assertion.
    fn confirmations(
        parts: &[&str],
        subscriptions: &Arc<Subscriptions>,
    ) -> anyhow::Result<Vec<DataType>> {
        subscribe(parts, subscriptions).execute(&create_test_storage())
    }

    /// The confirmation of a subscription to `channel`, leaving the connection
    /// subscribed to `count` channels: `subscribe` and the channel as bulk
    /// strings, the count as an integer - on the wire,
    /// `*3\r\n$9\r\nsubscribe\r\n$3\r\nfoo\r\n:1\r\n`.
    fn confirmation(channel: &str, count: i64) -> DataType {
        protocol::array(vec![
            protocol::bulk_string("subscribe"),
            protocol::bulk_string(channel),
            protocol::integer(count),
        ])
    }

    #[test]
    fn should_confirm_the_channel_it_subscribed_to() -> anyhow::Result<()> {
        let subscriptions = Arc::new(Subscriptions::new());

        assert_eq!(
            confirmations(&["SUBSCRIBE", "foo"], &subscriptions)?,
            vec![confirmation("foo", 1)]
        );
        Ok(())
    }

    #[test]
    fn should_count_up_as_the_connection_subscribes_to_more_channels() -> anyhow::Result<()> {
        let subscriptions = Arc::new(Subscriptions::new());

        assert_eq!(
            confirmations(&["SUBSCRIBE", "foo"], &subscriptions)?,
            vec![confirmation("foo", 1)]
        );
        assert_eq!(
            confirmations(&["SUBSCRIBE", "bar"], &subscriptions)?,
            vec![confirmation("bar", 2)]
        );
        Ok(())
    }

    #[test]
    fn should_confirm_a_channel_it_is_already_subscribed_to_at_the_same_count(
    ) -> anyhow::Result<()> {
        let subscriptions = Arc::new(Subscriptions::new());
        confirmations(&["SUBSCRIBE", "foo"], &subscriptions)?;

        assert_eq!(
            confirmations(&["SUBSCRIBE", "foo"], &subscriptions)?,
            vec![confirmation("foo", 1)]
        );
        Ok(())
    }

    #[test]
    fn should_confirm_every_channel_named_in_one_call() -> anyhow::Result<()> {
        let subscriptions = Arc::new(Subscriptions::new());

        assert_eq!(
            confirmations(&["SUBSCRIBE", "foo", "bar"], &subscriptions)?,
            vec![
                confirmation("foo", 1),
                confirmation("bar", 2),
            ]
        );
        Ok(())
    }

    #[test]
    fn should_leave_another_connections_subscriptions_alone() -> anyhow::Result<()> {
        let one = Arc::new(Subscriptions::new());
        let other = Arc::new(Subscriptions::new());
        confirmations(&["SUBSCRIBE", "foo"], &one)?;

        assert_eq!(
            confirmations(&["SUBSCRIBE", "bar"], &other)?,
            vec![confirmation("bar", 1)]
        );
        Ok(())
    }

    #[test]
    fn should_reject_a_call_that_names_no_channel() {
        let subscriptions = Arc::new(Subscriptions::new());

        let error = subscribe(&["SUBSCRIBE"], &subscriptions)
            .execute(&create_test_storage())
            .unwrap_err();

        assert_eq!(
            client_error_message(error),
            "ERR wrong number of arguments for 'subscribe' command"
        );
    }

    #[test]
    fn should_not_propagate_a_subscription_to_the_replicas() {
        let subscriptions = Arc::new(Subscriptions::new());
        let command = subscribe(&["SUBSCRIBE", "foo"], &subscriptions);

        assert!(!command.is_propagated_to_replicas());
        assert_eq!(command.name(), "SUBSCRIBE");
    }
}
