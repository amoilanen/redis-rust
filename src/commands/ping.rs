/// PING command - tests server connectivity.
///
/// Syntax: PING
/// Returns: +PONG

use std::sync::{Arc, Mutex};
use crate::protocol;
use crate::protocol::DataType;
use crate::storage::Storage;
use super::RedisCommand;
use super::pubsub::Subscriptions;

/// PING command implementation.
pub struct Ping {
    pub message: DataType,
    /// Subscriptions of the connection this SUBSCRIBE arrived on.
    pub subscriptions: Arc<Subscriptions>,
}

impl RedisCommand for Ping {
    fn execute(&self, _: &Mutex<Storage>) -> Result<Vec<DataType>, anyhow::Error> {
        let resp = if self.subscriptions.is_subscribed_mode()? {
            vec![protocol::array(vec![protocol::bulk_string("PONG"), protocol::bulk_string("")])]
        } else {
            vec![protocol::simple_string("PONG")]
        };
        Ok(resp)
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
        "PING"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::{command_message, create_test_storage};

    fn ping(channels_with_subscription: &[&str]) -> Result<Ping, anyhow::Error> {
        let message = command_message(&["PING"]);
        let subscriptions = Subscriptions::new();
        for channel in channels_with_subscription.iter() {
            subscriptions.subscribe(channel)?;
        }
        Ok(Ping { message, subscriptions: Arc::new(subscriptions) })
    }

    #[test]
    fn test_ping_command() -> Result<(), anyhow::Error> {
        let cmd = ping(&Vec::new())?;

        let storage = create_test_storage();
        let result = cmd.execute(&storage).unwrap();

        assert_eq!(result, vec![protocol::simple_string("PONG")]);
        assert!(!cmd.is_propagated_to_replicas());
        assert!(!cmd.should_always_reply());
        Ok(())
    }

    #[test]
    fn test_ping_command_in_subscription_mode() -> Result<(), anyhow::Error> {
        let cmd = ping(&vec!["channel1"])?;

        let storage = create_test_storage();
        let result = cmd.execute(&storage).unwrap();

        assert_eq!(result, vec![protocol::array(vec![protocol::bulk_string("PONG"), protocol::bulk_string("")])]);
        assert!(!cmd.is_propagated_to_replicas());
        assert!(!cmd.should_always_reply());
        Ok(())
    }
}
