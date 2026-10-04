//! Pub/Sub commands and the per-connection state they share.
//!
//! What a client is subscribed to belongs to its connection, not to the
//! server: SUBSCRIBE on one client must not subscribe any other, and a
//! disconnect takes its subscriptions with it.
//!
//! A connection subscribed to at least one channel is in *subscribed mode*,
//! where only the commands that manage subscriptions - plus PING, QUIT and
//! RESET - may run; [`ensure_allowed_in_subscribed_mode`] rejects the rest.

use std::collections::HashSet;
use std::sync::{Mutex, MutexGuard};

use anyhow::{anyhow, Result};

use crate::error::RedisError;

pub mod subscribe;

pub use subscribe::Subscribe;

/// The commands a connection in subscribed mode may still run, by the
/// upper-case names `RedisCommand::name` reports.
const ALLOWED_IN_SUBSCRIBED_MODE: [&str; 9] = [
    "SUBSCRIBE",
    "UNSUBSCRIBE",
    "PSUBSCRIBE",
    "PUNSUBSCRIBE",
    "SSUBSCRIBE",
    "SUNSUBSCRIBE",
    "PING",
    "QUIT",
    "RESET",
];

/// Rejects `command_name` unless it may run in subscribed mode, with the
/// error Redis replies with: the command named in lower case, then the list
/// of what is allowed.
pub fn ensure_allowed_in_subscribed_mode(command_name: &str) -> Result<(), RedisError> {
    if ALLOWED_IN_SUBSCRIBED_MODE.contains(&command_name.to_ascii_uppercase().as_str()) {
        return Ok(());
    }
    Err(RedisError::new(&format!(
        "ERR Can't execute '{}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context",
        command_name.to_ascii_lowercase()
    )))
}

/// The channels one connection is subscribed to.
///
/// Held by the connection's [`ConnectionState`](crate::connection::ConnectionState),
/// so the subscriptions die with the client that made them. The `Mutex` is
/// internal because commands only get `&self` in `RedisCommand::execute`.
pub struct Subscriptions {
    channels: Mutex<HashSet<String>>,
}

impl Subscriptions {
    pub fn new() -> Self {
        Subscriptions { channels: Mutex::new(HashSet::new()) }
    }

    /// Subscribes this connection to `channel`, answering with the number of
    /// channels it is subscribed to afterwards - the count SUBSCRIBE reports.
    ///
    /// A channel already subscribed to is not subscribed to twice, so the
    /// count stays where it was: Redis counts distinct channels.
    pub fn subscribe(&self, channel: &str) -> Result<usize> {
        let mut channels: MutexGuard<'_, HashSet<String>> = self.lock()?;
        channels.insert(channel.to_owned());
        Ok(channels.len())
    }

    /// How many channels this connection is subscribed to.
    pub fn count(&self) -> Result<usize> {
        Ok(self.lock()?.len())
    }

    /// Whether this connection is in subscribed mode - subscribed to at least
    /// one channel - and so limited to the commands
    /// [`ensure_allowed_in_subscribed_mode`] lets through.
    pub fn is_subscribed_mode(&self) -> Result<bool> {
        Ok(self.count()? > 0)
    }

    fn lock(&self) -> Result<MutexGuard<'_, HashSet<String>>> {
        self.channels
            .lock()
            .map_err(|e| anyhow!("Failed to lock the subscriptions: {}", e))
    }
}

impl Default for Subscriptions {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_a_new_connection_is_subscribed_to_nothing() -> Result<()> {
        assert_eq!(Subscriptions::new().count()?, 0);
        Ok(())
    }

    #[test]
    fn test_each_new_channel_raises_the_count() -> Result<()> {
        let subscriptions = Subscriptions::new();

        assert_eq!(subscriptions.subscribe("foo")?, 1);
        assert_eq!(subscriptions.subscribe("bar")?, 2);
        assert_eq!(subscriptions.subscribe("baz")?, 3);
        assert_eq!(subscriptions.count()?, 3);
        Ok(())
    }

    #[test]
    fn test_subscribing_again_to_the_same_channel_leaves_the_count_alone() -> Result<()> {
        let subscriptions = Subscriptions::new();
        subscriptions.subscribe("foo")?;

        assert_eq!(subscriptions.subscribe("foo")?, 1);
        assert_eq!(subscriptions.count()?, 1);
        Ok(())
    }

    #[test]
    fn test_a_connection_enters_subscribed_mode_with_its_first_channel() -> Result<()> {
        let subscriptions = Subscriptions::new();
        assert!(!subscriptions.is_subscribed_mode()?);

        subscriptions.subscribe("foo")?;

        assert!(subscriptions.is_subscribed_mode()?);
        Ok(())
    }

    #[test]
    fn test_subscription_and_connection_commands_are_allowed_in_subscribed_mode() {
        for name in ALLOWED_IN_SUBSCRIBED_MODE {
            assert_eq!(ensure_allowed_in_subscribed_mode(name), Ok(()), "{}", name);
        }
        assert_eq!(ensure_allowed_in_subscribed_mode("subscribe"), Ok(()));
    }

    #[test]
    fn test_other_commands_are_rejected_in_subscribed_mode_naming_the_command() {
        for (name, lower) in [("SET", "set"), ("GET", "get"), ("ECHO", "echo")] {
            let error = ensure_allowed_in_subscribed_mode(name).unwrap_err();
            assert_eq!(
                error.message,
                format!(
                    "ERR Can't execute '{}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context",
                    lower
                )
            );
        }
    }

    #[test]
    fn test_connections_are_subscribed_independently() -> Result<()> {
        // Why SUBSCRIBE on one client reports 1 while another client is
        // already subscribed to something else.
        let one = Subscriptions::new();
        let other = Subscriptions::new();

        one.subscribe("foo")?;

        assert_eq!(other.count()?, 0);
        assert_eq!(one.count()?, 1);
        Ok(())
    }
}
