//! Pub/Sub commands and the per-connection state they share.
//!
//! What a client is subscribed to belongs to its connection, not to the
//! server: SUBSCRIBE on one client must not subscribe any other, and a
//! disconnect takes its subscriptions with it.

use std::collections::HashSet;
use std::sync::{Mutex, MutexGuard};

use anyhow::{anyhow, Result};

pub mod subscribe;

pub use subscribe::Subscribe;

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
