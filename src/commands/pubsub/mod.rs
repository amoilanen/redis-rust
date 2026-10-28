//! Pub/Sub commands and the per-connection state they share.
//!
//! What a client is subscribed to is recorded server-wide, in the
//! [`ChannelRegistry`]: PUBLISH reaches across connections - it delivers its
//! message to every client subscribed to the channel - so the registry is
//! where it finds them. A connection is known there by its
//! [`ConnectionId`]; how to reach it is the business of the
//! [`ConnectionRegistry`](crate::connection_registry::ConnectionRegistry).
//!
//! Each connection works on its own entries through its
//! [`ChannelSubscriptions`]: SUBSCRIBE on one client must not subscribe any
//! other, and a disconnect takes the client's subscriptions with it.
//!
//! A connection subscribed to at least one channel is in *subscribed mode*,
//! where only the commands that manage subscriptions - plus PING, QUIT and
//! RESET - may run; [`ensure_allowed_in_subscribed_mode`] rejects the rest.

use std::sync::{Arc, Mutex, MutexGuard};

use anyhow::{anyhow, Result};

use crate::connection_registry::ConnectionId;
use crate::error::RedisError;
use membership::Membership;

mod membership;
pub mod publish;
pub mod subscribe;
pub mod unsubscribe;

pub use publish::Publish;
pub use subscribe::Subscribe;
pub use unsubscribe::Unsubscribe;

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

/// Who is subscribed to which channel, server-wide - the one record of it.
///
/// Kept in both directions by [`Membership`], which alone can change them and
/// keeps them in agreement; the registry holds it under one lock so no reader
/// sees one direction updated without the other.
///
/// Only ever changed through [`ChannelSubscriptions`], which subscribes its
/// own connection and takes it off every channel once the connection ends.
pub struct ChannelRegistry {
    membership: Mutex<Membership>,
}

impl ChannelRegistry {
    pub fn new() -> Self {
        ChannelRegistry { membership: Mutex::new(Membership::default()) }
    }

    /// The connections subscribed to `channel`, each once.
    pub fn subscribers(&self, channel: &str) -> Result<Vec<ConnectionId>> {
        Ok(self.lock()?.subscribers(channel))
    }

    /// Subscribes `connection_id` to `channel`, answering with the number of
    /// channels it is subscribed to afterwards. A channel it is already on is
    /// not counted twice.
    fn subscribe(&self, connection_id: ConnectionId, channel: &str) -> Result<usize> {
        Ok(self.lock()?.subscribe(connection_id, channel))
    }

    /// How many channels `connection_id` is subscribed to.
    fn channel_count(&self, connection_id: ConnectionId) -> Result<usize> {
        Ok(self.lock()?.channel_count(connection_id))
    }

    /// Unsubscribes `connection_id` from `channel`, answering with the number
    /// of channels it is still subscribed to. A channel it is not on changes
    /// nothing, so the count stays where it was.
    fn unsubscribe(&self, connection_id: ConnectionId, channel: &str) -> Result<usize> {
        Ok(self.lock()?.unsubscribe(connection_id, channel))
    }

    /// The channels `connection_id` is subscribed to, in name order.
    fn channels(&self, connection_id: ConnectionId) -> Result<Vec<String>> {
        Ok(self.lock()?.channels(connection_id))
    }

    /// Takes `connection_id` off every channel it is on.
    ///
    /// Run as a connection ends, so a poisoned lock is no reason to skip it:
    /// the entries it still holds are exactly what has to be undone.
    fn leave(&self, connection_id: ConnectionId) {
        self.membership
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .leave(connection_id);
    }

    fn lock(&self) -> Result<MutexGuard<'_, Membership>> {
        self.membership
            .lock()
            .map_err(|e| anyhow!("Failed to lock the channel registry: {}", e))
    }
}

impl Default for ChannelRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// One connection's subscriptions: its handle on its own entries in the
/// [`ChannelRegistry`], which is where the channels it is on are recorded.
///
/// Held by the connection's [`ConnectionState`](crate::connection::ConnectionState),
/// so the subscriptions die with the client that made them - dropping the
/// handle takes the connection off every channel.
pub struct ChannelSubscriptions {
    /// The connection these subscriptions are for.
    connection_id: ConnectionId,
    /// The registry the subscriptions are recorded in. Owned by
    /// `ServerState`; only kept here as a reference.
    channel_registry: Arc<ChannelRegistry>,
}

impl ChannelSubscriptions {
    /// The subscriptions of connection `connection_id` on the server whose
    /// channels `channel_registry` records.
    pub fn new(connection_id: ConnectionId, channel_registry: Arc<ChannelRegistry>) -> Self {
        ChannelSubscriptions { connection_id, channel_registry }
    }

    /// Subscribes this connection to `channel`, answering with the number of
    /// channels it is subscribed to afterwards - the count SUBSCRIBE reports.
    ///
    /// A channel already subscribed to is not subscribed to twice, so the
    /// count stays where it was: Redis counts distinct channels. Nor is the
    /// connection listed twice as one of the channel's subscribers, so it
    /// receives each message once.
    pub fn subscribe(&self, channel: &str) -> Result<usize> {
        self.channel_registry.subscribe(self.connection_id, channel)
    }

    /// Unsubscribes this connection from `channel`, answering with the number
    /// of channels it is still subscribed to - the count UNSUBSCRIBE reports.
    /// A channel it was not subscribed to leaves the count where it was.
    pub fn unsubscribe(&self, channel: &str) -> Result<usize> {
        self.channel_registry.unsubscribe(self.connection_id, channel)
    }

    /// The channels this connection is subscribed to, in name order - what a
    /// bare UNSUBSCRIBE unsubscribes it from.
    pub fn channels(&self) -> Result<Vec<String>> {
        self.channel_registry.channels(self.connection_id)
    }

    /// How many channels this connection is subscribed to.
    pub fn count(&self) -> Result<usize> {
        self.channel_registry.channel_count(self.connection_id)
    }

    /// Whether this connection is in subscribed mode - subscribed to at least
    /// one channel - and so limited to the commands
    /// [`ensure_allowed_in_subscribed_mode`] lets through.
    pub fn is_subscribed_mode(&self) -> Result<bool> {
        Ok(self.count()? > 0)
    }
}

/// The subscriptions of a connection on a server of its own: no other
/// connection's subscriptions are recorded alongside them.
impl Default for ChannelSubscriptions {
    fn default() -> Self {
        Self::new(ConnectionId::default(), Arc::new(ChannelRegistry::new()))
    }
}

/// A connection that ends is no longer subscribed to anything, so PUBLISH must
/// stop delivering to it.
impl Drop for ChannelSubscriptions {
    fn drop(&mut self) {
        self.channel_registry.leave(self.connection_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_a_new_connection_is_subscribed_to_nothing() -> Result<()> {
        assert_eq!(ChannelSubscriptions::default().count()?, 0);
        Ok(())
    }

    #[test]
    fn test_each_new_channel_raises_the_count() -> Result<()> {
        let subscriptions = ChannelSubscriptions::default();

        assert_eq!(subscriptions.subscribe("foo")?, 1);
        assert_eq!(subscriptions.subscribe("bar")?, 2);
        assert_eq!(subscriptions.subscribe("baz")?, 3);
        assert_eq!(subscriptions.count()?, 3);
        Ok(())
    }

    #[test]
    fn test_subscribing_again_to_the_same_channel_leaves_the_count_alone() -> Result<()> {
        let subscriptions = ChannelSubscriptions::default();
        subscriptions.subscribe("foo")?;

        assert_eq!(subscriptions.subscribe("foo")?, 1);
        assert_eq!(subscriptions.count()?, 1);
        Ok(())
    }

    #[test]
    fn test_a_connection_enters_subscribed_mode_with_its_first_channel() -> Result<()> {
        let subscriptions = ChannelSubscriptions::default();
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
        let one = ChannelSubscriptions::default();
        let other = ChannelSubscriptions::default();

        one.subscribe("foo")?;

        assert_eq!(other.count()?, 0);
        assert_eq!(one.count()?, 1);
        Ok(())
    }

    #[test]
    fn test_unsubscribing_lowers_the_count_and_leaves_subscribed_mode_at_zero() -> Result<()> {
        let subscriptions = ChannelSubscriptions::default();
        subscriptions.subscribe("foo")?;
        subscriptions.subscribe("bar")?;

        assert_eq!(subscriptions.unsubscribe("foo")?, 1);
        assert!(subscriptions.is_subscribed_mode()?);
        assert_eq!(subscriptions.unsubscribe("bar")?, 0);
        assert!(!subscriptions.is_subscribed_mode()?);
        Ok(())
    }

    #[test]
    fn test_unsubscribing_from_a_channel_not_subscribed_to_leaves_the_count_alone() -> Result<()> {
        let subscriptions = ChannelSubscriptions::default();
        assert_eq!(subscriptions.unsubscribe("foo")?, 0);

        subscriptions.subscribe("foo")?;

        assert_eq!(subscriptions.unsubscribe("bar")?, 1);
        assert_eq!(subscriptions.channels()?, vec!["foo".to_owned()]);
        Ok(())
    }

    /// Connection `n` on the server of `registry`.
    fn connection(n: u64, registry: &Arc<ChannelRegistry>) -> ChannelSubscriptions {
        ChannelSubscriptions::new(ConnectionId::new(n), Arc::clone(registry))
    }

    fn sorted(mut ids: Vec<ConnectionId>) -> Vec<ConnectionId> {
        ids.sort();
        ids
    }

    #[test]
    fn test_the_registry_lists_each_subscriber_of_a_channel_once() -> Result<()> {
        let registry = Arc::new(ChannelRegistry::new());
        let one = connection(1, &registry);
        let other = connection(2, &registry);
        let elsewhere = connection(3, &registry);

        one.subscribe("foo")?;
        one.subscribe("foo")?;
        other.subscribe("foo")?;
        elsewhere.subscribe("bar")?;

        assert_eq!(sorted(registry.subscribers("foo")?), vec![ConnectionId::new(1), ConnectionId::new(2)]);
        assert_eq!(registry.subscribers("baz")?, vec![]);
        Ok(())
    }

    #[test]
    fn test_a_connection_that_ends_leaves_every_channel_it_was_on() -> Result<()> {
        let registry = Arc::new(ChannelRegistry::new());
        let staying = connection(1, &registry);
        let leaving = connection(2, &registry);
        staying.subscribe("foo")?;
        leaving.subscribe("foo")?;
        leaving.subscribe("bar")?;

        drop(leaving);

        assert_eq!(registry.subscribers("foo")?, vec![ConnectionId::new(1)]);
        assert_eq!(registry.subscribers("bar")?, vec![]);
        assert_eq!(staying.count()?, 1);
        Ok(())
    }

    #[test]
    fn test_an_unsubscribed_connection_is_no_longer_a_subscriber_of_the_channel() -> Result<()> {
        let registry = Arc::new(ChannelRegistry::new());
        let staying = connection(1, &registry);
        let leaving = connection(2, &registry);
        staying.subscribe("foo")?;
        leaving.subscribe("foo")?;
        leaving.subscribe("bar")?;

        assert_eq!(leaving.unsubscribe("foo")?, 1);
        assert_eq!(leaving.unsubscribe("bar")?, 0);
        // Another connection's channel is not this one's to leave.
        assert_eq!(leaving.unsubscribe("foo")?, 0);

        assert_eq!(registry.subscribers("foo")?, vec![ConnectionId::new(1)]);
        assert_eq!(registry.subscribers("bar")?, vec![]);
        assert_eq!(staying.count()?, 1);
        Ok(())
    }

    #[test]
    fn test_connections_on_one_server_count_their_channels_separately() -> Result<()> {
        let registry = Arc::new(ChannelRegistry::new());
        let one = connection(1, &registry);
        let other = connection(2, &registry);

        one.subscribe("foo")?;
        one.subscribe("bar")?;

        assert_eq!(other.subscribe("foo")?, 1);
        assert_eq!(other.count()?, 1);
        assert_eq!(one.count()?, 2);
        Ok(())
    }
}
