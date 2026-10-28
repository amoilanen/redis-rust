//! Who is subscribed to which channel, kept in both directions.
//!
//! A module of its own so that the two maps are private to it: every change
//! goes through a method that updates both, and nothing outside can change
//! one without the other.

use std::collections::{HashMap, HashSet};

use crate::connection_registry::ConnectionId;

/// Channel membership in both directions: by channel, for PUBLISH to find a
/// channel's subscribers; and by connection, for counting a connection's
/// channels and for a connection that ends to leave exactly the channels it
/// was on.
///
/// The two maps always agree - a connection is among a channel's subscribers
/// exactly when the channel is among the connection's channels - and neither
/// keeps an empty entry, so they do not grow with every channel ever named or
/// connection ever made.
#[derive(Default)]
pub(super) struct Membership {
    by_channel: HashMap<String, HashSet<ConnectionId>>,
    by_connection: HashMap<ConnectionId, HashSet<String>>,
}

impl Membership {
    /// The connections subscribed to `channel`, each once.
    pub(super) fn subscribers(&self, channel: &str) -> Vec<ConnectionId> {
        self.by_channel
            .get(channel)
            .map(|subscribers| subscribers.iter().copied().collect())
            .unwrap_or_default()
    }

    /// The channels `connection_id` is subscribed to, in name order.
    pub(super) fn channels(&self, connection_id: ConnectionId) -> Vec<String> {
        let mut channels: Vec<String> = self
            .by_connection
            .get(&connection_id)
            .map(|channels| channels.iter().cloned().collect())
            .unwrap_or_default();
        channels.sort();
        channels
    }

    /// How many channels `connection_id` is subscribed to.
    pub(super) fn channel_count(&self, connection_id: ConnectionId) -> usize {
        self.by_connection.get(&connection_id).map_or(0, HashSet::len)
    }

    /// Subscribes `connection_id` to `channel`, answering with the number of
    /// channels it is subscribed to afterwards. A channel it is already on is
    /// not counted twice.
    pub(super) fn subscribe(&mut self, connection_id: ConnectionId, channel: &str) -> usize {
        self.by_channel
            .entry(channel.to_owned())
            .or_default()
            .insert(connection_id);
        let channels = self.by_connection.entry(connection_id).or_default();
        channels.insert(channel.to_owned());
        channels.len()
    }

    /// Unsubscribes `connection_id` from `channel`, answering with the number
    /// of channels it is still subscribed to. A channel it is not on changes
    /// nothing, so the count stays where it was.
    pub(super) fn unsubscribe(&mut self, connection_id: ConnectionId, channel: &str) -> usize {
        let remaining = match self.by_connection.get_mut(&connection_id) {
            Some(channels) => {
                channels.remove(channel);
                channels.len()
            }
            None => 0,
        };
        if remaining == 0 {
            self.by_connection.remove(&connection_id);
        }

        if let Some(subscribers) = self.by_channel.get_mut(channel) {
            subscribers.remove(&connection_id);
            if subscribers.is_empty() {
                self.by_channel.remove(channel);
            }
        }

        remaining
    }

    /// Takes `connection_id` off every channel it is on.
    pub(super) fn leave(&mut self, connection_id: ConnectionId) {
        for channel in self.channels(connection_id) {
            self.unsubscribe(connection_id, &channel);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn one() -> ConnectionId {
        ConnectionId::new(1)
    }

    fn other() -> ConnectionId {
        ConnectionId::new(2)
    }

    /// Fails unless the two maps agree entry for entry and neither keeps an
    /// empty entry.
    fn assert_consistent(membership: &Membership) {
        for (channel, subscribers) in &membership.by_channel {
            assert!(!subscribers.is_empty(), "empty subscriber set kept for {}", channel);
            for connection_id in subscribers {
                assert!(
                    membership
                        .by_connection
                        .get(connection_id)
                        .is_some_and(|channels| channels.contains(channel)),
                    "{:?} is a subscriber of {} the connection does not know of",
                    connection_id,
                    channel
                );
            }
        }
        for (connection_id, channels) in &membership.by_connection {
            assert!(!channels.is_empty(), "empty channel set kept for {:?}", connection_id);
            for channel in channels {
                assert!(
                    membership
                        .by_channel
                        .get(channel)
                        .is_some_and(|subscribers| subscribers.contains(connection_id)),
                    "{:?} is on {} without being one of its subscribers",
                    connection_id,
                    channel
                );
            }
        }
    }

    #[test]
    fn test_subscribing_records_both_directions() {
        let mut membership = Membership::default();

        assert_eq!(membership.subscribe(one(), "foo"), 1);
        assert_eq!(membership.subscribe(one(), "bar"), 2);
        assert_eq!(membership.subscribe(one(), "foo"), 2);
        assert_eq!(membership.subscribe(other(), "foo"), 1);

        assert_consistent(&membership);
        assert_eq!(membership.channels(one()), vec!["bar".to_owned(), "foo".to_owned()]);
        let mut foo = membership.subscribers("foo");
        foo.sort();
        assert_eq!(foo, vec![one(), other()]);
    }

    #[test]
    fn test_unsubscribing_removes_both_directions_and_empty_entries() {
        let mut membership = Membership::default();
        membership.subscribe(one(), "foo");
        membership.subscribe(one(), "bar");
        membership.subscribe(other(), "foo");

        assert_eq!(membership.unsubscribe(one(), "foo"), 1);
        assert_consistent(&membership);
        assert_eq!(membership.subscribers("foo"), vec![other()]);

        assert_eq!(membership.unsubscribe(one(), "bar"), 0);
        assert_consistent(&membership);
        assert!(!membership.by_connection.contains_key(&one()));
        assert!(!membership.by_channel.contains_key("bar"));
    }

    #[test]
    fn test_unsubscribing_from_a_channel_not_on_changes_nothing() {
        let mut membership = Membership::default();
        membership.subscribe(other(), "foo");

        assert_eq!(membership.unsubscribe(one(), "foo"), 0);
        membership.subscribe(one(), "bar");
        assert_eq!(membership.unsubscribe(one(), "foo"), 1);

        assert_consistent(&membership);
        assert_eq!(membership.subscribers("foo"), vec![other()]);
        assert_eq!(membership.channels(one()), vec!["bar".to_owned()]);
    }

    #[test]
    fn test_leaving_removes_the_connection_from_every_channel_and_nobody_else() {
        let mut membership = Membership::default();
        membership.subscribe(one(), "foo");
        membership.subscribe(one(), "bar");
        membership.subscribe(other(), "foo");

        membership.leave(one());
        membership.leave(one());

        assert_consistent(&membership);
        assert_eq!(membership.channel_count(one()), 0);
        assert_eq!(membership.subscribers("foo"), vec![other()]);
        assert_eq!(membership.subscribers("bar"), vec![]);
        assert_eq!(membership.channel_count(other()), 1);
    }
}
