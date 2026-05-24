/// Cross-connection blocking primitives.
///
/// Lives at the crate root because it's a *server-wide* resource: BLPOP
/// (and future blocking commands like BRPOP / BLMOVE) park threads in a
/// per-key FIFO queue here, and write commands like RPUSH / LPUSH consult
/// the same queue to hand newly-pushed elements directly to the longest-
/// waiting client.
///
/// The notifier is owned by `ServerState` so every connection thread can
/// reach it via the shared `Arc<ServerState>`.
///
/// ## Concurrency
///
/// `BlockingNotifier` does not hold the storage lock — that's deliberate.
/// Callers are expected to coordinate as follows:
///
/// - **Writers** (RPUSH / LPUSH) call `handoff` *while still holding the
///   storage `Mutex`*, so the post-push handoff is atomic with the push
///   itself and a concurrent BLPOP either sees the unmodified list (and
///   registers a waiter) or sees no waiters at hand-off time.
/// - **Readers** (BLPOP) call `register` *while holding the storage
///   `Mutex`* and then drop both locks before waiting on the returned
///   `Receiver`. This guarantees pushers that arrive afterwards observe
///   the waiter under their own `Mutex` acquisition.
///
/// The per-key wait queue is a `VecDeque<Sender<DataType>>`, so the
/// front-of-the-queue waiter is the one that registered first — matching
/// Redis's "respond to the client that has been waiting the longest"
/// semantics.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::sync::mpsc::{self, Receiver, Sender};

use anyhow::{anyhow, Result};

use crate::protocol::DataType;

pub struct BlockingNotifier {
    waiters: Mutex<HashMap<String, VecDeque<Sender<DataType>>>>,
}

impl BlockingNotifier {
    pub fn new() -> Self {
        BlockingNotifier {
            waiters: Mutex::new(HashMap::new()),
        }
    }

    /// Register a new waiter for `key` at the back of the FIFO queue.
    ///
    /// Returns the `Receiver` half of a fresh single-producer channel; the
    /// caller is expected to drop any held storage lock and then block on
    /// `Receiver::recv` (or `recv_timeout`) until a writer hands them an
    /// element.
    pub fn register(&self, key: &str) -> Result<Receiver<DataType>> {
        let (tx, rx) = mpsc::channel::<DataType>();
        let mut waiters = self
            .waiters
            .lock()
            .map_err(|e| anyhow!("Failed to lock waiters: {}", e))?;
        waiters
            .entry(key.to_string())
            .or_insert_with(VecDeque::new)
            .push_back(tx);
        Ok(rx)
    }

    /// Hand off head elements from `elements` to currently-waiting clients
    /// in FIFO order, mutating `elements` in place (removed-from-the-front).
    ///
    /// Stops when either the list is empty or the waiter queue is empty.
    /// If a `send` fails (receiver was dropped — typically a disconnected
    /// client) the element is restored at the head of the list and the
    /// next waiter is tried.
    pub fn handoff(&self, key: &str, elements: &mut Vec<DataType>) -> Result<()> {
        let mut waiters_map = self
            .waiters
            .lock()
            .map_err(|e| anyhow!("Failed to lock waiters: {}", e))?;
        if let Some(queue) = waiters_map.get_mut(key) {
            while !elements.is_empty() {
                let Some(tx) = queue.pop_front() else { break };
                let element = elements.remove(0);
                if let Err(err) = tx.send(element) {
                    // Receiver was dropped — restore the element at the head
                    // and try the next waiter (it may still be alive).
                    elements.insert(0, err.0);
                }
            }
            if queue.is_empty() {
                waiters_map.remove(key);
            }
        }
        Ok(())
    }
}

impl Default for BlockingNotifier {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol;
    use std::sync::mpsc::TryRecvError;

    #[test]
    fn register_then_handoff_delivers_element_in_fifo_order() -> anyhow::Result<()> {
        let notifier = BlockingNotifier::new();

        // Two waiters on the same key; the first registered should be the
        // first served.
        let rx1 = notifier.register("k")?;
        let rx2 = notifier.register("k")?;

        let mut elements = vec![
            protocol::bulk_string("first"),
            protocol::bulk_string("second"),
        ];
        notifier.handoff("k", &mut elements)?;

        assert!(elements.is_empty(), "both  elements should have been handed off");
        assert_eq!(rx1.recv()?, protocol::bulk_string("first"));
        assert_eq!(rx2.recv()?, protocol::bulk_string("second"));
        Ok(())
    }

    #[test]
    fn handoff_stops_when_no_waiters() -> anyhow::Result<()> {
        let notifier = BlockingNotifier::new();

        // No waiters registered — handoff should be a no-op.
        let mut elements = vec![protocol::bulk_string("untouched")];
        notifier.handoff("k", &mut elements)?;
        assert_eq!(elements.len(), 1);
        Ok(())
    }

    #[test]
    fn handoff_stops_when_list_emptied_before_waiters_consumed() -> anyhow::Result<()> {
        let notifier = BlockingNotifier::new();

        // Two waiters but only one element — second waiter stays parked.
        let rx1 = notifier.register("k")?;
        let rx2 = notifier.register("k")?;

        let mut elements = vec![protocol::bulk_string("only")];
        notifier.handoff("k", &mut elements)?;

        assert!(elements.is_empty());
        assert_eq!(rx1.recv()?, protocol::bulk_string("only"));
        // rx2 still parked; a follow-up handoff with a fresh element should
        // wake it.
        let mut more = vec![protocol::bulk_string("late")];
        notifier.handoff("k", &mut more)?;
        assert!(more.is_empty());
        assert_eq!(rx2.recv()?, protocol::bulk_string("late"));
        Ok(())
    }

    #[test]
    fn handoff_restores_element_when_waiter_disconnected() -> anyhow::Result<()> {
        let notifier = BlockingNotifier::new();

        // Register a waiter, then drop its receiver — simulates the client
        // going away before its element arrives.
        let rx = notifier.register("k")?;
        drop(rx);

        let mut elements = vec![protocol::bulk_string("kept")];
        notifier.handoff("k", &mut elements)?;

        // The element should be restored at the head, since no live waiter
        // claimed it.
        assert_eq!(elements, vec![protocol::bulk_string("kept")]);
        Ok(())
    }

    #[test]
    fn waiters_are_keyed() -> anyhow::Result<()> {
        let notifier = BlockingNotifier::new();

        // Two waiters on *different* keys. A handoff to "a" must wake only
        // the "a" waiter — the "b" waiter must stay parked until a handoff
        // arrives on its own key.
        let rx_a = notifier.register("a")?;
        let rx_b = notifier.register("b")?;

        let mut for_a = vec![protocol::bulk_string("for_a")];
        notifier.handoff("a", &mut for_a)?;

        assert!(for_a.is_empty(), "element for \"a\" should have been handed off");
        assert_eq!(rx_a.recv()?, protocol::bulk_string("for_a"));

        // rx_b must NOT have received anything from the "a" handoff.
        assert_eq!(
            rx_b.try_recv(),
            Err(TryRecvError::Empty),
            "waiter on \"b\" must not be woken by a handoff to \"a\""
        );

        // A handoff on an unrelated third key must also leave rx_b parked.
        let mut for_c = vec![protocol::bulk_string("for_c")];
        notifier.handoff("c", &mut for_c)?;
        assert_eq!(for_c.len(), 1, "handoff to key with no waiters is a no-op");
        assert_eq!(rx_b.try_recv(), Err(TryRecvError::Empty));

        // Once a handoff finally arrives on "b", rx_b receives it.
        let mut for_b = vec![protocol::bulk_string("for_b")];
        notifier.handoff("b", &mut for_b)?;
        assert!(for_b.is_empty());
        assert_eq!(rx_b.recv()?, protocol::bulk_string("for_b"));
        Ok(())
    }
}
