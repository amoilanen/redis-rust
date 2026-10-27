//! Every client connection the server has open, and how to reach each one.
//!
//! A connection mostly writes to its own peer - the replies to its commands -
//! and does that through its own [`ClientWriter`]. Some commands, though,
//! reach other connections: PUBLISH delivers a message to every client
//! subscribed to a channel. [`ConnectionRegistry`] is the server-wide
//! directory those commands look a connection up in, by its [`ConnectionId`].
//!
//! A connection is listed for exactly as long as it lasts: it registers when
//! it is accepted and holds the [`ClientConnectionRegistration`] it gets back,
//! whose `Drop` takes it off the list again.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use anyhow::{anyhow, Result};
use log::*;

use crate::connection::ClientWriter;

/// Names one connection for as long as the server runs: ids are never reused,
/// so an id held after its connection has ended cannot reach a newer one.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ConnectionId(u64);

#[cfg(test)]
impl ConnectionId {
    /// An id picked by a test, standing for a connection it never opened.
    pub(crate) fn new(id: u64) -> Self {
        ConnectionId(id)
    }
}

/// The writers of all open connections, by id - owned by `ServerState`.
pub struct ConnectionRegistry {
    next_id: AtomicU64,
    writers: Mutex<HashMap<ConnectionId, Arc<ClientWriter>>>,
}

impl ConnectionRegistry {
    pub fn new() -> Self {
        ConnectionRegistry {
            next_id: AtomicU64::new(1),
            writers: Mutex::new(HashMap::new()),
        }
    }

    /// Lists a new connection, reached through `writer`, under an id of its
    /// own. The connection stays listed until the returned registration is
    /// dropped.
    pub fn register(
        self: &Arc<Self>,
        writer: Arc<ClientWriter>,
    ) -> Result<ClientConnectionRegistration> {
        let connection_id = ConnectionId(self.next_id.fetch_add(1, Ordering::Relaxed));
        self.lock()?.insert(connection_id, writer);
        Ok(ClientConnectionRegistration {
            connection_id,
            connection_registry: Arc::clone(self),
        })
    }

    /// The writers of those of `connection_ids` still open, looked up under a
    /// single lock. A connection that has ended in the meantime is left out.
    pub fn writers(&self, connection_ids: &[ConnectionId]) -> Result<Vec<Arc<ClientWriter>>> {
        let writers = self.lock()?;
        Ok(connection_ids
            .iter()
            .filter_map(|connection_id| writers.get(connection_id).cloned())
            .collect())
    }

    /// Takes a connection off the list. A poisoned lock still holds the
    /// entry, and the entry is what has to go.
    fn deregister(&self, connection_id: ConnectionId) {
        self.writers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&connection_id);
    }

    fn lock(&self) -> Result<MutexGuard<'_, HashMap<ConnectionId, Arc<ClientWriter>>>> {
        self.writers
            .lock()
            .map_err(|e| anyhow!("Failed to lock the connection registry: {}", e))
    }
}

impl Default for ConnectionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// One connection's entry in the [`ConnectionRegistry`]: who the connection
/// is, server-wide.
///
/// Held by the connection's [`ConnectionState`](crate::connection::ConnectionState),
/// so the entry lasts exactly as long as the connection does - dropping the
/// registration takes it off the registry.
pub struct ClientConnectionRegistration {
    connection_id: ConnectionId,
    /// The registry this connection is listed in. Owned by `ServerState`;
    /// only kept here as a reference, so the registration can take its entry
    /// out again when dropped.
    connection_registry: Arc<ConnectionRegistry>,
}

impl ClientConnectionRegistration {
    pub fn connection_id(&self) -> ConnectionId {
        self.connection_id
    }
}

impl Drop for ClientConnectionRegistration {
    fn drop(&mut self) {
        debug!("Deregistering connection {:?}", self.connection_id);
        self.connection_registry.deregister(self.connection_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn writer() -> Arc<ClientWriter> {
        Arc::new(ClientWriter::discarding())
    }

    #[test]
    fn test_each_connection_gets_an_id_of_its_own() -> Result<()> {
        let registry = Arc::new(ConnectionRegistry::new());

        let one = registry.register(writer())?;
        let other = registry.register(writer())?;

        assert_ne!(one.connection_id(), other.connection_id());
        Ok(())
    }

    #[test]
    fn test_a_registered_connection_is_reached_through_its_own_writer() -> Result<()> {
        let registry = Arc::new(ConnectionRegistry::new());
        let one_writer = writer();
        let one = registry.register(Arc::clone(&one_writer))?;
        let _other = registry.register(writer())?;

        let found = registry.writers(&[one.connection_id()])?;

        assert_eq!(found.len(), 1);
        assert!(Arc::ptr_eq(&found[0], &one_writer));
        Ok(())
    }

    #[test]
    fn test_a_dropped_registration_takes_the_connection_off_the_registry() -> Result<()> {
        let registry = Arc::new(ConnectionRegistry::new());
        let staying = registry.register(writer())?;
        let leaving = registry.register(writer())?;
        let leaving_id = leaving.connection_id();

        drop(leaving);

        assert!(registry.writers(&[leaving_id])?.is_empty());
        assert_eq!(registry.writers(&[staying.connection_id()])?.len(), 1);
        Ok(())
    }
}
