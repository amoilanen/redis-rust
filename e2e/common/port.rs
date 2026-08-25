//! Handing out TCP ports that no other server will take.

use std::collections::hash_map::{Entry, HashMap};
use std::net::TcpListener;
use std::sync::{Mutex, OnceLock};

/// How many `bind(0)` attempts [`find_free_port`] makes before giving up on
/// finding a port that is not already reserved by this process.
const PORT_ALLOCATION_ATTEMPTS: usize = 100;

/// Ports handed out by [`find_free_port`] that no server has bound yet, each mapped
/// to the listener that keeps the OS from handing the port out again.
///
/// The listener is dropped by [`release_port`] immediately before the server
/// process is spawned, so the window in which another process could grab the
/// same port is as narrow as possible; [`ServerProcess::start_with_retry`]
/// covers what is left of it.
fn reserved_ports() -> &'static Mutex<HashMap<u16, TcpListener>> {
    static RESERVED: OnceLock<Mutex<HashMap<u16, TcpListener>>> = OnceLock::new();
    RESERVED.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Find a free TCP port by binding to port 0 and reading the assigned port.
///
/// The listener is *kept open* (see [`reserved_ports`]) until the server that
/// will use the port is about to start, so that neither a later `find_free_port`
/// call nor another process picks the same port in the meantime.
pub fn find_free_port() -> u16 {
    let mut reserved = reserved_ports().lock().expect("port registry poisoned");
    for _ in 0..PORT_ALLOCATION_ATTEMPTS {
        let listener = TcpListener::bind("127.0.0.1:0").expect("failed to bind to port 0");
        let port = listener.local_addr().unwrap().port();
        if let Entry::Vacant(slot) = reserved.entry(port) {
            slot.insert(listener);
            return port;
        }
        // Already reserved by this process: drop the listener and try again.
    }
    panic!(
        "failed to allocate an unused port after {} attempts",
        PORT_ALLOCATION_ATTEMPTS
    );
}

/// Drop the reservation for `port`, freeing it for the server about to bind it.
pub(super) fn release_port(port: u16) {
    reserved_ports()
        .lock()
        .expect("port registry poisoned")
        .remove(&port);
}
