use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use redis_starter_rust::cli;
use redis_starter_rust::connection;
use redis_starter_rust::replication;
use redis_starter_rust::server_state::ServerState;
use redis_starter_rust::storage::{Storage, StoredValue};
use std::collections::HashMap;

/// Default Redis port if not specified via command line
const DEFAULT_PORT: usize = 6379;

fn main() -> Result<(), anyhow::Error> {
    let args: Vec<String> = std::env::args().collect();

    // Parse command-line arguments
    let port = cli::get_port(&args)?.unwrap_or(DEFAULT_PORT);
    let replica_of = cli::get_replica_of(&args);

    // Initialize server state
    let shared_server_state = Arc::new(ServerState::new(replica_of.clone(), port));

    // Initialize storage
    let redis_data: HashMap<String, StoredValue> = HashMap::new();
    let storage: Arc<Mutex<Storage>> = Arc::new(Mutex::new(Storage::new(redis_data)));

    // If this is a replica, spawn a thread to connect to the master
    if let Some(replica_of_address) = shared_server_state.get_replica_of_address()? {
        let server_state = Arc::clone(&shared_server_state);
        let storage_clone = Arc::clone(&storage);
        thread::spawn(move || {
            if let Err(e) = replication::join_replica(&replica_of_address, &server_state, &storage_clone) {
                eprintln!("Failed to join replica: {}", e);
            }
        });
    }

    // Start listening for incoming connections
    let server_address = format!("127.0.0.1:{}", port);
    let listener = TcpListener::bind(&server_address)?;
    println!("Redis server listening on {}", server_address);

    // Accept incoming connections
    for incoming_connection in listener.incoming() {
        let mut stream = incoming_connection?;
        let storage = Arc::clone(&storage);
        let server_state = Arc::clone(&shared_server_state);

        // Set read timeout for the connection
        stream.set_read_timeout(Some(Duration::new(1, 0)))?;

        // Handle connection in a separate thread
        thread::spawn(move || {
            if let Err(e) = connection::handle_connection(&mut stream, &storage, &server_state, true) {
                eprintln!("Connection handler error: {}", e);
            }
        });
    }

    Ok(())
}
