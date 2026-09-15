use std::net::TcpListener;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use log::*;

use codecrafters_redis::config::ServerOptions;
use codecrafters_redis::connection::{self, ConnectionMode};
use codecrafters_redis::replication;
use codecrafters_redis::server_state::ServerState;

fn main() -> Result<(), anyhow::Error> {
    env_logger::init();

    // Read once, up front: from here on the command line is the server's
    // options, which it carries for as long as it runs and answers CONFIG GET
    // from.
    let args: Vec<String> = std::env::args().collect();
    let options = ServerOptions::from_args(&args)?;
    let port = options.port;

    let server_state = Arc::new(ServerState::new(options));

    // If this is a replica, spawn a thread to connect to the master
    if let Some(replica_of_address) = server_state.get_replica_of_address()? {
        let server_state = Arc::clone(&server_state);
        thread::spawn(move || {
            if let Err(e) = replication::join_as_replica(&replica_of_address, &server_state) {
                error!("Failed to join replica: {}", e);
            }
        });
    }

    let server_address = format!("127.0.0.1:{}", port);
    let listener = TcpListener::bind(&server_address)?;
    info!("Redis server listening on {}", server_address);

    // Accept incoming connections
    for incoming_connection in listener.incoming() {
        let mut stream = incoming_connection?;
        let server_state = Arc::clone(&server_state);

        // Set read timeout for the connection
        stream.set_read_timeout(Some(Duration::new(1, 0)))?;

        // Handle connection in a separate thread
        thread::spawn(move || {
            if let Err(e) =
                connection::handle_connection(&mut stream, &server_state, ConnectionMode::ConnectedClient)
            {
                error!("Connection handler error: {}", e);
            }
        });
    }

    Ok(())
}
