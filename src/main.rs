use std::env;
use std::{io::Write, net::{TcpListener, TcpStream}};
use std::collections::HashMap;
use std::time::Duration;
use std::thread;
use std::sync::{Arc, Mutex};

use redis_starter_rust::storage::{ Storage, StoredValue };
use redis_starter_rust::protocol;
use redis_starter_rust::io;
use redis_starter_rust::commands::{ self, RedisCommand };
use redis_starter_rust::server_state::ServerState;

fn main() -> Result<(), anyhow::Error> {
    const DEFAULT_PORT: usize = 6379;
    let args: Vec<String> = env::args().collect();
    //let args: Vec<String> = vec!["", "", "--port", "6380", "--replicaof", "'localhost 6379'"].iter().map(|x| x.to_string()).collect();
    let port = get_port(&args)?.unwrap_or(DEFAULT_PORT);
    let replica_of = get_replica_of(&args);
    let server_state = Arc::new(ServerState::new(replica_of));
    let server_address = format!("127.0.0.1:{}", port);
    let listener = TcpListener::bind(server_address)?;
    let redis_data: HashMap<String, StoredValue> = HashMap::new();
    let storage: Arc<Mutex<Storage>> = Arc::new(Mutex::new(Storage::new(redis_data)));
    for incoming_connection in listener.incoming() {
        let mut stream = incoming_connection?;
        let per_thread_storage = Arc::clone(&storage);
        let server_state = Arc::clone(&server_state);
        thread::spawn(move || {
            server_worker(&mut stream, &per_thread_storage, &server_state)
        });
    }
    Ok(())
}

fn get_replica_of(args: &[String]) -> Option<String> {
    get_option_value("replicaof", args)
}

fn get_port(args: &[String]) -> Result<Option<usize>, anyhow::Error> {
    match get_option_value("port", args) {
        Some(p) => Ok(Some(p.parse()?)),
        None => Ok(None)
    }
}

fn get_option_value(option_name: &str, args: &[String]) -> Option<String> {
    let mut option_value = None;
    if let Some(option_position) = args.iter().position(|x| x == &format!("--{}", option_name)) {
        if let Some(option_input) = args.get(option_position + 1) {
            option_value = Some(option_input.to_owned());
        }
    }
    option_value
}

fn server_worker(stream: &mut TcpStream, storage: &Arc<Mutex<Storage>>, server_state: &Arc<ServerState>) -> Result<(), anyhow::Error> {
    stream.set_read_timeout(Some(Duration::new(1, 0)))?;
    println!("accepted new connection");
    println!("{:?}", storage);
    loop {
        if let Some(received_message) = io::read_message(stream)? {
            println!("Received: {}", String::from_utf8_lossy(&received_message.serialize()));
            let command_name = commands::parse_command_name(&received_message)?;
            match &received_message {
                protocol::DataType::Array { elements } => {
                    let mut command: Option<Box<dyn RedisCommand>> = None;
                    let command_name = command_name.as_str();
                    if command_name == "ECHO" {
                        command = Some(Box::new(commands::Echo { argument: elements.get(1) }));
                    } else if command_name == "PING" {
                        command = Some(Box::new(commands::Ping {}));
                    } else if command_name == "SET" {
                        command = Some(Box::new(commands::Set { instructions: &received_message }));
                    } else if command_name == "GET" {
                        command = Some(Box::new(commands::Get { instructions: &received_message }));
                    } else if command_name == "COMMAND" {
                        command = Some(Box::new(commands::Command {}))
                    } else if command_name == "INFO" {
                        command = Some(Box::new(commands::Info { instructions: &received_message, server_state }))
                    }
                    if let Some(command) = command {
                        if let Some(reply) = command.execute(storage)? {
                            stream.write_all(&reply.serialize())?;
                        }
                    }
                },
                _ => ()
            }
        } else {
            //println!("No message has been read")
        }
        //TODO: Terminate the connection when requested by the client
    }
}