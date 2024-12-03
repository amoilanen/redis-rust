use std::{io::Write, net::{TcpListener, TcpStream}};
use std::collections::HashMap;
use std::time::Duration;
use std::thread;
use std::sync::{Arc, Mutex};

use redis_starter_rust::storage::{ Storage, StoredValue };
use redis_starter_rust::protocol;
use redis_starter_rust::io;
use redis_starter_rust::commands::{ self, Command };

fn main() -> Result<(), anyhow::Error> {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    let redis_data: HashMap<String, StoredValue> = HashMap::new();
    let storage: Arc<Mutex<Storage>> = Arc::new(Mutex::new(Storage::new(redis_data)));
    for incoming_connection in listener.incoming() {
        let mut stream = incoming_connection?;
        let per_thread_storage = Arc::clone(&storage);
        thread::spawn(move || {
            server_worker(&mut stream, &per_thread_storage)
        });
    }
    Ok(())
}

fn server_worker(stream: &mut TcpStream, storage: &Arc<Mutex<Storage>>) -> Result<(), anyhow::Error> {
    stream.set_read_timeout(Some(Duration::new(1, 0)))?;
    println!("accepted new connection");
    println!("{:?}", storage);
    loop {
        if let Some(received_message) = io::read_message(stream)? {
            println!("Received: {}", String::from_utf8_lossy(&received_message.serialize()));
            let command_name = commands::parse_command_name(&received_message)?;
            match &received_message {
                protocol::DataType::Array { elements } => {
                    let mut command: Option<Box<dyn Command>> = None;
                    let command_name = command_name.as_str();
                    if command_name == "ECHO" {
                        command = Some(Box::new(commands::Echo { argument: elements.get(1) }));
                    } else if command_name == "PING" {
                        command = Some(Box::new(commands::Ping {}));
                    } else if command_name == "SET" {
                        command = Some(Box::new(commands::Set { instructions: &received_message }));
                    } else if command_name == "GET" {
                        command = Some(Box::new(commands::Get { instructions: &received_message }));
                    }
                    if let Some(command) = command {
                        if let Some(reply) = command.execute(storage)? {
                            stream.write_all(&reply.serialize())?;
                        }
                    }
                },
                _ => ()
            }
        }
        //TODO: Terminate the connection when requested by the client
    }
}