use std::{io::Write, net::{TcpListener, TcpStream}};
use std::collections::HashMap;
use std::time::Duration;
use std::thread;
use std::sync::{Arc, Mutex};

use redis_starter_rust::storage::{ Storage, StoredValue };
use redis_starter_rust::error::RedisError;
use redis_starter_rust::protocol;
use redis_starter_rust::io;

fn server_worker(stream: &mut TcpStream, storage: &Arc<Mutex<Storage>>) -> Result<(), anyhow::Error> {
    stream.set_read_timeout(Some(Duration::new(1, 0)))?;
    println!("accepted new connection");
    println!("{:?}", storage);
    loop {
        if let Some(received_message) = io::read_message(stream)? {
            println!("Received: {}", String::from_utf8_lossy(&received_message.serialize()));
            let received_message_parts: Vec<String> = received_message.as_array()?;
            let command_parts: Vec<&str> = received_message_parts.iter().map(|x| x.as_str()).collect();
            let &command = command_parts.get(0).unwrap_or(&"");
            match received_message {
                protocol::DataType::Array { elements } => {
                    let mut reply: Option<protocol::DataType> = None;
                    if command == "ECHO" {
                        if let Some(echo_argument) = elements.get(1) {
                            reply = Some(echo_argument.clone());
                        }
                    } else if command == "PING" {
                        reply = Some(protocol::simple_string("PONG"));
                    } else if command == "SET" {
                        let error = RedisError { 
                            message: "Invalid SET command syntax".to_string()
                        };
                        let &key = command_parts.get(1).ok_or::<anyhow::Error>(error.clone().into())?;
                        let &value = command_parts.get(2).ok_or::<anyhow::Error>(error.clone().into())?;
                        let expires_in_ms = if let Some(&modifier) = command_parts.get(3) {
                            if modifier == "px" {
                                let expiration_time: u64 = command_parts.get(4).ok_or::<anyhow::Error>(error.clone().into())?.parse()?;
                                Some(expiration_time)
                            } else {
                                None
                            }
                        } else {
                            None
                        };
                        println!("SET {} {}", key, value);
                        println!("expiration_after = {:?}", expires_in_ms);
                        let mut data = storage.lock().unwrap(); //TODO: Avoid unwrap
                        data.set(key, value.as_bytes().to_vec(), expires_in_ms)?;
                        reply = Some(protocol::simple_string("OK"));
                    } else if command == "GET" {
                        let error = RedisError { 
                            message: "GET command should have one argument".to_string()
                        };
                        let key = command_parts.get(1).ok_or::<anyhow::Error>(error.clone().into())?;
                        println!("GET {}", key);
                        let mut data = storage.lock().unwrap(); //TODO: Avoid unwrap
                        reply = match data.get(&key.to_string())? {
                            Some(value) => 
                                Some(protocol::bulk_string(Some(value.clone()))),
                            None =>
                                Some(protocol::bulk_string(None))
                        };
                    }
                    if let Some(reply) = reply {
                        stream.write_all(&reply.serialize())?;
                    }
                },
                _ => ()
            }
        }
        //TODO: Terminate the connection when requested by the client
    }
}

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
