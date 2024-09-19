use std::{io::{Read, Write}, net::{TcpListener, TcpStream}};
use std::collections::HashMap;
use std::time::Duration;
use std::thread;
use std::sync::{Arc, Mutex};

use crate::error::RedisError;

mod protocol;
mod error;

const BUFFER_SIZE: usize = 2048;

pub(crate) fn read_message(stream: &mut TcpStream) -> Result<Option<protocol::DataType>, anyhow::Error> {
    let mut buffer = [0u8; BUFFER_SIZE];
    let mut message_bytes: Vec<u8> = Vec::new();
    let mut read_bytes = stream.read(&mut buffer)?;
    let mut total_read_bytes = read_bytes;
    message_bytes.extend(&buffer[0..read_bytes]);

    while read_bytes == BUFFER_SIZE {
        match stream.read(&mut buffer) {
            Ok(read_bytes) => {
                total_read_bytes = total_read_bytes + read_bytes;
                message_bytes.extend(&buffer[0..read_bytes]);
            },
            Err(_) => {
                read_bytes = 0
            }
        }
    }

    if total_read_bytes == 0 {
        Ok(None)
    } else {
        let (parsed, position) = protocol::DataType::parse(&message_bytes, 0)?;
        if position == message_bytes.len() {
            Ok(Some(parsed))
        } else {
            Err(RedisError { 
                message: format!("Could not parse '{}': symbols after position {} are left unconsumed, total symbols {}",
                    String::from_utf8_lossy(&message_bytes.clone()),
                    position,
                    message_bytes.len()
                )
            }.into())
        }
    }
}

fn server_worker(stream: &mut TcpStream, redis_data: &Arc<Mutex<HashMap<String, Vec<u8>>>>) -> Result<(), anyhow::Error> {
    stream.set_read_timeout(Some(Duration::new(1, 0)))?;
    println!("accepted new connection");
    println!("{:?}", redis_data);

    let ping_message = protocol::DataType::Array {
        elements: vec![
            protocol::DataType::BulkString { value: Some("PING".as_bytes().to_vec()) }
        ]
    };

    loop {
        if let Some(received_message) = read_message(stream)? {
            println!("Received: {}", String::from_utf8_lossy(&received_message.serialize()));
            if received_message == ping_message {
                let reply = protocol::DataType::SimpleString {
                    value: "PONG".as_bytes().to_vec()
                }.serialize();
                stream.write_all(&reply)?;
                println!("Replied with pong")
            } else {
                match received_message {
                    protocol::DataType::Array { elements } => {
                        if let Some(first_element) = elements.get(0) {
                            let first = first_element.as_string()?;
                            if first == "ECHO" {
                                println!("Received ECHO");
                                if let Some(echo_argument) = elements.get(1) {
                                    stream.write_all(&echo_argument.serialize())?
                                }
                            } else if first == "SET" {
                                let error = RedisError { 
                                    message: "SET command should have two arguments".to_string()
                                };
                                let key = elements.get(1).ok_or::<anyhow::Error>(error.clone().into())?.as_string()?;
                                let value = elements.get(2).ok_or::<anyhow::Error>(error.into())?.as_string()?;
                                println!("SET {} {}", key, value);
                                let mut data = redis_data.lock().unwrap(); //TODO: Avoid unwrap
                                data.insert(key, value.as_bytes().to_vec());
                                let reply = protocol::DataType::SimpleString { value: "OK".as_bytes().to_vec() };
                                stream.write_all(&reply.serialize())?;
                            } else if first == "GET" {
                                let error = RedisError { 
                                    message: "GET command should have one argument".to_string()
                                };
                                let key = elements.get(1).ok_or::<anyhow::Error>(error.clone().into())?.as_string()?;
                                println!("GET {}", key);
                                let data = redis_data.lock().unwrap(); //TODO: Avoid unwrap
                                let reply = match data.get(&key) {
                                    Some(value) => {
                                        protocol::DataType::BulkString { value: Some(value.clone()) }
                                    },
                                    None => {
                                        protocol::DataType::BulkString { value: None }
                                    }
                                };
                                stream.write_all(&reply.serialize())?;
                            }
                        }
                    },
                    _ => ()
                }
            }
        }
        //TODO: Terminate the connection when requested by the client
    }
}

fn main() -> Result<(), anyhow::Error> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    let redis_data: Arc<Mutex<HashMap<String, Vec<u8>>>> = Arc::new(Mutex::new(HashMap::new()));
    for incoming_connection in listener.incoming() {
        let mut stream = incoming_connection?;
        let per_thread_redis_data = Arc::clone(&redis_data);
        thread::spawn(move || {
            server_worker(&mut stream, &per_thread_redis_data)
        });
    }
    Ok(())
}
