use std::{io::Write, net::{TcpListener, TcpStream}};
use std::collections::HashMap;
use std::time::Duration;
use std::thread;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::RedisError;

mod protocol;
mod error;
mod io;

fn server_worker(stream: &mut TcpStream, redis_data: &Arc<Mutex<HashMap<String, StoredValue>>>) -> Result<(), anyhow::Error> {
    stream.set_read_timeout(Some(Duration::new(1, 0)))?;
    println!("accepted new connection");
    println!("{:?}", redis_data);

    let ping_message = protocol::DataType::Array {
        elements: vec![
            protocol::DataType::BulkString { value: Some("PING".as_bytes().to_vec()) }
        ]
    };

    loop {
        if let Some(received_message) = io::read_message(stream)? {
            println!("Received: {}", String::from_utf8_lossy(&received_message.serialize()));
            if received_message == ping_message {
                let reply = protocol::simple_string("PONG");
                stream.write_all(&reply.serialize())?;
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
                                    message: "Invalid SET command syntax".to_string()
                                };
                                let key = elements.get(1).ok_or::<anyhow::Error>(error.clone().into())?.as_string()?;
                                let value = elements.get(2).ok_or::<anyhow::Error>(error.clone().into())?.as_string()?;
                                let expires_in_ms = if let Some(modifier) = elements.get(3) {
                                    if modifier.as_string()? == "px" {
                                        let expiration_time: u64 = elements.get(4).ok_or::<anyhow::Error>(error.clone().into())?.as_string()?.parse()?;
                                        Some(expiration_time)
                                    } else {
                                        None
                                    }
                                } else {
                                    None
                                };
                                println!("SET {} {}", key, value);
                                println!("expiration_after = {:?}", expires_in_ms);
                                let mut data = redis_data.lock().unwrap(); //TODO: Avoid unwrap
                                data.insert(key, StoredValue::from(value.as_bytes().to_vec(), expires_in_ms)?);
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
                                    Some(stored_value) => {
                                        let current_time_ms = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
                                        let has_value_expired = if let Some(expires_in_ms) = stored_value.expires_in_ms {
                                            current_time_ms >= stored_value.last_modified_timestamp + expires_in_ms as u128
                                        } else {
                                            false
                                        };
                                        if has_value_expired {
                                            protocol::DataType::BulkString { value: None }
                                        } else {
                                            protocol::DataType::BulkString { value: Some(stored_value.value.clone()) }
                                        }
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

#[derive(Debug, PartialEq)]
struct StoredValue {
    expires_in_ms: Option<u64>,
    last_modified_timestamp: u128,
    value: Vec<u8>
}

impl StoredValue {
    fn from(value: Vec<u8>, expires_in_ms: Option<u64>) -> Result<StoredValue, anyhow::Error> {
        Ok(StoredValue {
            expires_in_ms,
            last_modified_timestamp: SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
            value
        })
    }
}

fn main() -> Result<(), anyhow::Error> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    let redis_data: Arc<Mutex<HashMap<String, StoredValue>>> = Arc::new(Mutex::new(HashMap::new()));
    for incoming_connection in listener.incoming() {
        let mut stream = incoming_connection?;
        let per_thread_redis_data = Arc::clone(&redis_data);
        thread::spawn(move || {
            server_worker(&mut stream, &per_thread_redis_data)
        });
    }
    Ok(())
}
