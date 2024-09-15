use std::{io::{Read, Write}, net::{TcpListener, TcpStream}};
use std::time::Duration;
use crate::protocol::DataType;

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

fn main() -> Result<(), anyhow::Error> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    for incoming_connection in listener.incoming() {
        let mut stream = incoming_connection?;
        stream.set_read_timeout(Some(Duration::new(1, 0)))?;
        println!("accepted new connection");

        let expected_ping_message = protocol::DataType::Array {
            elements: vec![
                protocol::DataType::BulkString { value: "PING".as_bytes().to_vec() }
            ]
        };

        loop {
            if let Some(received_message) = read_message(&mut stream)? {
                println!("Received: {}", String::from_utf8_lossy(&received_message.serialize()));
                if received_message == expected_ping_message {
                    let reply = protocol::DataType::SimpleString {
                        value: "PONG".as_bytes().to_vec()
                    }.serialize();
                    stream.write_all(&reply)?;
                    println!("Replied with pong")
                }
            }
        }
    }
    Ok(())
}
