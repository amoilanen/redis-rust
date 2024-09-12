use std::{io::{Read, Write}, net::{TcpListener, TcpStream}};
use std::time::Duration;
use crate::error::RedisError;
use crate::protocol::DataType;

mod protocol;
mod error;

const BUFFER_SIZE: usize = 2048;

pub(crate) fn read_message(stream: &mut TcpStream) -> Result<Option<Box<dyn DataType>>, anyhow::Error> {
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
        let (parsed, position) = protocol::parse_data_type(&message_bytes, 0)?;
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

        let mut first_message: Option<Box<dyn DataType>> = None;
        while first_message.is_none() {
            //TODO: Recover from a failing read_message call
            first_message = read_message(&mut stream)?;
        }
        if let Some(received_message) = first_message {
            println!("Received: {}", String::from_utf8_lossy(&received_message.serialize()));
        }
        let reply = protocol::SimpleString {
            value: "PONG".as_bytes().to_vec()
        }.serialize();
        stream.write_all(&reply)?;
        println!("Replied with pong")
    }
    Ok(())
}
