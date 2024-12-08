use std::{io::Read, net::TcpStream};
use crate::protocol;

const BUFFER_SIZE: usize = 2048;

pub fn read_message(stream: &mut TcpStream) -> Result<Option<protocol::DataType>, anyhow::Error> {
    if let Some(message_bytes) = read_bytes(stream)? {
        Ok(Some(protocol::read_message_from_bytes(&message_bytes)?))
    } else {
        Ok(None)
    }
}

fn read_next_bytes(stream: &mut TcpStream, buffer: &mut [u8]) -> usize {
    match stream.read(buffer) {
        Ok(read_bytes) => {
            read_bytes
        }
        Err(_) => {
            0
        }
    }
}

pub(crate) fn read_bytes(stream: &mut TcpStream) -> Result<Option<Vec<u8>>, anyhow::Error> {
    let mut buffer = [0u8; BUFFER_SIZE];
    let mut message_bytes: Vec<u8> = Vec::new();
    let mut total_read_bytes = 0;

    loop {
        let read_bytes = read_next_bytes(stream, &mut buffer);
        if read_bytes > 0 {
            total_read_bytes = total_read_bytes + read_bytes;
            message_bytes.extend(&buffer[0..read_bytes]);
        }
        if read_bytes < BUFFER_SIZE {
            break;
        }
    }
    if total_read_bytes == 0 {
        Ok(None)
    } else {
        Ok(Some(message_bytes))
    }
}