use std::{io::Read, net::TcpStream};
use crate::protocol;

const BUFFER_SIZE: usize = 2048;

pub(crate) fn read_message(stream: &mut TcpStream) -> Result<Option<protocol::DataType>, anyhow::Error> {
    if let Some(message_bytes) = read_bytes(stream)? {
        Ok(Some(protocol::read_message_from_bytes(&message_bytes)?))
    } else {
        Ok(None)
    }
}

pub(crate) fn read_bytes(stream: &mut TcpStream) -> Result<Option<Vec<u8>>, anyhow::Error> {
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
        Ok(Some(message_bytes))
    }
}