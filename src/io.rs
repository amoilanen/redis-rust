/// I/O utilities for reading from TCP streams.
///
/// This module provides functions for reading and parsing Redis protocol messages
/// from TCP streams with proper buffering and error handling.

use std::io::Read;
use crate::protocol;

/// Size of the read buffer for TCP operations
const BUFFER_SIZE: usize = 2048;

pub fn read_messages<R: Read>(reader: &mut R) -> Result<Vec<protocol::DataType>, anyhow::Error> {
    if let Some(message_bytes) = read_bytes(reader)? {
        Ok(protocol::read_messages_from_bytes(&message_bytes)?)
    } else {
        Ok(Vec::new())
    }
}

pub fn read_single_message<R: Read>(reader: &mut R) -> Result<Option<protocol::DataType>, anyhow::Error> {
    let messages = read_messages(reader)?;
    match messages.len() {
        0 => Ok(None),
        1 => Ok(messages.into_iter().next()),
        n => Err(anyhow::anyhow!("Expected at most 1 message, got {n}"))
    }
}

/// Reads the next chunk of bytes from a reader.
///
/// This is an internal helper function that handles read errors gracefully.
///
/// # Arguments
/// * `reader` - Any type implementing `Read` trait
/// * `buffer` - The buffer to read into
///
/// # Returns
/// The number of bytes read, or 0 on error or EOF
fn read_next_bytes<R: Read>(reader: &mut R, buffer: &mut [u8]) -> usize {
    match reader.read(buffer) {
        Ok(bytes_read) => bytes_read,
        Err(_) => 0,
    }
}

/// Reads raw bytes from a reader with buffering.
///
/// This function reads all available data from the reader up to `BUFFER_SIZE`
/// bytes at a time, accumulating it into a vector.
///
/// # Arguments
/// * `reader` - Any type implementing `Read` trait
///
/// # Returns
/// * `Ok(Some(bytes))` - Data was read successfully
/// * `Ok(None)` - No data available (connection closed)
/// * `Err(e)` - Error reading from reader
///
/// # Note
/// When using this with `TcpStream`, ensure a read timeout is set to avoid blocking indefinitely.
pub fn read_bytes<R: Read>(reader: &mut R) -> Result<Option<Vec<u8>>, anyhow::Error> {
    let mut buffer = [0u8; BUFFER_SIZE];
    let mut all_bytes_read: Vec<u8> = Vec::new();
    let mut total_read_bytes = 0;

    loop {
        let read_bytes = read_next_bytes(reader, &mut buffer);
        if read_bytes > 0 {
            total_read_bytes += read_bytes;
            all_bytes_read.extend_from_slice(&buffer[0..read_bytes]);
        }
        if read_bytes < BUFFER_SIZE {
            break;
        }
    }

    if total_read_bytes == 0 {
        Ok(None)
    } else {
        Ok(Some(all_bytes_read))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_read_bytes_nonempty() -> Result<(), Box<dyn std::error::Error>> {
        let data = b"some bytes";
        let mut cursor = Cursor::new(data.to_vec());

        let result = read_bytes(&mut cursor)?;
        assert_eq!(result.unwrap(), data);
        Ok(())
    }

    #[test]
    fn test_read_bytes_empty_stream() -> Result<(), Box<dyn std::error::Error>> {
        let mut cursor = Cursor::new(b"".to_vec());

        let result = read_bytes(&mut cursor)?;
        assert_eq!(result, None);
        Ok(())
    }

    #[test]
    fn test_read_bytes_large_message() -> Result<(), Box<dyn std::error::Error>> {
        // Test reading a message larger than the internal buffer
        let data: Vec<u8> = vec![b'X'; BUFFER_SIZE + 1];
        let mut cursor = Cursor::new(data.clone());

        let result = read_bytes(&mut cursor)?;
        assert_eq!(result.unwrap(), data);
        Ok(())
    }

    #[test]
    fn test_read_single_message() -> Result<(), Box<dyn std::error::Error>> {
        let data = b"+OK\r\n";
        let mut cursor = Cursor::new(data.to_vec());

        let result = read_single_message(&mut cursor)?;
        assert!(result.is_some());
        assert_eq!(result.unwrap().as_string()?, "OK");
        Ok(())
    }

    #[test]
    fn test_read_single_message_empty_stream() -> Result<(), Box<dyn std::error::Error>> {
        let mut cursor = Cursor::new(b"".to_vec());

        let result = read_single_message(&mut cursor)?;
        assert!(result.is_none());
        Ok(())
    }

    #[test]
    fn test_read_messages_multiple() -> Result<(), Box<dyn std::error::Error>> {
        let data = b"$5\r\nhello\r\n:42\r\n";
        let mut cursor = Cursor::new(data.to_vec());

        let msgs = read_messages(&mut cursor)?;
        assert_eq!(msgs.len(), 2);
        Ok(())
    }

    #[test]
    fn test_read_messages_empty_stream() -> Result<(), Box<dyn std::error::Error>> {
        let mut cursor = Cursor::new(b"".to_vec());

        let msgs = read_messages(&mut cursor)?;
        assert!(msgs.is_empty());
        Ok(())
    }
}
