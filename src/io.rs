/// I/O utilities for reading from TCP streams.
///
/// This module provides functions for reading and parsing Redis protocol messages
/// from TCP streams with proper buffering and error handling.

use std::io::Read;
use crate::protocol;

/// Size of the read buffer for TCP operations
const BUFFER_SIZE: usize = 2048;

/// Reads a single message from a reader.
///
/// # Arguments
/// * `reader` - Any type implementing `Read` trait
///
/// # Returns
/// * `Ok(Some(message))` - A complete message was read
/// * `Ok(None)` - No data available (connection likely closed)
/// * `Err(e)` - Error parsing or reading
///
/// # Examples
/// ```ignore
/// let mut stream = TcpStream::connect("127.0.0.1:6379")?;
/// if let Some(msg) = read_single_message(&mut stream)? {
///     println!("Received: {:?}", msg);
/// }
/// ```
pub fn read_single_message(reader: &mut dyn Read) -> Result<Option<protocol::DataType>, anyhow::Error> {
    if let Some(message_bytes) = read_bytes(reader)? {
        Ok(Some(protocol::read_message_from_bytes(&message_bytes)?))
    } else {
        Ok(None)
    }
}

/// Reads multiple messages from a reader.
///
/// # Arguments
/// * `reader` - Any type implementing `Read` trait
///
/// # Returns
/// * `Ok(messages)` - A vector of parsed messages (empty if no data)
/// * `Err(e)` - Error parsing or reading
///
/// # Examples
/// ```ignore
/// let mut stream = TcpStream::connect("127.0.0.1:6379")?;
/// let messages = read_messages(&mut stream)?;
/// for msg in messages {
///     println!("Received: {:?}", msg);
/// }
/// ```
pub fn read_messages(reader: &mut dyn Read) -> Result<Vec<protocol::DataType>, anyhow::Error> {
    if let Some(message_bytes) = read_bytes(reader)? {
        Ok(protocol::read_messages_from_bytes(&message_bytes)?)
    } else {
        Ok(Vec::new())
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
fn read_next_bytes(reader: &mut dyn Read, buffer: &mut [u8]) -> usize {
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
pub fn read_bytes(reader: &mut dyn Read) -> Result<Option<Vec<u8>>, anyhow::Error> {
    let mut buffer = [0u8; BUFFER_SIZE];
    let mut message_bytes: Vec<u8> = Vec::new();
    let mut total_read_bytes = 0;

    loop {
        let read_bytes = read_next_bytes(reader, &mut buffer);
        if read_bytes > 0 {
            total_read_bytes += read_bytes;
            message_bytes.extend_from_slice(&buffer[0..read_bytes]);
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_read_bytes_single_bulk_string() -> Result<(), Box<dyn std::error::Error>> {
        // Test reading a single bulk string: $5\r\nHello\r\n
        let data = b"$5\r\nHello\r\n";
        let mut cursor = Cursor::new(data.to_vec());

        let result = read_bytes(&mut cursor)?;
        assert!(result.is_some());
        assert_eq!(result.unwrap(), data);
        Ok(())
    }

    #[test]
    fn test_read_bytes_multiple_bulk_strings() -> Result<(), Box<dyn std::error::Error>> {
        // Test reading multiple bulk strings
        let data = b"$5\r\nHello\r\n$5\r\nWorld\r\n";
        let mut cursor = Cursor::new(data.to_vec());

        let result = read_bytes(&mut cursor)?;
        assert!(result.is_some());
        assert_eq!(result.unwrap(), data);
        Ok(())
    }

    #[test]
    fn test_read_bytes_empty_stream() -> Result<(), Box<dyn std::error::Error>> {
        let data = b"";
        let mut cursor = Cursor::new(data.to_vec());

        let result = read_bytes(&mut cursor)?;
        assert_eq!(result, None);
        Ok(())
    }

    #[test]
    fn test_read_bytes_simple_string() -> Result<(), Box<dyn std::error::Error>> {
        // Test reading a simple string: +PONG\r\n
        let data = b"+PONG\r\n";
        let mut cursor = Cursor::new(data.to_vec());

        let result = read_bytes(&mut cursor)?;
        assert!(result.is_some());
        assert_eq!(result.unwrap(), data);
        Ok(())
    }

    #[test]
    fn test_read_bytes_array() -> Result<(), Box<dyn std::error::Error>> {
        // Test reading an array: *2\r\n$4\r\nPING\r\n$4\r\ntest\r\n
        let data = b"*2\r\n$4\r\nPING\r\n$4\r\ntest\r\n";
        let mut cursor = Cursor::new(data.to_vec());

        let result = read_bytes(&mut cursor)?;
        assert!(result.is_some());
        assert_eq!(result.unwrap(), data);
        Ok(())
    }

    #[test]
    fn test_read_bytes_integer() -> Result<(), Box<dyn std::error::Error>> {
        // Test reading an integer: :1000\r\n
        let data = b":1000\r\n";
        let mut cursor = Cursor::new(data.to_vec());

        let result = read_bytes(&mut cursor)?;
        assert!(result.is_some());
        assert_eq!(result.unwrap(), data);
        Ok(())
    }

    #[test]
    fn test_read_bytes_with_binary_data() -> Result<(), Box<dyn std::error::Error>> {
        // Test reading bulk string with binary data
        let data = b"$4\r\n\x00\x01\x02\x03\r\n";
        let mut cursor = Cursor::new(data.to_vec());

        let result = read_bytes(&mut cursor)?;
        assert!(result.is_some());
        assert_eq!(result.unwrap(), data);
        Ok(())
    }

    #[test]
    fn test_read_bytes_large_message() -> Result<(), Box<dyn std::error::Error>> {
        // Test reading a message larger than typical buffer
        let mut data = Vec::new();
        data.extend_from_slice(b"$10000\r\n");
        data.extend(vec![b'X'; 10000]);
        data.extend_from_slice(b"\r\n");

        let mut cursor = Cursor::new(data.clone());

        let result = read_bytes(&mut cursor)?;
        assert!(result.is_some());
        assert_eq!(result.unwrap(), data);
        Ok(())
    }

    #[test]
    fn test_read_bytes_null_bulk_string() -> Result<(), Box<dyn std::error::Error>> {
        // Test reading null bulk string: $-1\r\n
        let data = b"$-1\r\n";
        let mut cursor = Cursor::new(data.to_vec());

        let result = read_bytes(&mut cursor)?;
        assert!(result.is_some());
        assert_eq!(result.unwrap(), data);
        Ok(())
    }

    #[test]
    fn test_read_single_message() -> Result<(), Box<dyn std::error::Error>> {
        // Test reading and parsing a single message
        let data = b"+OK\r\n";
        let mut cursor = Cursor::new(data.to_vec());

        let result = read_single_message(&mut cursor)?;
        assert!(result.is_some());

        // Verify it's a simple string
        if let Some(msg) = result {
            assert_eq!(msg.as_string()?, "OK");
        }
        Ok(())
    }

    #[test]
    fn test_read_messages_multiple() -> Result<(), Box<dyn std::error::Error>> {
        // Test reading multiple messages
        let data = b"$5\r\nhello\r\n:42\r\n";
        let mut cursor = Cursor::new(data.to_vec());

        let msgs = read_messages(&mut cursor)?;
        assert_eq!(msgs.len(), 2);
        Ok(())
    }

    #[test]
    fn test_read_messages_empty() -> Result<(), Box<dyn std::error::Error>> {
        // Test reading from empty stream
        let data = b"";
        let mut cursor = Cursor::new(data.to_vec());

        let msgs = read_messages(&mut cursor)?;
        assert_eq!(msgs.len(), 0);
        Ok(())
    }
}
