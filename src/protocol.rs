use log::*;

use crate::error::RedisError;

pub fn read_messages_from_bytes(message_bytes: &[u8]) -> Result<Vec<DataType>, anyhow::Error> {
    let mut messages: Vec<DataType> = Vec::new();
    let mut current_position = 0;
    let total_length = message_bytes.len();

    while current_position < total_length {
        let (parsed, new_position) = DataType::parse(&message_bytes, current_position)?;
        current_position = new_position;
        messages.push(parsed);
    }
    trace!("Read messages bytes {:?}", message_bytes);
    trace!("Parsed them as messages {:?}", messages);
    Ok(messages)
}

pub fn read_message_from_bytes(message_bytes: &[u8]) -> Result<DataType, anyhow::Error> {
    let mut messages = read_messages_from_bytes(message_bytes)?;
    match messages.len() {
        1 => Ok(messages.remove(0)),
        n => Err(RedisError {
            message: format!("Expected exactly 1 message in '{}', got {}",
                String::from_utf8_lossy(message_bytes), n)
        }.into())
    }
}

fn find_crlf(input: &[u8], from: usize) -> Result<usize, anyhow::Error> {
    input[from..].windows(2)
        .position(|w| w == b"\r\n")
        .map(|p| from + p)
        .ok_or_else(|| RedisError {
            message: format!("Expected \\r\\n in '{}'", String::from_utf8_lossy(input))
        }.into())
}

fn parse_simple_line(input: &[u8], from: usize) -> Result<(&[u8], usize), anyhow::Error> {
    let crlf = find_crlf(input, from)?;
    Ok((&input[from..crlf], crlf + 2))
}

fn parse_length_prefixed_payload(input: &[u8], from: usize) -> Result<(&[u8], usize), anyhow::Error> {
    let (length_bytes, payload_start) = parse_simple_line(input, from)?;
    let length: usize = std::str::from_utf8(length_bytes)?.parse()?;
    let payload_end = payload_start + length;
    let payload = input.get(payload_start..payload_end)
        .ok_or_else(|| RedisError {
            message: format!("Payload truncated in '{}'", String::from_utf8_lossy(input))
        })?;
    if input.get(payload_end..payload_end + 2) != Some(b"\r\n") {
        return Err(RedisError {
            message: format!("Missing trailing \\r\\n in '{}'", String::from_utf8_lossy(input))
        }.into());
    }
    Ok((payload, payload_end + 2))
}

#[derive(Debug, PartialEq, Clone)]
pub enum DataType {
    Double {
        value: f64
    },
    BigNumber {
        sign: u8,
        value: Vec<u8> // more efficient representation is possible
    },
    Integer {
        value: i64
    },
    SimpleError {
        value: Vec<u8>
    },
    BulkString {
        value: Option<Vec<u8>>
    },
    Rdb {
        value: Vec<u8>
    },
    BulkError {
        value: Vec<u8>
    },
    VerbatimString {
        encoding: Vec<u8>,
        value: Vec<u8>
    },
    SimpleString {
        value: Vec<u8>
    },
    Map {
        entries: Vec<(DataType, DataType)>
    },
    Set {
        elements: Vec<DataType>
    },
    Array {
        elements: Vec<DataType>
    },
    Push {
        elements: Vec<DataType>
    },
    Null,
    Boolean {
        value: bool
    }
}

pub fn double(value: f64) -> DataType {
    DataType::Double {
        value
    }
}

pub fn simple_string(value: &str) -> DataType {
    DataType::SimpleString {
        value: value.as_bytes().to_vec()
    }
}

pub fn bulk_string_from_bytes(value: Vec<u8>) -> DataType {
    DataType::BulkString {
        value: Some(value)
    }
}

pub fn bulk_string_empty() -> DataType {
    DataType::BulkString {
        value: None
    }
}

pub fn bulk_string(value: &str) -> DataType {
    DataType::BulkString {
        value: Some(value.as_bytes().to_vec())
    }
}

pub fn array(elements: Vec<DataType>) -> DataType {
    DataType::Array { elements }
}

//TODO: Implement the rest of the constructors
/*
    BigNumber {
        sign: u8,
        value: Vec<u8> // more efficient representation is possible
    },
    Integer {
        value: i64
    },
    SimpleError {
        value: Vec<u8>
    },
    BulkString {
        value: Option<Vec<u8>>
    },
    BulkError {
        value: Vec<u8>
    },
    VerbatimString {
        encoding: Vec<u8>,
        value: Vec<u8>
    },
    Map {
        entries: Vec<(DataType, DataType)>
    },
    Set {
        elements: Vec<DataType>
    },
    Push {
        elements: Vec<DataType>
    }
*/

/*
pub(crate) fn big_number(value: f64) -> DataType {
}

pub(crate) fn integer(value: f64) -> DataType {
}

pub(crate) fn simple_error(value: f64) -> DataType {
}

pub(crate) fn bulk_string(value: f64) -> DataType {
}

pub(crate) fn bulk_error(value: f64) -> DataType {
}

pub(crate) fn verbatim_string(value: f64) -> DataType {
}

pub(crate) fn map(value: f64) -> DataType {
}

pub(crate) fn set(value: f64) -> DataType {
}

pub(crate) fn push(value: f64) -> DataType {
}
*/

pub fn null() -> DataType {
    DataType::Null
}

pub fn boolean(value: bool) -> DataType {
    DataType::Boolean {
        value
    }
}

impl DataType {

    pub fn as_array(&self) -> Result<Vec<String>, anyhow::Error> {
        match &self {
            &DataType::Array { elements } => {
                let mut result: Vec<String> = Vec::new();
                for element in elements.iter() {
                    result.push(element.as_string()?);
                }
                Ok(result)
            },
            _ => {
                Ok(vec![self.as_string()?])
            }
        }
    }

    pub fn as_string(&self) -> Result<String, anyhow::Error> {
        let mut result: Vec<u8> = Vec::new();
        match &self {
            &DataType::Double { value } => {
                result.extend(value.to_string().as_bytes());
            },
            &DataType::BigNumber { sign, value } => {
                if *sign == b'-' {
                    result.push(*sign)
                }
                result.extend(value);
            },
            &DataType::Integer { value } => {
                result.extend(value.to_string().as_bytes());
            },
            &DataType::SimpleError { value } => {
                result.extend(value.as_slice());
            },
            &DataType::BulkString { value } => {
                match value {
                    Some(value) => {
                        result.extend(value.as_slice())
                    },
                    None => ()
                }
            },
            &DataType::Rdb { value } => {
                result.extend(value.as_slice());
            },
            &DataType::BulkError { value } => {
                result.extend(value.as_slice());
            },
            &DataType::VerbatimString { encoding: _, value } => {
                result.extend(value.as_slice());
            },
            &DataType::SimpleString { value } => {
                result.extend(value.as_slice());
            },
            &DataType::Map { entries } => {
                for element in entries.iter() {
                    result.extend(element.0.as_string()?.as_bytes());
                    result.push(b':');
                    result.extend(element.1.as_string()?.as_bytes());
                    result.push(b',');
                }
            },
            &DataType::Set { elements } => {
                for element in elements.iter() {
                    result.extend(element.as_string()?.as_bytes());
                    result.push(b',');
                }
            },
            &DataType::Array { elements } => {
                for element in elements.iter() {
                    result.extend(element.as_string()?.as_bytes());
                    result.push(b',');
                }
            },
            &DataType::Push { elements } => {
                for element in elements.iter() {
                    result.extend(element.as_string()?.as_bytes());
                    result.push(b',');
                }
            },
            &DataType::Null => {
                result.extend("".as_bytes().to_vec())
            },
            &DataType::Boolean { value } => {
                if *value {
                    result.push(b't');
                } else {
                    result.push(b'f');
                }
            }
        }
        String::from_utf8(result).map_err(|err| err.into())
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut result: Vec<u8> = Vec::new();
        match &self {
            &DataType::Double { value } => {
                result.push(b',');
                result.extend(value.to_string().as_bytes());
                result.extend("\r\n".as_bytes());
            },
            &DataType::BigNumber { sign, value } => {
                result.push(b'(');
                if *sign == b'-' {
                    result.push(*sign)
                }
                result.extend(value);
                result.extend("\r\n".as_bytes());
            },
            &DataType::Integer { value } => {
                result.push(b':');
                result.extend(value.to_string().as_bytes());
                result.extend("\r\n".as_bytes());
            },
            &DataType::SimpleError { value } => {
                result.push(b'-');
                result.extend(value.as_slice());
                result.extend("\r\n".as_bytes());
            },
            &DataType::BulkString { value } => {
                result.push(b'$');
                match value {
                    Some(value) => {
                        result.extend(value.len().to_string().as_bytes());
                        result.extend("\r\n".as_bytes());
                        result.extend(value.as_slice());
                    },
                    None => {
                        result.extend("-1".as_bytes());
                    }
                }
                result.extend("\r\n".as_bytes());
            },
            &DataType::Rdb { value } => {
                result.push(b'$');
                result.extend(value.len().to_string().as_bytes());
                result.extend("\r\n".as_bytes());
                result.extend(value.as_slice());
            },
            &DataType::BulkError { value } => {
                result.push(b'!');
                result.extend(value.len().to_string().as_bytes());
                result.extend("\r\n".as_bytes());
                result.extend(value.as_slice());
                result.extend("\r\n".as_bytes());
            },
            &DataType::VerbatimString { encoding, value } => {
                result.push(b'=');
                result.extend((value.len() + encoding.len() + 1).to_string().as_bytes());
                result.extend("\r\n".as_bytes());
                result.extend(encoding.as_slice());
                result.push(b':');
                result.extend(value.as_slice());
                result.extend("\r\n".as_bytes());
            },
            &DataType::SimpleString { value } => {
                result.push(b'+');
                result.extend(value.as_slice());
                result.extend("\r\n".as_bytes());
            },
            &DataType::Map { entries } => {
                result.push(b'%');
                result.extend(entries.len().to_string().as_bytes());
                result.extend("\r\n".as_bytes());
                for element in entries.iter() {
                    result.extend(element.0.serialize());
                    result.extend(element.1.serialize());
                }
            },
            &DataType::Set { elements } => {
                result.push(b'~');
                result.extend(elements.len().to_string().as_bytes());
                result.extend("\r\n".as_bytes());
                for element in elements.iter() {
                    result.extend(element.serialize());
                }
            },
            &DataType::Array { elements } => {
                result = serialize_array_like(&elements, b'*')
            },
            &DataType::Push { elements } => {
                result = serialize_array_like(&elements, b'>')
            },
            &DataType::Null => {
                result.extend("_\r\n".as_bytes().to_vec())
            },
            &DataType::Boolean { value } => {
                result.push(b'#');
                if *value {
                    result.push(b't');
                } else {
                    result.push(b'f');
                }
                result.extend("\r\n".as_bytes());
            }
        }
        result
    }

    pub(crate) fn parse(input: &[u8], position: usize) -> Result<(DataType, usize), anyhow::Error> {
        if let Some(prefix_symbol) = input.get(position) {
            match prefix_symbol {
                b',' => {
                    parse_double(input, position)
                },
                b'(' => {
                    parse_big_number(input, position)
                },
                b':' => {
                    parse_integer(input, position)
                },
                b'-' => {
                    parse_simple_error(input, position)
                },
                b'$' => {
                    parse_bulk_string_or_rdb(input, position)
                },
                b'!' => {
                    parse_bulk_error(input, position)
                },
                b'=' => {
                    parse_verbatim_string(input, position)
                },
                b'+' => {
                    parse_simple_string(input, position)
                },
                b'%' => {
                    parse_map(input, position)
                },
                b'~' => {
                    parse_set(input, position)
                },
                b'*' => {
                    parse_array(input, position)
                },
                b'>' => {
                    parse_push(input, position)
                },
                b'_' => {
                    parse_null(input, position)
                },
                b'#' => {
                    parse_boolean(input, position)
                },
                ch =>
                    Err(RedisError { 
                        message: format!("Could not read the next data type value '{}' at position {}, unsupported prefix '{}'",
                            String::from_utf8_lossy(input),
                            position,
                            String::from_utf8_lossy(&[*ch])
                        )
                    }.into())
            }
        } else {
            Err(RedisError { message: format!("Could not read the next data type value '{}' at position {}", String::from_utf8_lossy(input), position) }.into())
        }
    }
}

fn parse_double(input: &[u8], position: usize) -> Result<(DataType, usize), anyhow::Error> {
    let (line, new_pos) = parse_simple_line(input, position + 1)?;
    let value: f64 = std::str::from_utf8(line)?.parse()?;
    Ok((DataType::Double { value }, new_pos))
}

fn parse_big_number(input: &[u8], position: usize) -> Result<(DataType, usize), anyhow::Error> {
    let (line, new_pos) = parse_simple_line(input, position + 1)?;
    let (sign, digits) = match line.first() {
        Some(&b'+') => (b'+', &line[1..]),
        Some(&b'-') => (b'-', &line[1..]),
        _           => (b'+', line),
    };
    Ok((DataType::BigNumber { sign, value: digits.to_vec() }, new_pos))
}

fn parse_integer(input: &[u8], position: usize) -> Result<(DataType, usize), anyhow::Error> {
    let (line, new_pos) = parse_simple_line(input, position + 1)?;
    let value: i64 = std::str::from_utf8(line)?.parse()?;
    Ok((DataType::Integer { value }, new_pos))
}

fn parse_simple_error(input: &[u8], position: usize) -> Result<(DataType, usize), anyhow::Error> {
    let (value, new_pos) = parse_simple_line(input, position + 1)?;
    Ok((DataType::SimpleError { value: value.to_vec() }, new_pos))
}

// This can be either a BulkString or RDB: if input ends without a trailing \r\n it is an RDB file.
fn parse_bulk_string_or_rdb(input: &[u8], position: usize) -> Result<(DataType, usize), anyhow::Error> {
    let (length_bytes, payload_start) = parse_simple_line(input, position + 1)?;
    if length_bytes == b"-1" {
        return Ok((DataType::BulkString { value: None }, payload_start));
    }
    let length: usize = std::str::from_utf8(length_bytes)?.parse()?;
    let payload_end = payload_start + length;
    if input.get(payload_end..payload_end + 2) == Some(b"\r\n") {
        Ok((DataType::BulkString { value: Some(input[payload_start..payload_end].to_vec()) }, payload_end + 2))
    } else {
        Ok((DataType::Rdb { value: input[payload_start..payload_end].to_vec() }, payload_end))
    }
}

fn parse_bulk_error(input: &[u8], position: usize) -> Result<(DataType, usize), anyhow::Error> {
    let (value, new_pos) = parse_length_prefixed_payload(input, position + 1)?;
    Ok((DataType::BulkError { value: value.to_vec() }, new_pos))
}

fn parse_verbatim_string(input: &[u8], position: usize) -> Result<(DataType, usize), anyhow::Error> {
    let (payload, new_pos) = parse_length_prefixed_payload(input, position + 1)?;
    let sep = payload.iter().position(|&b| b == b':')
        .ok_or_else(|| RedisError {
            message: format!("Invalid VerbatimString '{}'", String::from_utf8_lossy(input))
        })?;
    Ok((DataType::VerbatimString {
        encoding: payload[..sep].to_vec(),
        value: payload[sep + 1..].to_vec(),
    }, new_pos))
}

fn parse_simple_string(input: &[u8], position: usize) -> Result<(DataType, usize), anyhow::Error> {
    let (value, new_pos) = parse_simple_line(input, position + 1)?;
    Ok((DataType::SimpleString { value: value.to_vec() }, new_pos))
}

fn parse_map(input: &[u8], position: usize) -> Result<(DataType, usize), anyhow::Error> {
    let (length_bytes, mut current_pos) = parse_simple_line(input, position + 1)?;
    let count: i64 = std::str::from_utf8(length_bytes)?.parse()?;
    let mut entries = Vec::new();
    for _ in 0..count {
        let (key, after_key) = DataType::parse(input, current_pos)?;
        let (value, after_value) = DataType::parse(input, after_key)?;
        entries.push((key, value));
        current_pos = after_value;
    }
    Ok((DataType::Map { entries }, current_pos))
}

fn parse_set(input: &[u8], position: usize) -> Result<(DataType, usize), anyhow::Error> {
    let (elements, new_pos) = parse_array_like(input, position)?;
    Ok((DataType::Set { elements }, new_pos))
}

fn serialize_array_like(elements: &Vec<DataType>, prefix: u8) -> Vec<u8> {
    let mut result: Vec<u8> = Vec::new();
    result.push(prefix);
    result.extend(elements.len().to_string().as_bytes());
    result.extend("\r\n".as_bytes());
    for element in elements.iter() {
        result.extend(element.serialize());
    }
    result
}

fn parse_array_like(input: &[u8], position: usize) -> Result<(Vec<DataType>, usize), anyhow::Error> {
    let (length_bytes, mut current_pos) = parse_simple_line(input, position + 1)?;
    let count: i64 = std::str::from_utf8(length_bytes)?.parse()?;
    let mut elements = Vec::new();
    for _ in 0..count {
        let (element, next_pos) = DataType::parse(input, current_pos)?;
        elements.push(element);
        current_pos = next_pos;
    }
    Ok((elements, current_pos))
}

fn parse_array(input: &[u8], position: usize) -> Result<(DataType, usize), anyhow::Error> {
    let (elements, new_pos) = parse_array_like(input, position)?;
    Ok((DataType::Array { elements }, new_pos))
}

fn parse_push(input: &[u8], position: usize) -> Result<(DataType, usize), anyhow::Error> {
    let (elements, new_pos) = parse_array_like(input, position)?;
    Ok((DataType::Push { elements }, new_pos))
}

fn parse_null(input: &[u8], position: usize) -> Result<(DataType, usize), anyhow::Error> {
    if input.get(position..position + 3) == Some(b"_\r\n") {
        Ok((DataType::Null {}, position + 3))
    } else {
        Err(RedisError { message: format!("Invalid Null in '{}'", String::from_utf8_lossy(input)) }.into())
    }
}

fn parse_boolean(input: &[u8], position: usize) -> Result<(DataType, usize), anyhow::Error> {
    match input.get(position..position + 4) {
        Some(b"#t\r\n") => Ok((DataType::Boolean { value: true }, position + 4)),
        Some(b"#f\r\n") => Ok((DataType::Boolean { value: false }, position + 4)),
        _ => Err(RedisError { message: format!("Invalid Boolean in '{}'", String::from_utf8_lossy(input)) }.into()),
    }
}

#[cfg(test)]
mod tests {
    use core::f64;
    use super::*;

    #[test]
    fn should_serialize_set() {
        assert_eq!(String::from_utf8_lossy(&DataType::Set {
            elements: vec![
                DataType::Integer {
                    value: 1
                },
                DataType::BulkString {
                    value: Some("hello".as_bytes().to_vec())
                }
            ]
        }.serialize()), "~2\r\n:1\r\n$5\r\nhello\r\n".to_string());
    }

    #[test]
    fn should_parse_set() -> Result<(), Box<dyn std::error::Error>> {
        let parsed = DataType::parse(&"~2\r\n:1\r\n$5\r\nhello\r\n".as_bytes().to_vec(), 0)?;
        assert_eq!(parsed.0, DataType::Set {
            elements: vec![
                DataType::Integer {
                    value: 1
                },
                DataType::BulkString {
                    value: Some("hello".as_bytes().to_vec())
                }
            ]
        });
        assert_eq!(parsed.1, 19);
        Ok(())
    }

    #[test]
    fn should_serialize_verbatim_string() {
        assert_eq!(String::from_utf8_lossy(&DataType::VerbatimString {
            encoding: "txt".as_bytes().to_vec(),
            value: "Some string".as_bytes().to_vec()
        }.serialize()), "=15\r\ntxt:Some string\r\n".to_string());
    }

    #[test]
    fn should_parse_verbatim_string() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(DataType::parse(&"=15\r\ntxt:Some string\r\n".as_bytes().to_vec(), 0)?, (DataType::VerbatimString {
            encoding: "txt".as_bytes().to_vec(),
            value: "Some string".as_bytes().to_vec()
        }, 22));
        Ok(())
    }

    #[test]
    fn should_serialize_bulk_error() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(String::from_utf8_lossy(&DataType::BulkError {
            value: "Some error".as_bytes().to_vec()
        }.serialize()), "!10\r\nSome error\r\n".to_string());
        Ok(())
    }

    #[test]
    fn should_parse_bulk_error() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(DataType::parse(&"!21\r\nSYNTAX invalid syntax\r\n".as_bytes().to_vec(), 0)?, (DataType::BulkError {
            value: "SYNTAX invalid syntax".as_bytes().to_vec()
        }, 28));
        Ok(())
    }

    #[test]
    fn should_serialize_big_number() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(String::from_utf8_lossy(&DataType::BigNumber { sign: b'+', value: "349".as_bytes().to_vec() }.serialize()), "(349\r\n".to_string());
        assert_eq!(String::from_utf8_lossy(&DataType::BigNumber { sign: b'-', value: "349".as_bytes().to_vec() }.serialize()), "(-349\r\n".to_string());
        Ok(())
    }

    #[test]
    fn should_parse_big_number() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(DataType::parse(&"(349\r\n".as_bytes().to_vec(), 0)?.0, DataType::BigNumber { sign: b'+', value: "349".as_bytes().to_vec() });
        assert_eq!(DataType::parse(&"(+349\r\n".as_bytes().to_vec(), 0)?.0, DataType::BigNumber { sign: b'+', value: "349".as_bytes().to_vec() });
        assert_eq!(DataType::parse(&"(-123\r\n".as_bytes().to_vec(), 0)?.0, DataType::BigNumber { sign: b'-', value: "123".as_bytes().to_vec() });
        Ok(())
    }

    #[test]
    fn should_serialize_double() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(String::from_utf8_lossy(&DataType::Double { value: 1.23 }.serialize()), ",1.23\r\n".to_string());
        Ok(())
    }

    #[test]
    fn should_parse_double() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(DataType::parse(&",10\r\n".as_bytes().to_vec(), 0)?.0, DataType::Double { value: 10.0 });
        assert_eq!(DataType::parse(&",1.23\r\n".as_bytes().to_vec(), 0)?.0, DataType::Double { value: 1.23 });
        assert_eq!(DataType::parse(&",inf\r\n".as_bytes().to_vec(), 0)?.0, DataType::Double { value: f64::INFINITY });
        assert_eq!(DataType::parse(&",-inf\r\n".as_bytes().to_vec(), 0)?.0, DataType::Double { value: f64::NEG_INFINITY });
        match DataType::parse(&",nan\r\n".as_bytes().to_vec(), 0)?.0 {
            DataType::Double { value } => assert!(value.is_nan()),
            _ => assert!(false)
        }
        Ok(())
    }

    #[test]
    fn should_serialize_boolean() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(String::from_utf8_lossy(&DataType::Boolean { value: true }.serialize()), "#t\r\n".to_string());
        assert_eq!(String::from_utf8_lossy(&DataType::Boolean { value: false }.serialize()), "#f\r\n".to_string());
        Ok(())
    }

    #[test]
    fn should_parse_boolean() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(DataType::parse(&"#t\r\n".as_bytes().to_vec(), 0)?.0, DataType::Boolean { value: true });
        assert_eq!(DataType::parse(&"#f\r\n".as_bytes().to_vec(), 0)?.0, DataType::Boolean { value: false });
        Ok(())
    }

    #[test]
    fn should_serialize_null() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(String::from_utf8_lossy(&DataType::Null {}.serialize()), "_\r\n".to_string());
        Ok(())
    }

    #[test]
    fn should_parse_null() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(DataType::parse(&"_\r\n".as_bytes().to_vec(), 0)?.0, DataType::Null {});
        Ok(())
    }

    #[test]
    fn should_serialize_map() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(String::from_utf8_lossy(&DataType::Map {
            entries: vec![
                (
                    DataType::Integer {
                        value: 1
                    },
                    DataType::BulkString {
                        value: Some("hello".as_bytes().to_vec())
                    }
                ),
                (
                    DataType::Integer {
                        value: 2
                    },
                    DataType::BulkString {
                        value: Some("world".as_bytes().to_vec())
                    }
                )
            ]
        }.serialize()), "%2\r\n:1\r\n$5\r\nhello\r\n:2\r\n$5\r\nworld\r\n".to_string());
        Ok(())
    }

    #[test]
    fn should_parse_map() -> Result<(), Box<dyn std::error::Error>> {
        let parsed = DataType::parse(&"%2\r\n:1\r\n$5\r\nhello\r\n:2\r\n$5\r\nworld\r\n".as_bytes().to_vec(), 0)?;
        assert_eq!(parsed.0, DataType::Map {
            entries: vec![
                (
                    DataType::Integer {
                        value: 1
                    },
                    DataType::BulkString {
                        value: Some("hello".as_bytes().to_vec())
                    }
                ),
                (
                    DataType::Integer {
                        value: 2
                    },
                    DataType::BulkString {
                        value: Some("world".as_bytes().to_vec())
                    }
                )
            ]
        });
        Ok(())
    }

    #[test]
    fn should_serialize_array() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(String::from_utf8_lossy(&DataType::Array {
            elements: vec![
                DataType::BulkString {
                    value: Some("hello".as_bytes().to_vec())
                },
                DataType::BulkString {
                    value: Some("world".as_bytes().to_vec())
                }
            ]
        }.serialize()), "*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n".to_string());
        Ok(())
    }

    #[test]
    fn should_parse_array() -> Result<(), Box<dyn std::error::Error>> {
        let mut parsed = DataType::parse(&"*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n".as_bytes().to_vec(), 0)?;
        assert_eq!(parsed.0, DataType::Array {
            elements: vec![
                DataType::BulkString {
                    value: Some("hello".as_bytes().to_vec())
                },
                DataType::BulkString {
                    value: Some("world".as_bytes().to_vec())
                }
            ]
        });
        assert_eq!(parsed.1, 26);

        parsed = DataType::parse(&"*-1\r\n".as_bytes().to_vec(), 0)?;
        assert_eq!(parsed.0, DataType::Array { elements: Vec::new() });
        assert_eq!(parsed.1, 5);
        Ok(())
    }

    #[test]
    fn should_serialize_push() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(String::from_utf8_lossy(&DataType::Push {
            elements: vec![
                DataType::BulkString {
                    value: Some("hello".as_bytes().to_vec())
                },
                DataType::BulkString {
                    value: Some("world".as_bytes().to_vec())
                }
            ]
        }.serialize()), ">2\r\n$5\r\nhello\r\n$5\r\nworld\r\n".to_string());
        Ok(())
    }

    #[test]
    fn should_parse_push() -> Result<(), Box<dyn std::error::Error>> {
        let parsed = DataType::parse(&">2\r\n$5\r\nhello\r\n$5\r\nworld\r\n".as_bytes().to_vec(), 0)?;
        assert_eq!(parsed.0, DataType::Push {
            elements: vec![
                DataType::BulkString {
                    value: Some("hello".as_bytes().to_vec())
                },
                DataType::BulkString {
                    value: Some("world".as_bytes().to_vec())
                }
            ]
        });
        assert_eq!(parsed.1, 26);
        Ok(())
    }

    #[test]
    fn should_serialize_bulk_string() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(String::from_utf8_lossy(&DataType::BulkString {
            value: Some("This is a bulk string\r\n One, two three".as_bytes().to_vec())
        }.serialize()), "$38\r\nThis is a bulk string\r\n One, two three\r\n".to_string());
        Ok(())
    }

    #[test]
    fn should_parse_bulk_string() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(DataType::parse(&"$5\r\nHello\r\n".as_bytes().to_vec(), 0)?, (DataType::BulkString {
            value: Some("Hello".as_bytes().to_vec())
        }, 11));
        assert_eq!(DataType::parse(&"$12\r\nHello\r\nworld\r\n".as_bytes().to_vec(), 0)?, (DataType::BulkString {
            value: Some("Hello\r\nworld".as_bytes().to_vec())
        }, 19));
        assert_eq!(DataType::parse(&"$-1\r\n".as_bytes().to_vec(), 0)?, (DataType::BulkString {
            value: None
        }, 5));
        assert_eq!(DataType::parse(&"$0\r\n\r\n".as_bytes().to_vec(), 0)?, (DataType::BulkString {
            value: Some(Vec::new())
        }, 6));
        Ok(())
    }

    #[test]
    fn should_parse_rdb() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(DataType::parse(&"$8\r\nfake_rdb".as_bytes().to_vec(), 0)?, (DataType::Rdb {
            value: "fake_rdb".as_bytes().to_vec()
        }, 12));
        assert_eq!(DataType::parse(&"$9\r\nfake\r\nrdb".as_bytes().to_vec(), 0)?, (DataType::Rdb {
            value: "fake\r\nrdb".as_bytes().to_vec()
        }, 13));
        Ok(())
    }

    #[test]
    fn should_serialize_integer() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(String::from_utf8_lossy(&DataType::Integer {
            value: 0
        }.serialize()), ":0\r\n".to_string());
        assert_eq!(String::from_utf8_lossy(&DataType::Integer {
            value: 101
        }.serialize()), ":101\r\n".to_string());
        assert_eq!(String::from_utf8_lossy(&DataType::Integer {
            value: -15
        }.serialize()), ":-15\r\n".to_string());
        Ok(())
    }

    #[test]
    fn should_parse_valid_integer() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(DataType::parse(&":+5\r\n".as_bytes().to_vec(), 0)?.0, DataType::Integer { value: 5 });
        assert_eq!(DataType::parse(&":0\r\n".as_bytes().to_vec(), 0)?.0, DataType::Integer { value: 0 });
        assert_eq!(DataType::parse(&":-98\r\n".as_bytes().to_vec(), 0)?.0, DataType::Integer { value: -98 });
        Ok(())
    }

    #[test]
    fn should_serialize_simple_error() -> Result<(), Box<dyn std::error::Error>> {
        let string_value = "Error message";
        let s = DataType::SimpleError {
            value: string_value.as_bytes().to_vec()
        };
        let serialization = s.serialize();
        assert_eq!(String::from_utf8(serialization)?, format!("-{}\r\n", string_value));
        Ok(())
    }

    #[test]
    fn should_parse_valid_simple_error() -> Result<(), Box<dyn std::error::Error>> {
        let input = "-Error message\r\n".as_bytes().to_vec();
        let result = DataType::parse(&input, 0)?;
        assert_eq!(result, (DataType::SimpleError {
            value: "Error message".as_bytes().to_vec()
        }, 16));
        Ok(())
    }

    #[test]
    fn should_serialize_simple_string() -> Result<(), Box<dyn std::error::Error>> {
        let string_value = "abcde";
        let s = DataType::SimpleString {
            value: string_value.as_bytes().to_vec()
        };
        let serialization = s.serialize();
        assert_eq!(String::from_utf8(serialization)?, format!("+{}\r\n", string_value));
        Ok(())
    }

    #[test]
    fn should_parse_valid_simple_string() -> Result<(), Box<dyn std::error::Error>> {
        let input = "+hello\r\n".as_bytes().to_vec();
        let result = DataType::parse(&input, 0)?;
        assert_eq!(result, (DataType::SimpleString {
            value: "hello".as_bytes().to_vec()
        }, 8));
        Ok(())
    }

    #[test]
    fn should_not_fail_parsing_if_more_bytes_are_provided() -> Result<(), Box<dyn std::error::Error>> {
        let input = "+hello\r\n+world\r\n";
        let result = DataType::parse(&input.as_bytes().to_vec(), 0)?;
        assert_eq!(result, (DataType::SimpleString {
            value: "hello".as_bytes().to_vec()
        }, 8));
        Ok(())
    }

    #[test]
    fn should_fail_parsing_invalid_simple_string() -> Result<(), Box<dyn std::error::Error>> {
        let input = "a+5\r\n";
        let error = DataType::parse(&input.as_bytes().to_vec(), 0).unwrap_err();
        assert_eq!(format!("{}", error), format!("RedisError: Could not read the next data type value '{}' at position 0, unsupported prefix 'a'", input));
        Ok(())
    }

    #[test]
    fn should_read_message_from_bytes() -> Result<(), Box<dyn std::error::Error>> {
        let parsed_single_message = read_messages_from_bytes(&"$5\r\nHello\r\n".as_bytes().to_vec())?;
        assert_eq!(parsed_single_message, vec![DataType::BulkString {
            value: Some("Hello".as_bytes().to_vec())
        }]);
        let parsed_messages = read_messages_from_bytes(&"$1\r\na\r\n$2\r\nbc\r\n$3\r\ndef\r\n".as_bytes().to_vec())?;
        assert_eq!(parsed_messages, vec![DataType::BulkString {
            value: Some("a".as_bytes().to_vec())
        }, DataType::BulkString {
            value: Some("bc".as_bytes().to_vec())
        }, DataType::BulkString {
            value: Some("def".as_bytes().to_vec())
        }]);
        Ok(())
    }

    // read_message_from_bytes

    #[test]
    fn should_read_exactly_one_message() -> Result<(), Box<dyn std::error::Error>> {
        let result = read_message_from_bytes("+OK\r\n".as_bytes())?;
        assert_eq!(result, DataType::SimpleString { value: "OK".as_bytes().to_vec() });
        Ok(())
    }

    #[test]
    fn should_fail_read_message_from_bytes_when_multiple_messages() {
        let err = read_message_from_bytes("+hello\r\n+world\r\n".as_bytes()).unwrap_err();
        assert!(format!("{}", err).contains("Expected exactly 1 message"));
    }

    #[test]
    fn should_fail_read_message_from_bytes_when_empty_input() {
        let err = read_message_from_bytes("".as_bytes()).unwrap_err();
        assert!(format!("{}", err).contains("Expected exactly 1 message"));
    }

    // as_string()

    #[test]
    fn should_convert_simple_string_to_string() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(DataType::SimpleString { value: "hello".as_bytes().to_vec() }.as_string()?, "hello");
        Ok(())
    }

    #[test]
    fn should_convert_bulk_string_to_string() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(DataType::BulkString { value: Some("world".as_bytes().to_vec()) }.as_string()?, "world");
        assert_eq!(DataType::BulkString { value: None }.as_string()?, "");
        Ok(())
    }

    #[test]
    fn should_convert_integer_to_string() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(DataType::Integer { value: 42 }.as_string()?, "42");
        assert_eq!(DataType::Integer { value: -7 }.as_string()?, "-7");
        Ok(())
    }

    #[test]
    fn should_convert_double_to_string() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(DataType::Double { value: 1.5 }.as_string()?, "1.5");
        Ok(())
    }

    #[test]
    fn should_convert_simple_error_to_string() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(DataType::SimpleError { value: "ERR bad".as_bytes().to_vec() }.as_string()?, "ERR bad");
        Ok(())
    }

    #[test]
    fn should_convert_null_to_string() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(DataType::Null {}.as_string()?, "");
        Ok(())
    }

    #[test]
    fn should_convert_boolean_to_string() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(DataType::Boolean { value: true }.as_string()?, "t");
        assert_eq!(DataType::Boolean { value: false }.as_string()?, "f");
        Ok(())
    }

    #[test]
    fn should_convert_big_number_to_string() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(DataType::BigNumber { sign: b'+', value: "12345".as_bytes().to_vec() }.as_string()?, "12345");
        assert_eq!(DataType::BigNumber { sign: b'-', value: "12345".as_bytes().to_vec() }.as_string()?, "-12345");
        Ok(())
    }

    #[test]
    fn should_convert_bulk_error_to_string() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(DataType::BulkError { value: "ERR details".as_bytes().to_vec() }.as_string()?, "ERR details");
        Ok(())
    }

    #[test]
    fn should_convert_verbatim_string_to_string() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(DataType::VerbatimString {
            encoding: "txt".as_bytes().to_vec(),
            value: "hello".as_bytes().to_vec()
        }.as_string()?, "hello");
        Ok(())
    }

    #[test]
    fn should_convert_rdb_to_string() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(DataType::Rdb { value: "rdbdata".as_bytes().to_vec() }.as_string()?, "rdbdata");
        Ok(())
    }

    #[test]
    fn should_convert_map_to_string() -> Result<(), Box<dyn std::error::Error>> {
        let result = DataType::Map {
            entries: vec![
                (
                    DataType::SimpleString { value: "k1".as_bytes().to_vec() },
                    DataType::SimpleString { value: "v1".as_bytes().to_vec() }
                ),
            ]
        }.as_string()?;
        assert_eq!(result, "k1:v1,");
        Ok(())
    }

    #[test]
    fn should_convert_set_to_string() -> Result<(), Box<dyn std::error::Error>> {
        let result = DataType::Set {
            elements: vec![
                DataType::SimpleString { value: "a".as_bytes().to_vec() },
                DataType::SimpleString { value: "b".as_bytes().to_vec() },
            ]
        }.as_string()?;
        assert_eq!(result, "a,b,");
        Ok(())
    }

    #[test]
    fn should_convert_array_to_string() -> Result<(), Box<dyn std::error::Error>> {
        let result = DataType::Array {
            elements: vec![
                DataType::Integer { value: 1 },
                DataType::Integer { value: 2 },
            ]
        }.as_string()?;
        assert_eq!(result, "1,2,");
        Ok(())
    }

    #[test]
    fn should_convert_push_to_string() -> Result<(), Box<dyn std::error::Error>> {
        let result = DataType::Push {
            elements: vec![
                DataType::SimpleString { value: "msg".as_bytes().to_vec() },
            ]
        }.as_string()?;
        assert_eq!(result, "msg,");
        Ok(())
    }

    // as_array()

    #[test]
    fn should_convert_array_as_array() -> Result<(), Box<dyn std::error::Error>> {
        let result = DataType::Array {
            elements: vec![
                DataType::SimpleString { value: "hello".as_bytes().to_vec() },
                DataType::Integer { value: 42 },
            ]
        }.as_array()?;
        assert_eq!(result, vec!["hello".to_string(), "42".to_string()]);
        Ok(())
    }

    #[test]
    fn should_wrap_non_array_as_single_element_array() -> Result<(), Box<dyn std::error::Error>> {
        let result = DataType::SimpleString { value: "hello".as_bytes().to_vec() }.as_array()?;
        assert_eq!(result, vec!["hello".to_string()]);
        Ok(())
    }

    // Empty collections

    #[test]
    fn should_serialize_empty_map() {
        assert_eq!(String::from_utf8_lossy(&DataType::Map { entries: vec![] }.serialize()), "%0\r\n");
    }

    #[test]
    fn should_parse_empty_map() -> Result<(), Box<dyn std::error::Error>> {
        let parsed = DataType::parse(&"%0\r\n".as_bytes().to_vec(), 0)?;
        assert_eq!(parsed.0, DataType::Map { entries: vec![] });
        assert_eq!(parsed.1, 4);
        Ok(())
    }

    #[test]
    fn should_serialize_empty_set() {
        assert_eq!(String::from_utf8_lossy(&DataType::Set { elements: vec![] }.serialize()), "~0\r\n");
    }

    #[test]
    fn should_parse_empty_set() -> Result<(), Box<dyn std::error::Error>> {
        let parsed = DataType::parse(&"~0\r\n".as_bytes().to_vec(), 0)?;
        assert_eq!(parsed.0, DataType::Set { elements: vec![] });
        assert_eq!(parsed.1, 4);
        Ok(())
    }

    #[test]
    fn should_serialize_empty_push() {
        assert_eq!(String::from_utf8_lossy(&DataType::Push { elements: vec![] }.serialize()), ">0\r\n");
    }

    #[test]
    fn should_parse_empty_push() -> Result<(), Box<dyn std::error::Error>> {
        let parsed = DataType::parse(&">0\r\n".as_bytes().to_vec(), 0)?;
        assert_eq!(parsed.0, DataType::Push { elements: vec![] });
        assert_eq!(parsed.1, 4);
        Ok(())
    }

    // Nested structures

    #[test]
    fn should_parse_nested_array() -> Result<(), Box<dyn std::error::Error>> {
        let parsed = DataType::parse(&"*2\r\n*1\r\n:1\r\n:2\r\n".as_bytes().to_vec(), 0)?;
        assert_eq!(parsed.0, DataType::Array {
            elements: vec![
                DataType::Array {
                    elements: vec![DataType::Integer { value: 1 }]
                },
                DataType::Integer { value: 2 }
            ]
        });
        assert_eq!(parsed.1, 16);
        Ok(())
    }

    // Non-zero parse position

    #[test]
    fn should_parse_at_non_zero_position() -> Result<(), Box<dyn std::error::Error>> {
        let input = "+hello\r\n+world\r\n";
        let result = DataType::parse(&input.as_bytes().to_vec(), 8)?;
        assert_eq!(result, (DataType::SimpleString {
            value: "world".as_bytes().to_vec()
        }, 16));
        Ok(())
    }

    // Error cases

    #[test]
    fn should_fail_parsing_at_position_past_end() {
        let err = DataType::parse(&"".as_bytes().to_vec(), 0).unwrap_err();
        assert!(format!("{}", err).contains("Could not read the next data type value"));
    }

    #[test]
    fn should_fail_parsing_integer_with_non_numeric_value() {
        let err = DataType::parse(&":abc\r\n".as_bytes().to_vec(), 0).unwrap_err();
        assert!(format!("{}", err).contains("invalid digit found in string"));
    }

    #[test]
    fn should_fail_parsing_double_with_non_numeric_value() {
        let err = DataType::parse(&",foo\r\n".as_bytes().to_vec(), 0).unwrap_err();
        assert!(format!("{}", err).contains("invalid float literal"));
    }

    #[test]
    fn should_fail_parsing_verbatim_string_without_colon_separator() {
        let err = DataType::parse(&"=3\r\ntxt\r\n".as_bytes().to_vec(), 0).unwrap_err();
        assert!(format!("{}", err).contains("Invalid VerbatimString"));
    }

    // Rdb serialization

    #[test]
    fn should_serialize_rdb() {
        assert_eq!(
            String::from_utf8_lossy(&DataType::Rdb { value: "fake_rdb".as_bytes().to_vec() }.serialize()),
            "$8\r\nfake_rdb"
        );
    }

    // Constructor functions

    #[test]
    fn should_create_types_with_constructors() {
        assert_eq!(double(1.5), DataType::Double { value: 1.5 });
        assert_eq!(simple_string("hello"), DataType::SimpleString { value: "hello".as_bytes().to_vec() });
        assert_eq!(bulk_string("hi"), DataType::BulkString { value: Some("hi".as_bytes().to_vec()) });
        assert_eq!(bulk_string_from_bytes(b"hi".to_vec()), DataType::BulkString { value: Some(b"hi".to_vec()) });
        assert_eq!(bulk_string_empty(), DataType::BulkString { value: None });
        assert_eq!(array(vec![DataType::Null {}]), DataType::Array { elements: vec![DataType::Null {}] });
        assert_eq!(null(), DataType::Null {});
        assert_eq!(boolean(true), DataType::Boolean { value: true });
        assert_eq!(boolean(false), DataType::Boolean { value: false });
    }
}
