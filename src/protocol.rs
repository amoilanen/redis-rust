use anyhow::{anyhow, ensure, Context};

use crate::error::RedisError;

pub fn read_messages_from_bytes(message_bytes: &Vec<u8>) -> Result<Vec<DataType>, anyhow::Error> {
    let mut messages: Vec<DataType> = Vec::new();
    let mut current_position = 0;
    let total_length = message_bytes.len();

    while current_position < total_length {
        let (parsed, new_position) = DataType::parse(&message_bytes, current_position)?;
        current_position = new_position;
        messages.push(parsed);
    }
    println!("Read messages bytes {:?}", message_bytes);
    println!("Parsed them as messages {:?}", messages);
    Ok(messages)
}

//TODO #2: This might return multiple messages at one time, messages are not necessarily received one by one
pub fn read_message_from_bytes(message_bytes: &Vec<u8>) -> Result<DataType, anyhow::Error> {
    let (parsed, position) = DataType::parse(&message_bytes, 0)?;
    println!("Read message bytes {:?}", message_bytes);
    println!("Parsed them as {:?}", parsed);
    if position == message_bytes.len() {
        Ok(parsed)
    } else {
        Err(RedisError { 
            message: format!("Could not parse '{:?}': symbols after position {:?} are left unconsumed, total symbols {:?}",
                String::from_utf8_lossy(&message_bytes.clone()),
                position,
                message_bytes.len()
            )
        }.into())
    }
}

fn read_and_assert_symbol(input: &Vec<u8>, symbol: u8, position: usize) -> Result<usize, anyhow::Error> {
    let error_message = format!("Expected symbol '{}' in '{}' at position {}", symbol as char, String::from_utf8_lossy(&input.clone()), position);
    let &actual_symbol = input.get(position).ok_or::<anyhow::Error>(RedisError {
        message: error_message.clone()
    }.into())?;
    if actual_symbol != symbol {
        Err(RedisError {
            message: error_message
        }.into())
    } else {
        Ok(position + 1)
    }
}

fn maybe_slice_of<T>(vec: &[T], start: usize, end: usize) -> Option<&[T]> {
    if start > vec.len() || end > vec.len() || start > end {
        None
    } else {
        Some(&vec[start..end])
    }
}

fn find_position_before_terminator(input: &Vec<u8>, terminator: &Vec<u8>, position: usize) -> usize {
    let mut current = position;
    let mut end_index: Option<usize> = None;
    while end_index == None && current < input.len() {
        let mut terminator_current = 0;
        while current < input.len() && terminator_current < terminator.len() && input[current] == terminator[terminator_current] {
            current = current + 1;
            terminator_current = terminator_current + 1;
        }
        if terminator_current == terminator.len() && terminator.len() > 0 {
            end_index = Some(current - terminator.len())
        } else {
            current = current + 1
        }
    }
    if let Some(new_position) = end_index {
        new_position
    } else {
        current
    }
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
            &DataType::VerbatimString { encoding, value } => {
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

    pub(crate) fn parse(input: &Vec<u8>, position: usize) -> Result<(DataType, usize), anyhow::Error> {
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
                            String::from_utf8_lossy(&input.clone()),
                            position,
                            String::from_utf8_lossy(&[*ch])
                        )
                    }.into())
            }
        } else {
            Err(RedisError { message: format!("Could not read the next data type value '{}' at position {}", String::from_utf8_lossy(&input.clone()), position) }.into())
        }
    }
}

fn parse_double(input: &Vec<u8>, position: usize) -> Result<(DataType, usize), anyhow::Error> {
    let error_message = format!("Invalid Double '{}'", String::from_utf8_lossy(&input.clone()));
    read_and_assert_symbol(input, b',', position).context(error_message.clone())?;
    let value_start = position + 1;
    let value_end = find_position_before_terminator(input, &"\r\n".as_bytes().to_vec(), value_start);
    read_and_assert_symbol(input, b'\r', value_end).context(error_message.clone())?;
    read_and_assert_symbol(input, b'\n', value_end + 1).context(error_message.clone())?;
    let value: f64 = String::from_utf8(input[value_start..value_end].to_vec())?.parse()?;
    Ok((DataType::Double {
        value
    }, value_end + 2))
}

fn parse_big_number(input: &Vec<u8>, position: usize) -> Result<(DataType, usize), anyhow::Error> {
    let error_message = format!("Invalid BigNumber '{}'", String::from_utf8_lossy(&input.clone()));
    read_and_assert_symbol(input, b'(', position).context(error_message.clone())?;
    let mut value_start = position + 1;
    let &maybe_sign = input.get(position + 1).ok_or::<anyhow::Error>(RedisError {
        message: error_message.clone()
    }.into())?;
    let mut sign: Option<u8> = None;
    if maybe_sign == b'+' || maybe_sign == b'-' {
        value_start = position + 2;
        sign = Some(maybe_sign);
    }
    let value_end = find_position_before_terminator(input, &"\r\n".as_bytes().to_vec(), value_start);
    read_and_assert_symbol(input, b'\r', value_end).context(error_message.clone())?;
    read_and_assert_symbol(input, b'\n', value_end + 1).context(error_message.clone())?;
    Ok((DataType::BigNumber {
        sign: sign.unwrap_or(b'+'),
        value: input[value_start..value_end].to_vec()
    }, value_end + 2))
}

fn parse_integer(input: &Vec<u8>, position: usize) -> Result<(DataType, usize), anyhow::Error> {
    let error_message = format!("Invalid Integer '{}'", String::from_utf8_lossy(&input.clone()));
    read_and_assert_symbol(input, b':', position).context(error_message.clone())?;
    let value_start = position + 1;
    let value_end = find_position_before_terminator(input, &"\r\n".as_bytes().to_vec(), value_start);
    read_and_assert_symbol(input, b'\r', value_end).context(error_message.clone())?;
    read_and_assert_symbol(input, b'\n', value_end + 1).context(error_message.clone())?;
    Ok((DataType::Integer {
        value: std::str::from_utf8(&input[value_start..value_end])?.parse()?
    }, value_end + 2))
}

fn parse_simple_error(input: &Vec<u8>, position: usize) -> Result<(DataType, usize), anyhow::Error> {
    let error_message = format!("Invalid SimpleError '{}'", String::from_utf8_lossy(&input.clone()));
    read_and_assert_symbol(input, b'-', position).context(error_message.clone())?;
    let value_start = position + 1;
    let value_end = find_position_before_terminator(input, &"\r\n".as_bytes().to_vec(), value_start);
    read_and_assert_symbol(input, b'\r', value_end).context(error_message.clone())?;
    read_and_assert_symbol(input, b'\n', value_end + 1).context(error_message.clone())?;
    Ok((DataType::SimpleError {
        value: input[value_start..value_end].to_vec()
    }, value_end + 2))
}

//TODO #1: This can be either a BulkString or RDB, if input ends abruptly without the ending \r\n or continues but not with \r\n it is an RDB file
fn parse_bulk_string_or_rdb(input: &Vec<u8>, position: usize) -> Result<(DataType, usize), anyhow::Error> {
    let error_message = format!("Invalid BulkString '{}'", String::from_utf8_lossy(&input.clone()));
    read_and_assert_symbol(input, b'$', position).context(error_message.clone())?;
    let length_start = position + 1;
    let first_length_symbol = input.get(length_start);

    let mut new_position = position ;
    if first_length_symbol != Some(&b'-') {
        let length_end = find_position_before_terminator(input, &"\r\n".as_bytes().to_vec(), length_start);
        let string_length: usize = String::from_utf8_lossy(&input[length_start..length_end]).parse()?;
        read_and_assert_symbol(input, b'\r', length_end).context(error_message.clone())?;
        read_and_assert_symbol(input, b'\n', length_end + 1).context(error_message.clone())?;
        let value_start = length_end + 2;
        let value_end = length_end + 2 + string_length;

        let maybe_bulk_string_end = maybe_slice_of(input, value_end, value_end + 2);
        if maybe_bulk_string_end == Some("\r\n".as_bytes()) {
            new_position = value_end + 2;
            Ok((DataType::BulkString {
                value: Some(input[value_start..value_end].to_vec())
            }, new_position))
        } else {
            Ok((DataType::Rdb {
                value: input[value_start..value_end].to_vec()
            }, value_end))
        }
    } else {
        new_position = new_position + "$-1\r\n".len();
        Ok((DataType::BulkString {
            value: None
        }, new_position))
    }
}

fn parse_bulk_error(input: &Vec<u8>, position: usize) -> Result<(DataType, usize), anyhow::Error> {
    let error_message = format!("Invalid BulkString '{}'", String::from_utf8_lossy(&input.clone()));
    read_and_assert_symbol(input, b'!', position).context(error_message.clone())?;
    let length_start = position + 1;
    let length_end = find_position_before_terminator(input, &"\r\n".as_bytes().to_vec(), length_start);
    let content_length: usize = String::from_utf8_lossy(&input[length_start..length_end]).parse()?;
    read_and_assert_symbol(input, b'\r', length_end).context(error_message.clone())?;
    read_and_assert_symbol(input, b'\n', length_end + 1).context(error_message.clone())?;
    let value_start = length_end + 2;
    let value_end = length_end + 2 + content_length;
    read_and_assert_symbol(input, b'\r', value_end).context(error_message.clone())?;
    read_and_assert_symbol(input, b'\n', value_end + 1).context(error_message.clone())?;
    Ok((DataType::BulkError {
        value: input[value_start..value_end].to_vec()
    }, value_end + 2))
}

fn parse_verbatim_string(input: &Vec<u8>, position: usize) -> Result<(DataType, usize), anyhow::Error> {
    let error_message = format!("Invalid VerbatimString '{}'", String::from_utf8_lossy(&input.clone()));
    read_and_assert_symbol(input, b'=', position).context(error_message.clone())?;
    let length_start = position + 1;
    let length_end = find_position_before_terminator(input, &"\r\n".as_bytes().to_vec(), length_start);
    let content_length: usize = String::from_utf8_lossy(&input[length_start..length_end]).parse()?;
    read_and_assert_symbol(input, b'\r', length_end).context(error_message.clone())?;
    read_and_assert_symbol(input, b'\n', length_end + 1).context(error_message.clone())?;
    let value_start = length_end + 2;
    let value_end = length_end + 2 + content_length;
    read_and_assert_symbol(input, b'\r', value_end).context(error_message.clone())?;
    read_and_assert_symbol(input, b'\n', value_end + 1).context(error_message.clone())?;
    let encoding_and_content = input[value_start..value_end].to_vec();
    let index_before_content = encoding_and_content.iter().position(|&ch| ch == b':').ok_or(RedisError {
        message: error_message.clone()
    })?;
    Ok((DataType::VerbatimString {
        encoding: input[value_start..(value_start + index_before_content)].to_vec(),
        value: input[(value_start + index_before_content + 1)..value_end].to_vec()
    }, value_end + 2))
}

fn parse_simple_string(input: &Vec<u8>, position: usize) -> Result<(DataType, usize), anyhow::Error> {
    let error_message = format!("Invalid SimpleString '{}'", String::from_utf8_lossy(&input.clone()));
    read_and_assert_symbol(input, b'+', position).context(error_message.clone())?;
    let value_start = position + 1;
    let value_end = find_position_before_terminator(input, &"\r\n".as_bytes().to_vec(), value_start);
    read_and_assert_symbol(input, b'\r', value_end).context(error_message.clone())?;
    read_and_assert_symbol(input, b'\n', value_end + 1).context(error_message.clone())?;
    Ok((DataType::SimpleString {
        value: input[value_start..value_end].to_vec()
    }, value_end + 2))
}

fn parse_map(input: &Vec<u8>, position: usize) -> Result<(DataType, usize), anyhow::Error> {
    let error_message = format!("Invalid Map '{}'", String::from_utf8_lossy(&input.clone()));
    read_and_assert_symbol(input, b'%', position).context(error_message.clone())?;
    let length_start = position + 1;
    let length_end = find_position_before_terminator(input, &"\r\n".as_bytes().to_vec(), length_start);
    let map_length: i64 = String::from_utf8_lossy(&input[length_start..length_end]).parse()?;
    read_and_assert_symbol(input, b'\r', length_end).context(error_message.clone())?;
    read_and_assert_symbol(input, b'\n', length_end + 1).context(error_message.clone())?;
    let mut entries: Vec<(DataType, DataType)> = Vec::new();
    let mut read_entry_count = 0;
    let mut current_position = length_end + 2;
    while read_entry_count < map_length {
        let next_read_key = DataType::parse(input, current_position)?;
        let next_read_value = DataType::parse(input, next_read_key.1)?;
        entries.push((next_read_key.0, next_read_value.0));
        current_position = next_read_value.1;
        read_entry_count = read_entry_count + 1;
    }
    Ok((DataType::Map {
        entries
    }, current_position))
}

fn parse_set(input: &Vec<u8>, position: usize) -> Result<(DataType, usize), anyhow::Error> {
    let error_message = format!("Invalid Set '{}'", String::from_utf8_lossy(&input.clone()));
    read_and_assert_symbol(input, b'~', position).context(error_message.clone())?;
    let length_start = position + 1;
    let length_end = find_position_before_terminator(input, &"\r\n".as_bytes().to_vec(), length_start);
    let map_length: i64 = String::from_utf8_lossy(&input[length_start..length_end]).parse()?;
    read_and_assert_symbol(input, b'\r', length_end).context(error_message.clone())?;
    read_and_assert_symbol(input, b'\n', length_end + 1).context(error_message.clone())?;
    let mut elements: Vec<DataType> = Vec::new();
    let mut read_element_count = 0;
    let mut current_position = length_end + 2;
    while read_element_count < map_length {
        let (next_element, next_position) = DataType::parse(input, current_position)?;
        elements.push(next_element);
        read_element_count = read_element_count + 1;
        current_position = next_position;
    }
    Ok((DataType::Set {
        elements
    }, current_position))
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

fn parse_array_like(input: &Vec<u8>, position: usize, prefix: u8) -> Result<(Vec<DataType>, usize), anyhow::Error> {
    let error_message = format!("Invalid Array-like '{}'", String::from_utf8_lossy(&input.clone()));
    read_and_assert_symbol(input, prefix, position).context(error_message.clone())?;
    let length_start = position + 1;
    let length_end = find_position_before_terminator(input, &"\r\n".as_bytes().to_vec(), length_start);
    let array_length: i64 = String::from_utf8_lossy(&input[length_start..length_end]).parse()?;
    read_and_assert_symbol(input, b'\r', length_end).context(error_message.clone())?;
    read_and_assert_symbol(input, b'\n', length_end + 1).context(error_message.clone())?;
    let mut elements: Vec<DataType> = Vec::new();
    let mut read_element_count = 0;
    let mut current_position = length_end + 2;
    while read_element_count < array_length {
        let next_read_element = DataType::parse(input, current_position)?;
        elements.push(next_read_element.0);
        current_position = next_read_element.1;
        read_element_count = read_element_count + 1;
    }
    Ok((elements, current_position))
}

fn parse_array(input: &Vec<u8>, position: usize) -> Result<(DataType, usize), anyhow::Error> {
    let (elements, updated_position) = parse_array_like(input, position, b'*')?;
    Ok((DataType::Array {
        elements
    }, updated_position))
}

fn parse_push(input: &Vec<u8>, position: usize) -> Result<(DataType, usize), anyhow::Error> {
    let (elements, updated_position) = parse_array_like(input, position, b'>')?;
    Ok((DataType::Push {
        elements
    }, updated_position))
}

fn parse_null(input: &Vec<u8>, position: usize) -> Result<(DataType, usize), anyhow::Error> {
    let error_message = format!("Invalid Null '{}'", String::from_utf8_lossy(&input.clone()));
    read_and_assert_symbol(input, b'_', position).context(error_message.clone())?;
    read_and_assert_symbol(input, b'\r', position + 1).context(error_message.clone())?;
    read_and_assert_symbol(input, b'\n', position + 2).context(error_message.clone())?;
    Ok((DataType::Null {}, position + 3))
}

fn parse_boolean(input: &Vec<u8>, position: usize) -> Result<(DataType, usize), anyhow::Error> {
    let error_message = format!("Invalid Null '{}'", String::from_utf8_lossy(&input.clone()));
    read_and_assert_symbol(input, b'#', position).context(error_message.clone())?;
    let &value_input = input.get(position + 1).ok_or::<anyhow::Error>(RedisError { message: error_message.clone() }.into())?;
    let value = value_input == b't';
    read_and_assert_symbol(input, b'\r', position + 2).context(error_message.clone())?;
    read_and_assert_symbol(input, b'\n', position + 3).context(error_message.clone())?;
    Ok((DataType::Boolean { value }, position + 4))
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
    fn should_parse_set() {
        let parsed = DataType::parse(&"~2\r\n:1\r\n$5\r\nhello\r\n".as_bytes().to_vec(), 0).unwrap();
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
    }

    #[test]
    fn should_serialize_verbatim_string() {
        assert_eq!(String::from_utf8_lossy(&DataType::VerbatimString {
            encoding: "txt".as_bytes().to_vec(),
            value: "Some string".as_bytes().to_vec()
        }.serialize()), "=15\r\ntxt:Some string\r\n".to_string());
    }

    #[test]
    fn should_parse_verbatim_string() {
        assert_eq!(DataType::parse(&"=15\r\ntxt:Some string\r\n".as_bytes().to_vec(), 0).unwrap(), (DataType::VerbatimString {
            encoding: "txt".as_bytes().to_vec(),
            value: "Some string".as_bytes().to_vec()
        }, 22));
    }

    #[test]
    fn should_serialize_bulk_error() {
        assert_eq!(String::from_utf8_lossy(&DataType::BulkError {
            value: "Some error".as_bytes().to_vec()
        }.serialize()), "!10\r\nSome error\r\n".to_string());
    }

    #[test]
    fn should_parse_bulk_error() {
        assert_eq!(DataType::parse(&"!21\r\nSYNTAX invalid syntax\r\n".as_bytes().to_vec(), 0).unwrap(), (DataType::BulkError {
            value: "SYNTAX invalid syntax".as_bytes().to_vec()
        }, 28));
    }

    #[test]
    fn should_serialize_big_number() {
        assert_eq!(String::from_utf8_lossy(&DataType::BigNumber { sign: b'+', value: "349".as_bytes().to_vec() }.serialize()), "(349\r\n".to_string());
        assert_eq!(String::from_utf8_lossy(&DataType::BigNumber { sign: b'-', value: "349".as_bytes().to_vec() }.serialize()), "(-349\r\n".to_string());
    }

    #[test]
    fn should_parse_big_number() {
        assert_eq!(DataType::parse(&"(349\r\n".as_bytes().to_vec(), 0).unwrap().0, DataType::BigNumber { sign: b'+', value: "349".as_bytes().to_vec() });
        assert_eq!(DataType::parse(&"(+349\r\n".as_bytes().to_vec(), 0).unwrap().0, DataType::BigNumber { sign: b'+', value: "349".as_bytes().to_vec() });
        assert_eq!(DataType::parse(&"(-123\r\n".as_bytes().to_vec(), 0).unwrap().0, DataType::BigNumber { sign: b'-', value: "123".as_bytes().to_vec() });
    }

    #[test]
    fn should_serialize_double() {
        assert_eq!(String::from_utf8_lossy(&DataType::Double { value: 1.23 }.serialize()), ",1.23\r\n".to_string());
    }

    #[test]
    fn should_parse_double() {
        assert_eq!(DataType::parse(&",10\r\n".as_bytes().to_vec(), 0).unwrap().0, DataType::Double { value: 10.0 });
        assert_eq!(DataType::parse(&",1.23\r\n".as_bytes().to_vec(), 0).unwrap().0, DataType::Double { value: 1.23 });
        assert_eq!(DataType::parse(&",inf\r\n".as_bytes().to_vec(), 0).unwrap().0, DataType::Double { value: f64::INFINITY });
        assert_eq!(DataType::parse(&",-inf\r\n".as_bytes().to_vec(), 0).unwrap().0, DataType::Double { value: f64::NEG_INFINITY });
        match DataType::parse(&",nan\r\n".as_bytes().to_vec(), 0).unwrap().0 {
            DataType::Double { value } => assert!(value.is_nan()),
            _ => assert!(false)
        }
    }

    #[test]
    fn should_serialize_boolean() {
        assert_eq!(String::from_utf8_lossy(&DataType::Boolean { value: true }.serialize()), "#t\r\n".to_string());
        assert_eq!(String::from_utf8_lossy(&DataType::Boolean { value: false }.serialize()), "#f\r\n".to_string());
    }

    #[test]
    fn should_parse_boolean() {
        assert_eq!(DataType::parse(&"#t\r\n".as_bytes().to_vec(), 0).unwrap().0, DataType::Boolean { value: true });
        assert_eq!(DataType::parse(&"#f\r\n".as_bytes().to_vec(), 0).unwrap().0, DataType::Boolean { value: false });
    }

    #[test]
    fn should_serialize_null() {
        assert_eq!(String::from_utf8_lossy(&DataType::Null {}.serialize()), "_\r\n".to_string());
    }

    #[test]
    fn should_parse_null() {
        assert_eq!(DataType::parse(&"_\r\n".as_bytes().to_vec(), 0).unwrap().0, DataType::Null {});
    }

    #[test]
    fn should_serialize_map() {
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
    }

    #[test]
    fn should_parse_map() {
        let parsed = DataType::parse(&"%2\r\n:1\r\n$5\r\nhello\r\n:2\r\n$5\r\nworld\r\n".as_bytes().to_vec(), 0).unwrap();
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
    }

    #[test]
    fn should_serialize_array() {
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
    }

    #[test]
    fn should_parse_array() {
        let mut parsed = DataType::parse(&"*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n".as_bytes().to_vec(), 0).unwrap();
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

        parsed = DataType::parse(&"*-1\r\n".as_bytes().to_vec(), 0).unwrap();
        assert_eq!(parsed.0, DataType::Array { elements: Vec::new() });
        assert_eq!(parsed.1, 5);
    }

    #[test]
    fn should_serialize_push() {
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
    }

    #[test]
    fn should_parse_push() {
        let parsed = DataType::parse(&">2\r\n$5\r\nhello\r\n$5\r\nworld\r\n".as_bytes().to_vec(), 0).unwrap();
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
    }

    #[test]
    fn should_serialize_bulk_string() {
        assert_eq!(String::from_utf8_lossy(&DataType::BulkString {
            value: Some("This is a bulk string\r\n One, two three".as_bytes().to_vec())
        }.serialize()), "$38\r\nThis is a bulk string\r\n One, two three\r\n".to_string());
    }

    #[test]
    fn should_parse_bulk_string() {
        assert_eq!(DataType::parse(&"$5\r\nHello\r\n".as_bytes().to_vec(), 0).unwrap(), (DataType::BulkString {
            value: Some("Hello".as_bytes().to_vec())
        }, 11));
        assert_eq!(DataType::parse(&"$12\r\nHello\r\nworld\r\n".as_bytes().to_vec(), 0).unwrap(), (DataType::BulkString {
            value: Some("Hello\r\nworld".as_bytes().to_vec())
        }, 19));
        assert_eq!(DataType::parse(&"$-1\r\n".as_bytes().to_vec(), 0).unwrap(), (DataType::BulkString {
            value: None
        }, 5));
        assert_eq!(DataType::parse(&"$0\r\n\r\n".as_bytes().to_vec(), 0).unwrap(), (DataType::BulkString {
            value: Some(Vec::new())
        }, 6));
    }

    #[test]
    fn should_parse_rdb() {
        assert_eq!(DataType::parse(&"$8\r\nfake_rdb".as_bytes().to_vec(), 0).unwrap(), (DataType::Rdb {
            value: "fake_rdb".as_bytes().to_vec()
        }, 12));
        assert_eq!(DataType::parse(&"$9\r\nfake\r\nrdb".as_bytes().to_vec(), 0).unwrap(), (DataType::Rdb {
            value: "fake\r\nrdb".as_bytes().to_vec()
        }, 13));
    }

    #[test]
    fn should_serialize_integer() {
        assert_eq!(String::from_utf8_lossy(&DataType::Integer {
            value: 0
        }.serialize()), ":0\r\n".to_string());
        assert_eq!(String::from_utf8_lossy(&DataType::Integer {
            value: 101
        }.serialize()), ":101\r\n".to_string());
        assert_eq!(String::from_utf8_lossy(&DataType::Integer {
            value: -15
        }.serialize()), ":-15\r\n".to_string());
    }

    #[test]
    fn should_parse_valid_integer() {
        assert_eq!(DataType::parse(&":+5\r\n".as_bytes().to_vec(), 0).unwrap().0, DataType::Integer { value: 5 });
        assert_eq!(DataType::parse(&":0\r\n".as_bytes().to_vec(), 0).unwrap().0, DataType::Integer { value: 0 });
        assert_eq!(DataType::parse(&":-98\r\n".as_bytes().to_vec(), 0).unwrap().0, DataType::Integer { value: -98 });
    }

    #[test]
    fn should_serialize_simple_error() {
        let string_value = "Error message";
        let s = DataType::SimpleError {
            value: string_value.as_bytes().to_vec()
        };
        let serialization = s.serialize();
        assert_eq!(String::from_utf8(serialization).unwrap(), format!("-{}\r\n", string_value))
    }

    #[test]
    fn should_parse_valid_simple_error() {
        let input = "-Error message\r\n".as_bytes().to_vec();
        let result = DataType::parse(&input, 0).unwrap();
        assert_eq!(result, (DataType::SimpleError {
            value: "Error message".as_bytes().to_vec()
        }, 16))
    }

    #[test]
    fn should_serialize_simple_string() {
        let string_value = "abcde";
        let s = DataType::SimpleString {
            value: string_value.as_bytes().to_vec()
        };
        let serialization = s.serialize();
        assert_eq!(String::from_utf8(serialization).unwrap(), format!("+{}\r\n", string_value))
    }

    #[test]
    fn should_parse_valid_simple_string() {
        let input = "+hello\r\n".as_bytes().to_vec();
        let result = DataType::parse(&input, 0).unwrap();
        assert_eq!(result, (DataType::SimpleString {
            value: "hello".as_bytes().to_vec()
        }, 8))
    }

    #[test]
    fn should_not_fail_parsing_if_more_bytes_are_provided() {
        let input = "+hello\r\n+world\r\n";
        let result = DataType::parse(&input.as_bytes().to_vec(), 0).unwrap();
        assert_eq!(result, (DataType::SimpleString {
            value: "hello".as_bytes().to_vec()
        }, 8))
    }

    #[test]
    fn should_fail_parsing_invalid_simple_string() {
        let input = "a+5\r\n";
        let error = DataType::parse(&input.as_bytes().to_vec(), 0).unwrap_err();
        assert_eq!(format!("{}", error), format!("RedisError: Could not read the next data type value '{}' at position 0, unsupported prefix 'a'", input))
    }

    #[test]
    fn should_read_message_from_bytes() {
        let parsed_single_message = read_messages_from_bytes(&"$5\r\nHello\r\n".as_bytes().to_vec()).unwrap();
        assert_eq!(parsed_single_message, vec![DataType::BulkString {
            value: Some("Hello".as_bytes().to_vec())
        }]);
        let parsed_messages = read_messages_from_bytes(&"$1\r\na\r\n$2\r\nbc\r\n$3\r\ndef\r\n".as_bytes().to_vec()).unwrap();
        assert_eq!(parsed_messages, vec![DataType::BulkString {
            value: Some("a".as_bytes().to_vec())
        }, DataType::BulkString {
            value: Some("bc".as_bytes().to_vec())
        }, DataType::BulkString {
            value: Some("def".as_bytes().to_vec())
        }]);
    }
}
