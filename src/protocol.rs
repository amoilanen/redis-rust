use anyhow::Context;

use crate::error::RedisError;

pub(crate) fn parse_data_type(input: &Vec<u8>, position: usize) -> Result<(Box<dyn DataType>, usize), anyhow::Error> {
    if let Some(prefix_symbol) = input.get(position) {
        match prefix_symbol {
            b'*' => {
                let result = Array::parse(input, position)?;
                Ok((Box::new(result.0), result.1))
            },
            b'+' => {
                let result = SimpleString::parse(input, position)?;
                Ok((Box::new(result.0), result.1))
            },
            b'$' => {
                let result = BulkString::parse(input, position)?;
                Ok((Box::new(result.0), result.1))
            },
            b'-' => {
                let result = SimpleError::parse(input, position)?;
                Ok((Box::new(result.0), result.1))
            },
            b':' => {
                let result = Integer::parse(input, position)?;
                Ok((Box::new(result.0), result.1))
            },
            b'_' => {
                let result = Null::parse(input, position)?;
                Ok((Box::new(result.0), result.1))
            },
            b'%' => {
                let result = Map::parse(input, position)?;
                Ok((Box::new(result.0), result.1))
            },
            b'#' => {
                let result = Boolean::parse(input, position)?;
                Ok((Box::new(result.0), result.1))
            },
            b'(' => {
                let result = BigNumber::parse(input, position)?;
                Ok((Box::new(result.0), result.1))
            },
            b'!' => {
                let result = BulkError::parse(input, position)?;
                Ok((Box::new(result.0), result.1))
            },
            b'~' => {
                let result = Set::parse(input, position)?;
                Ok((Box::new(result.0), result.1))
            },
            b'>' => {
                let result = Push::parse(input, position)?;
                Ok((Box::new(result.0), result.1))
            },
            ch =>
                Err(RedisError { 
                    message: format!("Could not read the next data type value '{}' at position {}, unsupported prefix {}",
                        String::from_utf8_lossy(&input.clone()),
                        position,
                        ch
                    )
                }.into())
        }
    } else {
        Err(RedisError { message: format!("Could not read the next data type value '{}' at position {}", String::from_utf8_lossy(&input.clone()), position) }.into())
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

fn read_until(input: &Vec<u8>, terminator: &Vec<u8>, position: usize) -> usize {
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

pub(crate) trait DataType: std::fmt::Debug {
    fn serialize(&self) -> Vec<u8>;
}

#[derive(Debug, PartialEq)]
pub(crate) struct Double {
    value: f64
}

impl DataType for Double {
    fn serialize(&self) -> Vec<u8> {
        let mut result: Vec<u8> = Vec::new();
        result.push(b',');
        result.extend(self.value.to_string().as_bytes());
        result.extend("\r\n".as_bytes());
        result
    }
}

impl Double {
    fn parse(input: &Vec<u8>, position: usize) -> Result<(Double, usize), anyhow::Error> {
        let error_message = format!("Invalid Double '{}'", String::from_utf8_lossy(&input.clone()));
        read_and_assert_symbol(input, b',', position).context(error_message.clone())?;
        let value_start = position + 1;
        let value_end = read_until(input, &"\r\n".as_bytes().to_vec(), value_start);
        read_and_assert_symbol(input, b'\r', value_end).context(error_message.clone())?;
        read_and_assert_symbol(input, b'\n', value_end + 1).context(error_message.clone())?;
        let value: f64 = String::from_utf8(input[value_start..value_end].to_vec())?.parse()?;
        Ok((Double {
            value
        }, value_end + 2))
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct BigNumber {
    sign: u8,
    value: Vec<u8> // more efficient representation is possible
}

impl DataType for BigNumber {
    fn serialize(&self) -> Vec<u8> {
        let mut result: Vec<u8> = Vec::new();
        result.push(b'(');
        if self.sign == b'-' {
            result.push(self.sign)
        }
        result.extend(&self.value);
        result.extend("\r\n".as_bytes());
        result
    }
}

impl BigNumber {
    fn parse(input: &Vec<u8>, position: usize) -> Result<(BigNumber, usize), anyhow::Error> {
        let error_message = format!("Invalid BigNumber '{}'", String::from_utf8_lossy(&input.clone()));
        read_and_assert_symbol(input, b'(', position).context(error_message.clone())?;
        let mut value_start = position + 1;
        let &maybeSign = input.get(position + 1).ok_or::<anyhow::Error>(RedisError {
            message: error_message.clone()
        }.into())?;
        let mut sign: Option<u8> = None;
        if maybeSign == b'+' || maybeSign == b'-' {
            value_start = position + 2;
            sign = Some(maybeSign);
        }
        let value_end = read_until(input, &"\r\n".as_bytes().to_vec(), value_start);
        read_and_assert_symbol(input, b'\r', value_end).context(error_message.clone())?;
        read_and_assert_symbol(input, b'\n', value_end + 1).context(error_message.clone())?;
        Ok((BigNumber {
            sign: sign.unwrap_or(b'+'),
            value: input[value_start..value_end].to_vec()
        }, value_end + 2))
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct Integer {
    value: i64
}

impl DataType for Integer {
    fn serialize(&self) -> Vec<u8> {
        let mut result: Vec<u8> = Vec::new();
        result.push(b':');
        result.extend(self.value.to_string().as_bytes());
        result.extend("\r\n".as_bytes());
        result
    }
}

impl Integer {
    fn parse(input: &Vec<u8>, position: usize) -> Result<(Integer, usize), anyhow::Error> {
        let error_message = format!("Invalid Integer '{}'", String::from_utf8_lossy(&input.clone()));
        read_and_assert_symbol(input, b':', position).context(error_message.clone())?;
        let value_start = position + 1;
        let value_end = read_until(input, &"\r\n".as_bytes().to_vec(), value_start);
        read_and_assert_symbol(input, b'\r', value_end).context(error_message.clone())?;
        read_and_assert_symbol(input, b'\n', value_end + 1).context(error_message.clone())?;
        Ok((Integer {
            value: std::str::from_utf8(&input[value_start..value_end])?.parse()?
        }, value_end + 2))
    }
}


#[derive(Debug, PartialEq)]
pub(crate) struct SimpleError {
    value: Vec<u8>
}

impl DataType for SimpleError {
    fn serialize(&self) -> Vec<u8> {
        let mut result: Vec<u8> = Vec::new();
        result.push(b'-');
        result.extend(self.value.as_slice());
        result.extend("\r\n".as_bytes());
        result
    }
}

impl SimpleError {
    fn parse(input: &Vec<u8>, position: usize) -> Result<(SimpleError, usize), anyhow::Error> {
        let error_message = format!("Invalid SimpleError '{}'", String::from_utf8_lossy(&input.clone()));
        read_and_assert_symbol(input, b'-', position).context(error_message.clone())?;
        let value_start = position + 1;
        let value_end = read_until(input, &"\r\n".as_bytes().to_vec(), value_start);
        read_and_assert_symbol(input, b'\r', value_end).context(error_message.clone())?;
        read_and_assert_symbol(input, b'\n', value_end + 1).context(error_message.clone())?;
        Ok((SimpleError {
            value: input[value_start..value_end].to_vec()
        }, value_end + 2))
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct BulkString {
    value: Vec<u8>
}

impl DataType for BulkString {
    fn serialize(&self) -> Vec<u8> {
        let mut result: Vec<u8> = Vec::new();
        result.push(b'$');
        result.extend(self.value.len().to_string().as_bytes());
        result.extend("\r\n".as_bytes());
        result.extend(self.value.as_slice());
        result.extend("\r\n".as_bytes());
        result
    }
}

impl BulkString {
    fn parse(input: &Vec<u8>, position: usize) -> Result<(BulkString, usize), anyhow::Error> {
        let error_message = format!("Invalid BulkString '{}'", String::from_utf8_lossy(&input.clone()));
        read_and_assert_symbol(input, b'$', position).context(error_message.clone())?;
        let length_start = position + 1;
        let first_length_symbol = input.get(length_start);

        let mut value: Vec<u8> = Vec::new();
        let mut new_position = position ;
        if first_length_symbol != Some(&b'-') {
            let length_end = read_until(input, &"\r\n".as_bytes().to_vec(), length_start);
            let string_length: usize = String::from_utf8_lossy(&input[length_start..length_end]).parse()?;
            read_and_assert_symbol(input, b'\r', length_end).context(error_message.clone())?;
            read_and_assert_symbol(input, b'\n', length_end + 1).context(error_message.clone())?;
            let value_start = length_end + 2;
            let value_end = length_end + 2 + string_length;
            read_and_assert_symbol(input, b'\r', value_end).context(error_message.clone())?;
            read_and_assert_symbol(input, b'\n', value_end + 1).context(error_message.clone())?;
            value = input[value_start..value_end].to_vec();
            new_position = value_end + 2;
        } else {
            new_position = new_position + "$-1\r\n".len();
        }
        Ok((BulkString {
            value
        }, new_position))
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct BulkError {
    value: Vec<u8>
}

impl DataType for BulkError {
    fn serialize(&self) -> Vec<u8> {
        let mut result: Vec<u8> = Vec::new();
        result.push(b'!');
        result.extend(self.value.len().to_string().as_bytes());
        result.extend("\r\n".as_bytes());
        result.extend(self.value.as_slice());
        result.extend("\r\n".as_bytes());
        result
    }
}

impl BulkError {
    fn parse(input: &Vec<u8>, position: usize) -> Result<(BulkError, usize), anyhow::Error> {
        let error_message = format!("Invalid BulkString '{}'", String::from_utf8_lossy(&input.clone()));
        read_and_assert_symbol(input, b'!', position).context(error_message.clone())?;
        let length_start = position + 1;
        let length_end = read_until(input, &"\r\n".as_bytes().to_vec(), length_start);
        let content_length: usize = String::from_utf8_lossy(&input[length_start..length_end]).parse()?;
        read_and_assert_symbol(input, b'\r', length_end).context(error_message.clone())?;
        read_and_assert_symbol(input, b'\n', length_end + 1).context(error_message.clone())?;
        let value_start = length_end + 2;
        let value_end = length_end + 2 + content_length;
        read_and_assert_symbol(input, b'\r', value_end).context(error_message.clone())?;
        read_and_assert_symbol(input, b'\n', value_end + 1).context(error_message.clone())?;
        Ok((BulkError {
            value: input[value_start..value_end].to_vec()
        }, value_end + 2))
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct VerbatimString {
    encoding: Vec<u8>,
    value: Vec<u8>
}

impl DataType for VerbatimString {
    fn serialize(&self) -> Vec<u8> {
        let mut result: Vec<u8> = Vec::new();
        result.push(b'=');
        result.extend((self.value.len() + self.encoding.len() + 1).to_string().as_bytes());
        result.extend("\r\n".as_bytes());
        result.extend(self.encoding.as_slice());
        result.push(b':');
        result.extend(self.value.as_slice());
        result.extend("\r\n".as_bytes());
        result
    }
}

impl VerbatimString {
    fn parse(input: &Vec<u8>, position: usize) -> Result<(VerbatimString, usize), anyhow::Error> {
        let error_message = format!("Invalid VerbatimString '{}'", String::from_utf8_lossy(&input.clone()));
        read_and_assert_symbol(input, b'=', position).context(error_message.clone())?;
        let length_start = position + 1;
        let length_end = read_until(input, &"\r\n".as_bytes().to_vec(), length_start);
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
        Ok((VerbatimString {
            encoding: input[value_start..(value_start + index_before_content)].to_vec(),
            value: input[(value_start + index_before_content + 1)..value_end].to_vec()
        }, value_end + 2))
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct SimpleString {
    pub(crate) value: Vec<u8>
}

impl DataType for SimpleString {
    fn serialize(&self) -> Vec<u8> {
        let mut result: Vec<u8> = Vec::new();
        result.push(b'+');
        result.extend(self.value.as_slice());
        result.extend("\r\n".as_bytes());
        result
    }
}

impl SimpleString {
    fn parse(input: &Vec<u8>, position: usize) -> Result<(SimpleString, usize), anyhow::Error> {
        let error_message = format!("Invalid SimpleString '{}'", String::from_utf8_lossy(&input.clone()));
        read_and_assert_symbol(input, b'+', position).context(error_message.clone())?;
        let value_start = position + 1;
        let value_end = read_until(input, &"\r\n".as_bytes().to_vec(), value_start);
        read_and_assert_symbol(input, b'\r', value_end).context(error_message.clone())?;
        read_and_assert_symbol(input, b'\n', value_end + 1).context(error_message.clone())?;
        Ok((SimpleString {
            value: input[value_start..value_end].to_vec()
        }, value_end + 2))
    }
}

#[derive(Debug)]
pub(crate) struct Map {
    entries: Vec<(Box<dyn DataType>, Box<dyn DataType>)>
}

impl DataType for Map {
    fn serialize(&self) -> Vec<u8> {
        let mut result: Vec<u8> = Vec::new();
        result.push(b'%');
        result.extend(self.entries.len().to_string().as_bytes());
        result.extend("\r\n".as_bytes());
        for element in self.entries.iter() {
            result.extend(element.0.serialize());
            result.extend(element.1.serialize());
        }
        result
    }
}

impl Map {
    fn parse(input: &Vec<u8>, position: usize) -> Result<(Map, usize), anyhow::Error> {
        let error_message = format!("Invalid Map '{}'", String::from_utf8_lossy(&input.clone()));
        read_and_assert_symbol(input, b'%', position).context(error_message.clone())?;
        let length_start = position + 1;
        let length_end = read_until(input, &"\r\n".as_bytes().to_vec(), length_start);
        let map_length: i64 = String::from_utf8_lossy(&input[length_start..length_end]).parse()?;
        read_and_assert_symbol(input, b'\r', length_end).context(error_message.clone())?;
        read_and_assert_symbol(input, b'\n', length_end + 1).context(error_message.clone())?;
        let mut entries: Vec<(Box<dyn DataType>, Box<dyn DataType>)> = Vec::new();
        let mut read_entry_count = 0;
        let mut current_position = length_end + 2;
        while read_entry_count < map_length {
            let next_read_key = parse_data_type(input, current_position)?;
            let next_read_value = parse_data_type(input, next_read_key.1)?;
            entries.push((next_read_key.0, next_read_value.0));
            current_position = next_read_value.1;
            read_entry_count = read_entry_count + 1;
        }
        Ok((Map {
            entries
        }, current_position))
    }
}

#[derive(Debug)]
pub(crate) struct Set {
    elements: Vec<Box<dyn DataType>>
}

impl DataType for Set {
    fn serialize(&self) -> Vec<u8> {
        let mut result: Vec<u8> = Vec::new();
        result.push(b'~');
        result.extend(self.elements.len().to_string().as_bytes());
        result.extend("\r\n".as_bytes());
        for element in self.elements.iter() {
            result.extend(element.serialize());
        }
        result
    }
}

impl Set {
    fn parse(input: &Vec<u8>, position: usize) -> Result<(Set, usize), anyhow::Error> {
        let error_message = format!("Invalid Set '{}'", String::from_utf8_lossy(&input.clone()));
        read_and_assert_symbol(input, b'~', position).context(error_message.clone())?;
        let length_start = position + 1;
        let length_end = read_until(input, &"\r\n".as_bytes().to_vec(), length_start);
        let map_length: i64 = String::from_utf8_lossy(&input[length_start..length_end]).parse()?;
        read_and_assert_symbol(input, b'\r', length_end).context(error_message.clone())?;
        read_and_assert_symbol(input, b'\n', length_end + 1).context(error_message.clone())?;
        let mut elements: Vec<Box<dyn DataType>> = Vec::new();
        let mut read_element_count = 0;
        let mut current_position = length_end + 2;
        while read_element_count < map_length {
            let next_element = parse_data_type(input, current_position)?;
            elements.push(next_element.0);
            read_element_count = read_element_count + 1;
            current_position = next_element.1;
        }
        Ok((Set {
            elements
        }, current_position))
    }
}

fn serialize_array_like(elements: &Vec<Box<dyn DataType>>, prefix: u8) -> Vec<u8> {
    let mut result: Vec<u8> = Vec::new();
    result.push(prefix);
    result.extend(elements.len().to_string().as_bytes());
    result.extend("\r\n".as_bytes());
    for element in elements.iter() {
        result.extend(element.serialize());
    }
    result
}

fn parse_array_like(input: &Vec<u8>, position: usize, prefix: u8) -> Result<(Vec<Box<dyn DataType>>, usize), anyhow::Error> {
    let error_message = format!("Invalid Array-like '{}'", String::from_utf8_lossy(&input.clone()));
    read_and_assert_symbol(input, prefix, position).context(error_message.clone())?;
    let length_start = position + 1;
    let length_end = read_until(input, &"\r\n".as_bytes().to_vec(), length_start);
    let array_length: i64 = String::from_utf8_lossy(&input[length_start..length_end]).parse()?;
    read_and_assert_symbol(input, b'\r', length_end).context(error_message.clone())?;
    read_and_assert_symbol(input, b'\n', length_end + 1).context(error_message.clone())?;
    let mut elements: Vec<Box<dyn DataType>> = Vec::new();
    let mut read_element_count = 0;
    let mut current_position = length_end + 2;
    while read_element_count < array_length {
        let next_read_element = parse_data_type(input, current_position)?;
        elements.push(next_read_element.0);
        current_position = next_read_element.1;
        read_element_count = read_element_count + 1;
    }
    Ok((elements, current_position))
}

#[derive(Debug)]
pub(crate) struct Array {
    elements: Vec<Box<dyn DataType>>
}

impl DataType for Array {
    fn serialize(&self) -> Vec<u8> {
        serialize_array_like(&self.elements, b'*')
    }
}

impl Array {
    fn parse(input: &Vec<u8>, position: usize) -> Result<(Array, usize), anyhow::Error> {
        let (elements, updated_position) = parse_array_like(input, position, b'*')?;
        Ok((Array {
            elements
        }, updated_position))
    }
}

#[derive(Debug)]
pub(crate) struct Push {
    elements: Vec<Box<dyn DataType>>
}

impl DataType for Push {
    fn serialize(&self) -> Vec<u8> {
        serialize_array_like(&self.elements, b'>')
    }
}

impl Push {
    fn parse(input: &Vec<u8>, position: usize) -> Result<(Array, usize), anyhow::Error> {
        let (elements, updated_position) = parse_array_like(input, position, b'>')?;
        Ok((Array {
            elements
        }, updated_position))
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct Null {}

impl DataType for Null {
    fn serialize(&self) -> Vec<u8> {
        "_\r\n".as_bytes().to_vec()
    }
}

impl Null {
    fn parse(input: &Vec<u8>, position: usize) -> Result<(Null, usize), anyhow::Error> {
        let error_message = format!("Invalid Null '{}'", String::from_utf8_lossy(&input.clone()));
        read_and_assert_symbol(input, b'_', position).context(error_message.clone())?;
        read_and_assert_symbol(input, b'\r', position + 1).context(error_message.clone())?;
        read_and_assert_symbol(input, b'\n', position + 2).context(error_message.clone())?;
        Ok((Null {}, position + 3))
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct Boolean {
    value: bool
}

impl DataType for Boolean {
    fn serialize(&self) -> Vec<u8> {
        let mut result: Vec<u8> = Vec::new();
        result.push(b'#');
        if self.value {
            result.push(b't');
        } else {
            result.push(b'f');
        }
        result.extend("\r\n".as_bytes());
        result
    }
}

impl Boolean {
    fn parse(input: &Vec<u8>, position: usize) -> Result<(Boolean, usize), anyhow::Error> {
        let error_message = format!("Invalid Null '{}'", String::from_utf8_lossy(&input.clone()));
        read_and_assert_symbol(input, b'#', position).context(error_message.clone())?;
        let &value_input = input.get(position + 1).ok_or::<anyhow::Error>(RedisError { message: error_message.clone() }.into())?;
        let value = value_input == b't';
        read_and_assert_symbol(input, b'\r', position + 2).context(error_message.clone())?;
        read_and_assert_symbol(input, b'\n', position + 3).context(error_message.clone())?;
        Ok((Boolean { value }, position + 4))
    }
}

#[cfg(test)]
mod tests {
    use core::f64;
    use super::*;

    #[test]
    fn should_serialize_set() {
        assert_eq!(String::from_utf8_lossy(&Set {
            elements: vec![
                Box::new(Integer {
                    value: 1
                }),
                Box::new(BulkString {
                    value: "hello".as_bytes().to_vec()
                })
            ]
        }.serialize()), "~2\r\n:1\r\n$5\r\nhello\r\n".to_string());
    }

    #[test]
    fn should_parse_set() {
        let parsed = Set::parse(&"~2\r\n:1\r\n$5\r\nhello\r\n".as_bytes().to_vec(), 0).unwrap();
        assert_eq!(parsed.0.elements.len(), 2);
        assert_eq!(parsed.1, 19);
        assert_eq!(String::from_utf8(parsed.0.elements[0].serialize()).unwrap(), ":1\r\n".to_string());
        assert_eq!(String::from_utf8(parsed.0.elements[1].serialize()).unwrap(), "$5\r\nhello\r\n".to_string());
    }

    #[test]
    fn should_serialize_verbatim_string() {
        assert_eq!(String::from_utf8_lossy(&VerbatimString {
            encoding: "txt".as_bytes().to_vec(),
            value: "Some string".as_bytes().to_vec()
        }.serialize()), "=15\r\ntxt:Some string\r\n".to_string());
    }

    #[test]
    fn should_parse_verbatim_string() {
        assert_eq!(VerbatimString::parse(&"=15\r\ntxt:Some string\r\n".as_bytes().to_vec(), 0).unwrap(), (VerbatimString {
            encoding: "txt".as_bytes().to_vec(),
            value: "Some string".as_bytes().to_vec()
        }, 22));
    }

    #[test]
    fn should_serialize_bulk_error() {
        assert_eq!(String::from_utf8_lossy(&BulkError {
            value: "Some error".as_bytes().to_vec()
        }.serialize()), "!10\r\nSome error\r\n".to_string());
    }

    #[test]
    fn should_parse_bulk_error() {
        assert_eq!(BulkError::parse(&"!21\r\nSYNTAX invalid syntax\r\n".as_bytes().to_vec(), 0).unwrap(), (BulkError {
            value: "SYNTAX invalid syntax".as_bytes().to_vec()
        }, 28));
    }

    #[test]
    fn should_serialize_big_number() {
        assert_eq!(String::from_utf8_lossy(&BigNumber { sign: b'+', value: "349".as_bytes().to_vec() }.serialize()), "(349\r\n".to_string());
        assert_eq!(String::from_utf8_lossy(&BigNumber { sign: b'-', value: "349".as_bytes().to_vec() }.serialize()), "(-349\r\n".to_string());
    }

    #[test]
    fn should_parse_big_number() {
        assert_eq!(BigNumber::parse(&"(349\r\n".as_bytes().to_vec(), 0).unwrap().0, BigNumber { sign: b'+', value: "349".as_bytes().to_vec() });
        assert_eq!(BigNumber::parse(&"(+349\r\n".as_bytes().to_vec(), 0).unwrap().0, BigNumber { sign: b'+', value: "349".as_bytes().to_vec() });
        assert_eq!(BigNumber::parse(&"(-123\r\n".as_bytes().to_vec(), 0).unwrap().0, BigNumber { sign: b'-', value: "123".as_bytes().to_vec() });
    }

    #[test]
    fn should_serialize_double() {
        assert_eq!(String::from_utf8_lossy(&Double { value: 1.23 }.serialize()), ",1.23\r\n".to_string());
    }

    #[test]
    fn should_parse_double() {
        assert_eq!(Double::parse(&",10\r\n".as_bytes().to_vec(), 0).unwrap().0, Double { value: 10.0 });
        assert_eq!(Double::parse(&",1.23\r\n".as_bytes().to_vec(), 0).unwrap().0, Double { value: 1.23 });
        assert_eq!(Double::parse(&",inf\r\n".as_bytes().to_vec(), 0).unwrap().0, Double { value: f64::INFINITY });
        assert_eq!(Double::parse(&",-inf\r\n".as_bytes().to_vec(), 0).unwrap().0, Double { value: f64::NEG_INFINITY });
        assert!(Double::parse(&",nan\r\n".as_bytes().to_vec(), 0).unwrap().0.value.is_nan());
    }

    #[test]
    fn should_serialize_boolean() {
        assert_eq!(String::from_utf8_lossy(&Boolean { value: true }.serialize()), "#t\r\n".to_string());
        assert_eq!(String::from_utf8_lossy(&Boolean { value: false }.serialize()), "#f\r\n".to_string());
    }

    #[test]
    fn should_parse_boolean() {
        assert_eq!(Boolean::parse(&"#t\r\n".as_bytes().to_vec(), 0).unwrap().0, Boolean { value: true });
        assert_eq!(Boolean::parse(&"#f\r\n".as_bytes().to_vec(), 0).unwrap().0, Boolean { value: false });
    }

    #[test]
    fn should_serialize_null() {
        assert_eq!(String::from_utf8_lossy(&Null {}.serialize()), "_\r\n".to_string());
    }

    #[test]
    fn should_parse_null() {
        assert_eq!(Null::parse(&"_\r\n".as_bytes().to_vec(), 0).unwrap().0, Null {});
    }

    #[test]
    fn should_serialize_map() {
        assert_eq!(String::from_utf8_lossy(&Map {
            entries: vec![
                (
                    Box::new(Integer {
                        value: 1
                    }),
                    Box::new(BulkString {
                        value: "hello".as_bytes().to_vec()
                    })
                ),
                (
                    Box::new(Integer {
                        value: 2
                    }),
                    Box::new(BulkString {
                        value: "world".as_bytes().to_vec()
                    })
                )
            ]
        }.serialize()), "%2\r\n:1\r\n$5\r\nhello\r\n:2\r\n$5\r\nworld\r\n".to_string());
    }

    #[test]
    fn should_parse_map() {
        let parsed = Map::parse(&"%2\r\n:1\r\n$5\r\nhello\r\n:2\r\n$5\r\nworld\r\n".as_bytes().to_vec(), 0).unwrap();
        assert_eq!(parsed.0.entries.len(), 2);
        assert_eq!(parsed.1, 34);
        assert_eq!(String::from_utf8(parsed.0.entries[0].0.serialize()).unwrap(), ":1\r\n".to_string());
        assert_eq!(String::from_utf8(parsed.0.entries[0].1.serialize()).unwrap(), "$5\r\nhello\r\n".to_string());
        assert_eq!(String::from_utf8(parsed.0.entries[1].0.serialize()).unwrap(), ":2\r\n".to_string());
        assert_eq!(String::from_utf8(parsed.0.entries[1].1.serialize()).unwrap(), "$5\r\nworld\r\n".to_string());
    }

    #[test]
    fn should_serialize_array() {
        assert_eq!(String::from_utf8_lossy(&Array {
            elements: vec![
                Box::new(BulkString {
                    value: "hello".as_bytes().to_vec()
                }),
                Box::new(BulkString {
                    value: "world".as_bytes().to_vec()
                })
            ]
        }.serialize()), "*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n".to_string());
    }

    #[test]
    fn should_parse_array() {
        let mut parsed = Array::parse(&"*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n".as_bytes().to_vec(), 0).unwrap();
        assert_eq!(parsed.0.elements.len(), 2);
        assert_eq!(parsed.1, 26);
        assert_eq!(String::from_utf8(parsed.0.elements[0].serialize()).unwrap(), "$5\r\nhello\r\n".to_string());
        assert_eq!(String::from_utf8(parsed.0.elements[1].serialize()).unwrap(), "$5\r\nworld\r\n".to_string());

        parsed = Array::parse(&"*-1\r\n".as_bytes().to_vec(), 0).unwrap();
        assert_eq!(parsed.0.elements.len(), 0);
        assert_eq!(parsed.1, 5);
    }

    #[test]
    fn should_serialize_push() {
        assert_eq!(String::from_utf8_lossy(&Push {
            elements: vec![
                Box::new(BulkString {
                    value: "hello".as_bytes().to_vec()
                }),
                Box::new(BulkString {
                    value: "world".as_bytes().to_vec()
                })
            ]
        }.serialize()), ">2\r\n$5\r\nhello\r\n$5\r\nworld\r\n".to_string());
    }

    #[test]
    fn should_parse_push() {
        let parsed = Push::parse(&">2\r\n$5\r\nhello\r\n$5\r\nworld\r\n".as_bytes().to_vec(), 0).unwrap();
        assert_eq!(parsed.0.elements.len(), 2);
        assert_eq!(parsed.1, 26);
        assert_eq!(String::from_utf8(parsed.0.elements[0].serialize()).unwrap(), "$5\r\nhello\r\n".to_string());
        assert_eq!(String::from_utf8(parsed.0.elements[1].serialize()).unwrap(), "$5\r\nworld\r\n".to_string());
    }

    #[test]
    fn should_serialize_bulk_string() {
        assert_eq!(String::from_utf8_lossy(&BulkString {
            value: "This is a bulk string\r\n One, two three".as_bytes().to_vec()
        }.serialize()), "$38\r\nThis is a bulk string\r\n One, two three\r\n".to_string());
    }

    #[test]
    fn should_parse_bulk_string() {
        assert_eq!(BulkString::parse(&"$5\r\nHello\r\n".as_bytes().to_vec(), 0).unwrap(), (BulkString {
            value: "Hello".as_bytes().to_vec()
        }, 11));
        assert_eq!(BulkString::parse(&"$12\r\nHello\r\nworld\r\n".as_bytes().to_vec(), 0).unwrap(), (BulkString {
            value: "Hello\r\nworld".as_bytes().to_vec()
        }, 19));
        assert_eq!(BulkString::parse(&"$-1\r\n".as_bytes().to_vec(), 0).unwrap(), (BulkString {
            value: Vec::new()
        }, 5));
        assert_eq!(BulkString::parse(&"$0\r\n\r\n".as_bytes().to_vec(), 0).unwrap(), (BulkString {
            value: Vec::new()
        }, 6));
    }

    #[test]
    fn should_serialize_integer() {
        assert_eq!(String::from_utf8_lossy(&Integer {
            value: 0
        }.serialize()), ":0\r\n".to_string());
        assert_eq!(String::from_utf8_lossy(&Integer {
            value: 101
        }.serialize()), ":101\r\n".to_string());
        assert_eq!(String::from_utf8_lossy(&Integer {
            value: -15
        }.serialize()), ":-15\r\n".to_string());
    }

    #[test]
    fn should_parse_valid_integer() {
        assert_eq!(Integer::parse(&":+5\r\n".as_bytes().to_vec(), 0).unwrap().0.value, 5);
        assert_eq!(Integer::parse(&":0\r\n".as_bytes().to_vec(), 0).unwrap().0.value, 0);
        assert_eq!(Integer::parse(&":-98\r\n".as_bytes().to_vec(), 0).unwrap().0.value, -98);
    }

    #[test]
    fn should_serialize_simple_error() {
        let string_value = "Error message";
        let s = SimpleError {
            value: string_value.as_bytes().to_vec()
        };
        let serialization = s.serialize();
        assert_eq!(String::from_utf8(serialization).unwrap(), format!("-{}\r\n", string_value))
    }

    #[test]
    fn should_parse_valid_simple_error() {
        let input = "-Error message\r\n".as_bytes().to_vec();
        let result = SimpleError::parse(&input, 0).unwrap();
        assert_eq!(result, (SimpleError {
            value: "Error message".as_bytes().to_vec()
        }, 16))
    }

    #[test]
    fn should_serialize_simple_string() {
        let string_value = "abcde";
        let s = SimpleString {
            value: string_value.as_bytes().to_vec()
        };
        let serialization = s.serialize();
        assert_eq!(String::from_utf8(serialization).unwrap(), format!("+{}\r\n", string_value))
    }

    #[test]
    fn should_parse_valid_simple_string() {
        let input = "+hello\r\n".as_bytes().to_vec();
        let result = SimpleString::parse(&input, 0).unwrap();
        assert_eq!(result, (SimpleString {
            value: "hello".as_bytes().to_vec()
        }, 8))
    }

    #[test]
    fn should_not_fail_parsing_if_more_bytes_are_provided() {
        let input = "+hello\r\n+world\r\n";
        let result = SimpleString::parse(&input.as_bytes().to_vec(), 0).unwrap();
        assert_eq!(result, (SimpleString {
            value: "hello".as_bytes().to_vec()
        }, 8))
    }

    #[test]
    fn should_fail_parsing_invalid_simple_string() {
        let input = ":+5\r\n";
        let error = SimpleString::parse(&input.as_bytes().to_vec(), 0).unwrap_err();
        assert_eq!(format!("{}", error), format!("Invalid SimpleString '{}'", input))
    }
}
