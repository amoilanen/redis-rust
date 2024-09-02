use anyhow::Context;

use crate::error::RedisError;

fn read_data_type(input: &Vec<u8>, position: usize) -> Result<(Box<dyn DataType>, usize), anyhow::Error> {
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
            }
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

//TODO: Convert into an enum?
trait DataType: std::fmt::Debug + PartialEq {
    fn serialize(&self) -> Vec<u8>;
}

#[derive(Debug, PartialEq)]
struct Integer {
    value: i64
}

impl DataType for Integer {
    fn serialize(&self) -> Vec<u8> {
        let mut result: Vec<u8> = Vec::new();
        result.push(b':');
        if self.value > 0 {
            result.push(b'+')
        }
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
        let value_end = read_until(input, &"\r\n".as_bytes().to_vec(), 1);
        read_and_assert_symbol(input, b'\r', value_end).context(error_message.clone())?;
        read_and_assert_symbol(input, b'\n', value_end + 1).context(error_message.clone())?;
        Ok((Integer {
            value: std::str::from_utf8(&input[value_start..value_end])?.parse()?
        }, value_end + 2))
    }
}


#[derive(Debug, PartialEq)]
struct SimpleError {
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
        let value_end = read_until(input, &"\r\n".as_bytes().to_vec(), 1);
        read_and_assert_symbol(input, b'\r', value_end).context(error_message.clone())?;
        read_and_assert_symbol(input, b'\n', value_end + 1).context(error_message.clone())?;
        Ok((SimpleError {
            value: input[value_start..value_end].to_vec()
        }, value_end + 2))
    }
}

#[derive(Debug, PartialEq)]
struct BulkString {
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
            let length_end = read_until(input, &"\r\n".as_bytes().to_vec(), 1);
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
struct SimpleString {
    value: Vec<u8>
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
        let value_end = read_until(input, &"\r\n".as_bytes().to_vec(), 1);
        read_and_assert_symbol(input, b'\r', value_end).context(error_message.clone())?;
        read_and_assert_symbol(input, b'\n', value_end + 1).context(error_message.clone())?;
        Ok((SimpleString {
            value: input[value_start..value_end].to_vec()
        }, value_end + 2))
    }
}

#[derive(PartialEq, Debug)]
struct Array {
    elements: Vec<Box<dyn DataType>>
}

impl DataType for Array {
    fn serialize(&self) -> Vec<u8> {
        let mut result: Vec<u8> = Vec::new();
        result.push(b'*');
        result.extend(self.elements.len().to_string().as_bytes());
        result.extend("\r\n".as_bytes());
        for element in self.elements.iter() {
            result.extend(element.serialize());
        }
        result
    }
}

impl Array {
    fn parse(input: &Vec<u8>, position: usize) -> Result<(Array, usize), anyhow::Error> {
        let error_message = format!("Invalid Array '{}'", String::from_utf8_lossy(&input.clone()));
        read_and_assert_symbol(input, b'*', position).context(error_message.clone())?;
        let length_start = 1;
        let length_end = read_until(input, &"\r\n".as_bytes().to_vec(), 1);
        let array_length: usize = String::from_utf8_lossy(&input[length_start..length_end]).parse()?;
        read_and_assert_symbol(input, b'\r', length_end).context(error_message.clone())?;
        read_and_assert_symbol(input, b'\n', length_end + 1).context(error_message.clone())?;
        let mut elements: Vec<Box<dyn DataType>> = Vec::new();
        let mut read_element_count = 0;
        let mut current_position = length_end + 2;
        while read_element_count < array_length {
            let next_read_element = read_data_type(input, current_position)?;
            elements.push(next_read_element.0);
            current_position = next_read_element.1;
            read_element_count = read_element_count + 1;
        }
        Ok((Array {
            elements: elements
        }, current_position))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert_eq!(Array::parse(&"*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n".as_bytes().to_vec(), 0).unwrap(),
            (Array {
                elements: vec![
                    Box::new(BulkString {
                        value: "hello".as_bytes().to_vec()
                    }),
                    Box::new(BulkString {
                        value: "world".as_bytes().to_vec()
                    })
                ]
            }, 26));
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
        }.serialize()), ":+101\r\n".to_string());
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
