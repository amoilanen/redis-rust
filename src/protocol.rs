use anyhow::Context;

use crate::error::RedisError;

#[derive(Debug, PartialEq)]
struct Integer {
    value: i64
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

impl Integer {
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
    fn parse(input: &Vec<u8>) -> Result<Integer, anyhow::Error> {
        let error_message = format!("Invalid Integer '{}'", String::from_utf8_lossy(&input.clone()));
        read_and_assert_symbol(input, b':', 0).context(error_message.clone())?;
        let value_start = 1;
        let value_end = read_until(input, &"\r\n".as_bytes().to_vec(), 1);
        read_and_assert_symbol(input, b'\r', value_end).context(error_message.clone())?;
        read_and_assert_symbol(input, b'\n', value_end + 1).context(error_message.clone())?;
        Ok(Integer {
            value: std::str::from_utf8(&input[value_start..value_end])?.parse()?
        })
    }
}


#[derive(Debug, PartialEq)]
struct SimpleError {
    value: Vec<u8>
}

impl SimpleError {
    fn serialize(&self) -> Vec<u8> {
        let mut result: Vec<u8> = Vec::new();
        result.push(b'-');
        result.extend(self.value.as_slice());
        result.extend("\r\n".as_bytes());
        result
    }
    fn parse(input: &Vec<u8>) -> Result<SimpleError, anyhow::Error> {
        let error_message = format!("Invalid SimpleError '{}'", String::from_utf8_lossy(&input.clone()));
        read_and_assert_symbol(input, b'-', 0).context(error_message.clone())?;
        let value_start = 1;
        let value_end = read_until(input, &"\r\n".as_bytes().to_vec(), 1);
        read_and_assert_symbol(input, b'\r', value_end).context(error_message.clone())?;
        read_and_assert_symbol(input, b'\n', value_end + 1).context(error_message.clone())?;
        Ok(SimpleError {
            value: input[value_start..value_end].to_vec()
        })
    }
}

#[derive(Debug, PartialEq)]
struct SimpleString {
    value: Vec<u8>
}

impl SimpleString {
    fn serialize(&self) -> Vec<u8> {
        let mut result: Vec<u8> = Vec::new();
        result.push(b'+');
        result.extend(self.value.as_slice());
        result.extend("\r\n".as_bytes());
        result
    }
    fn parse(input: &Vec<u8>) -> Result<SimpleString, anyhow::Error> {
        let error_message = format!("Invalid SimpleString '{}'", String::from_utf8_lossy(&input.clone()));
        read_and_assert_symbol(input, b'+', 0).context(error_message.clone())?;
        let value_start = 1;
        let value_end = read_until(input, &"\r\n".as_bytes().to_vec(), 1);
        read_and_assert_symbol(input, b'\r', value_end).context(error_message.clone())?;
        read_and_assert_symbol(input, b'\n', value_end + 1).context(error_message.clone())?;
        Ok(SimpleString {
            value: input[value_start..value_end].to_vec()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert_eq!(Integer::parse(&":+5\r\n".as_bytes().to_vec()).unwrap().value, 5);
        assert_eq!(Integer::parse(&":0\r\n".as_bytes().to_vec()).unwrap().value, 0);
        assert_eq!(Integer::parse(&":-98\r\n".as_bytes().to_vec()).unwrap().value, -98);
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
        let result = SimpleError::parse(&input).unwrap();
        assert_eq!(result, SimpleError {
            value: "Error message".as_bytes().to_vec()
        })
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
        let result = SimpleString::parse(&input).unwrap();
        assert_eq!(result, SimpleString {
            value: "hello".as_bytes().to_vec()
        })
    }

    #[test]
    fn should_not_fail_parsing_if_more_bytes_are_provided() {
        let input = "+hello\r\n+world\r\n";
        let result = SimpleString::parse(&input.as_bytes().to_vec()).unwrap();
        assert_eq!(result, SimpleString {
            value: "hello".as_bytes().to_vec()
        })
    }

    #[test]
    fn should_fail_parsing_invalid_simple_string() {
        let input = ":+5\r\n";
        let error = SimpleString::parse(&input.as_bytes().to_vec()).unwrap_err();
        assert_eq!(format!("{}", error), format!("Invalid SimpleString '{}'", input))
    }
}
