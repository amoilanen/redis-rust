use crate::error::RedisError;

fn starts_with_symbol_and_ends_with_r_n(symbol: u8, input: &Vec<u8>) -> bool {
    let mut matches = false;
    if input.len() >= 3 && input[0] == symbol {
        if let Some(r_index) = input.iter().position(|&ch| ch == b'\r') {
            let n_index = r_index + 1;
            if n_index == input.len() - 1 && input[n_index] == b'\n' {
                matches = true;
            }
        }
    }
    matches
}

#[derive(Debug, PartialEq)]
struct Integer {
    value: i64
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
        if starts_with_symbol_and_ends_with_r_n(b':', input) {
            Ok(Integer {
                value: std::str::from_utf8(&input[1..(input.len() - 2)])?.parse()?
            })
        } else {
            Err(RedisError {
                message: format!("Invalid Integer '{}'", String::from_utf8_lossy(&input.clone()))
            }.into())
        }
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
        if starts_with_symbol_and_ends_with_r_n(b'-', input) {
            Ok(SimpleError {
                value: input[1..(input.len() - 2)].to_vec()
            })
        } else {
            Err(RedisError {
                message: format!("Invalid SimpleError '{}'", String::from_utf8_lossy(&input.clone()))
            }.into())
        }
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
        if starts_with_symbol_and_ends_with_r_n(b'+', input) {
            Ok(SimpleString {
                value: input[1..(input.len() - 2)].to_vec()
            })
        } else {
            Err(RedisError {
                message: format!("Invalid SimpleString '{}'", String::from_utf8_lossy(&input.clone()))
            }.into())
        }
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
    fn should_fail_parsing_if_more_bytes_are_provided() {
        let input = "+hello\r\n+world\r\n";
        let error = SimpleString::parse(&input.as_bytes().to_vec()).unwrap_err();
        assert_eq!(format!("{}", error), format!("RedisError: Invalid SimpleString '{}'", input))
    }

    #[test]
    fn should_fail_parsing_invalid_simple_string() {
        let input = ":+5\r\n";
        let error = SimpleString::parse(&input.as_bytes().to_vec()).unwrap_err();
        assert_eq!(format!("{}", error), format!("RedisError: Invalid SimpleString '{}'", input))
    }
}
