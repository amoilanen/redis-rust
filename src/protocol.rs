use crate::error::RedisError;

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
    fn parse(input: &Vec<u8>) -> Result<SimpleString, RedisError> {
        let mut can_parse = false;
        if input.len() >= 3 && input[0] == b'+' {
            if let Some(r_index) = input.iter().position(|&ch| ch == b'\r') {
                let n_index = r_index + 1;
                if n_index == input.len() - 1 && input[n_index] == b'\n' {
                    can_parse = true;
                }
            }
        }

        if can_parse {
            Ok(SimpleString {
                value: input[1..(input.len() - 2)].to_vec()
            })
        } else {
            Err(RedisError {
                message: format!("Invalid SimpleString '{}'", String::from_utf8_lossy(&input.clone()))
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::error::RedisError;

    use super::*;

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
        assert_eq!(error, RedisError {
            message: format!("Invalid SimpleString '{}'", input)
        })
    }

    #[test]
    fn should_fail_parsing_invalid_simple_string() {
        let input = ":+5\r\n";
        let error = SimpleString::parse(&input.as_bytes().to_vec()).unwrap_err();
        assert_eq!(error, RedisError {
            message: format!("Invalid SimpleString '{}'", input)
        })
    }
}
