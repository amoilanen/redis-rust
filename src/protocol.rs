struct SimpleString {
    value: Vec<u8>
}

impl SimpleString {
    fn serialize(&self) -> Vec<u8> {
        let mut result: Vec<u8> = Vec::new();
        result.push('+' as u8);
        result.extend(self.value.as_slice());
        result.extend("\r\n".as_bytes());
        result
    }
    fn parse(input: &Vec<u8>) -> Result<SimpleString, anyhow::Error> {
        Ok(SimpleString {
            value: Vec::new()
        })
    }
}

#[cfg(test)]
mod tests {
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
        //TODO:
    }

    #[test]
    fn should_fail_parsing_invalid_simple_string() {
        //TODO:
    }
}
