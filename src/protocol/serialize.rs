use super::DataType;

impl DataType {
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

#[cfg(test)]
mod tests {
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
    fn should_serialize_verbatim_string() {
        assert_eq!(String::from_utf8_lossy(&DataType::VerbatimString {
            encoding: "txt".as_bytes().to_vec(),
            value: "Some string".as_bytes().to_vec()
        }.serialize()), "=15\r\ntxt:Some string\r\n".to_string());
    }

    #[test]
    fn should_serialize_bulk_error() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(String::from_utf8_lossy(&DataType::BulkError {
            value: "Some error".as_bytes().to_vec()
        }.serialize()), "!10\r\nSome error\r\n".to_string());
        Ok(())
    }

    #[test]
    fn should_serialize_big_number() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(String::from_utf8_lossy(&DataType::BigNumber { sign: b'+', value: "349".as_bytes().to_vec() }.serialize()), "(349\r\n".to_string());
        assert_eq!(String::from_utf8_lossy(&DataType::BigNumber { sign: b'-', value: "349".as_bytes().to_vec() }.serialize()), "(-349\r\n".to_string());
        Ok(())
    }

    #[test]
    fn should_serialize_double() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(String::from_utf8_lossy(&DataType::Double { value: 1.23 }.serialize()), ",1.23\r\n".to_string());
        Ok(())
    }

    #[test]
    fn should_serialize_boolean() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(String::from_utf8_lossy(&DataType::Boolean { value: true }.serialize()), "#t\r\n".to_string());
        assert_eq!(String::from_utf8_lossy(&DataType::Boolean { value: false }.serialize()), "#f\r\n".to_string());
        Ok(())
    }

    #[test]
    fn should_serialize_null() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(String::from_utf8_lossy(&DataType::Null {}.serialize()), "_\r\n".to_string());
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
    fn should_serialize_bulk_string() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(String::from_utf8_lossy(&DataType::BulkString {
            value: Some("This is a bulk string\r\n One, two three".as_bytes().to_vec())
        }.serialize()), "$38\r\nThis is a bulk string\r\n One, two three\r\n".to_string());
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
    fn should_serialize_simple_string() -> Result<(), Box<dyn std::error::Error>> {
        let string_value = "abcde";
        let s = DataType::SimpleString {
            value: string_value.as_bytes().to_vec()
        };
        let serialization = s.serialize();
        assert_eq!(String::from_utf8(serialization)?, format!("+{}\r\n", string_value));
        Ok(())
    }

    // Empty collections

    #[test]
    fn should_serialize_empty_map() {
        assert_eq!(String::from_utf8_lossy(&DataType::Map { entries: vec![] }.serialize()), "%0\r\n");
    }

    #[test]
    fn should_serialize_empty_set() {
        assert_eq!(String::from_utf8_lossy(&DataType::Set { elements: vec![] }.serialize()), "~0\r\n");
    }

    #[test]
    fn should_serialize_empty_push() {
        assert_eq!(String::from_utf8_lossy(&DataType::Push { elements: vec![] }.serialize()), ">0\r\n");
    }

    // Rdb serialization

    #[test]
    fn should_serialize_rdb() {
        assert_eq!(
            String::from_utf8_lossy(&DataType::Rdb { value: "fake_rdb".as_bytes().to_vec() }.serialize()),
            "$8\r\nfake_rdb"
        );
    }
}
