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

pub fn integer(value: i64) -> DataType {
    DataType::Integer { value }
}

pub fn simple_error(value: &str) -> DataType {
    DataType::SimpleError { value: value.as_bytes().to_vec() }
}

pub fn bulk_error(value: &str) -> DataType {
    DataType::BulkError { value: value.as_bytes().to_vec() }
}

// value may include a leading '+' or '-'; an unsigned value is treated as positive.
pub fn big_number(value: &str) -> DataType {
    let (sign, digits) = match value.as_bytes().first() {
        Some(&b'-') => (b'-', &value[1..]),
        Some(&b'+') => (b'+', &value[1..]),
        _           => (b'+', value),
    };
    DataType::BigNumber { sign, value: digits.as_bytes().to_vec() }
}

pub fn verbatim_string(encoding: &str, value: &str) -> DataType {
    DataType::VerbatimString {
        encoding: encoding.as_bytes().to_vec(),
        value: value.as_bytes().to_vec(),
    }
}

pub fn map(entries: Vec<(DataType, DataType)>) -> DataType {
    DataType::Map { entries }
}

pub fn set(elements: Vec<DataType>) -> DataType {
    DataType::Set { elements }
}

pub fn push(elements: Vec<DataType>) -> DataType {
    DataType::Push { elements }
}

pub fn null() -> DataType {
    DataType::Null
}

pub fn boolean(value: bool) -> DataType {
    DataType::Boolean {
        value
    }
}

impl DataType {

    pub fn as_vec(&self) -> Result<Vec<String>, anyhow::Error> {
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
}

#[cfg(test)]
mod tests {
    use super::*;

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

    // as_vec()

    #[test]
    fn should_convert_array_as_array() -> Result<(), Box<dyn std::error::Error>> {
        let result = DataType::Array {
            elements: vec![
                DataType::SimpleString { value: "hello".as_bytes().to_vec() },
                DataType::Integer { value: 42 },
            ]
        }.as_vec()?;
        assert_eq!(result, vec!["hello".to_string(), "42".to_string()]);
        Ok(())
    }

    #[test]
    fn should_wrap_non_array_as_single_element_array() -> Result<(), Box<dyn std::error::Error>> {
        let result = DataType::SimpleString { value: "hello".as_bytes().to_vec() }.as_vec()?;
        assert_eq!(result, vec!["hello".to_string()]);
        Ok(())
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
        assert_eq!(integer(42), DataType::Integer { value: 42 });
        assert_eq!(integer(-7), DataType::Integer { value: -7 });
        assert_eq!(simple_error("ERR bad"), DataType::SimpleError { value: b"ERR bad".to_vec() });
        assert_eq!(bulk_error("ERR details"), DataType::BulkError { value: b"ERR details".to_vec() });
        assert_eq!(big_number("349"), DataType::BigNumber { sign: b'+', value: b"349".to_vec() });
        assert_eq!(big_number("+349"), DataType::BigNumber { sign: b'+', value: b"349".to_vec() });
        assert_eq!(big_number("-349"), DataType::BigNumber { sign: b'-', value: b"349".to_vec() });
        assert_eq!(verbatim_string("txt", "hello"), DataType::VerbatimString {
            encoding: b"txt".to_vec(),
            value: b"hello".to_vec(),
        });
        assert_eq!(map(vec![(integer(1), bulk_string("a"))]), DataType::Map {
            entries: vec![(DataType::Integer { value: 1 }, DataType::BulkString { value: Some(b"a".to_vec()) })]
        });
        assert_eq!(set(vec![integer(1)]), DataType::Set { elements: vec![DataType::Integer { value: 1 }] });
        assert_eq!(push(vec![integer(1)]), DataType::Push { elements: vec![DataType::Integer { value: 1 }] });
    }
}
