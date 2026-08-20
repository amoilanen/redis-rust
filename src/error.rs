use std::fmt;

use crate::protocol::{self, DataType};

#[derive(Debug, PartialEq, Clone)]
pub struct RedisError {
    pub message: String,
}

impl RedisError {
    pub(crate) fn new(message: &str) -> RedisError {
        RedisError {
            message: message.to_owned()
        }
    }

    pub(crate) fn as_protocol_error(&self) -> DataType {
        let formatted = format!("ERR {}", self.message);
        protocol::simple_error(&formatted)
    }
}

impl std::error::Error for RedisError {}

impl fmt::Display for RedisError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "RedisError: {}", self.message)
    }
}