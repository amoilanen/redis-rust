use std::fmt;

#[derive(Debug, PartialEq, Clone)]
pub struct RedisError {
    pub message: String,
}

impl std::error::Error for RedisError {}

impl fmt::Display for RedisError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "RedisError: {}", self.message)
    }
}