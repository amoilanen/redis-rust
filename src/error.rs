/// Custom error type for Redis protocol errors.
///
/// This error type is used for protocol-level errors that need to be
/// communicated back to the client as Redis error responses.

use std::fmt;

/// A Redis protocol error.
///
/// This error type wraps error messages that should be sent to clients
/// as Redis error responses.
#[derive(Debug, PartialEq, Clone)]
pub struct RedisError {
    /// The error message to send to the client
    pub message: String,
}

impl std::error::Error for RedisError {}

impl fmt::Display for RedisError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "RedisError: {}", self.message)
    }
}