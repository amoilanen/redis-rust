use std::sync::{Arc, Mutex};
use anyhow::anyhow;
use log::*;
use crate::protocol;
use crate::protocol::DataType;
use crate::storage::Storage;
use crate::error::RedisError;
use super::RedisCommand;

/// RPush command implementation.
pub struct RPush {
    pub message: DataType,
}

impl RedisCommand for RPush {
    fn execute(&self, storage: &Arc<Mutex<Storage>>) -> Result<Vec<DataType>, anyhow::Error> {
        let instructions: Vec<String> = self.message.as_vec()?;
        let error = RedisError {
            message: "Invalid RPUSH command syntax".to_string(),
        };

        let key = instructions.get(1).ok_or::<anyhow::Error>(error.clone().into())?;
        let value = instructions.get(2).ok_or::<anyhow::Error>(error.clone().into())?;

        debug!("RPUSH {} {}", key, value);

        let mut data = storage
            .lock()
            .map_err(|e| anyhow!("Failed to lock storage: {}", e))?;

        let stored_raw_value = data.get(key)?;
        let stored_value = stored_raw_value.map(|value| protocol::read_message_from_bytes(&value)).transpose()?;
        let mut stored_elements = match stored_value {
            Some(DataType::Array { elements }) => {
                Ok(elements)
            },
            None => {
                Ok(Vec::new())
            },
            Some(_) => Err(anyhow!("Not an Array is stored in storage")),
        }?;
        stored_elements.push(protocol::simple_string(value));
        data.set(key, protocol::array(stored_elements.clone()).serialize(), None)?;
        Ok(vec![protocol::integer(stored_elements.len() as i64)])
    }

    fn is_propagated_to_replicas(&self) -> bool {
        true
    }

    fn should_always_reply(&self) -> bool {
        false
    }

    fn serialize(&self) -> Vec<u8> {
        self.message.serialize()
    }
}
