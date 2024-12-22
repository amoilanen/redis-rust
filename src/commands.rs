use std::sync::{Arc, Mutex};

use crate::error::RedisError;
use crate::protocol;
use crate::server_state;
use crate::storage;

pub fn parse_command_name(received_message: &protocol::DataType) -> Result<String, anyhow::Error> {
    let received_message_parts: Vec<String> = received_message.as_array()?;
    let command_parts: Vec<&str> = received_message_parts.iter().map(|x| x.as_str()).collect();
    let command_name = command_parts.get(0).unwrap_or(&"").to_string();
    Ok(command_name)
}

pub trait RedisCommand {
    fn execute(&self, storage: &Arc<Mutex<storage::Storage>>) -> Result<Option<protocol::DataType>, anyhow::Error>;
}

pub struct Echo<'a> {
    pub argument: Option<&'a protocol::DataType>
}

impl RedisCommand for Echo<'_> {
    fn execute(&self, _: &Arc<Mutex<storage::Storage>>) -> Result<Option<protocol::DataType>, anyhow::Error> {
        let mut reply: Option<protocol::DataType> = None;
        if let Some(echo_argument) = self.argument {
            reply = Some(echo_argument.clone());
        }
        return Ok(reply);
    }
}

pub struct Ping {}

impl RedisCommand for Ping {
    fn execute(&self, _: &Arc<Mutex<storage::Storage>>) -> Result<Option<protocol::DataType>, anyhow::Error> {
        return Ok(Some(protocol::simple_string("PONG")));
    }
}

pub struct Command {}

impl RedisCommand for Command {
    fn execute(&self, _: &Arc<Mutex<storage::Storage>>) -> Result<Option<protocol::DataType>, anyhow::Error> {
        //TODO: Should return the list of all the available commands and their documentation instead
        return Ok(Some(protocol::simple_string("OK")));
    }
}

pub struct Set<'a> {
    pub instructions: &'a protocol::DataType
}

impl RedisCommand for Set<'_> {
    fn execute(&self, storage: &Arc<Mutex<storage::Storage>>) -> Result<Option<protocol::DataType>, anyhow::Error> {
        let instructions: Vec<String> = self.instructions.as_array()?;
        let error = RedisError { 
            message: "Invalid SET command syntax".to_string()
        };
        let key = instructions.get(1).ok_or::<anyhow::Error>(error.clone().into())?;
        let value = instructions.get(2).ok_or::<anyhow::Error>(error.clone().into())?;
        let expires_in_ms = if let Some(modifier) = instructions.get(3) {
            if modifier == "px" {
                let expiration_time: u64 = instructions.get(4).ok_or::<anyhow::Error>(error.clone().into())?.parse()?;
                Some(expiration_time)
            } else {
                None
            }
        } else {
            None
        };
        println!("SET {} {}", key, value);
        println!("expiration_after = {:?}", expires_in_ms);
        let mut data = storage.lock().unwrap(); //TODO: Avoid unwrap
        data.set(key, value.as_bytes().to_vec(), expires_in_ms)?;
        return Ok(Some(protocol::simple_string("OK")));
    }
}

pub struct Get<'a> {
    pub instructions: &'a protocol::DataType
}

impl RedisCommand for Get<'_> {
    fn execute(&self, storage: &Arc<Mutex<storage::Storage>>) -> Result<Option<protocol::DataType>, anyhow::Error> {
        let instructions: Vec<String> = self.instructions.as_array()?;
        let error = RedisError { 
            message: "GET command should have one argument".to_string()
        };
        let key = instructions.get(1).ok_or::<anyhow::Error>(error.clone().into())?;
        println!("GET {}", key);
        let mut data = storage.lock().unwrap(); //TODO: Avoid unwrap
        let reply = match data.get(key)? {
            Some(value) => 
                Some(protocol::bulk_string_from_bytes(value.clone())),
            None =>
                Some(protocol::bulk_string_empty())
        };
        Ok(reply)
    }
}

pub struct Info<'a> {
    pub instructions: &'a protocol::DataType,
    pub server_state: &'a server_state::ServerState
}

impl RedisCommand for Info<'_> {
    fn execute(&self, _: &Arc<Mutex<storage::Storage>>) -> Result<Option<protocol::DataType>, anyhow::Error> {
        let instructions: Vec<String> = self.instructions.as_array()?;
        let error = RedisError { 
            message: "INFO command should have one argument".to_string()
        };
        let argument = instructions.get(1).ok_or::<anyhow::Error>(error.clone().into())?;

        let reply = if argument == "replication" {
            let role = match &self.server_state.replica_of {
                Some(_) => "slave",
                None => "master"
            };
            let additional_info = match role {
                "slave" => "".to_owned(),
                "master" => format!("master_replid:{}\r\nmaster_repl_offset:{}\r\n",
                    &self.server_state.master_replication_id.clone().unwrap_or("".to_owned()),
                    &self.server_state.master_replication_offset.unwrap_or(0)
                ),
                _ => "".to_owned()
            };
            Some(protocol::bulk_string(&format!("# Replication\r\nrole:{}\r\n{}", role, additional_info)))
        } else {
            Some(protocol::bulk_string_empty())
        };
        Ok(reply)
    }
}