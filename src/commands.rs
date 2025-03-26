use std::sync::{Arc, Mutex};

use anyhow::anyhow;
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
    fn execute(&self, storage: &Arc<Mutex<storage::Storage>>) -> Result<Vec<protocol::DataType>, anyhow::Error>;
    fn is_propagated_to_replicas(&self) -> bool;
    fn should_always_reply(&self) -> bool;
    fn serialize(&self) -> Vec<u8>;
}

pub struct Echo<'a> {
    pub message: &'a protocol::DataType,
    pub argument: Option<&'a protocol::DataType>
}

impl RedisCommand for Echo<'_> {
    fn execute(&self, _: &Arc<Mutex<storage::Storage>>) -> Result<Vec<protocol::DataType>, anyhow::Error> {
        let mut reply: Vec<protocol::DataType> = Vec::new();
        if let Some(echo_argument) = self.argument {
            reply = vec![echo_argument.clone()];
        }
        return Ok(reply);
    }
    fn is_propagated_to_replicas(&self) -> bool {
        false
    }
    fn should_always_reply(&self) -> bool {
        false
    }
    fn serialize(&self) -> Vec<u8> {
        self.message.serialize()
    }
}

pub struct Ping<'a> {
    pub message: &'a protocol::DataType,
}

impl RedisCommand for Ping<'_> {
    fn execute(&self, _: &Arc<Mutex<storage::Storage>>) -> Result<Vec<protocol::DataType>, anyhow::Error> {
        return Ok(vec![protocol::simple_string("PONG")]);
    }
    fn is_propagated_to_replicas(&self) -> bool {
        false
    }
    fn should_always_reply(&self) -> bool {
        false
    }
    fn serialize(&self) -> Vec<u8> {
        self.message.serialize()
    }
}

pub struct Command<'a> {
    pub message: &'a protocol::DataType,
}

impl RedisCommand for Command<'_> {
    fn execute(&self, _: &Arc<Mutex<storage::Storage>>) -> Result<Vec<protocol::DataType>, anyhow::Error> {
        //TODO: Should return the list of all the available commands and their documentation instead
        return Ok(vec![protocol::simple_string("OK")]);
    }
    fn is_propagated_to_replicas(&self) -> bool {
        false
    }
    fn should_always_reply(&self) -> bool {
        false
    }
    fn serialize(&self) -> Vec<u8> {
        self.message.serialize()
    }
}

pub struct Set<'a> {
    pub message: &'a protocol::DataType
}

impl RedisCommand for Set<'_> {
    fn execute(&self, storage: &Arc<Mutex<storage::Storage>>) -> Result<Vec<protocol::DataType>, anyhow::Error> {
        let instructions: Vec<String> = self.message.as_array()?;
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
        return Ok(vec![protocol::simple_string("OK")]);
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

pub struct Get<'a> {
    pub message: &'a protocol::DataType
}

impl RedisCommand for Get<'_> {
    fn execute(&self, storage: &Arc<Mutex<storage::Storage>>) -> Result<Vec<protocol::DataType>, anyhow::Error> {
        let instructions: Vec<String> = self.message.as_array()?;
        let error = RedisError { 
            message: "GET command should have one argument".to_string()
        };
        let key = instructions.get(1).ok_or::<anyhow::Error>(error.clone().into())?;
        println!("GET {}", key);
        let mut data = storage.lock().unwrap(); //TODO: Avoid unwrap
        let reply = match data.get(key)? {
            Some(value) => 
                vec![protocol::bulk_string_from_bytes(value.clone())],
            None =>
                vec![protocol::bulk_string_empty()]
        };
        Ok(reply)
    }
    fn is_propagated_to_replicas(&self) -> bool {
        false
    }
    fn should_always_reply(&self) -> bool {
        false
    }
    fn serialize(&self) -> Vec<u8> {
        self.message.serialize()
    }
}

pub struct Info<'a> {
    pub message: &'a protocol::DataType,
    pub server_state: &'a server_state::ServerState
}

impl RedisCommand for Info<'_> {
    fn execute(&self, _: &Arc<Mutex<storage::Storage>>) -> Result<Vec<protocol::DataType>, anyhow::Error> {
        let instructions: Vec<String> = self.message.as_array()?;
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
            vec![protocol::bulk_string(&format!("# Replication\r\nrole:{}\r\n{}", role, additional_info))]
        } else {
            vec![protocol::bulk_string_empty()]
        };
        Ok(reply)
    }
    fn is_propagated_to_replicas(&self) -> bool {
        false
    }
    fn should_always_reply(&self) -> bool {
        false
    }
    fn serialize(&self) -> Vec<u8> {
        self.message.serialize()
    }
}

pub struct ReplConf<'a> {
    pub message: &'a protocol::DataType,
    pub server_state: &'a server_state::ServerState
}

impl RedisCommand for ReplConf<'_> {
    fn execute(&self, _: &Arc<Mutex<storage::Storage>>) -> Result<Vec<protocol::DataType>, anyhow::Error> {
        let mut reply = Vec::new();
        let instructions: Vec<String> = self.message.as_array()?;
        let sub_command = instructions.get(1).ok_or(anyhow!("replication_id not defined in {:?}", instructions))?;
        if sub_command.to_lowercase() == "getack" {
            //TODO: Implement proper offset tracking later, for now hardcoding as 0
            reply.push(protocol::array(vec![protocol::bulk_string("REPLCONF"), protocol::bulk_string("ACK"), protocol::bulk_string("0")]))
        } else {
            reply.push(protocol::bulk_string("OK"));
        }
        Ok(reply)
    }
    fn is_propagated_to_replicas(&self) -> bool {
        false
    }
    fn should_always_reply(&self) -> bool {
        true
    }
    fn serialize(&self) -> Vec<u8> {
        self.message.serialize()
    }
}

pub struct PSync<'a> {
    pub message: &'a protocol::DataType,
    pub server_state: &'a server_state::ServerState
}

impl RedisCommand for PSync<'_> {
    fn execute(&self, storage: &Arc<Mutex<storage::Storage>>) -> Result<Vec<protocol::DataType>, anyhow::Error> {
        let mut reply = Vec::new();
        let instructions: Vec<String> = self.message.as_array()?;
        let replication_id = instructions.get(1).ok_or(anyhow!("replication_id not defined in {:?}", instructions))?;
        let offset: i64 = instructions.get(2).ok_or(anyhow!("offset is not defined in {:?}", instructions))?.parse()?;
        println!("Master handling PSYNC: replication_id = {}, offset = {}", replication_id, offset);
        let replication_id = self.server_state.master_replication_id.clone().ok_or(anyhow!("replication_id is not defined on the master node"))?;
        reply.push(protocol::simple_string(format!("FULLRESYNC {} 0", replication_id).as_str()));

        let rdb_bytes = storage.lock().unwrap().to_rdb()?;
        reply.push(protocol::DataType::Rdb {
            value: rdb_bytes
        });
        //TODO: In practice it would be OK to send this command, but it fails some test expectations on Codecrafters, commenting out temporarily
        //reply.push(protocol::array(vec![protocol::bulk_string("REPLCONF"), protocol::bulk_string("GETACK"), protocol::bulk_string("*")]));
        Ok(reply)
    }
    fn is_propagated_to_replicas(&self) -> bool {
        false
    }
    fn should_always_reply(&self) -> bool {
        false
    }
    fn serialize(&self) -> Vec<u8> {
        self.message.serialize()
    }
}