use anyhow::{ensure, anyhow};
use std::env;
use std::{io::Write, net::{TcpListener, TcpStream}};
use std::collections::HashMap;
use std::time::Duration;
use std::thread;
use std::sync::{Arc, Mutex};

use redis_starter_rust::storage::{ Storage, StoredValue };
use redis_starter_rust::protocol;
use redis_starter_rust::io;
use redis_starter_rust::commands::{ self, RedisCommand };
use redis_starter_rust::server_state::ServerState;

fn main() -> Result<(), anyhow::Error> {
    const DEFAULT_PORT: usize = 6379;
    let args: Vec<String> = env::args().collect();
    //let args: Vec<String> = vec!["", "", "--port", "6380", "--replicaof", "'localhost 6379'"].iter().map(|x| x.to_string()).collect();
    //let args: Vec<String> = vec!["", "", "--port", "6379"].iter().map(|x| x.to_string()).collect();
    let port = get_port(&args)?.unwrap_or(DEFAULT_PORT);
    let replica_of = get_replica_of(&args);
    let shared_server_state = Arc::new(ServerState::new(replica_of, port));
    let server_address = format!("127.0.0.1:{}", port);
    let listener = TcpListener::bind(server_address)?;
    let redis_data: HashMap<String, StoredValue> = HashMap::new();
    let storage: Arc<Mutex<Storage>> = Arc::new(Mutex::new(Storage::new(redis_data)));
    {
        let server_state = Arc::clone(&shared_server_state);
        if let Some(replica_of_address) = shared_server_state.get_replica_of_address()? {
            let storage = Arc::clone(&storage);
            thread::spawn(move || {
                join_cluster(&replica_of_address, &server_state, &storage).unwrap();
            });
        }
    }
    for incoming_connection in listener.incoming() {
        let mut stream = incoming_connection?;
        let storage = Arc::clone(&storage);
        let server_state = Arc::clone(&shared_server_state);
        stream.set_read_timeout(Some(Duration::new(1, 0)))?;
        thread::spawn(move || {
            //TODO: Handle failing connection_handler, at least print to the console
            connection_handler(&mut stream, &storage, &server_state)
        });
    }
    Ok(())
}

fn get_replica_of(args: &[String]) -> Option<String> {
    get_option_value("replicaof", args)
}

fn get_port(args: &[String]) -> Result<Option<usize>, anyhow::Error> {
    match get_option_value("port", args) {
        Some(p) => Ok(Some(p.parse()?)),
        None => Ok(None)
    }
}

fn get_option_value(option_name: &str, args: &[String]) -> Option<String> {
    let mut option_value = None;
    if let Some(option_position) = args.iter().position(|x| x == &format!("--{}", option_name)) {
        if let Some(option_input) = args.get(option_position + 1) {
            option_value = Some(option_input.to_owned());
        }
    }
    option_value
}

fn join_cluster(replica_of_address: &str, server_state: &Arc<ServerState>, storage: &Arc<Mutex<Storage>>) -> Result<(), anyhow::Error> {
    let mut stream = TcpStream::connect(replica_of_address)?;
    stream.set_read_timeout(Some(Duration::new(5, 0)))?;
    let ping = protocol::array(vec![protocol::bulk_string("PING")]);
    stream.write_all(&ping.serialize())?;
    if let Some(pong) = io::read_message(&mut stream)? {
        ensure!(pong.as_string()? == "PONG".to_owned(), "Should receive PONG from the master node")
    } else {
        Err(anyhow!("Should receive PONG from the master node"))?
    }
    let port_replconf = protocol::array(vec![
        protocol::bulk_string("REPLCONF"),
        protocol::bulk_string("listening-port"),
        protocol::bulk_string(&server_state.port.to_string())
    ]);
    stream.write_all(&port_replconf.serialize())?;
    if let Some(ok) = io::read_message(&mut stream)? {
        ensure!(ok.as_string()? == "OK".to_owned(), "Should receive OK from the master node")
    } else {
        Err(anyhow!("Should receive OK from the master node"))?
    }
    let capa_replconf = protocol::array(vec![
        protocol::bulk_string("REPLCONF"),
        protocol::bulk_string("capa"),
        protocol::bulk_string("psync2")
    ]);
    stream.write_all(&capa_replconf.serialize())?;
    if let Some(ok) = io::read_message(&mut stream)? {
        ensure!(ok.as_string()? == "OK".to_owned(), "Should receive OK from the master node")
    } else {
        Err(anyhow!("Should receive OK from the master node"))?
    }
    let psync = protocol::array(vec![
        protocol::bulk_string("PSYNC"),
        protocol::bulk_string("?"),
        protocol::bulk_string("-1")
    ]);
    stream.write_all(&psync.serialize())?;
    if let Some(psync_reply) = io::read_message(&mut stream)? {
        let reply = psync_reply.as_string()?;
        //println!("Received from server {}", reply);
        let reply_parts: Vec<&str> = reply.split(" ").collect();
        let replication_id = reply_parts.get(1).ok_or(anyhow!("Could not read replication_id from the server reply {:?}", reply))?;
        println!("Received replication_id {} from the server", replication_id);
    } else {
        Err(anyhow!("Should receive reply to PSYNC from the master node"))?
    }
    /*
    let mut rdb_bytes = io::read_bytes(&mut stream)?;
    while rdb_bytes.is_none() {
        println!("Not yet received RDB...");
        rdb_bytes = io::read_bytes(&mut stream)?;
    }
    println!("Received RDB bytes {:?}", rdb_bytes);*/
    if let Some(rdb_bytes) = io::read_bytes(&mut stream)? {
        let rdb = protocol::DataType::parse_rdb(&rdb_bytes)?;
        let received_storage = match rdb {
            protocol::DataType::Rdb { value } =>
                Storage::from_rdb(&value),
            _ =>
                Err(anyhow!("Expected protocol::DataType::Rdb"))
        }?;
        println!("Received storage {:?}", &received_storage);
        {
            let mut storage = storage.lock().unwrap();
            for (key, value) in received_storage.data.into_iter() {
                storage.data.insert(key, value);
            }
        }
    } else {
        println!("RDB file not received")
    }

    Ok(())
}

fn connection_handler(stream: &mut TcpStream, storage: &Arc<Mutex<Storage>>, server_state: &Arc<ServerState>) -> Result<(), anyhow::Error> {
    stream.set_read_timeout(Some(Duration::new(1, 0)))?;
    //println!("accepted new connection");
    loop {
        if let Some(received_message) = io::read_message(stream)? {
            println!("Received: {}", String::from_utf8_lossy(&received_message.serialize()));
            let command_name = commands::parse_command_name(&received_message)?;
            match &received_message {
                protocol::DataType::Array { elements } => {
                    let mut command: Option<Box<dyn RedisCommand>> = None;
                    let command_name = command_name.as_str();
                    if command_name == "ECHO" {
                        command = Some(Box::new(commands::Echo { argument: elements.get(1) }));
                    } else if command_name == "PING" {
                        command = Some(Box::new(commands::Ping {}));
                    } else if command_name == "SET" {
                        command = Some(Box::new(commands::Set { instructions: &received_message }));
                    } else if command_name == "GET" {
                        command = Some(Box::new(commands::Get { instructions: &received_message }));
                    } else if command_name == "COMMAND" {
                        command = Some(Box::new(commands::Command {}))
                    } else if command_name == "INFO" {
                        command = Some(Box::new(commands::Info { instructions: &received_message, server_state }))
                    } else if command_name == "REPLCONF" {
                        command = Some(Box::new(commands::ReplConf { instructions: &received_message, server_state }))
                    } else if command_name == "PSYNC" {
                        command = Some(Box::new(commands::PSync { instructions: &received_message, server_state }));
                        server_state.replica_connections.lock().unwrap().push(stream.try_clone()?);
                    }
                    if let Some(command) = command {
                        let reply = command.execute(storage)?;
                        for message in reply.into_iter() {
                            //println!("Sending: {:?}", message);
                            let message_bytes = &message.serialize();
                            //println!("which serializes to {:?}", message_bytes);
                            {
                                stream.write_all(message_bytes)?;
                            }
                        }
                    }
                },
                _ => ()
            }
        } else {
            //println!("No message has been read")
        }
        //TODO: Terminate the connection when requested by the client
    }
}