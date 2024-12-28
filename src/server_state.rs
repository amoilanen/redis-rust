use rand::Rng;
use crate::error::RedisError;

pub struct ServerState {
    pub port: usize,
    pub replica_of: Option<String>,
    pub master_replication_id: Option<String>,
    pub master_replication_offset: Option<usize>
}

impl ServerState {

    const REPLICATION_ID_LENGTH: usize = 20;

    pub fn get_replica_of_address(&self) -> Result<Option<String>, anyhow::Error> {
        match &self.replica_of {
            Some(replica_of) => {
                let error = RedisError { 
                    message: format!("Cannot parse replica_of {}", replica_of)
                };
                let mut replica_of_parts = replica_of.split(" ");
                let host = replica_of_parts.next().ok_or::<anyhow::Error>(error.clone().into())?;
                let port = replica_of_parts.next().ok_or::<anyhow::Error>(error.clone().into())?;
                Ok(Some(format!("{}:{}", host, port)))
            },
            None => {
                Ok(None)
            }
        }
    }

    fn generate_replication_id() -> String {
        let mut generator = rand::thread_rng();
        let random_bytes: Vec<u8> = (0..ServerState::REPLICATION_ID_LENGTH).map(|_| generator.gen()).collect();
        let formatted_bytes: String = random_bytes.iter().map(|x| format!("{:02x}", x)).collect();
        formatted_bytes
    }

    pub fn new(replica_of: Option<String>, port: usize) -> ServerState {
        match replica_of {
            Some(replica_of) =>
                ServerState {
                    port,
                    replica_of: Some(replica_of),
                    master_replication_id: None,
                    master_replication_offset: None
                },
            None =>
                ServerState {
                    port,
                    replica_of: None,
                    master_replication_id: Some(ServerState::generate_replication_id()),
                    master_replication_offset: Some(0)
                }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_set_replication_id_and_offset_for_master() {
        let state = ServerState::new(None);
        assert_eq!(state.replica_of, None);
        assert_eq!(state.master_replication_offset, Some(0));
        println!("{:?}", state.master_replication_id);
        assert_eq!(state.master_replication_id.map(|x| x.len()).unwrap_or(0), 40);
    }

    #[test]
    fn should_set_replication_id_and_offset_for_slave() {
        let state = ServerState::new(Some("localhost 6379".to_owned()));
        assert_eq!(state.replica_of, Some("localhost 6379".to_owned()));
        assert_eq!(state.master_replication_offset, None);
        assert_eq!(state.master_replication_id, None);
    }
}