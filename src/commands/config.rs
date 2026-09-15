/// CONFIG command - reports the options this server was started with.
///
/// Syntax: CONFIG GET <parameter> [parameter ...]
/// Returns: a RESP array of bulk strings, alternating parameter name and
/// value - `*2\r\n$3\r\ndir\r\n$16\r\n/tmp/redis-files\r\n` for a single one.
///
/// A parameter this server does not have, or was never given, is left out of
/// the reply rather than answered with a null, which is how Redis reports one:
/// `CONFIG GET` describes what is set, so asking about three parameters of
/// which one is unknown gets two pairs back.

use std::sync::{Arc, Mutex};
use log::*;
use crate::error::RedisError;
use crate::protocol;
use crate::protocol::DataType;
use crate::server_state::ServerState;
use crate::storage::Storage;
use super::RedisCommand;

/// CONFIG command implementation.
pub struct Config {
    pub message: DataType,
    pub server_state: Arc<ServerState>,
}

impl Config {
    /// The pairs `CONFIG GET` answers with, in the order the parameters were
    /// asked for, flattened into the name, value, name, value the wire format
    /// calls for.
    ///
    /// The name comes back as the client spelled it, not as this server spells
    /// it internally - `CONFIG GET DIR` is answered `DIR` - which is what
    /// Redis does.
    fn get(&self, parameters: &[String]) -> Vec<DataType> {
        parameters
            .iter()
            .filter_map(|parameter| {
                let value = self.server_state.options.get(parameter)?;
                Some([protocol::bulk_string(parameter), protocol::bulk_string(value)])
            })
            .flatten()
            .collect()
    }
}

impl RedisCommand for Config {
    fn execute(&self, _: &Mutex<Storage>) -> Result<Vec<DataType>, anyhow::Error> {
        let instructions: Vec<String> = self.message.as_string_vec()?;

        let subcommand: &String = instructions.get(1).ok_or_else(|| {
            RedisError::new("ERR wrong number of arguments for 'config' command")
        })?;
        let parameters = &instructions[2.min(instructions.len())..];

        // Only GET so far. SET, RESETSTAT and REWRITE are refused by name
        // rather than ignored, so a client asking for one is told this server
        // cannot do it instead of being left waiting for a reply.
        if !subcommand.eq_ignore_ascii_case("GET") {
            return Err(RedisError::new(&format!(
                "ERR Unknown CONFIG subcommand or wrong number of arguments for '{}'",
                subcommand
            ))
            .into());
        }
        if parameters.is_empty() {
            return Err(RedisError::new(
                "ERR wrong number of arguments for 'config|get' command",
            )
            .into());
        }

        debug!("CONFIG GET {}", parameters.join(" "));

        Ok(vec![protocol::array(self.get(parameters))])
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

    fn name(&self) -> &str {
        "CONFIG"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::{client_error_message, command_message, create_test_storage};
    use crate::config::ServerOptions;

    /// A CONFIG command against a server started with `--dir /tmp/redis-files
    /// --dbfilename dump.rdb`.
    fn default_config_command(parts: &[&str]) -> Config {
        config_command(
            parts,
            ServerOptions::initialize().with_port(6379).with_rdb_file("/tmp/redis-files", "dump.rdb"),
        )
    }

    /// A CONFIG command against a server started with `options`.
    fn config_command(parts: &[&str], options: ServerOptions) -> Config {
        Config {
            message: command_message(parts),
            server_state: Arc::new(ServerState::new(options)),
        }
    }

    /// The reply's elements, which are always a single array.
    fn pairs(reply: Vec<DataType>) -> Vec<String> {
        assert_eq!(reply.len(), 1, "CONFIG GET replies with one array");
        reply[0].as_string_vec().unwrap()
    }

    #[test]
    fn should_report_the_directory_the_rdb_file_lives_in() -> anyhow::Result<()> {
        let storage = create_test_storage();

        let reply = default_config_command(&["CONFIG", "GET", "dir"]).execute(&storage)?;

        assert_eq!(pairs(reply), vec!["dir", "/tmp/redis-files"]);
        Ok(())
    }

    #[test]
    fn should_report_the_name_of_the_rdb_file() -> anyhow::Result<()> {
        let storage = create_test_storage();

        let reply = default_config_command(&["CONFIG", "GET", "dbfilename"]).execute(&storage)?;

        assert_eq!(pairs(reply), vec!["dbfilename", "dump.rdb"]);
        Ok(())
    }

    #[test]
    fn should_reply_with_bulk_strings() -> anyhow::Result<()> {
        // The tester reads the reply as bulk strings, byte for byte:
        // *2\r\n$3\r\ndir\r\n$16\r\n/tmp/redis-files\r\n
        let storage = create_test_storage();

        let reply = default_config_command(&["CONFIG", "GET", "dir"]).execute(&storage)?;

        assert_eq!(
            reply[0],
            protocol::array(vec![
                protocol::bulk_string("dir"),
                protocol::bulk_string("/tmp/redis-files"),
            ])
        );
        Ok(())
    }

    #[test]
    fn should_answer_a_parameter_however_the_client_spelled_it() -> anyhow::Result<()> {
        let storage = create_test_storage();

        let reply = default_config_command(&["CONFIG", "GET", "DIR"]).execute(&storage)?;

        // Looked up case-insensitively, but echoed back as it was asked for.
        assert_eq!(pairs(reply), vec!["DIR", "/tmp/redis-files"]);
        Ok(())
    }

    #[test]
    fn should_answer_every_parameter_asked_for_in_order() -> anyhow::Result<()> {
        let storage = create_test_storage();

        let reply = default_config_command(&["CONFIG", "GET", "dbfilename", "dir"]).execute(&storage)?;

        assert_eq!(
            pairs(reply),
            vec!["dbfilename", "dump.rdb", "dir", "/tmp/redis-files"]
        );
        Ok(())
    }

    #[test]
    fn should_leave_out_a_parameter_this_server_does_not_have() -> anyhow::Result<()> {
        let storage = create_test_storage();

        let reply = default_config_command(&["CONFIG", "GET", "maxmemory"]).execute(&storage)?;

        // An empty array, not an error: CONFIG GET reports what is set.
        assert_eq!(pairs(reply), Vec::<String>::new());
        Ok(())
    }

    #[test]
    fn should_answer_the_known_parameters_among_unknown_ones() -> anyhow::Result<()> {
        let storage = create_test_storage();

        let reply =
            default_config_command(&["CONFIG", "GET", "maxmemory", "dir", "appendonly"]).execute(&storage)?;

        assert_eq!(pairs(reply), vec!["dir", "/tmp/redis-files"]);
        Ok(())
    }

    #[test]
    fn should_leave_out_a_parameter_this_server_was_never_given() -> anyhow::Result<()> {
        // Started without --dir or --dbfilename: the parameters exist, but
        // this server has no value for them.
        let storage = create_test_storage();
        let command = config_command(
            &["CONFIG", "GET", "dir", "dbfilename"],
            ServerOptions::initialize().with_port(6379),
        );

        let reply = command.execute(&storage)?;

        assert_eq!(pairs(reply), Vec::<String>::new());
        Ok(())
    }

    #[test]
    fn should_reject_a_subcommand_this_server_cannot_do() {
        let storage = create_test_storage();

        let error = default_config_command(&["CONFIG", "SET", "dir", "/tmp"])
            .execute(&storage)
            .unwrap_err();

        assert_eq!(
            client_error_message(error),
            "ERR Unknown CONFIG subcommand or wrong number of arguments for 'SET'"
        );
    }

    #[test]
    fn should_reject_a_config_with_no_subcommand() {
        let storage = create_test_storage();

        let error = default_config_command(&["CONFIG"]).execute(&storage).unwrap_err();

        assert_eq!(
            client_error_message(error),
            "ERR wrong number of arguments for 'config' command"
        );
    }

    #[test]
    fn should_reject_a_get_with_no_parameter_to_get() {
        let storage = create_test_storage();

        let error = default_config_command(&["CONFIG", "GET"]).execute(&storage).unwrap_err();

        assert_eq!(
            client_error_message(error),
            "ERR wrong number of arguments for 'config|get' command"
        );
    }

    #[test]
    fn should_not_travel_down_the_replication_stream() {
        // Reading the configuration changes nothing, and each server has its
        // own command line to answer from.
        assert!(!default_config_command(&["CONFIG", "GET", "dir"]).is_propagated_to_replicas());
    }
}
