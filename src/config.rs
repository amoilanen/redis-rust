//! The options a server is started with.
//!
//! Everything the command line has to say about this server lives in
//! [`ServerOptions`], read from `argv` once at startup and then carried on the
//! server state. Commands that report configuration - `CONFIG GET` - read it
//! back through [`ServerOptions::get`], so the wire-facing name of an option
//! is decided here rather than in the command.

use std::path::PathBuf;

use crate::cli;

/// The port a server listens on when `--port` is not given.
pub const DEFAULT_PORT: usize = 6379;

/// The command line this server was started with.
///
/// One struct rather than a value per flag: the options are read together,
/// travel together, and `CONFIG GET` needs to look one up by name, which is
/// only possible where they all sit side by side.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServerOptions {
    /// The port to listen on, defaulted rather than optional: a server always
    /// has one, whether or not it was told which.
    pub port: usize,
    /// `--replicaof "<host> <port>"`, the master this server follows. `None`
    /// on a master.
    pub replica_of: Option<String>,
    /// `--dir`, the directory the RDB file lives in.
    ///
    /// Optional, unlike real Redis, which defaults it to the working
    /// directory: `None` says nothing was configured, which is what lets a
    /// server started without the flag run without an RDB file at all rather
    /// than inventing a path it was never given.
    pub dir: Option<String>,
    /// `--dbfilename`, the name of the RDB file within [`dir`](Self::dir).
    pub dbfilename: Option<String>,
}

impl ServerOptions {
    /// Reads the options out of `argv`.
    ///
    /// Fails only on an argument that cannot be made sense of - a non-numeric
    /// `--port`. A flag that is simply absent is not an error; it takes its
    /// default.
    ///
    /// Every field is named here rather than built up from
    /// [`unconfigured`](Self::unconfigured) through the builders: a new option
    /// added to the struct should stop this compiling until it is read off the
    /// command line too, which is exactly what an exhaustive literal does.
    pub fn from_args(args: &[String]) -> Result<ServerOptions, anyhow::Error> {
        Ok(ServerOptions {
            port: cli::get_port(args)?.unwrap_or(DEFAULT_PORT),
            replica_of: cli::get_replica_of(args),
            dir: cli::get_dir(args),
            dbfilename: cli::get_dbfilename(args),
        })
    }

    /// A server with nothing configured: no port, no master to follow, no RDB
    /// file. The one starting point the builders below build on.
    ///
    /// Port 0, not [`DEFAULT_PORT`]: this is the empty command line, and
    /// answering it with 6379 would be inventing a decision nobody made.
    /// Supplying the default is [`from_args`](Self::from_args)'s business,
    /// where an absent `--port` is the question the default answers. A port 0
    /// that reaches the listener is a builder someone forgot to call, and it
    /// is better for that to be a server on an unpredictable port than one
    /// quietly squatting on 6379.
    pub fn initialize() -> ServerOptions {
        ServerOptions {
            port: 0,
            replica_of: None,
            dir: None,
            dbfilename: None,
        }
    }

    /// The same options, listening on `port`.
    pub fn with_port(self, port: usize) -> ServerOptions {
        ServerOptions { port, ..self }
    }

    /// The same options, following `replica_of` - a `"<host> <port>"` pair, as
    /// `--replicaof` spells it.
    pub fn replicating(self, replica_of: &str) -> ServerOptions {
        ServerOptions {
            replica_of: Some(replica_of.to_owned()),
            ..self
        }
    }

    /// The same options, with the RDB file named and placed.
    pub fn with_rdb_file(self, dir: &str, dbfilename: &str) -> ServerOptions {
        ServerOptions {
            dir: Some(dir.to_owned()),
            dbfilename: Some(dbfilename.to_owned()),
            ..self
        }
    }

    /// The value of the configuration parameter `name`, as `CONFIG GET` asks
    /// for it, or `None` when this server has no such parameter or was never
    /// given one.
    ///
    /// Case-insensitive, as Redis is: `CONFIG GET DIR` and `CONFIG GET dir`
    /// ask the same question. The caller still answers with the name the
    /// client sent, which is also what Redis does.
    ///
    /// The two are deliberately indistinguishable here: `CONFIG GET` omits an
    /// unknown parameter and an unset one alike, so nothing downstream has to
    /// tell them apart.
    pub fn get(&self, name: &str) -> Option<&str> {
        let value = match name.to_lowercase().as_str() {
            "dir" => &self.dir,
            "dbfilename" => &self.dbfilename,
            _ => return None,
        };
        value.as_deref()
    }

    /// Where this server's RDB file is, or `None` if it was not told.
    ///
    /// Both halves are needed: a directory with no file name names no file,
    /// and a file name with no directory would have to guess at one. Nothing
    /// reads the file yet - persistence arrives in a later stage - but the
    /// path it will be read from is a property of the options, so it is
    /// answered here.
    pub fn rdb_path(&self) -> Option<PathBuf> {
        let dir = self.dir.as_ref()?;
        let dbfilename = self.dbfilename.as_ref()?;
        Some(PathBuf::from(dir).join(dbfilename))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An argv as `std::env::args` would hand it over, program name included.
    fn args(parts: &[&str]) -> Vec<String> {
        std::iter::once("prog")
            .chain(parts.iter().copied())
            .map(str::to_owned)
            .collect()
    }

    #[test]
    fn should_read_every_option_from_the_command_line() {
        let options = ServerOptions::from_args(&args(&[
            "--port",
            "6380",
            "--replicaof",
            "localhost 6379",
            "--dir",
            "/tmp/redis-files",
            "--dbfilename",
            "dump.rdb",
        ]))
        .unwrap();

        assert_eq!(options.port, 6380);
        assert_eq!(options.replica_of, Some("localhost 6379".to_owned()));
        assert_eq!(options.dir, Some("/tmp/redis-files".to_owned()));
        assert_eq!(options.dbfilename, Some("dump.rdb".to_owned()));
    }

    #[test]
    fn should_start_from_a_command_line_that_said_nothing() {
        let options = ServerOptions::initialize();

        // Port 0 rather than 6379: nothing has been configured yet, and the
        // builders are what configure it.
        assert_eq!(options.port, 0);
        assert_eq!(options.replica_of, None);
        assert_eq!(options.dir, None);
        assert_eq!(options.dbfilename, None);
    }

    #[test]
    fn should_build_the_options_up_one_at_a_time() {
        // Each builder adds to what came before rather than replacing it, in
        // whatever order they are called.
        let options = ServerOptions::initialize()
            .with_rdb_file("/tmp/redis-files", "dump.rdb")
            .replicating("localhost 6379")
            .with_port(6380);

        assert_eq!(
            options,
            ServerOptions {
                port: 6380,
                replica_of: Some("localhost 6379".to_owned()),
                dir: Some("/tmp/redis-files".to_owned()),
                dbfilename: Some("dump.rdb".to_owned()),
            }
        );
    }

    #[test]
    fn should_default_the_port_and_leave_the_rest_unset() {
        let options = ServerOptions::from_args(&args(&[])).unwrap();

        assert_eq!(options, ServerOptions::initialize().with_port(DEFAULT_PORT));
    }

    #[test]
    fn should_reject_a_port_that_is_not_a_number() {
        assert!(ServerOptions::from_args(&args(&["--port", "later"])).is_err());
    }

    #[test]
    fn should_read_the_rdb_options_independently_of_each_other() {
        let only_dir = ServerOptions::from_args(&args(&["--dir", "/tmp"])).unwrap();

        assert_eq!(only_dir.dir, Some("/tmp".to_owned()));
        assert_eq!(only_dir.dbfilename, None);
    }

    #[test]
    fn should_look_an_option_up_by_name_whatever_its_case() {
        let options = ServerOptions::initialize().with_port(6379).with_rdb_file("/tmp/redis-files", "dump.rdb");

        assert_eq!(options.get("dir"), Some("/tmp/redis-files"));
        assert_eq!(options.get("DIR"), Some("/tmp/redis-files"));
        assert_eq!(options.get("dbfilename"), Some("dump.rdb"));
        assert_eq!(options.get("DbFileName"), Some("dump.rdb"));
    }

    #[test]
    fn should_know_nothing_of_an_option_that_was_never_set_or_never_existed() {
        let options = ServerOptions::initialize().with_port(6379);

        assert_eq!(options.get("dir"), None);
        assert_eq!(options.get("dbfilename"), None);
        assert_eq!(options.get("maxmemory"), None);
        // Not every field is a configuration parameter: the ones CONFIG GET
        // answers for are named above, and the port is not among them.
        assert_eq!(options.get("port"), None);
    }

    #[test]
    fn should_place_the_rdb_file_inside_the_configured_directory() {
        let options = ServerOptions::initialize().with_port(6379).with_rdb_file("/tmp/redis-files", "dump.rdb");

        assert_eq!(
            options.rdb_path(),
            Some(PathBuf::from("/tmp/redis-files/dump.rdb"))
        );
    }

    #[test]
    fn should_have_no_rdb_file_until_both_halves_of_its_path_are_known() {
        let neither = ServerOptions::initialize();
        let only_dir = ServerOptions {
            dir: Some("/tmp".to_owned()),
            ..ServerOptions::initialize()
        };
        let only_name = ServerOptions {
            dbfilename: Some("dump.rdb".to_owned()),
            ..ServerOptions::initialize()
        };

        assert_eq!(neither.rdb_path(), None);
        assert_eq!(only_dir.rdb_path(), None);
        assert_eq!(only_name.rdb_path(), None);
    }

    #[test]
    fn should_build_the_options_of_a_replica() {
        let options = ServerOptions::initialize().with_port(6380).replicating("localhost 6379");

        assert_eq!(options.port, 6380);
        assert_eq!(options.replica_of, Some("localhost 6379".to_owned()));
    }
}
