//! PUBLISH command - posts a message to a channel.
//!
//! Syntax: PUBLISH <channel> <message>
//! Returns: the number of clients subscribed to the channel, as an integer:
//!
//! ```text
//! :2\r\n
//! ```
//!
//! Every connection subscribed to the channel - server-wide, not just the
//! publishing one - is sent the message as an array of three bulk strings:
//! `message`, the channel and the message itself:
//!
//! ```text
//! *3\r\n$7\r\nmessage\r\n$3\r\nfoo\r\n$5\r\nhello\r\n
//! ```
//!
//! Each connection receives it once, however often it subscribed, and the
//! reply counts the connections it was delivered to.

use std::sync::{Arc, Mutex};

use log::*;

use super::ChannelRegistry;
use crate::commands::RedisCommand;
use crate::connection_registry::ConnectionRegistry;
use crate::error::RedisError;
use crate::protocol;
use crate::protocol::DataType;
use crate::storage::Storage;

/// PUBLISH command implementation.
pub struct Publish {
    pub message: DataType,
    /// Who is subscribed to which channel, server-wide.
    pub channel_registry: Arc<ChannelRegistry>,
    /// How to reach each of those subscribers.
    pub connection_registry: Arc<ConnectionRegistry>,
}

impl Publish {
    /// Sends `message` to every connection subscribed to `channel`, as the
    /// array `["message", <channel>, <message>]`, and answers with how many of
    /// them it reached.
    ///
    /// The subscribers are looked up first and their writers after, each
    /// registry locked on its own and neither while writing: a subscriber
    /// that is slow to read must not hold up every other connection.
    ///
    /// A subscriber that cannot be written to - or has gone between the two
    /// lookups - has gone away, which is no fault of the publisher: it is
    /// passed over, and not counted.
    fn deliver(&self, channel: &str, message: &str) -> Result<usize, anyhow::Error> {
        let subscribers = self.channel_registry.subscribers(channel)?;
        let writers = self.connection_registry.writers(&subscribers)?;

        let delivery = [protocol::array(vec![
            protocol::bulk_string("message"),
            protocol::bulk_string(channel),
            protocol::bulk_string(message),
        ])];

        let mut delivered = 0;
        for writer in writers {
            match writer.send(&delivery) {
                Ok(()) => delivered += 1,
                Err(error) => warn!("Could not deliver a message on {}: {:#}", channel, error),
            }
        }
        Ok(delivered)
    }
}

impl RedisCommand for Publish {
    fn execute(&self, _: &Mutex<Storage>) -> Result<Vec<DataType>, anyhow::Error> {
        let instructions: Vec<String> = self.message.as_string_vec()?;

        let [_, channel, message] = instructions.as_slice() else {
            return Err(
                RedisError::new("ERR wrong number of arguments for 'publish' command").into(),
            );
        };

        let delivered = self.deliver(channel, message)?;

        debug!("PUBLISH {}: delivered to {} subscriber(s)", channel, delivered);

        Ok(vec![protocol::integer(delivered as i64)])
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
        "PUBLISH"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::net::{Shutdown, TcpListener, TcpStream};

    use crate::commands::{client_error_message, command_message};
    use crate::config::ServerOptions;
    use crate::connection::{ClientWriter, ConnectionMode, ConnectionState};
    use crate::server_state::ServerState;

    /// What a client was sent, kept for the test to read back - the one
    /// stand-in here, for the socket a real connection writes to.
    #[derive(Clone, Default)]
    struct Inbox(Arc<Mutex<Vec<u8>>>);

    impl Inbox {
        /// Everything received so far, as the RESP values it was sent as.
        fn received(&self) -> anyhow::Result<Vec<DataType>> {
            let bytes = self.0.lock().unwrap();
            if bytes.is_empty() {
                return Ok(vec![]);
            }
            protocol::read_messages_from_bytes(&bytes)
        }
    }

    impl Write for Inbox {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    fn server() -> ServerState {
        ServerState::new(ServerOptions::initialize().with_port(6379))
    }

    /// A client connection to `server`, writing to `writer` and subscribed to
    /// `channels`. Dropping it ends the connection.
    fn connect(
        server: &ServerState,
        writer: impl Write + Send + 'static,
        channels: &[&str],
    ) -> anyhow::Result<ConnectionState> {
        let connection = ConnectionState::new(
            ConnectionMode::ConnectedClient,
            Arc::new(ClientWriter::new(writer)),
            server,
        )?;
        for channel in channels {
            connection.channel_subscriptions.subscribe(channel)?;
        }
        Ok(connection)
    }

    /// A client connection to `server`, subscribed to `channels`, together
    /// with the inbox what it is sent lands in.
    fn subscriber(
        server: &ServerState,
        channels: &[&str],
    ) -> anyhow::Result<(ConnectionState, Inbox)> {
        let inbox = Inbox::default();
        Ok((connect(server, inbox.clone(), channels)?, inbox))
    }

    /// The server's end of a real TCP connection that has been shut down for
    /// writing, so every write to it fails.
    fn shut_down_stream() -> anyhow::Result<TcpStream> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let _client_end = TcpStream::connect(listener.local_addr()?)?;
        let (server_end, _) = listener.accept()?;
        server_end.shutdown(Shutdown::Write)?;
        Ok(server_end)
    }

    fn publish_command(parts: &[&str], server: &ServerState) -> Publish {
        Publish {
            message: command_message(parts),
            channel_registry: Arc::clone(&server.channel_registry),
            connection_registry: Arc::clone(&server.connection_registry),
        }
    }

    /// What PUBLISH `message` on `channel` replies with.
    fn publish(server: &ServerState, channel: &str, message: &str) -> anyhow::Result<Vec<DataType>> {
        publish_command(&["PUBLISH", channel, message], server).execute(server.storage())
    }

    /// What a subscriber of `channel` is sent when `message` is published:
    /// `message`, the channel and the message, as bulk strings.
    fn message(channel: &str, message: &str) -> DataType {
        protocol::array(vec![
            protocol::bulk_string("message"),
            protocol::bulk_string(channel),
            protocol::bulk_string(message),
        ])
    }

    #[test]
    fn should_deliver_to_every_subscriber_of_the_channel_and_count_them() -> anyhow::Result<()> {
        let server = server();
        let (_foo_one, foo_one) = subscriber(&server, &["foo"])?;
        let (_foo_two, foo_two) = subscriber(&server, &["foo"])?;
        let (_bar, bar) = subscriber(&server, &["bar"])?;

        assert_eq!(publish(&server, "foo", "hello")?, vec![protocol::integer(2)]);

        assert_eq!(foo_one.received()?, vec![message("foo", "hello")]);
        assert_eq!(foo_two.received()?, vec![message("foo", "hello")]);
        assert_eq!(bar.received()?, vec![]);
        Ok(())
    }

    #[test]
    fn should_reach_nobody_on_a_channel_nobody_is_subscribed_to() -> anyhow::Result<()> {
        let server = server();
        let (_foo, foo) = subscriber(&server, &["foo"])?;

        assert_eq!(publish(&server, "baz", "hello")?, vec![protocol::integer(0)]);

        assert_eq!(foo.received()?, vec![]);
        Ok(())
    }

    #[test]
    fn should_reach_a_client_that_subscribed_twice_once() -> anyhow::Result<()> {
        let server = server();
        let (_foo, foo) = subscriber(&server, &["foo", "foo"])?;

        assert_eq!(publish(&server, "foo", "hello")?, vec![protocol::integer(1)]);

        assert_eq!(foo.received()?, vec![message("foo", "hello")]);
        Ok(())
    }

    #[test]
    fn should_reach_a_client_on_each_of_its_channels_in_publishing_order() -> anyhow::Result<()> {
        let server = server();
        let (_both, both) = subscriber(&server, &["foo", "bar"])?;

        assert_eq!(publish(&server, "foo", "first")?, vec![protocol::integer(1)]);
        assert_eq!(publish(&server, "bar", "second")?, vec![protocol::integer(1)]);

        assert_eq!(
            both.received()?,
            vec![message("foo", "first"), message("bar", "second")]
        );
        Ok(())
    }

    #[test]
    fn should_stop_reaching_a_client_once_its_connection_is_gone() -> anyhow::Result<()> {
        let server = server();
        let (_staying, staying) = subscriber(&server, &["foo"])?;
        let (leaving_connection, leaving) = subscriber(&server, &["foo", "bar"])?;

        drop(leaving_connection);

        assert_eq!(publish(&server, "foo", "hello")?, vec![protocol::integer(1)]);
        assert_eq!(publish(&server, "bar", "hello")?, vec![protocol::integer(0)]);

        assert_eq!(staying.received()?, vec![message("foo", "hello")]);
        assert_eq!(leaving.received()?, vec![]);
        Ok(())
    }

    #[test]
    fn should_not_count_a_subscriber_that_could_not_be_written_to() -> anyhow::Result<()> {
        let server = server();
        let (_reachable, reachable) = subscriber(&server, &["foo"])?;
        let _unreachable = connect(&server, shut_down_stream()?, &["foo"])?;

        assert_eq!(publish(&server, "foo", "hello")?, vec![protocol::integer(1)]);

        assert_eq!(reachable.received()?, vec![message("foo", "hello")]);
        Ok(())
    }

    #[test]
    fn should_not_reach_subscribers_of_another_server() -> anyhow::Result<()> {
        let elsewhere = server();
        let (_foo, foo) = subscriber(&elsewhere, &["foo"])?;
        let server = server();

        assert_eq!(publish(&server, "foo", "hello")?, vec![protocol::integer(0)]);

        assert_eq!(foo.received()?, vec![]);
        Ok(())
    }

    #[test]
    fn should_reject_a_call_without_exactly_a_channel_and_a_message() {
        let server = server();

        for parts in [
            &["PUBLISH"][..],
            &["PUBLISH", "foo"][..],
            &["PUBLISH", "foo", "msg", "extra"][..],
        ] {
            let error = publish_command(parts, &server)
                .execute(server.storage())
                .unwrap_err();

            assert_eq!(
                client_error_message(error),
                "ERR wrong number of arguments for 'publish' command",
                "{:?}",
                parts
            );
        }
    }

    #[test]
    fn should_not_propagate_a_publish_to_the_replicas() {
        let command = publish_command(&["PUBLISH", "foo", "msg"], &server());

        assert!(!command.is_propagated_to_replicas());
        assert_eq!(command.name(), "PUBLISH");
    }
}
