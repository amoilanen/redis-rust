//! A minimal RESP2 client, just enough to drive the tests over raw TCP.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::time::Duration;

/// A parsed RESP2 value, returned by [`RespClient::read_resp`].
///
/// Separates wire-reading from output formatting so that
/// [`read_response`](RespClient::read_response) and
/// [`read_response_json`](RespClient::read_response_json) can share a single
/// I/O path and only differ in how they render the result.
enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(String),
    BulkString(Option<String>), // None = null ($-1)
    Array(Option<Vec<RespValue>>), // None = null (*-1)
}

/// A minimal Redis client that speaks just enough RESP2 to drive E2E tests.
///
/// Supports:
/// - Sending commands as RESP arrays of bulk strings
/// - Reading simple strings (`+OK\r\n`), bulk strings (`$N\r\n...\r\n`),
///   null bulk strings (`$-1\r\n`), and errors (`-ERR ...\r\n`).
pub struct RespClient {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
}

impl RespClient {
    /// Connect to a server, panicking on failure.
    pub fn connect(port: u16) -> Self {
        Self::try_connect(port).unwrap_or_else(|e| {
            panic!("failed to connect to 127.0.0.1:{}: {}", port, e);
        })
    }

    /// Try to connect; returns Err on failure.
    pub fn try_connect(port: u16) -> Result<Self, std::io::Error> {
        let stream = TcpStream::connect_timeout(
            &format!("127.0.0.1:{}", port).parse().unwrap(),
            Duration::from_secs(2),
        )?;
        stream.set_read_timeout(Some(Duration::from_secs(5)))?;
        stream.set_write_timeout(Some(Duration::from_secs(5)))?;
        let writer = stream.try_clone()?;
        let reader = BufReader::new(stream);
        Ok(Self { reader, writer })
    }

    /// Serialize `args` as a RESP array of bulk strings and write it out.
    ///
    /// Public so that tests can send a command the server does not reply to and
    /// then keep using the connection.
    pub fn write_command(&mut self, args: &[&str]) -> anyhow::Result<()> {
        let mut buf = format!("*{}\r\n", args.len());
        for arg in args {
            buf.push_str(&format!("${}\r\n{}\r\n", arg.len(), arg));
        }
        self.writer.write_all(buf.as_bytes())?;
        self.writer.flush()?;
        Ok(())
    }

    /// Send a command (as an array of bulk strings) and read the response.
    ///
    /// Returns the response as a `String`.  For null bulk strings, returns
    /// the special value `"(nil)"`.  Arrays are flattened to a comma-separated
    /// string; use [`send_command_json`](Self::send_command_json) when the
    /// nested structure of the reply matters.
    pub fn send_command(&mut self, args: &[&str]) -> anyhow::Result<String> {
        self.write_command(args)?;
        self.read_response()
    }

    /// Send a command and read the response as a JSON-encoded string.
    ///
    /// Unlike [`send_command`](Self::send_command), this preserves the nested
    /// structure of the reply: arrays become `[...]`, bulk/simple strings become
    /// quoted strings, integers are bare numbers, and nulls become `null`. This
    /// lets tests assert on the exact shape of nested replies such as `XRANGE`.
    pub fn send_command_json(&mut self, args: &[&str]) -> anyhow::Result<String> {
        self.write_command(args)?;
        self.read_response_json()
    }

    /// Read a single RESP response from the stream.
    ///
    /// Returns the response as a plain `String`.  Null bulk strings and null
    /// arrays become `"(nil)"`.  Arrays are flattened to a comma-separated
    /// string; use [`send_command_json`](Self::send_command_json) when the
    /// nested structure matters.
    pub fn read_response(&mut self) -> anyhow::Result<String> {
        resp_to_plain(self.read_resp()?)
    }

    /// Read a single RESP response and render it as a JSON-encoded string,
    /// preserving nested array structure.
    pub fn read_response_json(&mut self) -> anyhow::Result<String> {
        resp_to_json(self.read_resp()?)
    }

    /// Parse one RESP2 frame from the stream into a [`RespValue`].
    ///
    /// This is the single source of wire-reading logic shared by
    /// [`read_response`](Self::read_response) and
    /// [`read_response_json`](Self::read_response_json).
    fn read_resp(&mut self) -> anyhow::Result<RespValue> {
        let mut line = String::new();
        self.reader.read_line(&mut line)?;
        let line = line.trim_end_matches("\r\n").trim_end_matches('\n');

        if line.is_empty() {
            anyhow::bail!("empty response");
        }

        let prefix = &line[..1];
        let payload = &line[1..];

        match prefix {
            "+" => Ok(RespValue::SimpleString(payload.to_string())),
            "-" => Ok(RespValue::Error(payload.to_string())),
            ":" => Ok(RespValue::Integer(payload.to_string())),
            "$" => {
                let len: i64 = payload.parse()?;
                if len < 0 {
                    return Ok(RespValue::BulkString(None));
                }
                let len = len as usize;
                let mut data = vec![0u8; len + 2]; // +2 for trailing \r\n
                self.reader.read_exact(&mut data)?;
                data.truncate(len); // strip \r\n
                Ok(RespValue::BulkString(Some(String::from_utf8(data)?)))
            }
            "*" => {
                let count: i64 = payload.parse()?;
                if count < 0 {
                    return Ok(RespValue::Array(None));
                }
                let mut items = Vec::new();
                for _ in 0..count {
                    items.push(self.read_resp()?);
                }
                Ok(RespValue::Array(Some(items)))
            }
            _ => Err(anyhow::anyhow!("unknown RESP prefix: {}", prefix)),
        }
    }
}

/// Render a [`RespValue`] as a plain string (the `read_response` format).
///
/// - Simple/bulk strings → their content
/// - Integers → their decimal text
/// - Nulls → `"(nil)"`
/// - Arrays → comma-separated elements (recursive)
/// - Errors → `Err`
fn resp_to_plain(value: RespValue) -> anyhow::Result<String> {
    match value {
        RespValue::SimpleString(s) | RespValue::BulkString(Some(s)) => Ok(s),
        RespValue::Error(e) => Err(anyhow::anyhow!("{}", e)),
        RespValue::Integer(n) => Ok(n),
        RespValue::BulkString(None) | RespValue::Array(None) => Ok("(nil)".to_string()),
        RespValue::Array(Some(items)) => items
            .into_iter()
            .map(resp_to_plain)
            .collect::<anyhow::Result<Vec<_>>>()
            .map(|parts| parts.join(",")),
    }
}

/// Render a [`RespValue`] as a JSON-encoded string (the `read_response_json` format).
///
/// - Simple/bulk strings → JSON string (`"..."`)
/// - Integers → bare number
/// - Nulls → `null`
/// - Arrays → `[...]` with elements rendered recursively
/// - Errors → `Err`
fn resp_to_json(value: RespValue) -> anyhow::Result<String> {
    match value {
        RespValue::SimpleString(s) => Ok(json_string(&s)),
        RespValue::Error(e) => Err(anyhow::anyhow!("{}", e)),
        RespValue::Integer(n) => Ok(n),
        RespValue::BulkString(None) | RespValue::Array(None) => Ok("null".to_string()),
        RespValue::BulkString(Some(s)) => Ok(json_string(&s)),
        RespValue::Array(Some(items)) => items
            .into_iter()
            .map(resp_to_json)
            .collect::<anyhow::Result<Vec<_>>>()
            .map(|parts| format!("[{}]", parts.join(","))),
    }
}

/// Encode a string as a JSON string literal, escaping `"` and `\`.
fn json_string(value: &str) -> String {
    let escaped = value.replace('\\', "\\\\").replace('"', "\\\"");
    format!("\"{}\"", escaped)
}
