use std::fmt;

use crate::error::RedisError;

/// A stream entry ID: `<milliseconds>-<sequence>`
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub struct StreamId {
    pub milliseconds: u64,
    pub sequence: u64,
}

impl StreamId {
    pub const ZERO: StreamId = StreamId { milliseconds: 0, sequence: 0 };

    pub fn parse(id: &str) -> Result<StreamId, RedisError> {
        let invalid = || RedisError {
            message: "ERR Invalid stream ID specified as stream command argument".to_string(),
        };

        let (ms, seq) = id.split_once('-').ok_or_else(invalid)?;
        Ok(StreamId {
            milliseconds: ms.parse().map_err(|_| invalid())?,
            sequence: seq.parse().map_err(|_| invalid())?,
        })
    }
}

impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}-{}", self.milliseconds, self.sequence)
    }
}

/// A single entry in a stream: a unique ID plus its field-value pairs.
#[derive(Debug, PartialEq, Clone)]
pub struct StreamEntry {
    pub id: String,
    pub fields: Vec<(String, String)>,
}

/// An ordered collection of stream entries stored at a key.
#[derive(Debug, PartialEq, Clone, Default)]
pub struct Stream {
    pub entries: Vec<StreamEntry>,
}

impl Stream {
    pub fn new() -> Stream {
        Stream { entries: Vec::new() }
    }

    pub fn add(&mut self, id: &str, fields: Vec<(String, String)>) -> Result<String, RedisError> {
        let new_id = StreamId::parse(id)?;
        self.validate_id(new_id)?;

        let stored_id = new_id.to_string();
        self.entries.push(StreamEntry { id: stored_id.clone(), fields });
        Ok(stored_id)
    }

    fn last_id(&self) -> Option<StreamId> {
        self.entries
            .last()
            .map(|entry| StreamId::parse(&entry.id).ok())
            .flatten()
    }

    fn validate_id(&self, new_id: StreamId) -> Result<(), RedisError> {
        if new_id <= StreamId::ZERO {
            return Err(RedisError {
                message: "ERR The ID specified in XADD must be greater than 0-0".to_string(),
            });
        }

        if let Some(last_id) = self.last_id()
            && new_id <= last_id
        {
            return Err(RedisError {
                message: "ERR The ID specified in XADD is equal or smaller than the target stream top item"
                    .to_string(),
            });
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;

    #[test]
    fn test_stream_new_is_empty() {
        let stream = Stream::new();
        assert!(stream.entries.is_empty());
    }

    #[test]
    fn test_stream_add_appends_and_returns_id() -> Result<()> {
        let mut stream = Stream::new();
        let returned = stream.add("0-1", vec![("foo".to_string(), "bar".to_string())])?;

        assert_eq!(returned, "0-1");
        assert_eq!(stream.entries.len(), 1);
        assert_eq!(stream.entries[0].id, "0-1");
        assert_eq!(
            stream.entries[0].fields,
            vec![("foo".to_string(), "bar".to_string())],
        );
        Ok(())
    }

    #[test]
    fn test_stream_add_preserves_order() -> Result<()> {
        let mut stream = Stream::new();
        stream.add("0-1", vec![])?;
        stream.add("0-2", vec![])?;

        let ids: Vec<&str> = stream.entries.iter().map(|e| e.id.as_str()).collect();
        assert_eq!(ids, vec!["0-1", "0-2"]);
        Ok(())
    }

    #[test]
    fn test_stream_id_parse_valid() -> Result<()> {
        assert_eq!(
            StreamId::parse("1526919030474-0")?,
            StreamId { milliseconds: 1526919030474, sequence: 0 },
        );
        Ok(())
    }

    #[test]
    fn test_stream_id_parse_invalid() {
        assert!(StreamId::parse("1").is_err());
        assert!(StreamId::parse("1-").is_err());
        assert!(StreamId::parse("-1").is_err());
        assert!(StreamId::parse("a-b").is_err());
        assert!(StreamId::parse("1-2-3").is_err());
    }

    #[test]
    fn test_stream_id_ordering() {
        let a = StreamId { milliseconds: 1, sequence: 1 };
        let b = StreamId { milliseconds: 1, sequence: 2 };
        let c = StreamId { milliseconds: 2, sequence: 0 };
        assert!(a < b);
        assert!(b < c);
        assert!(a < c);
    }

    #[test]
    fn test_add_rejects_zero_id() {
        let mut stream = Stream::new();
        let err = stream.add("0-0", vec![]).unwrap_err();
        assert_eq!(err.message, "ERR The ID specified in XADD must be greater than 0-0");
        assert!(stream.entries.is_empty());
    }

    #[test]
    fn test_add_rejects_equal_id() -> Result<()> {
        let mut stream = Stream::new();
        stream.add("1-1", vec![])?;
        let err = stream.add("1-1", vec![]).unwrap_err();
        assert_eq!(
            err.message,
            "ERR The ID specified in XADD is equal or smaller than the target stream top item"
        );
        assert_eq!(stream.entries.len(), 1);
        Ok(())
    }

    #[test]
    fn test_add_rejects_smaller_time() -> Result<()> {
        let mut stream = Stream::new();
        stream.add("1-1", vec![])?;
        let err = stream.add("0-3", vec![]).unwrap_err();
        assert_eq!(
            err.message,
            "ERR The ID specified in XADD is equal or smaller than the target stream top item"
        );
        Ok(())
    }

    #[test]
    fn test_add_rejects_smaller_sequence() -> Result<()> {
        let mut stream = Stream::new();
        stream.add("5-5", vec![])?;
        assert!(stream.add("5-4", vec![]).is_err());
        Ok(())
    }

    #[test]
    fn test_add_accepts_strictly_greater_ids() -> Result<()> {
        let mut stream = Stream::new();
        stream.add("0-1", vec![])?;
        stream.add("1-1", vec![])?;
        stream.add("1-2", vec![])?;
        stream.add("2-0", vec![])?;
        assert_eq!(stream.entries.len(), 4);
        Ok(())
    }
}
