use std::{fmt, time::UNIX_EPOCH};
use anyhow::Result;

use crate::error::RedisError;
use crate::clock::{Clock, SystemClock};

/// A stream entry ID: `<milliseconds>-<sequence>`
#[derive(Debug, Default, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub struct StreamId {
    pub milliseconds: u128,
    pub sequence: u64,
}

impl StreamId {
    pub const ZERO: StreamId = StreamId { milliseconds: 0, sequence: 0 };

    pub fn new(milliseconds: u128, sequence: u64) -> StreamId {
        StreamId {
            milliseconds,
            sequence
        }
    }

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
        self.add_with_clock(id, fields, &SystemClock {})
    }

    pub fn add_with_clock<C: Clock>(&mut self, id: &str, fields: Vec<(String, String)>, c: &C) -> Result<String, RedisError> {
        let new_id = if id == "*" {
            self.new_id_fully_generated(c)?
        } else if id.ends_with("-*") {
            let invalid = || RedisError {
                message: "ERR Invalid stream ID specified as stream command argument".to_string(),
            };
            let time_part: u128 = id[..(id.len() - 2)].parse().map_err(|_| invalid())?;
            self.new_id_with_generated_sequence_number(time_part)
        } else {
            let parsed_id = StreamId::parse(id)?;
            self.validate_id(parsed_id)?;
            parsed_id
        };


        let stored_id = new_id.to_string();
        self.entries.push(StreamEntry { id: stored_id.clone(), fields });
        Ok(stored_id)
    }

    fn new_id_fully_generated<C: Clock>(&self, c: &C) -> Result<StreamId, RedisError> {
        let current_timestamp = c.now().duration_since(UNIX_EPOCH).map_err(|e| RedisError::new(&format!("Error getting current time {}", e)) )?;
        Ok(StreamId::new(current_timestamp.as_millis(), 0))
    }

    fn new_id_with_generated_sequence_number(&self, time_part: u128) -> StreamId {
        match self.last_id_filtered_by_time_part(Some(time_part)) {
            Some(last) => StreamId::new(time_part, last.sequence + 1),
            None if time_part == 0 => StreamId::new(time_part, 1),
            None => StreamId::new(time_part, 0)
        }
    }

    fn last_id(&self) -> Option<StreamId> {
        self.last_id_filtered_by_time_part(None)
    }

    fn last_id_filtered_by_time_part(&self, time_part: Option<u128>) -> Option<StreamId> {
        self.entries
            .iter()
            .flat_map(|entry| StreamId::parse(&entry.id).ok())
            .filter(|id| time_part.is_none() || time_part == Some(id.milliseconds))
            .last()
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
    use std::time::{ Duration, SystemTime };

    struct FixedClock(SystemTime);
    impl Clock for FixedClock {
        fn now(&self) -> SystemTime {
            self.0
        }
    }

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
    fn test_stream_add_stream_is_empty_time_part_is_not_0() -> Result<()> {
        let mut stream = Stream::new();
        let returned = stream.add("1526919030473-*", vec![("foo".to_string(), "bar".to_string())])?;

        assert_eq!(returned, "1526919030473-0");
        assert_eq!(stream.entries.len(), 1);
        assert_eq!(stream.entries[0].id, "1526919030473-0");
        assert_eq!(
            stream.entries[0].fields,
            vec![("foo".to_string(), "bar".to_string())],
        );
        Ok(())
    }

    #[test]
    fn test_stream_add_stream_is_empty_time_part_is_0() -> Result<()> {
        let mut stream = Stream::new();
        let returned = stream.add("0-*", vec![("foo".to_string(), "bar".to_string())])?;

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
    fn test_stream_add_non_empty_stream_has_same_time_part() -> Result<()> {
        let mut stream = Stream::new();
        stream.add("1526919030473-2", Vec::new())?;

        let returned = stream.add("1526919030473-*", vec![("foo".to_string(), "bar".to_string())])?;

        assert_eq!(returned, "1526919030473-3");
        assert_eq!(stream.entries.len(), 2);
        assert_eq!(stream.entries[1].id, "1526919030473-3");
        assert_eq!(
            stream.entries[1].fields,
            vec![("foo".to_string(), "bar".to_string())],
        );
        Ok(())
    }

    #[test]
    fn test_stream_add_non_empty_stream_has_different_time_part() -> Result<()> {
        let mut stream = Stream::new();
        stream.add("1526919030473-2", Vec::new())?;
        let returned = stream.add("1526919035000-*", vec![("foo".to_string(), "bar".to_string())])?;

        assert_eq!(returned, "1526919035000-0");
        assert_eq!(stream.entries.len(), 2);
        assert_eq!(stream.entries[1].id, "1526919035000-0");
        assert_eq!(
            stream.entries[1].fields,
            vec![("foo".to_string(), "bar".to_string())],
        );
        Ok(())
    }

    #[test]
    fn test_stream_add_with_fully_generated_id_non_empty_stream() -> Result<()> {
        let mut stream = Stream::new();
        let timestamp = UNIX_EPOCH + Duration::from_millis(123456789);
        let clock = FixedClock(timestamp);
        stream.add("0-1", Vec::new())?;
        let returned = stream.add_with_clock("*", vec![("foo".to_string(), "bar".to_string())], &clock)?;

        assert_eq!(returned, "123456789-0");
        assert_eq!(stream.entries.len(), 2);
        assert_eq!(stream.entries[1].id, "123456789-0");
        assert_eq!(
            stream.entries[1].fields,
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
