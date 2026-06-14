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

    pub fn add(&mut self, id: String, fields: Vec<(String, String)>) -> String {
        self.entries.push(StreamEntry { id: id.clone(), fields });
        id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stream_new_is_empty() {
        let stream = Stream::new();
        assert!(stream.entries.is_empty());
    }

    #[test]
    fn test_stream_add_appends_and_returns_id() {
        let mut stream = Stream::new();
        let returned = stream.add("0-1".to_string(), vec![("foo".to_string(), "bar".to_string())]);

        assert_eq!(returned, "0-1");
        assert_eq!(stream.entries.len(), 1);
        assert_eq!(stream.entries[0].id, "0-1");
        assert_eq!(
            stream.entries[0].fields,
            vec![("foo".to_string(), "bar".to_string())],
        );
    }

    #[test]
    fn test_stream_add_preserves_order() {
        let mut stream = Stream::new();
        stream.add("0-1".to_string(), vec![]);
        stream.add("0-2".to_string(), vec![]);

        let ids: Vec<&str> = stream.entries.iter().map(|e| e.id.as_str()).collect();
        assert_eq!(ids, vec!["0-1", "0-2"]);
    }
}
