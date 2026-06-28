/// Stream command family.
///
/// Groups every Redis command that operates on the stream data type (XADD,
/// XRANGE, XREAD, ...) together with the helpers they share.
///
/// Helpers are private to this module - by Rust's default visibility rules they
/// remain accessible from descendant modules (xrange, xread, and their
/// `#[cfg(test)] mod tests` children) but invisible to anything outside the
/// stream family.

use crate::protocol;
use crate::protocol::DataType;
use crate::stream::StreamEntry;

pub mod xadd;
pub mod xrange;
pub mod xread;

pub use xadd::XAdd;
pub use xrange::XRange;
pub use xread::XRead;

/// Encodes stream entries as a RESP array of `[id, [field, value, ...]]` pairs.
///
/// This is the shape shared by XRANGE's top-level reply and the per-stream
/// payload nested inside XREAD's reply. Each entry becomes a two-element array
/// of the entry ID (bulk string) and an array of its field/value pairs (bulk
/// strings) in insertion order.
fn encode_entries(entries: &[StreamEntry]) -> DataType {
    let encoded = entries.iter().map(encode_entry).collect();
    protocol::array(encoded)
}

fn encode_entry(entry: &StreamEntry) -> DataType {
    let mut fields: Vec<DataType> = Vec::with_capacity(entry.fields.len() * 2);
    for (field, value) in &entry.fields {
        fields.push(protocol::bulk_string(field));
        fields.push(protocol::bulk_string(value));
    }
    protocol::array(vec![
        protocol::bulk_string(&entry.id.to_string()),
        protocol::array(fields),
    ])
}

#[cfg(test)]
fn xadd(parts: &[&str]) -> XAdd {
    let elements = parts.iter().map(|p| protocol::bulk_string(p)).collect();
    XAdd { message: protocol::array(elements) }
}