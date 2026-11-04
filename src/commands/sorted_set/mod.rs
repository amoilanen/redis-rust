/// Sorted set command family.
///
/// Groups every Redis command that operates on the sorted set data type
/// (ZADD, ZRANK, ...) together with the helpers they share.

pub mod zadd;
pub mod zrank;

pub use zadd::ZAdd;
pub use zrank::ZRank;

/// Build a `ZADD` command from its parts, e.g.
/// `zadd(&["ZADD", "z", "1", "m"])`.
///
/// Private, so only this family and its tests see it; seeding a sorted set
/// is a fixture for every other sorted set command's tests.
#[cfg(test)]
fn zadd(parts: &[&str]) -> ZAdd {
    ZAdd { message: crate::commands::command_message(parts) }
}
