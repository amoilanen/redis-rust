/// ZADD <key> <score> <member> [<score> <member> ...]
///
/// Adds members with the given scores to a sorted set, creating it if needed.
/// A member already in the set gets its score updated instead. Replies with
/// the number of members newly added, as a RESP integer.
///
/// The NX/XX/GT/LT/CH/INCR options are not supported yet.

use std::sync::Mutex;

use anyhow::anyhow;
use log::*;

use crate::commands::{RedisCommand, parse_score};
use crate::error::RedisError;
use crate::protocol;
use crate::protocol::DataType;
use crate::sorted_set::ScoredMember;
use crate::storage::Storage;

/// ZADD command implementation.
pub struct ZAdd {
    pub message: DataType,
}

impl RedisCommand for ZAdd {
    fn execute(&self, storage: &Mutex<Storage>) -> Result<Vec<DataType>, anyhow::Error> {
        let instructions: Vec<String> = self.message.as_string_vec()?;

        if instructions.len() < 4 {
            return Err(
                RedisError::new("ERR wrong number of arguments for 'zadd' command").into(),
            );
        }
        if (instructions.len() - 2) % 2 != 0 {
            return Err(RedisError::new("ERR syntax error").into());
        }

        let key = &instructions[1];
        // Every score is checked before anything is stored, so a bad one
        // leaves the set untouched.
        let members: Vec<ScoredMember> = instructions[2..]
            .chunks(2)
            .map(|pair| Ok(ScoredMember::new(parse_score(&pair[0])?, pair[1].clone())))
            .collect::<Result<_, anyhow::Error>>()?;

        debug!("ZADD {} {:?}", key, members);

        let added = storage
            .lock()
            .map_err(|e| anyhow!("Failed to lock storage: {}", e))?
            .zadd(key, &members)?;

        Ok(vec![protocol::integer(added as i64)])
    }

    fn is_propagated_to_replicas(&self) -> bool {
        true
    }

    fn should_always_reply(&self) -> bool {
        false
    }

    fn serialize(&self) -> Vec<u8> {
        self.message.serialize()
    }

    fn name(&self) -> &str {
        "ZADD"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::{client_error_message, command_message, create_test_storage, set};

    fn zadd(parts: &[&str]) -> ZAdd {
        ZAdd { message: command_message(parts) }
    }

    fn score(storage: &Mutex<Storage>, key: &str, member: &str) -> Option<f64> {
        storage
            .lock()
            .unwrap()
            .get_sorted_set(key)
            .and_then(|set| set.score(member))
    }

    fn sorted_set_exists(storage: &Mutex<Storage>, key: &str) -> bool {
        storage.lock().unwrap().get_sorted_set(key).is_some()
    }

    #[test]
    fn test_zadd_creates_sorted_set_and_returns_one() -> anyhow::Result<()> {
        let storage = create_test_storage();
        let cmd = zadd(&["ZADD", "zset_key", "10.0", "zset_member"]);

        assert_eq!(cmd.execute(&storage)?, vec![protocol::integer(1)]);
        assert_eq!(score(&storage, "zset_key", "zset_member"), Some(10.0));
        Ok(())
    }

    #[test]
    fn test_zadd_is_propagated_to_replicas() {
        assert!(zadd(&["ZADD", "z", "1", "m"]).is_propagated_to_replicas());
    }

    #[test]
    fn test_zadd_existing_member_updates_score_and_returns_zero() -> anyhow::Result<()> {
        let storage = create_test_storage();
        zadd(&["ZADD", "z", "1", "m"]).execute(&storage)?;

        assert_eq!(zadd(&["ZADD", "z", "2.5", "m"]).execute(&storage)?, vec![protocol::integer(0)]);
        assert_eq!(score(&storage, "z", "m"), Some(2.5));
        Ok(())
    }

    #[test]
    fn test_zadd_multiple_pairs_counts_only_new_members() -> anyhow::Result<()> {
        let storage = create_test_storage();
        zadd(&["ZADD", "z", "1", "existing"]).execute(&storage)?;

        // "existing" is already there and "new" is repeated, so only one
        // member is actually added; every pair still sets its score, the
        // last one winning for the repeated member.
        let result = zadd(&["ZADD", "z", "2", "existing", "3", "new", "4", "new"]).execute(&storage)?;

        assert_eq!(result, vec![protocol::integer(1)]);
        assert_eq!(score(&storage, "z", "existing"), Some(2.0));
        assert_eq!(score(&storage, "z", "new"), Some(4.0));
        Ok(())
    }

    #[test]
    fn test_zadd_accepts_infinite_scores() -> anyhow::Result<()> {
        let storage = create_test_storage();
        let result = zadd(&["ZADD", "z", "-inf", "bottom", "+inf", "top"]).execute(&storage)?;

        assert_eq!(result, vec![protocol::integer(2)]);
        assert_eq!(score(&storage, "z", "bottom"), Some(f64::NEG_INFINITY));
        assert_eq!(score(&storage, "z", "top"), Some(f64::INFINITY));
        Ok(())
    }

    #[test]
    fn test_zadd_rejects_invalid_score_without_storing() {
        let storage = create_test_storage();
        for bad in ["abc", "nan"] {
            // The valid pair comes first, so it must not be stored either.
            let error = zadd(&["ZADD", "z", "1", "a", bad, "b"]).execute(&storage).unwrap_err();
            assert_eq!(client_error_message(error), "ERR value is not a valid float");
            assert!(!sorted_set_exists(&storage, "z"));
        }
    }

    #[test]
    fn test_zadd_too_few_arguments() {
        let storage = create_test_storage();
        for parts in [&["ZADD", "z", "1"][..], &["ZADD", "z"], &["ZADD"]] {
            let error = zadd(parts).execute(&storage).unwrap_err();
            assert_eq!(client_error_message(error), "ERR wrong number of arguments for 'zadd' command");
        }
    }

    #[test]
    fn test_zadd_score_without_member_is_syntax_error() {
        let storage = create_test_storage();

        let error = zadd(&["ZADD", "z", "1", "a", "2"]).execute(&storage).unwrap_err();
        assert_eq!(client_error_message(error), "ERR syntax error");
        assert!(!sorted_set_exists(&storage, "z"));
    }

    #[test]
    fn test_zadd_on_string_key_is_wrong_type() -> anyhow::Result<()> {
        let storage = create_test_storage();
        set(&["SET", "k", "v"]).execute(&storage)?;

        let error = zadd(&["ZADD", "k", "1", "m"]).execute(&storage).unwrap_err();
        assert_eq!(
            client_error_message(error),
            "WRONGTYPE Operation against a key holding the wrong kind of value"
        );
        Ok(())
    }
}
