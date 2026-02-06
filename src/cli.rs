/// CLI argument parsing for the Redis server.
///
/// This module handles parsing command-line arguments for the Redis server,
/// including port configuration and replication settings.

/// Extracts an option value from command-line arguments.
///
/// Looks for `--{option_name}` and returns the following argument as the value.
///
/// # Arguments
/// * `option_name` - The name of the option (without `--` prefix)
/// * `args` - The command-line arguments
///
/// # Returns
/// The option value if present, None otherwise
///
/// # Examples
/// ```
/// let args = vec!["program".to_string(), "--port".to_string(), "6380".to_string()];
/// let port = redis_starter_rust::cli::get_port(&args).unwrap();
/// assert_eq!(port, Some(6380));
/// ```
fn get_option_value(option_name: &str, args: &[String]) -> Option<String> {
    let option_flag = format!("--{}", option_name);
    if let Some(option_position) = args.iter().position(|x| x == &option_flag) {
        args.get(option_position + 1).cloned()
    } else {
        None
    }
}

/// Parses the port from command-line arguments.
///
/// # Arguments
/// * `args` - The command-line arguments
///
/// # Returns
/// * `Ok(Some(port))` - If port argument is present and valid
/// * `Ok(None)` - If no port argument is provided
/// * `Err(e)` - If port argument is present but invalid
pub fn get_port(args: &[String]) -> Result<Option<usize>, anyhow::Error> {
    match get_option_value("port", args) {
        Some(p) => p.parse().map(Some).map_err(Into::into),
        None => Ok(None),
    }
}

/// Parses the replica-of address from command-line arguments.
///
/// # Arguments
/// * `args` - The command-line arguments
///
/// # Returns
/// The replica-of address if present, None otherwise
///
/// # Example
/// ```
/// let args = vec!["program".to_string(), "--replicaof".to_string(), "localhost 6379".to_string()];
/// let replica_of = redis_starter_rust::cli::get_replica_of(&args);
/// assert_eq!(replica_of, Some("localhost 6379".to_string()));
/// ```
pub fn get_replica_of(args: &[String]) -> Option<String> {
    get_option_value("replicaof", args)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_option_value_found() {
        let args = vec!["prog".to_string(), "--port".to_string(), "6380".to_string()];
        assert_eq!(get_option_value("port", &args), Some("6380".to_string()));
    }

    #[test]
    fn test_get_option_value_not_found() {
        let args = vec!["prog".to_string(), "--other".to_string(), "value".to_string()];
        assert_eq!(get_option_value("port", &args), None);
    }

    #[test]
    fn test_get_option_value_missing_value() {
        let args = vec!["prog".to_string(), "--port".to_string()];
        assert_eq!(get_option_value("port", &args), None);
    }

    #[test]
    fn test_get_port_valid() {
        let args = vec!["prog".to_string(), "--port".to_string(), "6380".to_string()];
        assert_eq!(get_port(&args).unwrap(), Some(6380));
    }

    #[test]
    fn test_get_port_not_provided() {
        let args = vec!["prog".to_string()];
        assert_eq!(get_port(&args).unwrap(), None);
    }

    #[test]
    fn test_get_port_invalid() {
        let args = vec!["prog".to_string(), "--port".to_string(), "invalid".to_string()];
        assert!(get_port(&args).is_err());
    }

    #[test]
    fn test_get_replica_of_found() {
        let args = vec![
            "prog".to_string(),
            "--replicaof".to_string(),
            "localhost 6379".to_string(),
        ];
        assert_eq!(get_replica_of(&args), Some("localhost 6379".to_string()));
    }

    #[test]
    fn test_get_replica_of_not_found() {
        let args = vec!["prog".to_string()];
        assert_eq!(get_replica_of(&args), None);
    }
}
