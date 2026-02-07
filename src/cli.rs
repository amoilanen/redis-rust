use std::num::ParseIntError;

fn get_option_value(option_name: &str, args: &[String]) -> Option<String> {
    let option_flag = format!("--{}", option_name);
    if let Some(option_position) = args.iter().position(|x| x == &option_flag) {
        args.get(option_position + 1).cloned()
    } else {
        None
    }
}

pub fn get_port(args: &[String]) -> Result<Option<usize>, anyhow::Error> {
    get_option_value("port", args)
        .map(|p|
            p.parse().map_err(|e: ParseIntError| e.into())
        )
        .transpose()
}

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
