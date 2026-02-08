/// End-to-end integration tests for Redis server.
///
/// These tests verify the full Redis server functionality including:
/// - Command execution
/// - Data persistence
/// - Expiration handling
/// - Protocol compliance
/// - Complex real-world scenarios

use codecrafters_redis::commands::*;
use codecrafters_redis::protocol;
use codecrafters_redis::storage::{Storage, StoredValue};
use codecrafters_redis::server_state::ServerState;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use std::error::Error;

fn create_test_storage() -> Arc<Mutex<Storage>> {
    let data: HashMap<String, StoredValue> = HashMap::new();
    Arc::new(Mutex::new(Storage::new(data)))
}

// ============= PING TESTS =============

#[test]
fn e2e_ping_works() -> Result<(), Box<dyn Error>> {
    let message = protocol::array(vec![protocol::bulk_string("PING")]);
    let cmd = Ping { message: &message };

    let storage = create_test_storage();
    let result = cmd.execute(&storage)?;

    assert_eq!(result[0].as_string()?, "PONG");
    Ok(())
}

// ============= ECHO TESTS =============

#[test]
fn e2e_echo_returns_argument() -> Result<(), Box<dyn Error>> {
    let echo_msg = protocol::bulk_string("Hello Redis!");
    let message = protocol::array(vec![
        protocol::bulk_string("ECHO"),
        echo_msg.clone(),
    ]);
    let elements: Vec<protocol::DataType> = message.as_array()?
        .iter()
        .map(|s| protocol::bulk_string(s))
        .collect();

    let cmd = Echo {
        message: &message,
        argument: Some(&elements[1]),
    };

    let storage = create_test_storage();
    let result = cmd.execute(&storage)?;

    assert_eq!(result[0].as_string()?, "Hello Redis!");
    Ok(())
}

// ============= SET/GET TESTS =============

#[test]
fn e2e_set_get_basic() -> Result<(), Box<dyn Error>> {
    let storage = create_test_storage();

    // Set a value
    let set_msg = protocol::array(vec![
        protocol::bulk_string("SET"),
        protocol::bulk_string("username"),
        protocol::bulk_string("alice"),
    ]);
    let set_cmd = Set { message: &set_msg };
    let set_result = set_cmd.execute(&storage)?;
    assert_eq!(set_result[0].as_string()?, "OK");

    // Get the value
    let get_msg = protocol::array(vec![
        protocol::bulk_string("GET"),
        protocol::bulk_string("username"),
    ]);
    let get_cmd = Get { message: &get_msg };
    let get_result = get_cmd.execute(&storage)?;
    assert_eq!(get_result[0].as_string()?, "alice");
    Ok(())
}

#[test]
fn e2e_multiple_keys() -> Result<(), Box<dyn Error>> {
    let storage = create_test_storage();

    // Set multiple values
    let keys_values = vec![
        ("user:1:name", "Alice"),
        ("user:1:email", "alice@example.com"),
        ("user:2:name", "Bob"),
        ("user:2:email", "bob@example.com"),
    ];

    for (key, value) in &keys_values {
        let msg = protocol::array(vec![
            protocol::bulk_string("SET"),
            protocol::bulk_string(key),
            protocol::bulk_string(value),
        ]);
        let cmd = Set { message: &msg };
        cmd.execute(&storage)?;
    }

    // Verify all values
    for (key, expected_value) in &keys_values {
        let msg = protocol::array(vec![
            protocol::bulk_string("GET"),
            protocol::bulk_string(key),
        ]);
        let cmd = Get { message: &msg };
        let result = cmd.execute(&storage)?;
        assert_eq!(result[0].as_string()?, *expected_value);
    }
    Ok(())
}

#[test]
fn e2e_get_nonexistent_key() -> Result<(), Box<dyn Error>> {
    let storage = create_test_storage();

    let msg = protocol::array(vec![
        protocol::bulk_string("GET"),
        protocol::bulk_string("does_not_exist"),
    ]);
    let cmd = Get { message: &msg };
    let result = cmd.execute(&storage)?;

    // Should return empty bulk string
    assert_eq!(result[0].as_string()?, "");
    Ok(())
}

#[test]
fn e2e_overwrite_key() -> Result<(), Box<dyn Error>> {
    let storage = create_test_storage();

    // Set initial value
    let msg1 = protocol::array(vec![
        protocol::bulk_string("SET"),
        protocol::bulk_string("counter"),
        protocol::bulk_string("10"),
    ]);
    Set { message: &msg1 }.execute(&storage)?;

    // Get it
    let msg2 = protocol::array(vec![
        protocol::bulk_string("GET"),
        protocol::bulk_string("counter"),
    ]);
    let result1 = Get { message: &msg2 }.execute(&storage)?;
    assert_eq!(result1[0].as_string()?, "10");

    // Overwrite it
    let msg3 = protocol::array(vec![
        protocol::bulk_string("SET"),
        protocol::bulk_string("counter"),
        protocol::bulk_string("20"),
    ]);
    Set { message: &msg3 }.execute(&storage)?;

    // Get new value
    let result2 = Get { message: &msg2 }.execute(&storage)?;
    assert_eq!(result2[0].as_string()?, "20");
    Ok(())
}

// ============= EXPIRATION TESTS =============

#[test]
fn e2e_key_expires() -> Result<(), Box<dyn Error>> {
    let storage = create_test_storage();

    // Set with 100ms expiration
    let msg = protocol::array(vec![
        protocol::bulk_string("SET"),
        protocol::bulk_string("temp_key"),
        protocol::bulk_string("temp_value"),
        protocol::bulk_string("px"),
        protocol::bulk_string("100"),
    ]);
    Set { message: &msg }.execute(&storage)?;

    // Should exist immediately
    let get_msg = protocol::array(vec![
        protocol::bulk_string("GET"),
        protocol::bulk_string("temp_key"),
    ]);
    let result1 = Get { message: &get_msg }.execute(&storage)?;
    assert_eq!(result1[0].as_string()?, "temp_value");

    // Wait for expiration
    thread::sleep(Duration::from_millis(150));

    // Should be gone now
    let result2 = Get { message: &get_msg }.execute(&storage)?;
    assert_eq!(result2[0].as_string()?, "");
    Ok(())
}

#[test]
fn e2e_key_expires_uppercase_px() -> Result<(), Box<dyn Error>> {
    let storage = create_test_storage();

    // Set with 100ms expiration using uppercase PX (as sent by redis-cli)
    let msg = protocol::array(vec![
        protocol::bulk_string("SET"),
        protocol::bulk_string("blueberry"),
        protocol::bulk_string("raspberry"),
        protocol::bulk_string("PX"),
        protocol::bulk_string("100"),
    ]);
    Set { message: &msg }.execute(&storage)?;

    // Should exist immediately
    let get_msg = protocol::array(vec![
        protocol::bulk_string("GET"),
        protocol::bulk_string("blueberry"),
    ]);
    let result1 = Get { message: &get_msg }.execute(&storage)?;
    assert_eq!(result1[0].as_string()?, "raspberry");

    // Wait for expiration
    thread::sleep(Duration::from_millis(150));

    // Should be gone now
    let result2 = Get { message: &get_msg }.execute(&storage)?;
    assert_eq!(result2[0].as_string()?, "");
    Ok(())
}

#[test]
fn e2e_long_lived_key() -> Result<(), Box<dyn Error>> {
    let storage = create_test_storage();

    // Set with 5 second expiration
    let msg = protocol::array(vec![
        protocol::bulk_string("SET"),
        protocol::bulk_string("session"),
        protocol::bulk_string("session_data"),
        protocol::bulk_string("px"),
        protocol::bulk_string("5000"),
    ]);
    Set { message: &msg }.execute(&storage)?;

    // Should still exist after 100ms
    thread::sleep(Duration::from_millis(100));
    let get_msg = protocol::array(vec![
        protocol::bulk_string("GET"),
        protocol::bulk_string("session"),
    ]);
    let result = Get { message: &get_msg }.execute(&storage)?;
    assert_eq!(result[0].as_string()?, "session_data");
    Ok(())
}

// ============= BINARY DATA TESTS =============

#[test]
fn e2e_binary_data_preserved() -> Result<(), Box<dyn Error>> {
    let storage = create_test_storage();

    // Manually insert binary data
    let binary_data = vec![0u8, 1, 2, 3, 255, 254, 127];
    {
        let mut data = storage.lock().map_err(|_| "Failed to lock storage".to_string())?;
        let _ = data.set("binary", binary_data.clone(), None);
    }

    // Retrieve it
    let msg = protocol::array(vec![
        protocol::bulk_string("GET"),
        protocol::bulk_string("binary"),
    ]);
    let cmd = Get { message: &msg };
    let result = cmd.execute(&storage)?;

    match &result[0] {
        protocol::DataType::BulkString { value: Some(v) } => {
            assert_eq!(v, &binary_data);
        }
        _ => return Err("Expected bulk string".into()),
    }
    Ok(())
}

// ============= ERROR HANDLING TESTS =============

#[test]
fn e2e_set_missing_value_fails() -> Result<(), Box<dyn Error>> {
    let storage = create_test_storage();

    let msg = protocol::array(vec![
        protocol::bulk_string("SET"),
        protocol::bulk_string("key_only"),
    ]);
    let cmd = Set { message: &msg };
    let result = cmd.execute(&storage);

    assert!(result.is_err());
    Ok(())
}

#[test]
fn e2e_get_missing_key_fails() -> Result<(), Box<dyn Error>> {
    let storage = create_test_storage();

    let msg = protocol::array(vec![
        protocol::bulk_string("GET"),
    ]);
    let cmd = Get { message: &msg };
    let result = cmd.execute(&storage);

    assert!(result.is_err());
    Ok(())
}

// ============= REPLICATION TESTS =============

#[test]
fn e2e_info_command_master() -> Result<(), Box<dyn Error>> {
    let server_state = ServerState::new(None, 6379);
    let msg = protocol::array(vec![
        protocol::bulk_string("INFO"),
        protocol::bulk_string("replication"),
    ]);
    let cmd = Info {
        message: &msg,
        server_state: &server_state,
    };

    let storage = create_test_storage();
    let result = cmd.execute(&storage)?;

    let info = result[0].as_string()?;
    assert!(info.contains("role:master"));
    assert!(info.contains("master_replid"));
    assert!(info.contains("master_repl_offset"));
    Ok(())
}

#[test]
fn e2e_info_command_replica() -> Result<(), Box<dyn Error>> {
    let server_state = ServerState::new(Some("localhost 6379".to_owned()), 6380);
    let msg = protocol::array(vec![
        protocol::bulk_string("INFO"),
        protocol::bulk_string("replication"),
    ]);
    let cmd = Info {
        message: &msg,
        server_state: &server_state,
    };

    let storage = create_test_storage();
    let result = cmd.execute(&storage)?;

    let info = result[0].as_string()?;
    assert!(info.contains("role:slave"));
    Ok(())
}

// ============= COMPLEX SCENARIOS =============

#[test]
fn e2e_mixed_operations() -> Result<(), Box<dyn Error>> {
    let storage = create_test_storage();

    // 1. Set multiple cache entries
    let entries = vec![
        ("cache:user:1", "alice"),
        ("cache:user:2", "bob"),
        ("cache:config:timeout", "30000"),
    ];

    for (key, value) in &entries {
        let msg = protocol::array(vec![
            protocol::bulk_string("SET"),
            protocol::bulk_string(key),
            protocol::bulk_string(value),
        ]);
        Set { message: &msg }.execute(&storage)?;
    }

    // 2. Get and verify
    for (key, expected_value) in &entries {
        let msg = protocol::array(vec![
            protocol::bulk_string("GET"),
            protocol::bulk_string(key),
        ]);
        let result = Get { message: &msg }.execute(&storage)?;
        assert_eq!(result[0].as_string()?, *expected_value);
    }

    // 3. Update one
    let msg = protocol::array(vec![
        protocol::bulk_string("SET"),
        protocol::bulk_string("cache:config:timeout"),
        protocol::bulk_string("60000"),
    ]);
    Set { message: &msg }.execute(&storage)?;

    // 4. Verify update
    let msg = protocol::array(vec![
        protocol::bulk_string("GET"),
        protocol::bulk_string("cache:config:timeout"),
    ]);
    let result = Get { message: &msg }.execute(&storage)?;
    assert_eq!(result[0].as_string()?, "60000");

    // 5. Test nonexistent
    let msg = protocol::array(vec![
        protocol::bulk_string("GET"),
        protocol::bulk_string("cache:nonexistent"),
    ]);
    let result = Get { message: &msg }.execute(&storage)?;
    assert_eq!(result[0].as_string()?, "");
    Ok(())
}

#[test]
fn e2e_session_simulation() -> Result<(), Box<dyn Error>> {
    let storage = create_test_storage();

    // Simulate a user session cache
    let session_id = "session:abc123";
    let user_id = "user:123";
    let expiry_ms = 3600000; // 1 hour

    // Create session
    let msg = protocol::array(vec![
        protocol::bulk_string("SET"),
        protocol::bulk_string(session_id),
        protocol::bulk_string(user_id),
        protocol::bulk_string("px"),
        protocol::bulk_string(&expiry_ms.to_string()),
    ]);
    Set { message: &msg }.execute(&storage)?;

    // Retrieve session
    let msg = protocol::array(vec![
        protocol::bulk_string("GET"),
        protocol::bulk_string(session_id),
    ]);
    let result = Get { message: &msg }.execute(&storage)?;
    assert_eq!(result[0].as_string()?, user_id);

    // Session should still be valid after 100ms
    thread::sleep(Duration::from_millis(100));
    let result = Get { message: &msg }.execute(&storage)?;
    assert_eq!(result[0].as_string()?, user_id);
    Ok(())
}

#[test]
fn e2e_user_profile_caching() -> Result<(), Box<dyn Error>> {
    let storage = create_test_storage();

    // Simulate caching user profile
    let user_id = "user:42";
    let profile_json = r#"{"name":"John Doe","email":"john@example.com","verified":true}"#;

    // Store user profile
    let msg = protocol::array(vec![
        protocol::bulk_string("SET"),
        protocol::bulk_string(&format!("profile:{}", user_id)),
        protocol::bulk_string(profile_json),
        protocol::bulk_string("px"),
        protocol::bulk_string("600000"), // 10 minutes
    ]);
    Set { message: &msg }.execute(&storage)?;

    // Retrieve user profile
    let msg = protocol::array(vec![
        protocol::bulk_string("GET"),
        protocol::bulk_string(&format!("profile:{}", user_id)),
    ]);
    let result = Get { message: &msg }.execute(&storage)?;
    assert_eq!(result[0].as_string()?, profile_json);
    Ok(())
}

#[test]
fn e2e_rate_limiting_with_expiration() -> Result<(), Box<dyn Error>> {
    let storage = create_test_storage();

    // Simulate rate limiter that expires after 60 seconds
    let user_ip = "192.168.1.100";
    let rate_limit_key = format!("rate_limit:{}", user_ip);

    // Record 3 requests
    for i in 1..=3 {
        let msg = protocol::array(vec![
            protocol::bulk_string("SET"),
            protocol::bulk_string(&rate_limit_key),
            protocol::bulk_string(&i.to_string()),
            protocol::bulk_string("px"),
            protocol::bulk_string("60000"),
        ]);
        Set { message: &msg }.execute(&storage)?;
    }

    // Verify final count
    let msg = protocol::array(vec![
        protocol::bulk_string("GET"),
        protocol::bulk_string(&rate_limit_key),
    ]);
    let result = Get { message: &msg }.execute(&storage)?;
    assert_eq!(result[0].as_string()?, "3");
    Ok(())
}
