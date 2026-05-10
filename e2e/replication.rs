/// E2E tests for master-replica replication.
///
/// These tests start a master and multiple replicas, write data to the master,
/// and verify it appears on all replicas.

mod common;

use anyhow::Result;
use common::{start_master_and_replicas, REPLICATION_PROPAGATION_WAIT};
use std::thread;

// ========================= Replica handshake =========================

#[test]
fn test_replica_responds_to_ping() -> Result<()> {
    let (_master, replicas) = start_master_and_replicas();
    for replica in &replicas {
        let mut client = replica.client();
        let resp = client.send_command(&["PING"])?;
        assert_eq!(resp, "PONG", "replica on port {} did not PONG", replica.port);
    }
    Ok(())
}

#[test]
fn test_replica_info_shows_slave_role() -> Result<()> {
    let (_master, replicas) = start_master_and_replicas();
    for replica in &replicas {
        let mut client = replica.client();
        let resp = client.send_command(&["INFO", "replication"])?;
        assert!(
            resp.contains("role:slave"),
            "replica on port {} should report role:slave, got: {}",
            replica.port,
            resp
        );
    }
    Ok(())
}

#[test]
fn test_master_info_shows_master_role() -> Result<()> {
    let (master, _replicas) = start_master_and_replicas();
    let mut client = master.client();
    let resp = client.send_command(&["INFO", "replication"])?;
    assert!(
        resp.contains("role:master"),
        "master should report role:master, got: {}",
        resp
    );
    Ok(())
}

// ========================= Write propagation =========================

#[test]
fn test_single_key_propagates() -> Result<()> {
    let (master, replicas) = start_master_and_replicas();
    let mut mc = master.client();

    mc.send_command(&["SET", "replicated_key", "replicated_value"])?;

    thread::sleep(REPLICATION_PROPAGATION_WAIT);

    for replica in &replicas {
        let mut rc = replica.client();
        let resp = rc.send_command(&["GET", "replicated_key"])?;
        assert_eq!(
            resp, "replicated_value",
            "replica on port {} expected 'replicated_value', got '{}'",
            replica.port, resp
        );
    }
    Ok(())
}

#[test]
fn test_multiple_keys_propagate() -> Result<()> {
    let (master, replicas) = start_master_and_replicas();
    let mut mc = master.client();

    let test_data = vec![
        ("user:1", "Alice"),
        ("user:2", "Bob"),
        ("user:3", "Charlie"),
        ("counter", "42"),
        ("status", "active"),
    ];

    for (key, value) in &test_data {
        mc.send_command(&["SET", key, value])?;
    }

    thread::sleep(REPLICATION_PROPAGATION_WAIT);

    for replica in &replicas {
        let mut rc = replica.client();
        for (key, expected) in &test_data {
            let resp = rc.send_command(&["GET", key])?;
            assert_eq!(
                resp, *expected,
                "replica port {}: key={} expected={} got={}",
                replica.port, key, expected, resp
            );
        }
    }
    Ok(())
}

#[test]
fn test_overwrite_propagates() -> Result<()> {
    let (master, replicas) = start_master_and_replicas();
    let mut mc = master.client();

    mc.send_command(&["SET", "mutable_key", "initial"])?;
    thread::sleep(REPLICATION_PROPAGATION_WAIT);

    // Verify initial value on replicas
    for replica in &replicas {
        let mut rc = replica.client();
        let resp = rc.send_command(&["GET", "mutable_key"])?;
        assert_eq!(resp, "initial", "replica port {}", replica.port);
    }

    // Overwrite on master
    mc.send_command(&["SET", "mutable_key", "updated"])?;
    thread::sleep(REPLICATION_PROPAGATION_WAIT);

    // Verify updated value on replicas
    for replica in &replicas {
        let mut rc = replica.client();
        let resp = rc.send_command(&["GET", "mutable_key"])?;
        assert_eq!(
            resp, "updated",
            "replica port {}: expected 'updated', got '{}'",
            replica.port, resp
        );
    }
    Ok(())
}

#[test]
fn test_sequential_writes_propagate_in_order() -> Result<()> {
    let (master, replicas) = start_master_and_replicas();
    let mut mc = master.client();

    // Write the same key multiple times
    for i in 0..5 {
        mc.send_command(&["SET", "seq_key", &format!("version_{}", i)])?;
    }

    thread::sleep(REPLICATION_PROPAGATION_WAIT);

    // All replicas should have the final version
    for replica in &replicas {
        let mut rc = replica.client();
        let resp = rc.send_command(&["GET", "seq_key"])?;
        assert_eq!(
            resp, "version_4",
            "replica port {}: expected 'version_4', got '{}'",
            replica.port, resp
        );
    }
    Ok(())
}

#[test]
fn test_many_keys_propagate() -> Result<()> {
    let (master, replicas) = start_master_and_replicas();
    let mut mc = master.client();

    let num_keys = 50;
    for i in 0..num_keys {
        mc.send_command(&["SET", &format!("bulk_key_{}", i), &format!("bulk_value_{}", i)])?;
    }

    // Give extra time for bulk propagation
    thread::sleep(REPLICATION_PROPAGATION_WAIT * 2);

    for replica in &replicas {
        let mut rc = replica.client();
        for i in 0..num_keys {
            let resp = rc.send_command(&["GET", &format!("bulk_key_{}", i)])?;
            assert_eq!(
                resp,
                format!("bulk_value_{}", i),
                "replica port {}: bulk_key_{}",
                replica.port,
                i
            );
        }
    }
    Ok(())
}

// ========================= Replication with expiry =========================

#[test]
fn test_expiring_key_propagates() -> Result<()> {
    let (master, replicas) = start_master_and_replicas();
    let mut mc = master.client();

    // Set a key with 5-second expiry on master
    mc.send_command(&["SET", "expiring_replicated", "temp_value", "px", "5000"])?;

    thread::sleep(REPLICATION_PROPAGATION_WAIT);

    // Key should exist on replicas
    for replica in &replicas {
        let mut rc = replica.client();
        let resp = rc.send_command(&["GET", "expiring_replicated"])?;
        assert_eq!(
            resp, "temp_value",
            "replica port {}: expected 'temp_value', got '{}'",
            replica.port, resp
        );
    }
    Ok(())
}

#[test]
fn test_non_expiring_key_persists_on_replicas() -> Result<()> {
    let (master, replicas) = start_master_and_replicas();
    let mut mc = master.client();

    mc.send_command(&["SET", "permanent_replicated", "forever"])?;

    thread::sleep(REPLICATION_PROPAGATION_WAIT);

    // Wait a bit more and verify key still exists
    thread::sleep(std::time::Duration::from_secs(1));

    for replica in &replicas {
        let mut rc = replica.client();
        let resp = rc.send_command(&["GET", "permanent_replicated"])?;
        assert_eq!(resp, "forever", "replica port {}", replica.port);
    }
    Ok(())
}

// ========================= Master still works after replicas connect =========================

#[test]
fn test_master_get_works() -> Result<()> {
    let (master, _replicas) = start_master_and_replicas();
    let mut client = master.client();

    client.send_command(&["SET", "master_test", "master_value"])?;
    let resp = client.send_command(&["GET", "master_test"])?;
    assert_eq!(resp, "master_value");
    Ok(())
}

#[test]
fn test_master_ping_works() -> Result<()> {
    let (master, _replicas) = start_master_and_replicas();
    let mut client = master.client();

    let resp = client.send_command(&["PING"])?;
    assert_eq!(resp, "PONG");
    Ok(())
}

#[test]
fn test_master_echo_works() -> Result<()> {
    let (master, _replicas) = start_master_and_replicas();
    let mut client = master.client();

    let resp = client.send_command(&["ECHO", "test"])?;
    assert_eq!(resp, "test");
    Ok(())
}
