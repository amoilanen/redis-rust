/// Integration tests for RDB file format reading/writing.
///
/// These tests generate binary RDB fixture files and verify they can be
/// loaded correctly. The fixtures exercise various RDB format features
/// as described in https://rdb.fnordig.de/file_format.html

use anyhow::{Context, Result};
use std::collections::HashMap;
use std::fs;
use std::io::Cursor;
use std::path::PathBuf;

use codecrafters_redis::rdb::{from_rdb, crc64, write_string, encode_length};
use codecrafters_redis::storage::Storage;

/// Build a complete RDB file: header + body + EOF + CRC64.
fn build_rdb(version: &str, body: &[u8]) -> Vec<u8> {
    let mut rdb = Vec::new();
    rdb.extend_from_slice(b"REDIS");
    rdb.extend_from_slice(version.as_bytes());
    rdb.extend_from_slice(body);
    rdb.push(0xFF); // EOF
    let checksum = crc64(&rdb);
    rdb.extend_from_slice(&checksum.to_le_bytes());
    rdb
}

fn fixtures_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

fn write_fixture(name: &str, data: &[u8]) -> Result<()> {
    let path = fixtures_dir().join(name);
    fs::write(&path, data).with_context(|| format!("Failed to write {}", path.display()))?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Fixture generators
// ---------------------------------------------------------------------------

/// Empty database: header + aux fields + DB0 + resize(0,0) + EOF + CRC
fn generate_empty_db() -> Vec<u8> {
    let mut body = Vec::new();
    // Aux fields
    body.push(0xFA);
    write_string(&mut body, b"redis-ver");
    write_string(&mut body, b"7.0.0");
    // DB 0
    body.push(0xFE);
    body.extend(encode_length(0));
    // Resize: 0 keys, 0 expiry
    body.push(0xFB);
    body.extend(encode_length(0));
    body.extend(encode_length(0));
    build_rdb("0009", &body)
}

/// Three string key-value pairs, no expiry
fn generate_string_keys() -> Vec<u8> {
    let mut body = Vec::new();
    body.push(0xFE);
    body.extend(encode_length(0));
    body.push(0xFB);
    body.extend(encode_length(3));
    body.extend(encode_length(0));

    for (k, v) in [("name", "Redis"), ("version", "7.0.0"), ("lang", "C")] {
        body.push(0x00); // String type
        write_string(&mut body, k.as_bytes());
        write_string(&mut body, v.as_bytes());
    }
    build_rdb("0009", &body)
}

/// Keys with millisecond expiry (far future so they don't expire during tests)
fn generate_with_expiry() -> Vec<u8> {
    let mut body = Vec::new();
    body.push(0xFE);
    body.extend(encode_length(0));
    body.push(0xFB);
    body.extend(encode_length(3));
    body.extend(encode_length(2));

    // Key with ms expiry (year ~2100)
    let future_ms: u64 = 4_102_444_800_000;
    body.push(0xFC); // EXPIRETIMEMS
    body.extend_from_slice(&future_ms.to_le_bytes());
    body.push(0x00);
    write_string(&mut body, b"session:abc");
    write_string(&mut body, b"user123");

    // Key with second expiry (year ~2100)
    let future_sec: u32 = 4_102_444_800;
    body.push(0xFD); // EXPIRETIME
    body.extend_from_slice(&future_sec.to_le_bytes());
    body.push(0x00);
    write_string(&mut body, b"session:def");
    write_string(&mut body, b"user456");

    // Key without expiry
    body.push(0x00);
    write_string(&mut body, b"permanent");
    write_string(&mut body, b"stays");

    build_rdb("0009", &body)
}

/// Values encoded as int8, int16, int32
fn generate_integer_encoded() -> Vec<u8> {
    let mut body = Vec::new();
    body.push(0xFE);
    body.extend(encode_length(0));
    body.push(0xFB);
    body.extend(encode_length(4));
    body.extend(encode_length(0));

    // int8 value: 42
    body.push(0x00);
    write_string(&mut body, b"small_num");
    body.push(0xC0); // int8 encoding
    body.push(42);

    // int8 negative: -5
    body.push(0x00);
    write_string(&mut body, b"neg_num");
    body.push(0xC0);
    body.push((-5i8) as u8);

    // int16 value: 10000
    body.push(0x00);
    write_string(&mut body, b"medium_num");
    body.push(0xC1);
    body.extend_from_slice(&10000i16.to_le_bytes());

    // int32 value: 1000000
    body.push(0x00);
    write_string(&mut body, b"large_num");
    body.push(0xC2);
    body.extend_from_slice(&1000000i32.to_le_bytes());

    build_rdb("0009", &body)
}

/// Multiple aux fields + resize DB opcode
fn generate_aux_and_resize() -> Vec<u8> {
    let mut body = Vec::new();
    // Several aux fields (as Redis would write them)
    body.push(0xFA);
    write_string(&mut body, b"redis-ver");
    write_string(&mut body, b"7.2.4");
    body.push(0xFA);
    write_string(&mut body, b"redis-bits");
    write_string(&mut body, b"64");
    body.push(0xFA);
    write_string(&mut body, b"ctime");
    write_string(&mut body, b"1700000000");
    body.push(0xFA);
    write_string(&mut body, b"used-mem");
    write_string(&mut body, b"1048576");
    body.push(0xFA);
    write_string(&mut body, b"aof-base");
    write_string(&mut body, b"0");

    body.push(0xFE);
    body.extend(encode_length(0));
    body.push(0xFB);
    body.extend(encode_length(1));
    body.extend(encode_length(0));

    body.push(0x00);
    write_string(&mut body, b"greeting");
    write_string(&mut body, b"hello");

    build_rdb("0011", &body)
}

/// DB 0 and DB 1 entries
fn generate_multiple_databases() -> Vec<u8> {
    let mut body = Vec::new();
    // DB 0
    body.push(0xFE);
    body.extend(encode_length(0));
    body.push(0xFB);
    body.extend(encode_length(2));
    body.extend(encode_length(0));
    body.push(0x00);
    write_string(&mut body, b"db0:key1");
    write_string(&mut body, b"val1");
    body.push(0x00);
    write_string(&mut body, b"db0:key2");
    write_string(&mut body, b"val2");

    // DB 1
    body.push(0xFE);
    body.extend(encode_length(1));
    body.push(0xFB);
    body.extend(encode_length(1));
    body.extend(encode_length(0));
    body.push(0x00);
    write_string(&mut body, b"db1:key1");
    write_string(&mut body, b"db1val");

    build_rdb("0009", &body)
}

/// Fixture with a mix of string + unsupported types (hash, list)
fn generate_mixed_types() -> Vec<u8> {
    let mut body = Vec::new();
    body.push(0xFE);
    body.extend(encode_length(0));

    // String key (should be loaded)
    body.push(0x00);
    write_string(&mut body, b"string_key");
    write_string(&mut body, b"string_val");

    // Hash key (type 4, should be skipped)
    body.push(0x04);
    write_string(&mut body, b"hash_key");
    body.extend(encode_length(1)); // 1 field-value pair
    write_string(&mut body, b"field");
    write_string(&mut body, b"value");

    // Set key (type 2, should be skipped)
    body.push(0x02);
    write_string(&mut body, b"set_key");
    body.extend(encode_length(2));
    write_string(&mut body, b"member1");
    write_string(&mut body, b"member2");

    // Another string key (should be loaded)
    body.push(0x00);
    write_string(&mut body, b"another_string");
    write_string(&mut body, b"another_val");

    build_rdb("0009", &body)
}

/// Fixture with expired keys
fn generate_with_expired_keys() -> Vec<u8> {
    let mut body = Vec::new();
    body.push(0xFE);
    body.extend(encode_length(0));
    body.push(0xFB);
    body.extend(encode_length(3));
    body.extend(encode_length(2));

    // Expired key (timestamp in the past: year 2000)
    let past_ms: u64 = 946_684_800_000;
    body.push(0xFC);
    body.extend_from_slice(&past_ms.to_le_bytes());
    body.push(0x00);
    write_string(&mut body, b"expired1");
    write_string(&mut body, b"old_data");

    // Expired key via seconds (year 2000)
    let past_sec: u32 = 946_684_800;
    body.push(0xFD);
    body.extend_from_slice(&past_sec.to_le_bytes());
    body.push(0x00);
    write_string(&mut body, b"expired2");
    write_string(&mut body, b"old_data2");

    // Valid key
    body.push(0x00);
    write_string(&mut body, b"valid");
    write_string(&mut body, b"fresh_data");

    build_rdb("0009", &body)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn fixture_empty_db() -> Result<()> {
    let rdb = generate_empty_db();
    write_fixture("empty_db.rdb", &rdb)?;

    let data = fs::read(fixtures_dir().join("empty_db.rdb"))?;
    let storage = from_rdb(Cursor::new(&data))?;
    assert_eq!(storage.data.len(), 0);
    Ok(())
}

#[test]
fn fixture_string_keys() -> Result<()> {
    let rdb = generate_string_keys();
    write_fixture("string_keys.rdb", &rdb)?;

    let data = fs::read(fixtures_dir().join("string_keys.rdb"))?;
    let storage = from_rdb(Cursor::new(&data))?;
    assert_eq!(storage.data.len(), 3);
    assert_eq!(
        storage.data.get("name").context("missing key: name")?.value,
        b"Redis"
    );
    assert_eq!(
        storage.data.get("version").context("missing key: version")?.value,
        b"7.0.0"
    );
    assert_eq!(
        storage.data.get("lang").context("missing key: lang")?.value,
        b"C"
    );
    Ok(())
}

#[test]
fn fixture_with_expiry() -> Result<()> {
    let rdb = generate_with_expiry();
    write_fixture("with_expiry.rdb", &rdb)?;

    let data = fs::read(fixtures_dir().join("with_expiry.rdb"))?;
    let storage = from_rdb(Cursor::new(&data))?;
    assert_eq!(storage.data.len(), 3);
    assert_eq!(
        storage
            .data
            .get("session:abc")
            .context("missing key: session:abc")?
            .value,
        b"user123"
    );
    assert_eq!(
        storage
            .data
            .get("session:def")
            .context("missing key: session:def")?
            .value,
        b"user456"
    );
    assert_eq!(
        storage
            .data
            .get("permanent")
            .context("missing key: permanent")?
            .value,
        b"stays"
    );

    // Verify expiry is set on session keys but not on permanent
    assert!(
        storage
            .data
            .get("session:abc")
            .context("missing key: session:abc")?
            .expires_at_ms()
            .is_some()
    );
    assert!(
        storage
            .data
            .get("session:def")
            .context("missing key: session:def")?
            .expires_at_ms()
            .is_some()
    );
    assert!(
        storage
            .data
            .get("permanent")
            .context("missing key: permanent")?
            .expires_at_ms()
            .is_none()
    );
    Ok(())
}

#[test]
fn fixture_integer_encoded() -> Result<()> {
    let rdb = generate_integer_encoded();
    write_fixture("integer_encoded.rdb", &rdb)?;

    let data = fs::read(fixtures_dir().join("integer_encoded.rdb"))?;
    let storage = from_rdb(Cursor::new(&data))?;
    assert_eq!(storage.data.len(), 4);
    assert_eq!(
        storage
            .data
            .get("small_num")
            .context("missing key: small_num")?
            .value,
        b"42"
    );
    assert_eq!(
        storage
            .data
            .get("neg_num")
            .context("missing key: neg_num")?
            .value,
        b"-5"
    );
    assert_eq!(
        storage
            .data
            .get("medium_num")
            .context("missing key: medium_num")?
            .value,
        b"10000"
    );
    assert_eq!(
        storage
            .data
            .get("large_num")
            .context("missing key: large_num")?
            .value,
        b"1000000"
    );
    Ok(())
}

#[test]
fn fixture_aux_and_resize() -> Result<()> {
    let rdb = generate_aux_and_resize();
    write_fixture("aux_and_resize.rdb", &rdb)?;

    let data = fs::read(fixtures_dir().join("aux_and_resize.rdb"))?;
    let storage = from_rdb(Cursor::new(&data))?;
    assert_eq!(storage.data.len(), 1);
    assert_eq!(
        storage
            .data
            .get("greeting")
            .context("missing key: greeting")?
            .value,
        b"hello"
    );
    Ok(())
}

#[test]
fn fixture_multiple_databases() -> Result<()> {
    let rdb = generate_multiple_databases();
    write_fixture("multiple_databases.rdb", &rdb)?;

    let data = fs::read(fixtures_dir().join("multiple_databases.rdb"))?;
    let storage = from_rdb(Cursor::new(&data))?;
    // All keys from all databases loaded into our single storage
    assert_eq!(storage.data.len(), 3);
    assert_eq!(
        storage
            .data
            .get("db0:key1")
            .context("missing key: db0:key1")?
            .value,
        b"val1"
    );
    assert_eq!(
        storage
            .data
            .get("db0:key2")
            .context("missing key: db0:key2")?
            .value,
        b"val2"
    );
    assert_eq!(
        storage
            .data
            .get("db1:key1")
            .context("missing key: db1:key1")?
            .value,
        b"db1val"
    );
    Ok(())
}

#[test]
fn fixture_mixed_types() -> Result<()> {
    let rdb = generate_mixed_types();
    write_fixture("mixed_types.rdb", &rdb)?;

    let data = fs::read(fixtures_dir().join("mixed_types.rdb"))?;
    let storage = from_rdb(Cursor::new(&data))?;
    // Only string keys should be loaded
    assert_eq!(storage.data.len(), 2);
    assert_eq!(
        storage
            .data
            .get("string_key")
            .context("missing key: string_key")?
            .value,
        b"string_val"
    );
    assert_eq!(
        storage
            .data
            .get("another_string")
            .context("missing key: another_string")?
            .value,
        b"another_val"
    );
    assert!(storage.data.get("hash_key").is_none());
    assert!(storage.data.get("set_key").is_none());
    Ok(())
}

#[test]
fn fixture_with_expired_keys() -> Result<()> {
    let rdb = generate_with_expired_keys();
    write_fixture("with_expired_keys.rdb", &rdb)?;

    let data = fs::read(fixtures_dir().join("with_expired_keys.rdb"))?;
    let storage = from_rdb(Cursor::new(&data))?;
    // Expired keys should be filtered out
    assert!(storage.data.get("expired1").is_none());
    assert!(storage.data.get("expired2").is_none());
    assert_eq!(
        storage
            .data
            .get("valid")
            .context("missing key: valid")?
            .value,
        b"fresh_data"
    );
    Ok(())
}

#[test]
fn rdb_round_trip_via_storage_api() -> Result<()> {
    let mut storage = Storage::new(HashMap::new());
    storage.set("user:1", b"alice".to_vec(), None)?;
    storage.set("user:2", b"bob".to_vec(), Some(3_600_000))?;
    storage.set("counter", b"42".to_vec(), None)?;

    let rdb_bytes = storage.to_rdb()?;
    let loaded = Storage::from_rdb(&rdb_bytes)?;

    assert_eq!(loaded.to_pairs().get("user:1"), Some(&b"alice".to_vec()));
    assert_eq!(loaded.to_pairs().get("user:2"), Some(&b"bob".to_vec()));
    assert_eq!(loaded.to_pairs().get("counter"), Some(&b"42".to_vec()));
    Ok(())
}

#[test]
fn rdb_round_trip_binary_values() -> Result<()> {
    let mut storage = Storage::new(HashMap::new());
    let binary = vec![0u8, 1, 2, 127, 128, 254, 255];
    storage.set("binary", binary.clone(), None)?;

    let rdb_bytes = storage.to_rdb()?;
    let loaded = Storage::from_rdb(&rdb_bytes)?;

    assert_eq!(loaded.to_pairs().get("binary"), Some(&binary));
    Ok(())
}

#[test]
fn rdb_round_trip_empty_values() -> Result<()> {
    let mut storage = Storage::new(HashMap::new());
    storage.set("empty", b"".to_vec(), None)?;
    storage.set("notempty", b"x".to_vec(), None)?;

    let rdb_bytes = storage.to_rdb()?;
    let loaded = Storage::from_rdb(&rdb_bytes)?;

    assert_eq!(loaded.to_pairs().get("empty"), Some(&b"".to_vec()));
    assert_eq!(loaded.to_pairs().get("notempty"), Some(&b"x".to_vec()));
    Ok(())
}
