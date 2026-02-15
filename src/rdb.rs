//! Redis RDB file format serialization and deserialization.
//!
//! Implements reading and writing of RDB version 9 files with support for:
//! - All opcodes: AUX (0xFA), RESIZEDB (0xFB), EXPIRETIMEMS (0xFC),
//!   EXPIRETIME (0xFD), SELECTDB (0xFE), EOF (0xFF)
//! - String encoding: raw, integer (int8/16/32), LZF compressed
//! - Expiry timestamps (absolute, millisecond precision)
//! - Redis-compatible CRC64 checksum (Jones polynomial)
//!
//! Spec reference: https://rdb.fnordig.de/file_format.html

use std::io::{Cursor, Read, Write};

use anyhow::{anyhow, ensure, Context, Result};
use log::*;
use std::collections::HashMap;

use crate::storage::{Storage, StoredValue};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const RDB_VERSION: &str = "0009";

// Opcodes (spec: https://rdb.fnordig.de/file_format.html#opcodes)
const RDB_OPCODE_AUX: u8 = 0xFA;
const RDB_OPCODE_RESIZEDB: u8 = 0xFB;
const RDB_OPCODE_EXPIRETIMEMS: u8 = 0xFC;
const RDB_OPCODE_EXPIRETIME: u8 = 0xFD;
const RDB_OPCODE_SELECTDB: u8 = 0xFE;
const RDB_OPCODE_EOF: u8 = 0xFF;

// Value type codes
const RDB_TYPE_STRING: u8 = 0;
const RDB_TYPE_LIST: u8 = 1;
const RDB_TYPE_SET: u8 = 2;
const RDB_TYPE_ZSET: u8 = 3;
const RDB_TYPE_HASH: u8 = 4;
// 5-8 unused in modern Redis
const RDB_TYPE_ZIPMAP: u8 = 9;
#[allow(dead_code)]
const RDB_TYPE_ZIPLIST: u8 = 10;
#[allow(dead_code)]
const RDB_TYPE_INTSET: u8 = 11;
#[allow(dead_code)]
const RDB_TYPE_ZSET_ZIPLIST: u8 = 12;
#[allow(dead_code)]
const RDB_TYPE_HASH_ZIPLIST: u8 = 13;
const RDB_TYPE_LIST_QUICKLIST: u8 = 14;

// Special encoding subtypes (within the 0b11 length-encoding prefix)
const RDB_ENC_INT8: u8 = 0;
const RDB_ENC_INT16: u8 = 1;
const RDB_ENC_INT32: u8 = 2;
const RDB_ENC_LZF: u8 = 3;

// ---------------------------------------------------------------------------
// CRC64 — Redis-compatible (Jones polynomial 0xad93d23594c935a9, reflected)
// ---------------------------------------------------------------------------

/// Lookup table computed at compile time. Matches the algorithm in Redis src/crc64.c.
/// Redis uses a MSB-first CRC with reflected I/O and polynomial 0xad93d23594c935a9.
/// Each table entry = _crc64(0, &[byte], 1) from the Redis reference implementation.
const CRC64_TABLE: [u64; 256] = build_crc64_table();

const fn reflect64(mut data: u64) -> u64 {
    let mut ret = data & 0x01;
    let mut i = 1;
    while i < 64 {
        data >>= 1;
        ret = (ret << 1) | (data & 0x01);
        i += 1;
    }
    ret
}

/// Compute the CRC64 of a single byte using the bit-by-bit algorithm from Redis.
/// This mirrors Redis _crc64() for a single byte with initial CRC of 0.
const fn crc64_single_byte(byte: u8) -> u64 {
    const POLY: u64 = 0xad93d23594c935a9;
    let mut crc: u64 = 0;
    let mut bit_mask: u8 = 0x01; // LSB first (ReflectIn = true)
    loop {
        let crc_high = (crc >> 63) & 1;
        let data_bit = ((byte & bit_mask) != 0) as u64;
        // XOR when data bit differs from CRC high bit
        crc <<= 1;
        if (crc_high ^ data_bit) != 0 {
            crc ^= POLY;
        }
        if bit_mask == 0x80 {
            break;
        }
        bit_mask <<= 1;
    }
    reflect64(crc) // ReflectOut = true
}

const fn build_crc64_table() -> [u64; 256] {
    let mut table = [0u64; 256];
    let mut i: usize = 0;
    while i < 256 {
        table[i] = crc64_single_byte(i as u8);
        i += 1;
    }
    table
}

pub fn crc64(data: &[u8]) -> u64 {
    let mut crc: u64 = 0;
    for &byte in data {
        let index = ((crc ^ byte as u64) & 0xFF) as usize;
        crc = CRC64_TABLE[index] ^ (crc >> 8);
    }
    crc
}

// ---------------------------------------------------------------------------
// LZF decompression
// ---------------------------------------------------------------------------

/// Decompress LZF data. Only decompression is needed for reading RDB files.
/// Format: control bytes interleaved with literal data and back-references.
fn lzf_decompress(input: &[u8], expected_len: usize) -> Result<Vec<u8>> {
    let mut output = Vec::with_capacity(expected_len);
    let mut i = 0;
    while i < input.len() {
        let ctrl = input[i] as usize;
        i += 1;
        if ctrl < 32 {
            // Literal run: ctrl + 1 bytes
            let run_len = ctrl + 1;
            ensure!(i + run_len <= input.len(), "LZF: literal overrun");
            output.extend_from_slice(&input[i..i + run_len]);
            i += run_len;
        } else {
            // Back-reference: high 3 bits encode length, low 5 bits + next byte encode offset
            let mut len = (ctrl >> 5) + 2;
            if len == 9 {
                // When the 3-bit length field is maxed (7), read an extra byte
                ensure!(i < input.len(), "LZF: missing extended length byte");
                len += input[i] as usize;
                i += 1;
            }
            ensure!(i < input.len(), "LZF: missing offset byte");
            let offset = ((ctrl & 0x1f) << 8) | input[i] as usize;
            i += 1;
            let offset = offset + 1; // offset is 1-based
            ensure!(offset <= output.len(), "LZF: back-reference beyond start of output");
            let start = output.len() - offset;
            for j in 0..len {
                output.push(output[start + j]);
            }
        }
    }
    ensure!(
        output.len() == expected_len,
        "LZF: expected {} bytes, got {}",
        expected_len,
        output.len()
    );
    Ok(output)
}

// ---------------------------------------------------------------------------
// Length encoding (spec: https://rdb.fnordig.de/file_format.html#length-encoding)
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq)]
enum LengthOrSpecial {
    Length(usize),
    /// Special encoding subtype: 0=int8, 1=int16, 2=int32, 3=LZF
    Special(u8),
}

fn decode_length_or_special<R: Read>(reader: &mut R) -> Result<LengthOrSpecial> {
    let mut first = [0u8; 1];
    reader
        .read_exact(&mut first)
        .context("Could not read length byte")?;
    let prefix = first[0] >> 6;
    match prefix {
        0b00 => Ok(LengthOrSpecial::Length((first[0] & 0x3F) as usize)),
        0b01 => {
            let mut second = [0u8; 1];
            reader
                .read_exact(&mut second)
                .context("Could not read second length byte")?;
            let len = ((first[0] & 0x3F) as usize) << 8 | second[0] as usize;
            Ok(LengthOrSpecial::Length(len))
        }
        0b10 => {
            let mut buf = [0u8; 4];
            reader
                .read_exact(&mut buf)
                .context("Could not read 32-bit length")?;
            Ok(LengthOrSpecial::Length(u32::from_be_bytes(buf) as usize))
        }
        0b11 => Ok(LengthOrSpecial::Special(first[0] & 0x3F)),
        _ => unreachable!(),
    }
}

/// Decode a plain length (errors on special encoding).
fn decode_length<R: Read>(reader: &mut R) -> Result<usize> {
    match decode_length_or_special(reader)? {
        LengthOrSpecial::Length(len) => Ok(len),
        LengthOrSpecial::Special(s) => Err(anyhow!(
            "Expected plain length, got special encoding type {}",
            s
        )),
    }
}

pub fn encode_length(len: usize) -> Vec<u8> {
    if len < 1 << 6 {
        vec![len as u8]
    } else if len < 1 << 14 {
        vec![(0x40) | (len >> 8) as u8, (len & 0xFF) as u8]
    } else {
        let mut v = vec![0x80];
        v.extend_from_slice(&(len as u32).to_be_bytes());
        v
    }
}

// ---------------------------------------------------------------------------
// String encoding (spec: https://rdb.fnordig.de/file_format.html#string-encoding)
// ---------------------------------------------------------------------------

/// Read an RDB-encoded string, handling raw, integer, and LZF encodings.
fn read_string<R: Read>(reader: &mut R) -> Result<Vec<u8>> {
    match decode_length_or_special(reader)? {
        LengthOrSpecial::Length(len) => {
            let mut buf = vec![0u8; len];
            reader.read_exact(&mut buf)?;
            Ok(buf)
        }
        LengthOrSpecial::Special(encoding) => match encoding {
            RDB_ENC_INT8 => {
                let mut buf = [0u8; 1];
                reader.read_exact(&mut buf)?;
                Ok(format!("{}", buf[0] as i8).into_bytes())
            }
            RDB_ENC_INT16 => {
                let mut buf = [0u8; 2];
                reader.read_exact(&mut buf)?;
                Ok(format!("{}", i16::from_le_bytes(buf)).into_bytes())
            }
            RDB_ENC_INT32 => {
                let mut buf = [0u8; 4];
                reader.read_exact(&mut buf)?;
                Ok(format!("{}", i32::from_le_bytes(buf)).into_bytes())
            }
            RDB_ENC_LZF => {
                let compressed_len = decode_length(reader)?;
                let uncompressed_len = decode_length(reader)?;
                let mut compressed = vec![0u8; compressed_len];
                reader.read_exact(&mut compressed)?;
                lzf_decompress(&compressed, uncompressed_len)
            }
            other => Err(anyhow!("Unknown special string encoding: {}", other)),
        },
    }
}

/// Write a raw length-prefixed string.
pub fn write_string(buf: &mut Vec<u8>, data: &[u8]) {
    buf.extend(encode_length(data.len()));
    buf.extend_from_slice(data);
}

// ---------------------------------------------------------------------------
// Value skipping (for unsupported types)
// ---------------------------------------------------------------------------

/// Skip over a value of the given type without storing it.
/// This allows the parser to continue past unsupported Redis data types.
fn skip_value<R: Read>(reader: &mut R, value_type: u8) -> Result<()> {
    match value_type {
        RDB_TYPE_STRING => {
            let _ = read_string(reader)?;
        }
        RDB_TYPE_LIST | RDB_TYPE_SET => {
            // Sequence of length-prefixed strings
            let count = decode_length(reader)?;
            for _ in 0..count {
                let _ = read_string(reader)?;
            }
        }
        RDB_TYPE_ZSET => {
            // Pairs of (member-string, score-as-double-string)
            let count = decode_length(reader)?;
            for _ in 0..count {
                let _ = read_string(reader)?;
                // Score: 1-byte length then that many ASCII bytes, or 0xFD/FE/FF for special values
                let mut score_len = [0u8; 1];
                reader.read_exact(&mut score_len)?;
                if score_len[0] < 0xFD {
                    let mut score = vec![0u8; score_len[0] as usize];
                    reader.read_exact(&mut score)?;
                }
                // 0xFD=NaN, 0xFE=+inf, 0xFF=-inf have no additional bytes
            }
        }
        RDB_TYPE_HASH => {
            // Pairs of (field-string, value-string)
            let count = decode_length(reader)?;
            for _ in 0..count {
                let _ = read_string(reader)?;
                let _ = read_string(reader)?;
            }
        }
        // Types 9-14: compact encodings stored as a single opaque string blob
        RDB_TYPE_ZIPMAP..=RDB_TYPE_LIST_QUICKLIST if value_type != RDB_TYPE_LIST_QUICKLIST => {
            let _ = read_string(reader)?;
        }
        RDB_TYPE_LIST_QUICKLIST => {
            // Linked list of ziplist blobs
            let count = decode_length(reader)?;
            for _ in 0..count {
                let _ = read_string(reader)?;
            }
        }
        _ => return Err(anyhow!("Unknown RDB value type: {}", value_type)),
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Public API: from_rdb (reader)
// ---------------------------------------------------------------------------

/// Parse an RDB byte stream and return a Storage containing all non-expired string keys.
///
/// Uses a two-phase approach: read all bytes, verify CRC64 checksum, then parse.
/// Unsupported value types (lists, sets, hashes, etc.) are skipped with a warning.
/// Expired keys are discarded per the RDB spec.
pub fn from_rdb<R>(input: R) -> Result<Storage>
where
    R: Read,
{
    let mut reader = input;
    let mut all_bytes = Vec::new();
    reader.read_to_end(&mut all_bytes)?;

    ensure!(all_bytes.len() >= 9 + 8, "RDB file too short");

    // Split off the 8-byte CRC64 checksum (little-endian) at the end
    let (data_bytes, checksum_bytes) = all_bytes.split_at(all_bytes.len() - 8);
    let stored_checksum = u64::from_le_bytes(
        checksum_bytes
            .try_into()
            .context("Failed to read checksum bytes")?,
    );

    // Verify checksum (all-zeros means checksum is disabled)
    if stored_checksum != 0 {
        let computed = crc64(data_bytes);
        ensure!(
            computed == stored_checksum,
            "CRC64 mismatch: stored {:016x}, computed {:016x}",
            stored_checksum,
            computed
        );
    }

    let mut cursor = Cursor::new(data_bytes);

    // Header: "REDIS" + 4-digit version
    let mut header = [0u8; 9];
    cursor.read_exact(&mut header)?;
    ensure!(
        header.starts_with(b"REDIS"),
        "Not an RDB file: missing REDIS magic"
    );
    let version_str = std::str::from_utf8(&header[5..9])?;
    let version: u32 = version_str
        .parse()
        .context(format!("Invalid RDB version: {}", version_str))?;
    info!("Reading RDB version {:04}", version);

    let mut data: HashMap<String, StoredValue> = HashMap::new();

    loop {
        let mut opcode = [0u8; 1];
        cursor.read_exact(&mut opcode)?;

        match opcode[0] {
            RDB_OPCODE_AUX => {
                let key = read_string(&mut cursor)?;
                let value = read_string(&mut cursor)?;
                info!(
                    "RDB aux: {} = {}",
                    String::from_utf8_lossy(&key),
                    String::from_utf8_lossy(&value)
                );
            }

            RDB_OPCODE_SELECTDB => {
                let db_number = decode_length(&mut cursor)?;
                info!("RDB selecting database {}", db_number);
                if db_number != 0 {
                    warn!("Non-zero database {} encountered, keys will still be loaded into our single-db storage", db_number);
                }
            }

            RDB_OPCODE_RESIZEDB => {
                let db_size = decode_length(&mut cursor)?;
                let expires_size = decode_length(&mut cursor)?;
                info!(
                    "RDB resize db hint: {} keys, {} with expiry",
                    db_size, expires_size
                );
                data.reserve(db_size);
            }

            RDB_OPCODE_EXPIRETIMEMS => {
                // 8-byte little-endian millisecond timestamp
                let mut buf = [0u8; 8];
                cursor.read_exact(&mut buf)?;
                let expires_at_ms = u64::from_le_bytes(buf);

                let mut type_byte = [0u8; 1];
                cursor.read_exact(&mut type_byte)?;
                let key = read_string(&mut cursor)?;
                let key_str = String::from_utf8(key)?;

                if type_byte[0] == RDB_TYPE_STRING {
                    let value = read_string(&mut cursor)?;
                    let stored = StoredValue::with_absolute_expiry(value, Some(expires_at_ms))?;
                    if !stored.is_expired() {
                        data.insert(key_str, stored);
                    } else {
                        debug!("Skipping expired key '{}' during RDB load", key_str);
                    }
                } else {
                    warn!(
                        "Skipping unsupported value type {} for key '{}'",
                        type_byte[0], key_str
                    );
                    skip_value(&mut cursor, type_byte[0])?;
                }
            }

            RDB_OPCODE_EXPIRETIME => {
                // 4-byte little-endian second timestamp
                let mut buf = [0u8; 4];
                cursor.read_exact(&mut buf)?;
                let expires_at_ms = u32::from_le_bytes(buf) as u64 * 1000;

                let mut type_byte = [0u8; 1];
                cursor.read_exact(&mut type_byte)?;
                let key = read_string(&mut cursor)?;
                let key_str = String::from_utf8(key)?;

                if type_byte[0] == RDB_TYPE_STRING {
                    let value = read_string(&mut cursor)?;
                    let stored = StoredValue::with_absolute_expiry(value, Some(expires_at_ms))?;
                    if !stored.is_expired() {
                        data.insert(key_str, stored);
                    } else {
                        debug!("Skipping expired key '{}' during RDB load", key_str);
                    }
                } else {
                    warn!(
                        "Skipping unsupported value type {} for key '{}'",
                        type_byte[0], key_str
                    );
                    skip_value(&mut cursor, type_byte[0])?;
                }
            }

            RDB_OPCODE_EOF => {
                info!("RDB parsing complete");
                break;
            }

            // Any other byte is a value type code
            value_type => {
                let key = read_string(&mut cursor)?;
                let key_str = String::from_utf8(key)?;

                if value_type == RDB_TYPE_STRING {
                    let value = read_string(&mut cursor)?;
                    data.insert(key_str, StoredValue::from(value, None)?);
                } else {
                    warn!(
                        "Skipping unsupported value type {} for key '{}'",
                        value_type, key_str
                    );
                    skip_value(&mut cursor, value_type)?;
                }
            }
        }
    }

    Ok(Storage { data })
}

// ---------------------------------------------------------------------------
// Public API: to_rdb (writer)
// ---------------------------------------------------------------------------

/// Serialize a Storage to RDB format (version 0009).
pub fn to_rdb<W>(storage: &Storage, output: &mut W) -> Result<()>
where
    W: Write,
{
    let mut buf = Vec::new();

    // Header
    buf.extend_from_slice(b"REDIS");
    buf.extend_from_slice(RDB_VERSION.as_bytes());

    // Auxiliary metadata
    write_aux_field(&mut buf, b"redis-ver", b"7.0.0");
    write_aux_field(&mut buf, b"redis-bits", b"64");

    // Database selector: DB 0
    buf.push(RDB_OPCODE_SELECTDB);
    buf.extend(encode_length(0));

    // Resize DB hint
    let total_keys = storage.data.len();
    let expiry_keys = storage
        .data
        .values()
        .filter(|v| v.expires_at_ms().is_some())
        .count();
    buf.push(RDB_OPCODE_RESIZEDB);
    buf.extend(encode_length(total_keys));
    buf.extend(encode_length(expiry_keys));

    // Key-value entries
    for (key, stored_value) in &storage.data {
        // Write expiry prefix if present
        if let Some(expires_at) = stored_value.expires_at_ms() {
            buf.push(RDB_OPCODE_EXPIRETIMEMS);
            buf.extend_from_slice(&expires_at.to_le_bytes());
        }

        // Value type: String
        buf.push(RDB_TYPE_STRING);

        // Key
        write_string(&mut buf, key.as_bytes());

        // Value
        write_string(&mut buf, &stored_value.value);
    }

    // EOF marker
    buf.push(RDB_OPCODE_EOF);

    // CRC64 checksum (8 bytes, little-endian)
    let checksum = crc64(&buf);
    buf.extend_from_slice(&checksum.to_le_bytes());

    output.write_all(&buf)?;
    Ok(())
}

fn write_aux_field(buf: &mut Vec<u8>, key: &[u8], value: &[u8]) {
    buf.push(RDB_OPCODE_AUX);
    write_string(buf, key);
    write_string(buf, value);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::io::Cursor;
    // -- CRC64 tests --

    #[test]
    fn crc64_empty() {
        assert_eq!(crc64(b""), 0);
    }

    #[test]
    fn crc64_known_test_vector() {
        // Redis test vector from src/crc64.c
        assert_eq!(crc64(b"123456789"), 0xe9c6d914c4b8d9ca);
    }

    // -- LZF decompression tests --

    #[test]
    fn lzf_decompress_literals_only() {
        // Control byte 0x04 = literal run of 5 bytes
        let input = vec![0x04, b'H', b'e', b'l', b'l', b'o'];
        let result = lzf_decompress(&input, 5).unwrap();
        assert_eq!(result, b"Hello");
    }

    #[test]
    fn lzf_decompress_with_backreference() {
        // "abcabc": first "abc" as literal, then back-reference offset=3 len=3
        // Literal: ctrl=0x02 (3 bytes), data: a, b, c
        // Back-ref: ctrl = (1 << 5) | 0x00 = 0x20 (len=1+2=3), offset high=0, next byte=0x02 (offset=2+1=3)
        let input = vec![0x02, b'a', b'b', b'c', 0x20, 0x02];
        let result = lzf_decompress(&input, 6).unwrap();
        assert_eq!(result, b"abcabc");
    }

    #[test]
    fn lzf_decompress_length_mismatch_errors() {
        let input = vec![0x01, b'H', b'i'];
        assert!(lzf_decompress(&input, 10).is_err());
    }

    // -- Length encoding tests --

    #[test]
    fn encode_decode_length_6bit() {
        for len in [0, 1, 14, 63] {
            let encoded = encode_length(len);
            let mut cursor = Cursor::new(&encoded);
            assert_eq!(decode_length(&mut cursor).unwrap(), len);
        }
    }

    #[test]
    fn encode_decode_length_14bit() {
        for len in [64, 256, 1024, 16383] {
            let encoded = encode_length(len);
            let mut cursor = Cursor::new(&encoded);
            assert_eq!(decode_length(&mut cursor).unwrap(), len);
        }
    }

    #[test]
    fn encode_decode_length_32bit() {
        for len in [16384, 65536, 1 << 20] {
            let encoded = encode_length(len);
            let mut cursor = Cursor::new(&encoded);
            assert_eq!(decode_length(&mut cursor).unwrap(), len);
        }
    }

    #[test]
    fn decode_special_encoding_types() {
        // 0xC0 = 0b11_000000 → Special(0) = INT8
        let mut cursor = Cursor::new(vec![0xC0]);
        assert_eq!(
            decode_length_or_special(&mut cursor).unwrap(),
            LengthOrSpecial::Special(0)
        );

        // 0xC1 = Special(1) = INT16
        let mut cursor = Cursor::new(vec![0xC1]);
        assert_eq!(
            decode_length_or_special(&mut cursor).unwrap(),
            LengthOrSpecial::Special(1)
        );

        // 0xC2 = Special(2) = INT32
        let mut cursor = Cursor::new(vec![0xC2]);
        assert_eq!(
            decode_length_or_special(&mut cursor).unwrap(),
            LengthOrSpecial::Special(2)
        );

        // 0xC3 = Special(3) = LZF
        let mut cursor = Cursor::new(vec![0xC3]);
        assert_eq!(
            decode_length_or_special(&mut cursor).unwrap(),
            LengthOrSpecial::Special(3)
        );
    }

    // -- String encoding tests --

    #[test]
    fn read_string_raw() {
        let mut cursor = Cursor::new(vec![0x05, b'h', b'e', b'l', b'l', b'o']);
        assert_eq!(read_string(&mut cursor).unwrap(), b"hello");
    }

    #[test]
    fn read_string_empty() {
        let mut cursor = Cursor::new(vec![0x00]);
        assert_eq!(read_string(&mut cursor).unwrap(), b"");
    }

    #[test]
    fn read_string_int8() {
        let mut cursor = Cursor::new(vec![0xC0, 42]);
        assert_eq!(read_string(&mut cursor).unwrap(), b"42");
    }

    #[test]
    fn read_string_int8_negative() {
        let mut cursor = Cursor::new(vec![0xC0, 0xFE]); // -2 as i8
        assert_eq!(read_string(&mut cursor).unwrap(), b"-2");
    }

    #[test]
    fn read_string_int16() {
        // 0xC1 = int16, 0xE8 0x03 = 1000 in LE
        let mut cursor = Cursor::new(vec![0xC1, 0xE8, 0x03]);
        assert_eq!(read_string(&mut cursor).unwrap(), b"1000");
    }

    #[test]
    fn read_string_int16_negative() {
        // -1000 as i16 LE = 0x18 0xFC
        let mut cursor = Cursor::new(vec![0xC1, 0x18, 0xFC]);
        assert_eq!(read_string(&mut cursor).unwrap(), b"-1000");
    }

    #[test]
    fn read_string_int32() {
        // 0xC2 = int32, 1000000 = 0x40420F00 in LE
        let mut cursor = Cursor::new(vec![0xC2, 0x40, 0x42, 0x0F, 0x00]);
        assert_eq!(read_string(&mut cursor).unwrap(), b"1000000");
    }

    // -- Round-trip tests --

    #[test]
    fn round_trip_empty_storage() -> Result<()> {
        let storage = Storage::new(HashMap::new());

        let mut buffer = Vec::new();
        to_rdb(&storage, &mut Cursor::new(&mut buffer))?;
        let loaded = from_rdb(Cursor::new(&buffer))?;

        assert_eq!(loaded.data.len(), 0);
        Ok(())
    }

    #[test]
    fn round_trip_string_keys() -> Result<()> {
        let mut data: HashMap<String, StoredValue> = HashMap::new();
        data.insert("key1".into(), StoredValue::from(b"value1".to_vec(), None)?);
        data.insert("key2".into(), StoredValue::from(b"hello".to_vec(), None)?);
        data.insert("key3".into(), StoredValue::from(vec![0x01, 0x02, 0x03], None)?);
        let storage = Storage::new(data);

        let mut buffer = Vec::new();
        to_rdb(&storage, &mut Cursor::new(&mut buffer))?;
        let loaded = from_rdb(Cursor::new(&buffer))?;

        assert_eq!(storage.to_pairs(), loaded.to_pairs());
        Ok(())
    }

    #[test]
    fn round_trip_with_expiry() -> Result<()> {
        let mut data: HashMap<String, StoredValue> = HashMap::new();
        data.insert(
            "session".into(),
            StoredValue::from(b"data".to_vec(), Some(3_600_000))?, // 1 hour
        );
        data.insert(
            "permanent".into(),
            StoredValue::from(b"forever".to_vec(), None)?,
        );
        let storage = Storage::new(data);

        let mut buffer = Vec::new();
        to_rdb(&storage, &mut Cursor::new(&mut buffer))?;
        let loaded = from_rdb(Cursor::new(&buffer))?;

        assert_eq!(loaded.data.get("permanent").unwrap().value, b"forever");
        assert_eq!(loaded.data.get("session").unwrap().value, b"data");
        assert!(loaded.data.get("session").unwrap().expires_at_ms().is_some());
        Ok(())
    }

    #[test]
    fn round_trip_many_keys() -> Result<()> {
        let mut data: HashMap<String, StoredValue> = HashMap::new();
        for i in 0..100 {
            data.insert(
                format!("key:{}", i),
                StoredValue::from(format!("value:{}", i).into_bytes(), None)?,
            );
        }
        let storage = Storage::new(data);

        let mut buffer = Vec::new();
        to_rdb(&storage, &mut Cursor::new(&mut buffer))?;
        let loaded = from_rdb(Cursor::new(&buffer))?;

        assert_eq!(storage.to_pairs(), loaded.to_pairs());
        Ok(())
    }

    // -- Format compliance tests --

    #[test]
    fn header_format() -> Result<()> {
        let storage = Storage::new(HashMap::new());
        let mut buffer = Vec::new();
        to_rdb(&storage, &mut Cursor::new(&mut buffer))?;
        assert_eq!(&buffer[0..9], b"REDIS0009");
        Ok(())
    }

    #[test]
    fn checksum_is_little_endian() -> Result<()> {
        let storage = Storage::new(HashMap::new());
        let mut buffer = Vec::new();
        to_rdb(&storage, &mut Cursor::new(&mut buffer))?;

        let data_len = buffer.len() - 8;
        let expected_crc = crc64(&buffer[..data_len]);
        let stored_crc = u64::from_le_bytes(buffer[data_len..].try_into().unwrap());
        assert_eq!(expected_crc, stored_crc);
        Ok(())
    }

    #[test]
    fn disabled_checksum_accepted() -> Result<()> {
        let storage = Storage::new(HashMap::new());
        let mut buffer = Vec::new();
        to_rdb(&storage, &mut Cursor::new(&mut buffer))?;

        // Replace checksum with all zeros (disabled)
        let data_len = buffer.len() - 8;
        for i in data_len..buffer.len() {
            buffer[i] = 0;
        }

        let loaded = from_rdb(Cursor::new(&buffer))?;
        assert_eq!(loaded.data.len(), 0);
        Ok(())
    }

    #[test]
    fn corrupted_checksum_rejected() -> Result<()> {
        let storage = Storage::new(HashMap::new());
        let mut buffer = Vec::new();
        to_rdb(&storage, &mut Cursor::new(&mut buffer))?;

        // Corrupt the checksum
        let last = buffer.len() - 1;
        buffer[last] ^= 0xFF;

        assert!(from_rdb(Cursor::new(&buffer)).is_err());
        Ok(())
    }

    // -- Hand-crafted RDB parsing tests --

    /// Helper: build a valid RDB file from raw parts and compute the CRC64 checksum.
    fn build_rdb(body: &[u8]) -> Vec<u8> {
        let mut rdb = Vec::new();
        rdb.extend_from_slice(b"REDIS0009");
        rdb.extend_from_slice(body);
        rdb.push(RDB_OPCODE_EOF);
        let checksum = crc64(&rdb);
        rdb.extend_from_slice(&checksum.to_le_bytes());
        rdb
    }

    #[test]
    fn parse_aux_fields() -> Result<()> {
        let mut body = Vec::new();
        // AUX: redis-ver = 7.0.0
        body.push(RDB_OPCODE_AUX);
        write_string(&mut body, b"redis-ver");
        write_string(&mut body, b"7.0.0");
        // AUX: redis-bits = 64
        body.push(RDB_OPCODE_AUX);
        write_string(&mut body, b"redis-bits");
        write_string(&mut body, b"64");
        // DB selector + resize
        body.push(RDB_OPCODE_SELECTDB);
        body.extend(encode_length(0));
        body.push(RDB_OPCODE_RESIZEDB);
        body.extend(encode_length(1));
        body.extend(encode_length(0));
        // One string key
        body.push(RDB_TYPE_STRING);
        write_string(&mut body, b"mykey");
        write_string(&mut body, b"myval");

        let rdb = build_rdb(&body);
        let loaded = from_rdb(Cursor::new(&rdb))?;
        assert_eq!(loaded.data.get("mykey").unwrap().value, b"myval");
        Ok(())
    }

    #[test]
    fn parse_expiry_milliseconds() -> Result<()> {
        let mut body = Vec::new();
        body.push(RDB_OPCODE_SELECTDB);
        body.extend(encode_length(0));
        body.push(RDB_OPCODE_RESIZEDB);
        body.extend(encode_length(1));
        body.extend(encode_length(1));

        // Key with far-future expiry (year ~2100)
        let far_future_ms: u64 = 4_102_444_800_000;
        body.push(RDB_OPCODE_EXPIRETIMEMS);
        body.extend_from_slice(&far_future_ms.to_le_bytes());
        body.push(RDB_TYPE_STRING);
        write_string(&mut body, b"session");
        write_string(&mut body, b"active");

        let rdb = build_rdb(&body);
        let loaded = from_rdb(Cursor::new(&rdb))?;
        assert_eq!(loaded.data.get("session").unwrap().value, b"active");
        assert!(loaded.data.get("session").unwrap().expires_at_ms().is_some());
        Ok(())
    }

    #[test]
    fn parse_expiry_seconds() -> Result<()> {
        let mut body = Vec::new();
        body.push(RDB_OPCODE_SELECTDB);
        body.extend(encode_length(0));
        body.push(RDB_OPCODE_RESIZEDB);
        body.extend(encode_length(1));
        body.extend(encode_length(1));

        // Key with far-future expiry (year ~2038+)
        let far_future_sec: u32 = 4_102_444_800;
        body.push(RDB_OPCODE_EXPIRETIME);
        body.extend_from_slice(&far_future_sec.to_le_bytes());
        body.push(RDB_TYPE_STRING);
        write_string(&mut body, b"persistent");
        write_string(&mut body, b"data");

        let rdb = build_rdb(&body);
        let loaded = from_rdb(Cursor::new(&rdb))?;
        assert_eq!(loaded.data.get("persistent").unwrap().value, b"data");
        Ok(())
    }

    #[test]
    fn expired_keys_filtered_on_load() -> Result<()> {
        let mut body = Vec::new();
        body.push(RDB_OPCODE_SELECTDB);
        body.extend(encode_length(0));
        body.push(RDB_OPCODE_RESIZEDB);
        body.extend(encode_length(2));
        body.extend(encode_length(1));

        // Expired key: timestamp in the past (year 2000)
        let past_ms: u64 = 946_684_800_000;
        body.push(RDB_OPCODE_EXPIRETIMEMS);
        body.extend_from_slice(&past_ms.to_le_bytes());
        body.push(RDB_TYPE_STRING);
        write_string(&mut body, b"expired_key");
        write_string(&mut body, b"gone");

        // Non-expired key
        body.push(RDB_TYPE_STRING);
        write_string(&mut body, b"alive_key");
        write_string(&mut body, b"here");

        let rdb = build_rdb(&body);
        let loaded = from_rdb(Cursor::new(&rdb))?;

        assert!(loaded.data.get("expired_key").is_none());
        assert_eq!(loaded.data.get("alive_key").unwrap().value, b"here");
        Ok(())
    }

    #[test]
    fn parse_integer_encoded_values() -> Result<()> {
        let mut body = Vec::new();
        body.push(RDB_OPCODE_SELECTDB);
        body.extend(encode_length(0));
        body.push(RDB_OPCODE_RESIZEDB);
        body.extend(encode_length(3));
        body.extend(encode_length(0));

        // Key with int8-encoded value: 42
        body.push(RDB_TYPE_STRING);
        write_string(&mut body, b"int8_key");
        body.push(0xC0); // special encoding: int8
        body.push(42);

        // Key with int16-encoded value: 1000
        body.push(RDB_TYPE_STRING);
        write_string(&mut body, b"int16_key");
        body.push(0xC1); // special encoding: int16
        body.extend_from_slice(&1000i16.to_le_bytes());

        // Key with int32-encoded value: 1000000
        body.push(RDB_TYPE_STRING);
        write_string(&mut body, b"int32_key");
        body.push(0xC2); // special encoding: int32
        body.extend_from_slice(&1000000i32.to_le_bytes());

        let rdb = build_rdb(&body);
        let loaded = from_rdb(Cursor::new(&rdb))?;

        assert_eq!(loaded.data.get("int8_key").unwrap().value, b"42");
        assert_eq!(loaded.data.get("int16_key").unwrap().value, b"1000");
        assert_eq!(loaded.data.get("int32_key").unwrap().value, b"1000000");
        Ok(())
    }

    #[test]
    fn skip_unsupported_value_types() -> Result<()> {
        let mut body = Vec::new();
        body.push(RDB_OPCODE_SELECTDB);
        body.extend(encode_length(0));
        body.push(RDB_OPCODE_RESIZEDB);
        body.extend(encode_length(3));
        body.extend(encode_length(0));

        // A Hash entry (type 4) — should be skipped
        body.push(RDB_TYPE_HASH);
        write_string(&mut body, b"myhash");
        body.extend(encode_length(2)); // 2 field-value pairs
        write_string(&mut body, b"field1");
        write_string(&mut body, b"val1");
        write_string(&mut body, b"field2");
        write_string(&mut body, b"val2");

        // A List entry (type 1) — should be skipped
        body.push(RDB_TYPE_LIST);
        write_string(&mut body, b"mylist");
        body.extend(encode_length(2));
        write_string(&mut body, b"item1");
        write_string(&mut body, b"item2");

        // A String entry — should be loaded
        body.push(RDB_TYPE_STRING);
        write_string(&mut body, b"mystring");
        write_string(&mut body, b"hello");

        let rdb = build_rdb(&body);
        let loaded = from_rdb(Cursor::new(&rdb))?;

        assert!(loaded.data.get("myhash").is_none());
        assert!(loaded.data.get("mylist").is_none());
        assert_eq!(loaded.data.get("mystring").unwrap().value, b"hello");
        Ok(())
    }

    #[test]
    fn skip_compact_encoded_types() -> Result<()> {
        let mut body = Vec::new();
        body.push(RDB_OPCODE_SELECTDB);
        body.extend(encode_length(0));

        // A Ziplist entry (type 10) — stored as single string blob, should be skipped
        body.push(RDB_TYPE_ZIPLIST);
        write_string(&mut body, b"myziplist");
        write_string(&mut body, b"\x00\x00\x00\x00"); // fake ziplist blob

        // A String entry
        body.push(RDB_TYPE_STRING);
        write_string(&mut body, b"real");
        write_string(&mut body, b"value");

        let rdb = build_rdb(&body);
        let loaded = from_rdb(Cursor::new(&rdb))?;
        assert_eq!(loaded.data.get("real").unwrap().value, b"value");
        Ok(())
    }

    #[test]
    fn parse_multiple_databases() -> Result<()> {
        let mut body = Vec::new();

        // DB 0
        body.push(RDB_OPCODE_SELECTDB);
        body.extend(encode_length(0));
        body.push(RDB_TYPE_STRING);
        write_string(&mut body, b"db0_key");
        write_string(&mut body, b"db0_val");

        // DB 1
        body.push(RDB_OPCODE_SELECTDB);
        body.extend(encode_length(1));
        body.push(RDB_TYPE_STRING);
        write_string(&mut body, b"db1_key");
        write_string(&mut body, b"db1_val");

        let rdb = build_rdb(&body);
        let loaded = from_rdb(Cursor::new(&rdb))?;

        // Both are loaded into our single-db storage
        assert_eq!(loaded.data.get("db0_key").unwrap().value, b"db0_val");
        assert_eq!(loaded.data.get("db1_key").unwrap().value, b"db1_val");
        Ok(())
    }

    #[test]
    fn parse_older_rdb_version() -> Result<()> {
        // Version 0003: no aux/resize, just DB selector + entries + EOF + CRC
        let mut rdb = Vec::new();
        rdb.extend_from_slice(b"REDIS0003");
        rdb.push(RDB_OPCODE_SELECTDB);
        rdb.extend(encode_length(0));
        rdb.push(RDB_TYPE_STRING);
        write_string(&mut rdb, b"old_key");
        write_string(&mut rdb, b"old_val");
        rdb.push(RDB_OPCODE_EOF);
        let checksum = crc64(&rdb);
        rdb.extend_from_slice(&checksum.to_le_bytes());

        let loaded = from_rdb(Cursor::new(&rdb))?;
        assert_eq!(loaded.data.get("old_key").unwrap().value, b"old_val");
        Ok(())
    }

    #[test]
    fn write_includes_aux_and_resize() -> Result<()> {
        let mut data: HashMap<String, StoredValue> = HashMap::new();
        data.insert("k".into(), StoredValue::from(b"v".to_vec(), None)?);
        let storage = Storage::new(data);

        let mut buffer = Vec::new();
        to_rdb(&storage, &mut Cursor::new(&mut buffer))?;

        // Verify aux fields are present in the output
        let rdb_str = String::from_utf8_lossy(&buffer);
        assert!(buffer.contains(&RDB_OPCODE_AUX));
        assert!(rdb_str.contains("redis-ver"));
        assert!(buffer.contains(&RDB_OPCODE_RESIZEDB));
        Ok(())
    }

    #[test]
    fn write_includes_expiry_when_present() -> Result<()> {
        let mut data: HashMap<String, StoredValue> = HashMap::new();
        data.insert(
            "expiring".into(),
            StoredValue::from(b"val".to_vec(), Some(60_000))?,
        );
        let storage = Storage::new(data);

        let mut buffer = Vec::new();
        to_rdb(&storage, &mut Cursor::new(&mut buffer))?;

        assert!(buffer.contains(&RDB_OPCODE_EXPIRETIMEMS));
        Ok(())
    }
}
