use std::io::{Read, Write};
use anyhow::{anyhow, ensure, Error, Result };
use std::collections::HashMap;
use crc64::Crc64;
use crate::storage::Storage;

// This is not a complete RDB format implementation, but rather a truncated/simplified version of it:
// only a single database, all values are assumed to be Strings, expiration information is not encoded
// Format explanation https://github.com/sripathikrishnan/redis-rdb-tools/wiki/Redis-RDB-Dump-File-Format

pub fn to_rdb<W>(storage: &Storage, output: &mut W) -> Result<(), Error>
where W: Write {
    let mut result = Vec::new();
    //Header
    let header = format!("REDIS0007\r\n");
    result.extend(header.as_bytes());
    //Database selector: first database 0x00
    result.extend(&[0xFE, 0x00]);
    for (key, stored_value) in storage.data.iter() {
        //For simplicity designating value type as String though it might not actually be a string
        result.push(0x00);
        result.extend(encode_length(key.len()));
        result.extend(key.as_bytes());
        result.extend(encode_length(stored_value.value.len()));
        result.extend(&stored_value.value);
    }
    //End of RDB file marker
    result.push(0xFF);
    output.write_all(&result)?;
    output.write_all(&compute_checksum(&result)?.to_be_bytes())?;
    Ok(())
}

fn compute_checksum(bytes: &Vec<u8>) -> Result<u64, Error> {
    let mut checksum_calculator = Crc64::new();
    checksum_calculator.write(bytes)?;
    Ok(checksum_calculator.get())
}

pub fn from_rdb<R>(input: R) -> Result<Storage, Error>
where R: Read {
    //TODO: Implement parsing
    Ok(Storage {
        data: HashMap::new()
    })
}

fn encode_length(len: usize) -> Vec<u8> {
    let mut len_encoding: Vec<u8> = Vec::new();
    if len < 1 << 6 {
        len_encoding.push(len as u8);
    } else if len < 1 << 14 {
        let first_encoded_byte = (1 << 6) | (len >> 8) as u8;
        let second_encoded_byte = (len & 0xff) as u8;
        len_encoding.extend(&[first_encoded_byte, second_encoded_byte]);
    } else {
        len_encoding.push(1 << 7);
        len_encoding.extend(&(len as u32).to_be_bytes());
    }
    len_encoding
}

fn decode_length(encoded: &[u8]) -> Result<usize, Error> {
    let first_byte = encoded.get(0).ok_or(anyhow!("{:?} should have at least one byte", encoded))?;
    let prefix = first_byte >> 6;

    match prefix {
        0b00 => Ok(*first_byte as usize),
        0b01 => {
            let second_byte = encoded.get(1).ok_or(anyhow!("{:?} should have at least two bytes", encoded))?;
            let len = ((first_byte & 0b00111111) as u16) << 8 | (*second_byte as u16);
            Ok(len as usize)
        },
        0b10 => {
            ensure!(encoded.len() >= 5, "encoded length must contain at least 5 bytes");
            let len: [u8; 4] = encoded[1..5].try_into()?;
            Ok(u32::from_be_bytes(len) as usize)
        }
        0b11 => Err(anyhow!("Special encoding not implemented, failed to parse length {:?}", encoded)),
        _ => unreachable!()
    }
}

#[cfg(test)]
mod tests {

    //TODO: Test length encoding and decoding

    #[test]
    fn should_serialize_and_deserialize_empty_storage() {
        //TODO:
    }

    #[test]
    fn should_serialize_and_deserialize_storage_containing_strings_and_numbers() {
        //TODO:
    }
}