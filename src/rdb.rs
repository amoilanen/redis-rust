use std::io::{BufReader, Read, Write};
use anyhow::{anyhow, ensure, Context, Error, Result };
use std::collections::HashMap;
use crc64::Crc64;
use crate::storage::{Storage, StoredValue};

// This is not a complete RDB format implementation, but rather a truncated/simplified version of it:
// only a single database, all values are assumed to be Strings, expiration information is not encoded
// Format explanation https://github.com/sripathikrishnan/redis-rdb-tools/wiki/Redis-RDB-Dump-File-Format

//TODO: Implement support of value expiration encoding
pub fn to_rdb<W>(storage: &Storage, output: &mut W) -> Result<(), Error>
where W: Write {
    let mut result = Vec::new();
    //Header
    let header = format!("REDIS0007");
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
    let mut values: HashMap<String, Vec<u8>> = HashMap::new();
    let mut rdb_bytes: Vec<u8> = Vec::new();
    let mut reader = BufReader::new(input);
    let mut header= [0; 9];
    reader.read_exact(&mut header)?;
    rdb_bytes.extend(&header);
    ensure!(header.starts_with(b"REDIS"), "{:?} must start with {:?}", header, b"REDIS");

    let mut db_selector = [0; 2];
    reader.read_exact(&mut db_selector)?;
    ensure!(db_selector == [0xFE, 0x00], "{:?} not supported db selector", db_selector);
    rdb_bytes.extend(&db_selector);

    let mut next_byte = [0; 1];
    reader.read_exact(&mut next_byte)?;
    while next_byte[0] == 0x00 {
        rdb_bytes.extend(&next_byte);
        let (key_length, key_length_bytes) = decode_length(&mut reader)?;
        rdb_bytes.extend(&key_length_bytes);
        let mut key = vec![0; key_length];
        reader.read_exact(&mut key)?;
        //println!("key_length = {}, key = {:?}", key_length, key);
        rdb_bytes.extend(&key);

        let (value_length, value_length_bytes) = decode_length(&mut reader)?;
        rdb_bytes.extend(&value_length_bytes);
        let mut value = vec![0; value_length];
        reader.read_exact(&mut value)?;
        //println!("value_length = {}, value = {:?}", value_length, value);
        rdb_bytes.extend(&value);

        values.insert(String::from_utf8(key)?, value);

        reader.read_exact(&mut next_byte)?;
    }
    ensure!(next_byte == [0xFF], "{:?} is not an RDB end", next_byte);
    rdb_bytes.extend(&next_byte);

    let mut checksum = [0; 8];
    reader.read_exact(&mut checksum)?;
    verify_checksum(&rdb_bytes, u64::from_be_bytes(checksum))?;

    let mut data: HashMap<String, StoredValue> = HashMap::new();
    for (key, value) in values.into_iter() {
        data.insert(key, StoredValue::from(value, None)?);
    }
    Ok(Storage {
        data
    })
}

fn verify_checksum(bytes: &[u8], checksum: u64) -> Result<(), Error> {
    let mut checksum_calculator = Crc64::new();
    checksum_calculator.write(bytes)?;
    let computed = checksum_calculator.get();
    ensure!(computed == checksum, "Expected checksum {}, instead got {}, RDB file is corrupted?", checksum, computed);
    Ok(())
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

fn decode_length<R: Read>(reader: &mut R) -> Result<(usize, Vec<u8>), Error> {
    let mut first_byte = [0; 1];
    reader.read_exact(&mut first_byte).context(format!("Could not read first length byte"))?;
    let prefix = first_byte[0] >> 6;

    match prefix {
        0b00 => Ok((first_byte[0] as usize, first_byte.to_vec())),
        0b01 => {
            let mut second_byte = [0; 1];
            reader.read_exact(&mut second_byte).context(format!("Could not read second lenght byte"))?;
            let len = ((first_byte[0] & 0b00111111) as u16) << 8 | (second_byte[0] as u16);
            Ok((len as usize, vec![first_byte[0], second_byte[0]]))
        },
        0b10 => {
            let mut encoded_length = [0; 4];
            reader.read_exact(&mut encoded_length).context("encoded length must contain 4 bytes")?;
            Ok((u32::from_be_bytes(encoded_length) as usize, vec![first_byte.to_vec(), encoded_length.to_vec()].concat()))
        }
        0b11 => Err(anyhow!("Special encoding not implemented, failed to parse length")),
        _ => unreachable!()
    }
}

#[cfg(test)]
mod tests {

    use std::{collections::HashMap, io::Cursor};
    use crate::storage::{Storage, StoredValue};

    use super::{from_rdb, to_rdb, encode_length, decode_length};

    fn test_encode_decode(len: usize) {
        let mut encoded = encode_length(len);
        let mut cursor = Cursor::new(&mut encoded);
        assert_eq!(decode_length(&mut cursor).unwrap(), (len, encoded))
    }

    #[test]
    fn should_encode_and_decode_length() {
        test_encode_decode(14);
        test_encode_decode(1 << 10);
        test_encode_decode(1 << 15);
    }

    #[test]
    fn should_serialize_and_deserialize_empty_storage() {
        let storage = Storage::new(HashMap::new());

        let mut buffer: Vec<u8> = Vec::new();
        let mut writer = Cursor::new(&mut buffer);
        to_rdb(&storage, &mut writer).unwrap();
        
        let mut reader = Cursor::new(&mut buffer);
        let deserialized_storage = from_rdb(&mut reader).unwrap();

        assert_eq!(storage, deserialized_storage);
    }

    #[test]
    fn should_serialize_and_deserialize_storage_containing_strings_and_numbers() {
        let mut data: HashMap<String, StoredValue> = HashMap::new();
        data.insert("key1".to_owned(), StoredValue::from(5u64.to_be_bytes().to_vec(), None).unwrap());
        data.insert("key2".to_owned(), StoredValue::from("abcde".as_bytes().to_vec(), None).unwrap());
        data.insert("key3".to_owned(), StoredValue::from(vec![0x01, 0x02, 0x03], None).unwrap());
        let storage = Storage::new(data);

        let mut buffer: Vec<u8> = Vec::new();
        let mut writer = Cursor::new(&mut buffer);
        to_rdb(&storage, &mut writer).unwrap();

        let mut reader = Cursor::new(&mut buffer);
        let deserialized_storage = from_rdb(&mut reader).unwrap();

        assert_eq!(storage.to_pairs(), deserialized_storage.to_pairs());
    }

    #[test]
    fn should_parse_rdb_received_from_test_server() {
        //TODO:
        let mut buffer: Vec<u8>  = vec![82, 69, 68, 73, 83, 48, 48, 49, 49, 250, 9, 114, 101, 100, 105, 115, 45, 118, 101, 114, 5, 55, 46, 50, 46, 48, 250, 10, 114, 101, 100, 105, 115, 45, 98, 105, 116, 115, 192, 64, 250, 5, 99, 116, 105, 109, 101, 194, 109, 8, 188, 101, 250, 8, 117, 115, 101, 100, 45, 109, 101, 109, 194, 176, 196, 16, 0, 250, 8, 97, 111, 102, 45, 98, 97, 115, 101, 192, 0, 255, 240, 110, 59, 254, 192, 255, 90, 162];
        let mut reader = Cursor::new(&mut buffer);
        let deserialized_storage = from_rdb(&mut reader).unwrap();

        println!("{:?}", deserialized_storage);
        assert_eq!(deserialized_storage, deserialized_storage);
    }
}