//! RDB fixtures for the E2E suites: a file on disk for a server under test to
//! load, and the server started on top of it.

use super::{find_free_port, temp_dir, write_file, ServerProcess, TempDir};

/// The file name a server under test is told to keep its RDB file in.
pub const RDB_FILENAME: &str = "dump.rdb";

/// Build a complete RDB file holding `pairs`, byte for byte as the spec has
/// it: `REDIS0011`, one aux field, a `SELECTDB 0`, a `RESIZEDB` hint, a
/// `<type><key><value>` record per pair, `EOF`, and the CRC64 of everything
/// before it.
///
/// Assembled by hand from the opcodes rather than through this crate's own
/// writer: the file the tester hands the server is written by real Redis, and
/// a fixture produced by the code under test could agree with the reader about
/// a format neither has right. It lives here, out of `src`, for the same
/// reason - close to `to_rdb` it would sooner or later be replaced by it.
pub fn rdb_file(pairs: &[(&str, &str)]) -> Vec<u8> {
    use codecrafters_redis::rdb::{crc64, encode_length, write_string};

    let mut rdb = Vec::new();
    rdb.extend_from_slice(b"REDIS0011");

    // AUX redis-ver 7.2.0
    rdb.push(0xFA);
    write_string(&mut rdb, b"redis-ver");
    write_string(&mut rdb, b"7.2.0");

    // SELECTDB 0
    rdb.push(0xFE);
    rdb.extend(encode_length(0));

    // RESIZEDB: this many keys, none of them with an expiry
    rdb.push(0xFB);
    rdb.extend(encode_length(pairs.len()));
    rdb.extend(encode_length(0));

    for (key, value) in pairs {
        rdb.push(0x00); // type: string
        write_string(&mut rdb, key.as_bytes());
        write_string(&mut rdb, value.as_bytes());
    }

    rdb.push(0xFF); // EOF
    let checksum = crc64(&rdb);
    rdb.extend_from_slice(&checksum.to_le_bytes());
    rdb
}

/// A master started on a temp directory holding an RDB file with `pairs` in
/// it - the command line the tester runs, with a real file at the end of it.
///
/// The directory comes back alongside the server and has to outlive it: it
/// deletes itself when dropped.
pub fn server_loaded_from_rdb(pairs: &[(&str, &str)]) -> (ServerProcess, TempDir) {
    let dir = temp_dir();
    write_file(&dir, RDB_FILENAME, &rdb_file(pairs));
    let server = ServerProcess::start_master_with_rdb_file(find_free_port(), dir.path(), RDB_FILENAME);
    (server, dir)
}
