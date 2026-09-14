/// E2E tests for master-replica replication.
///
/// These tests start a master and multiple replicas, write data to the master,
/// and verify it appears on all replicas.

mod common;

use anyhow::Result;
use common::{
    find_free_port, start_master_and_replicas, RespClient, ServerProcess,
    REPLICATION_PROPAGATION_WAIT,
};
use std::collections::HashMap;
use std::net::TcpListener;
use std::thread;
use std::time::{Duration, Instant};

use codecrafters_redis::storage::Storage;

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

// ========================= Replication offset =========================

/// A stand-in master, driven by the test itself.
///
/// A real master never sends `REPLCONF GETACK`, so the offsets a replica
/// acknowledges can only be checked by writing the replication stream by hand:
/// this answers the handshake and then sends whatever the test asks it to.
struct FakeMaster {
    listener: TcpListener,
    port: u16,
}

impl FakeMaster {
    /// Binds a port and starts listening, so a replica pointed at
    /// [`FakeMaster::port`] can connect straight away.
    fn start() -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let port = listener.local_addr()?.port();
        Ok(Self { listener, port })
    }

    /// Accepts the replica and answers its handshake, ending in FULLRESYNC and
    /// an empty RDB snapshot - the point from which the offset counts.
    ///
    /// The returned client is the master's end of the replication stream: the
    /// two ends speak the same RESP, so driving a replica needs nothing a
    /// client driving a server does not already do.
    fn accept_replica(&self) -> Result<RespClient> {
        let (stream, _) = self.listener.accept()?;
        let mut replica = RespClient::from_stream(stream)?;

        expect_command(&mut replica, &["PING"])?;
        replica.write_raw(b"+PONG\r\n")?;
        expect_command(&mut replica, &["REPLCONF", "listening-port"])?;
        replica.write_raw(b"+OK\r\n")?;
        expect_command(&mut replica, &["REPLCONF", "capa"])?;
        replica.write_raw(b"+OK\r\n")?;
        expect_command(&mut replica, &["PSYNC"])?;
        replica.write_raw(b"+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n")?;

        // A bulk string without the trailing \r\n, as a master sends it.
        let snapshot = Storage::new(HashMap::new()).to_rdb()?;
        replica.write_raw(format!("${}\r\n", snapshot.len()).as_bytes())?;
        replica.write_raw(&snapshot)?;

        Ok(replica)
    }
}

/// Reads one command the replica sent up the replication link, checks it opens
/// with `expected`, and returns the arguments that follow.
///
/// `link` is the master's end of the replication stream (see
/// [`FakeMaster::accept_replica`]), so what it reads is what the replica sent.
///
/// Command names are matched the way Redis matches them, ignoring case. The
/// caller decides how much of the command to name: everything, when only the
/// command matters, or just the leading words whose arguments it wants back.
fn expect_command(link: &mut RespClient, expected: &[&str]) -> Result<Vec<String>> {
    let mut command = link.read_response_parts()?;
    let opens_as_expected = command.len() >= expected.len()
        && command
            .iter()
            .zip(expected)
            .all(|(part, expected_part)| part.eq_ignore_ascii_case(expected_part));
    if !opens_as_expected {
        anyhow::bail!("expected {:?} from the replica, got {:?}", expected, command);
    }
    Ok(command.split_off(expected.len()))
}

/// Reads the offset out of the replica's `REPLCONF ACK <offset>` reply.
fn read_ack(link: &mut RespClient) -> Result<usize> {
    let arguments = expect_command(link, &["REPLCONF", "ACK"])?;
    let offset = arguments
        .first()
        .ok_or_else(|| anyhow::anyhow!("REPLCONF ACK arrived without an offset"))?;
    Ok(offset.parse()?)
}

#[test]
fn test_replica_acknowledges_the_bytes_it_has_processed() -> Result<()> {
    let master = FakeMaster::start()?;
    // The replica process. Kept alive for the test's duration but never queried
    // directly here; the test drives it through the replication link below.
    let _replica_server = ServerProcess::start_replica(find_free_port(), master.port);
    // The master's end of the replication stream: writing here propagates a
    // command *down* to the replica, exactly as a real master would.
    let mut replication_link = master.accept_replica()?;

    // Nothing has been processed yet, so the very first request acknowledges 0.
    replication_link.write_command(&["REPLCONF", "GETACK", "*"])?;
    assert_eq!(read_ack(&mut replication_link)?, 0);

    // 37 for the GETACK just answered, plus 14 for a PING processed silently.
    replication_link.write_command(&["PING"])?;
    replication_link.write_command(&["REPLCONF", "GETACK", "*"])?;
    assert_eq!(read_ack(&mut replication_link)?, 51);

    // 51 + 37 for the second GETACK + 29 for each SET.
    replication_link.write_command(&["SET", "foo", "1"])?;
    replication_link.write_command(&["SET", "bar", "2"])?;
    replication_link.write_command(&["REPLCONF", "GETACK", "*"])?;
    assert_eq!(read_ack(&mut replication_link)?, 146);

    Ok(())
}

#[test]
fn test_replica_applies_the_commands_it_counts() -> Result<()> {
    let master = FakeMaster::start()?;
    let replica_server = ServerProcess::start_replica(find_free_port(), master.port);

    // Two *different* sockets reach this replica, pointing opposite ways:
    //   * `replication_link` is the master's end of the replication stream. The
    //     replica dialled *out* to us during its handshake, so the test plays the
    //     master here: writing a command propagates it *down* to the replica,
    //     which applies it silently and answers only with `REPLCONF ACK`.
    //   * `replica_server.client()` (below) dials *in* to the replica's own port
    //     as an ordinary client. That front door is the only socket that answers
    //     a GET — and the only one a plain `SET` here would be rejected on, since
    //     a replica is read-only.
    // The two are therefore NOT interchangeable: one pushes as the master, the
    // other asks as a client. This test proves that a write pushed in over
    // replication becomes visible to a front-door client.
    let mut replication_link = master.accept_replica()?;

    // Propagate a write down the replication link, as a real master would.
    replication_link.write_command(&["SET", "counted", "value"])?;
    // The acknowledgement proves the SET was processed, so no sleep is needed.
    replication_link.write_command(&["REPLCONF", "GETACK", "*"])?;
    assert_eq!(read_ack(&mut replication_link)?, 37);

    // Query the replica through its front door; the propagated write is there.
    let mut client = replica_server.client();
    assert_eq!(client.send_command(&["GET", "counted"])?, "value");
    Ok(())
}

// ========================= WAIT =========================

#[test]
fn test_wait_without_replicas_answers_zero_immediately() -> Result<()> {
    // The tester's sequence: a master nobody replicates from is asked to wait
    // for no replicas, so there is nothing to wait for and the generous timeout
    // must not be spent.
    let master = ServerProcess::start_master(find_free_port());
    let mut client = master.client();

    let started_at = Instant::now();
    let response = client.send_command(&["WAIT", "0", "60000"])?;

    assert_eq!(response, "0");
    assert!(
        started_at.elapsed() < Duration::from_secs(1),
        "WAIT should answer immediately, took {:?}",
        started_at.elapsed()
    );
    Ok(())
}

#[test]
fn test_wait_answers_at_once_when_nothing_has_been_written() -> Result<()> {
    // Nothing has been propagated, so the three replicas that finished their
    // handshake in `start_master_and_replicas` have nothing left to process:
    // there is nothing to ask them and the timeout must not be spent.
    let (master, replicas) = start_master_and_replicas();
    let mut client = master.client();

    let started_at = Instant::now();
    let response = client.send_command(&["WAIT", "3", "60000"])?;

    assert_eq!(response, replicas.len().to_string());
    assert!(
        started_at.elapsed() < Duration::from_secs(1),
        "WAIT should answer immediately, took {:?}",
        started_at.elapsed()
    );
    Ok(())
}

/// A replica that finishes the handshake and then never says another word.
///
/// Real replicas answer `REPLCONF GETACK`, which hides a whole class of offset
/// bug: an answer drags them up to whatever the master is waiting for, right or
/// wrong. The tester's replicas at this stage stay silent, so what the master
/// waits for has to be right on its own.
///
/// Nothing sent down the link is ever read - not the FULLRESYNC, not the RDB,
/// not the requests for acknowledgement. Staying silent is the whole job.
struct SilentReplica {
    _link: RespClient,
}

impl SilentReplica {
    /// Completes the replication handshake against the master on `master_port`.
    fn join(master_port: u16) -> Result<Self> {
        let mut link = RespClient::connect(master_port);
        assert_eq!(link.send_command(&["PING"])?, "PONG");
        assert_eq!(
            link.send_command(&["REPLCONF", "listening-port", "6380"])?,
            "OK"
        );
        assert_eq!(link.send_command(&["REPLCONF", "capa", "psync2"])?, "OK");
        link.write_command(&["PSYNC", "?", "-1"])?;
        Ok(Self { _link: link })
    }
}

/// Blocks until the master has registered `expected` replicas.
///
/// `WAIT 0` asks for no replicas at all, so it is answered at once with the
/// ones that are up to date - which, while nothing has been written, is every
/// replica that has finished its PSYNC.
fn await_registered_replicas(client: &mut RespClient, expected: usize) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if client.send_command(&["WAIT", "0", "0"])? == expected.to_string() {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(20));
    }
    anyhow::bail!("the master never registered {} replicas", expected)
}

#[test]
fn test_repeated_waits_are_not_thrown_off_by_their_own_requests() -> Result<()> {
    // The tester's sequence: WAIT after WAIT with nothing written in between,
    // each asking for more replicas than the last. Every one must report the
    // replicas that are connected - with no writes outstanding they are all up
    // to date, however little they have to say for themselves.
    //
    // The regression this guards: a WAIT that cannot be satisfied asks the
    // replicas where they have got to, and that request lengthens the
    // replication stream. A WAIT waiting for the end of the stream rather than
    // for the writes in it then waits for bytes no replica was ever sent, and
    // answers 0.
    let master = ServerProcess::start_master(find_free_port());
    let replica_count = 3;
    let _replicas = (0..replica_count)
        .map(|_| SilentReplica::join(master.port))
        .collect::<Result<Vec<_>>>()?;
    let mut client = master.client();
    await_registered_replicas(&mut client, replica_count)?;

    for requested in 1..=replica_count + 2 {
        let response = client.send_command(&["WAIT", &requested.to_string(), "100"])?;

        assert_eq!(response, replica_count.to_string(), "WAIT {} 100", requested);
    }
    Ok(())
}

#[test]
fn test_wait_counts_the_replicas_that_processed_a_write() -> Result<()> {
    // The tester's sequence: a write, then a WAIT for the replicas to confirm
    // they have it. All three answer, well inside the timeout.
    let (master, replicas) = start_master_and_replicas();
    let mut client = master.client();
    client.send_command(&["SET", "foo", "123"])?;

    let started_at = Instant::now();
    let response = client.send_command(&["WAIT", "3", "2000"])?;

    assert_eq!(response, replicas.len().to_string());
    assert!(
        started_at.elapsed() < Duration::from_millis(2000),
        "WAIT should answer as soon as the replicas acknowledge, took {:?}",
        started_at.elapsed()
    );
    Ok(())
}

#[test]
fn test_wait_reports_fewer_replicas_than_asked_for_when_the_timeout_expires() -> Result<()> {
    // Asking for more replicas than exist: the timeout is spent in full and
    // the ones that did acknowledge are reported.
    let (master, replicas) = start_master_and_replicas();
    let mut client = master.client();
    client.send_command(&["SET", "foo", "123"])?;

    let started_at = Instant::now();
    let response = client.send_command(&["WAIT", "7", "500"])?;

    assert_eq!(response, replicas.len().to_string());
    assert!(
        started_at.elapsed() >= Duration::from_millis(500),
        "WAIT should have waited out its timeout, took only {:?}",
        started_at.elapsed()
    );
    Ok(())
}

#[test]
fn test_wait_counts_a_replica_that_joined_after_earlier_writes() -> Result<()> {
    // A replica joining midway counts the stream from its own snapshot, not
    // from the master's first ever write, so its acknowledgements only line up
    // with the master's offset once they are read relative to where it joined.
    let master = ServerProcess::start_master(find_free_port());
    let mut client = master.client();
    client.send_command(&["SET", "written_before_the_replica", "1"])?;

    let response = client.send_command(&["WAIT", "1", "100"])?;
    let started_at = Instant::now();
    assert_eq!(response, "0");

    let _replica = ServerProcess::start_replica(find_free_port(), master.port);
    thread::sleep(REPLICATION_PROPAGATION_WAIT);
    client.send_command(&["SET", "written_after_the_replica", "2"])?;

    let started_at_after_replica_joined = Instant::now();
    let response_after_replica_joined = client.send_command(&["WAIT", "1", "2000"])?;
    assert_eq!(response_after_replica_joined, "1");
    assert!(
        started_at_after_replica_joined.elapsed() < Duration::from_millis(2000),
        "WAIT should answer as soon as the replica acknowledges, took {:?}",
        started_at.elapsed()
    );
    Ok(())
}

#[test]
fn test_wait_keeps_up_with_writes_between_rounds() -> Result<()> {
    // Writes interleaved with WAITs, as the tester issues them: each round
    // moves the master's offset on, so each WAIT has to ask again rather than
    // answer from what the replicas reported last time.
    let (master, replicas) = start_master_and_replicas();
    let mut client = master.client();
    let expected = replicas.len().to_string();

    for round in 0..3 {
        client.send_command(&["SET", &format!("round_{}", round), "value"])?;

        let response = client.send_command(&["WAIT", "3", "2000"])?;

        assert_eq!(response, expected, "round {}", round);
    }
    Ok(())
}
