# Manual Testing Guide

This document describes how to manually test the Redis server implementation.

## Prerequisites

- **Rust toolchain** installed (`cargo` available in PATH)
- **redis-cli** installed (comes with Redis, or install via `apt install redis-tools` / `brew install redis`)
- Multiple terminal windows/tabs available

## Building the Server

```bash
cargo build
```

For a release build:

```bash
cargo build --release
```

---

## 1. Basic Server Startup

### Start a standalone master server

```bash
cargo run -- --port 6379
```

**Verify**: The server should start without errors. You can enable debug logging with:

```bash
RUST_LOG=trace cargo run -- --port 6379
```

---

## 2. PING Command

**Purpose**: Verify the server is responsive and accepting connections.

### Steps

1. Start the server:
   ```bash
   cargo run -- --port 6379
   ```

2. In another terminal, connect with `redis-cli`:
   ```bash
   redis-cli -p 6379
   ```

3. Send the PING command:
   ```
   127.0.0.1:6379> PING
   ```

**Expected result**: `PONG`

---

## 3. ECHO Command

**Purpose**: Verify the server echoes back a message.

### Steps

1. Connect to the server:
   ```bash
   redis-cli -p 6379
   ```

2. Send ECHO:
   ```
   127.0.0.1:6379> ECHO "Hello, Redis!"
   ```

**Expected result**: `"Hello, Redis!"`

---

## 4. SET and GET Commands

**Purpose**: Verify key-value storage works correctly.

### 4.1 Basic SET/GET

1. Connect to the server:
   ```bash
   redis-cli -p 6379
   ```

2. Set a value:
   ```
   127.0.0.1:6379> SET mykey myvalue
   ```
   **Expected**: `OK`

3. Get the value:
   ```
   127.0.0.1:6379> GET mykey
   ```
   **Expected**: `"myvalue"`

### 4.2 Overwriting a Key

1. Set a key, then overwrite it:
   ```
   127.0.0.1:6379> SET name Alice
   127.0.0.1:6379> SET name Bob
   127.0.0.1:6379> GET name
   ```
   **Expected**: `"Bob"`

### 4.3 GET Non-Existent Key

```
127.0.0.1:6379> GET nonexistent
```
**Expected**: `(nil)`

### 4.4 SET with Expiration (PX)

1. Set a key with a 5-second (5000ms) expiration:
   ```
   127.0.0.1:6379> SET tempkey tempvalue PX 5000
   ```
   **Expected**: `OK`

2. Immediately get the value:
   ```
   127.0.0.1:6379> GET tempkey
   ```
   **Expected**: `"tempvalue"`

3. Wait 5+ seconds, then get again:
   ```
   127.0.0.1:6379> GET tempkey
   ```
   **Expected**: `(nil)` (key has expired)

### 4.5 Multiple Keys

Set and retrieve multiple keys:
```
127.0.0.1:6379> SET key1 value1
127.0.0.1:6379> SET key2 value2
127.0.0.1:6379> SET key3 value3
127.0.0.1:6379> GET key1
127.0.0.1:6379> GET key2
127.0.0.1:6379> GET key3
```
**Expected**: Each GET returns the corresponding value.

---

## 5. INFO Command

**Purpose**: Verify server reports replication information correctly.

### 5.1 INFO on Master

1. Start as master:
   ```bash
   cargo run -- --port 6379
   ```

2. Query INFO:
   ```bash
   redis-cli -p 6379 INFO replication
   ```

**Expected output** (should contain):
```
# Replication
role:master
master_replid:<40-character hex string>
master_repl_offset:0
```

### 5.2 INFO on Replica

1. Start a master, then a replica:
   ```bash
   # Terminal 1 - master
   cargo run -- --port 6379

   # Terminal 2 - replica
   cargo run -- --replicaof "127.0.0.1 6379" --port 6380
   ```

2. Query INFO on the replica:
   ```bash
   redis-cli -p 6380 INFO replication
   ```

**Expected output** (should contain):
```
# Replication
role:slave
```

---

## 6. COMMAND Command

**Purpose**: Verify the COMMAND command responds.

```bash
redis-cli -p 6379 COMMAND
```

**Expected**: `OK`

---

## 7. Replication: Master with Multiple Replicas

This is the core replication test scenario.

### 7.1 Setup: 1 Master + 3 Replicas

Open **4 terminal windows** and start the servers:

**Terminal 1 - Master (port 6379):**
```bash
cargo run -- --port 6379
```

**Terminal 2 - Replica 1 (port 6380):**
```bash
cargo run -- --replicaof "127.0.0.1 6379" --port 6380
```

**Terminal 3 - Replica 2 (port 6381):**
```bash
cargo run -- --replicaof "127.0.0.1 6379" --port 6381
```

**Terminal 4 - Replica 3 (port 6382):**
```bash
cargo run -- --replicaof "127.0.0.1 6379" --port 6382
```

Wait a few seconds for the replicas to complete the handshake with the master.

### 7.2 Test: Write to Master, Read from Replicas

1. **Connect to the master** and set some values:
   ```bash
   redis-cli -p 6379
   ```
   ```
   127.0.0.1:6379> SET user:1 Alice
   127.0.0.1:6379> SET user:2 Bob
   127.0.0.1:6379> SET counter 42
   ```

2. **Verify on master**:
   ```
   127.0.0.1:6379> GET user:1
   ```
   **Expected**: `"Alice"`

3. **Verify on each replica** (open new terminal for each):
   ```bash
   # Check replica 1
   redis-cli -p 6380 GET user:1
   redis-cli -p 6380 GET user:2
   redis-cli -p 6380 GET counter

   # Check replica 2
   redis-cli -p 6381 GET user:1
   redis-cli -p 6381 GET user:2
   redis-cli -p 6381 GET counter

   # Check replica 3
   redis-cli -p 6382 GET user:1
   redis-cli -p 6382 GET user:2
   redis-cli -p 6382 GET counter
   ```

**Expected**: All replicas should return `"Alice"`, `"Bob"`, and `"42"` respectively.

### 7.3 Test: Sequential Writes Propagate to All Replicas

1. Set multiple values on master in sequence:
   ```bash
   redis-cli -p 6379 SET msg1 hello
   redis-cli -p 6379 SET msg2 world
   redis-cli -p 6379 SET msg3 foo
   ```

2. Verify all values are replicated:
   ```bash
   for port in 6380 6381 6382; do
     echo "=== Replica on port $port ==="
     redis-cli -p $port GET msg1
     redis-cli -p $port GET msg2
     redis-cli -p $port GET msg3
   done
   ```

**Expected**: All three replicas return the same values as the master.

### 7.4 Test: Overwrite Propagation

1. Set a key, then overwrite it on the master:
   ```bash
   redis-cli -p 6379 SET status active
   redis-cli -p 6379 SET status inactive
   ```

2. Verify the overwrite propagated:
   ```bash
   for port in 6380 6381 6382; do
     echo "Port $port:"
     redis-cli -p $port GET status
   done
   ```

**Expected**: All replicas return `"inactive"`.

---

## 8. Expiration with Replication

### Steps

1. Start master + replicas as in section 7.1.

2. Set a key with expiration on master:
   ```bash
   redis-cli -p 6379 SET session token123 PX 5000
   ```

3. Immediately check replicas:
   ```bash
   redis-cli -p 6380 GET session
   redis-cli -p 6381 GET session
   ```
   **Expected**: `"token123"`

4. Wait 5+ seconds and check again:
   ```bash
   redis-cli -p 6380 GET session
   redis-cli -p 6381 GET session
   ```
   **Expected**: `(nil)` (if expiration is replicated) or `"token123"` (if only SET is replicated without PX)

> **Note**: The current implementation propagates the full SET command including PX to replicas, but the expiration timing on replicas depends on when they received the command.

---

## 9. Concurrent Clients

**Purpose**: Verify the server handles multiple simultaneous connections.

### Steps

1. Start the server:
   ```bash
   cargo run -- --port 6379
   ```

2. Open multiple `redis-cli` sessions simultaneously:
   ```bash
   # Terminal A
   redis-cli -p 6379

   # Terminal B
   redis-cli -p 6379

   # Terminal C
   redis-cli -p 6379
   ```

3. In each terminal, set and get different keys:
   - **Terminal A**: `SET a_key a_value` then `GET a_key`
   - **Terminal B**: `SET b_key b_value` then `GET b_key`
   - **Terminal C**: `SET c_key c_value` then `GET c_key`

4. Cross-read keys:
   - **Terminal A**: `GET b_key` and `GET c_key`

**Expected**: All keys are accessible from all clients.

---

## 10. Edge Cases

### 10.1 Large Values

```
127.0.0.1:6379> SET bigkey aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
127.0.0.1:6379> GET bigkey
```

### 10.2 Special Characters in Values

```
127.0.0.1:6379> SET special "hello world with spaces"
127.0.0.1:6379> GET special
```

### 10.3 Empty-ish Values

```
127.0.0.1:6379> SET emptyish ""
127.0.0.1:6379> GET emptyish
```

### 10.4 Numeric Values

```
127.0.0.1:6379> SET num 12345
127.0.0.1:6379> GET num
```
**Expected**: `"12345"` (stored as string)

---

## 11. Custom Port

**Purpose**: Verify the server can run on a non-default port.

```bash
cargo run -- --port 7000
```

Then:
```bash
redis-cli -p 7000 PING
```

**Expected**: `PONG`

---

## Quick Smoke Test Checklist

| # | Test | Command | Expected |
|---|------|---------|----------|
| 1 | Server starts | `cargo run -- --port 6379` | No errors |
| 2 | PING | `redis-cli -p 6379 PING` | PONG |
| 3 | ECHO | `redis-cli -p 6379 ECHO hello` | "hello" |
| 4 | SET/GET | SET foo bar, GET foo | "bar" |
| 5 | GET missing | GET nonexistent | (nil) |
| 6 | SET PX | SET k v PX 2000, wait 3s, GET k | (nil) |
| 7 | INFO | INFO replication | role:master |
| 8 | Replication | SET on master, GET on replica | Same value |
| 9 | Multiple replicas | SET on master, GET on 3 replicas | All same |
| 10 | Overwrite replication | SET + overwrite on master | Replicas have new value |
