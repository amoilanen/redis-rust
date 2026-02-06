# Redis Server Testing Guide

Complete guide on testing the Redis server implementation, including unit tests, integration tests, and manual testing instructions.

## Test Summary

- **Unit Tests**: 95 tests
  - CLI: 8 tests
  - Commands: 20 tests (spread across individual command modules)
  - Storage: 14 tests
  - IO: 12 tests
  - Protocol: 50+ tests
  - RDB: 6 tests
  - Server State: 2 tests
  - Replication: 1 test
  - Connection: 1 test

- **Integration Tests**: 17 e2e tests covering:
  - PING/ECHO commands
  - SET/GET operations (basic, multiple keys, overwrites)
  - Key expiration
  - Binary data handling
  - Replication info
  - Complex scenarios (caching, rate limiting, sessions)

**Total: 112 tests**

## Running Tests

### All Tests
```bash
cargo test
```

### Unit Tests Only
```bash
cargo test --lib
```

### Integration Tests Only
```bash
cargo test --test integration_tests
```

### Specific Test Module
```bash
# CLI tests
cargo test --lib cli::tests

# Command tests
cargo test --lib commands::ping::tests
cargo test --lib commands::get::tests
cargo test --lib commands::set::tests
cargo test --lib commands::echo::tests
cargo test --lib commands::info::tests
cargo test --lib commands::replconf::tests
cargo test --lib commands::psync::tests
cargo test --lib commands::command::tests

# Storage tests
cargo test --lib storage::tests

# Protocol tests
cargo test --lib protocol::tests

# IO tests
cargo test --lib io::tests

# RDB tests
cargo test --lib rdb::tests
```

### Run Specific Test
```bash
# Run a single test
cargo test --lib e2e_set_get_basic -- --exact

# Run e2e tests matching pattern
cargo test --test integration_tests e2e_
```

### Verbose Output
```bash
# See all test names and results
cargo test --lib -- --nocapture

# With println! output
cargo test --lib -- --nocapture --test-threads=1
```

## Supported Commands

The Redis server supports the following commands:

### Connection Commands

#### PING
Ping-pong test for connection.

```
PING
→ +PONG
```

**Test Coverage**: `test_ping_command`, `e2e_ping_works`

#### ECHO
Echo back the provided argument.

```
ECHO "hello"
→ $5\r\nhello\r\n
```

**Test Coverage**: `test_echo_command_with_message`, `test_echo_command_without_message`, `e2e_echo_returns_argument`

### String Commands

#### SET
Store a key-value pair with optional expiration.

```
SET key value
→ +OK

SET key value PX 1000
→ +OK  (expires after 1000ms)
```

**Options**:
- `PX milliseconds` - Set expiration time in milliseconds

**Test Coverage**:
- `test_set_command_basic`
- `test_set_command_with_expiration`
- `test_set_command_invalid_syntax`
- `e2e_set_get_basic`
- `e2e_multiple_keys`
- `e2e_overwrite_key`
- `e2e_key_expires`
- `e2e_long_lived_key`

#### GET
Retrieve the value of a key.

```
GET key
→ $5\r\nvalue\r\n

GET nonexistent
→ $-1\r\n
```

**Test Coverage**:
- `test_get_command_found`
- `test_get_command_not_found`
- `test_get_command_invalid_syntax`
- `test_set_and_get_roundtrip`
- `test_get_with_binary_data`
- `test_multiple_keys`
- `e2e_get_nonexistent_key`
- `e2e_binary_data_preserved`

### Server Commands

#### COMMAND
Get command information (returns OK for now).

```
COMMAND
→ +OK
```

**Test Coverage**: `test_command_command`

#### INFO
Get server information.

```
INFO replication
→ <replication info>
```

**Sections**:
- `replication` - Returns role (master/slave) and replication ID

**Test Coverage**:
- `test_info_replication_master`
- `test_info_replication_slave`
- `e2e_info_command_master`
- `e2e_info_command_replica`

### Replication Commands

#### REPLCONF
Replication configuration during handshake.

```
REPLCONF listening-port 6380
→ +OK

REPLCONF capa psync2
→ +OK

REPLCONF getack *
→ *3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$1\r\n0\r\n
```

**Test Coverage**: `test_replconf_listening_port`, `test_replconf_getack`

#### PSYNC
Partial resynchronization for replication.

```
PSYNC ? -1
→ +FULLRESYNC <replication_id> 0
→ <RDB snapshot>
```

**Test Coverage**: `test_psync_returns_fullresync`

## Using Commands Examples

### Basic Key-Value Operations

```bash
# Using redis-cli
redis-cli
> PING
PONG
> SET user:1:name "Alice"
OK
> GET user:1:name
"Alice"
> SET temp_session "session_data" PX 3600000
OK
```

### Caching Pattern

```
# Store user profile with 10-minute expiration
SET profile:user:42 '{"name":"John","email":"john@example.com"}' PX 600000

# Retrieve profile
GET profile:user:42
```

### Rate Limiting Pattern

```
# Count requests from IP
SET rate_limit:192.168.1.1 1 PX 60000

# Increment counter
SET rate_limit:192.168.1.1 2 PX 60000
SET rate_limit:192.168.1.1 3 PX 60000

# Counter automatically expires after 60 seconds
```

### Session Management

```
# Create session (1 hour expiration)
SET session:abc123 "user:123" PX 3600000

# Retrieve session
GET session:abc123

# Session automatically expires after 1 hour
```

## Test Categories

### Unit Tests

#### Command Tests
Each command has its own module with dedicated tests:
- Echo: Tests with/without message
- Ping: Basic pong response
- Get: Found, not found, invalid syntax
- Set: Basic, expiration, invalid syntax
- Info: Master and replica modes
- ReplConf: Different subcommands

#### Storage Tests
- Create, read, update operations
- Expiration logic (not expired, expired, no expiration)
- Multiple keys
- Binary data handling
- Empty values

#### Protocol Tests
- Parse and serialize all RESP types
- Bulk strings, arrays, simple strings, integers
- Map, set, push types
- RDB format

#### IO Tests
- Read single/multiple messages
- Various message types
- Large messages
- Binary data

### Integration Tests (E2E)

#### Basic Operations
- `e2e_set_get_basic` - Simple SET then GET
- `e2e_multiple_keys` - Multiple key-value pairs
- `e2e_get_nonexistent_key` - Missing key handling
- `e2e_overwrite_key` - Updating existing key

#### Expiration
- `e2e_key_expires` - Short-lived key (100ms)
- `e2e_long_lived_key` - Long-lived key (5s)

#### Data Types
- `e2e_binary_data_preserved` - Binary data round-trip

#### Replication
- `e2e_info_command_master` - Master INFO response
- `e2e_info_command_replica` - Replica INFO response

#### Complex Scenarios
- `e2e_mixed_operations` - Set, get, update, delete operations
- `e2e_session_simulation` - Session management
- `e2e_user_profile_caching` - Caching use case
- `e2e_rate_limiting_with_expiration` - Rate limit counter

## Manual Testing

### Start Server

```bash
# Build release binary
cargo build --release

# Start on default port (6379)
./target/release/redis-starter-rust

# Start on custom port
./target/release/redis-starter-rust --port 6380

# Start as replica
./target/release/redis-starter-rust --port 6380 --replicaof localhost 6379
```

### Test with redis-cli

```bash
# In another terminal, connect to the server
redis-cli -p 6379

# Test PING
> PING
PONG

# Test ECHO
> ECHO "Hello Redis"
"Hello Redis"

# Test SET/GET
> SET mykey "Hello"
OK
> GET mykey
"Hello"

# Test SET with expiration
> SET temp "data" PX 3000
OK
> GET temp
"data"
# Wait 3+ seconds
> GET temp
(nil)

# Test non-existent key
> GET nonexistent
(nil)

# Test INFO
> INFO replication
# Replication
role:master
master_replid:<replication-id>
master_repl_offset:0

# Multiple keys
> SET user:1 "alice"
OK
> SET user:2 "bob"
OK
> GET user:1
"alice"
> GET user:2
"bob"
```

### Test Replication

**Terminal 1 - Master:**
```bash
./target/release/redis-starter-rust --port 6379
```

**Terminal 2 - Replica:**
```bash
./target/release/redis-starter-rust --port 6380 --replicaof localhost 6379
```

**Terminal 3 - Test:**
```bash
redis-cli -p 6379
> SET key1 "value1"
OK
> SET key2 "value2"
OK

# Connect to replica
redis-cli -p 6380
> INFO replication
# role:slave

> GET key1  # Should have the data from master
"value1"
```

## Performance Expectations

- **SET operation**: < 1ms
- **GET operation**: < 1ms
- **Expiration check**: O(1) per GET
- **Message parsing**: Depends on size, typically < 1ms
- **RDB serialization**: Depends on data size

## Debugging Tips

### Enable Debug Output
```bash
RUST_LOG=debug cargo test --lib -- --nocapture
```

### Run Single Test
```bash
cargo test --lib e2e_set_get_basic -- --nocapture
```

### Check Compiler Warnings
```bash
cargo clippy --lib
```

## Known Limitations

1. **Commands**: Only basic commands implemented (PING, ECHO, SET, GET, INFO, REPLCONF, PSYNC, COMMAND)
2. **RDB Format**: Simplified format (strings only, no expiration encoding)
3. **Data Types**: Only strings supported
4. **Offset Tracking**: Replication offset not fully implemented
5. **Persistence**: In-memory only (no AOF or automatic RDB persistence)

## Future Test Enhancements

1. Add end-to-end tests with actual TCP sockets
2. Add performance benchmarks
3. Add stress tests with concurrent operations
4. Add replication synchronization tests
5. Add RDB persistence tests
6. Add command case-insensitivity tests
7. Add memory limit tests
8. Add concurrent client tests

## Continuous Integration

Run tests before committing:

```bash
# Run all tests
cargo test

# Check code quality
cargo clippy

# Check formatting
cargo fmt --check
```

## Test Organization

### Module-Level Tests
Tests are organized at the module level following Rust conventions:
- Each module has a `#[cfg(test)] mod tests` section
- Tests are collocated with implementation
- Test helpers are defined in the same module

### Integration Tests
- Located in `tests/integration_tests.rs`
- Tests full end-to-end scenarios
- Tests with multiple components working together
- Simulates real-world usage patterns

## Success Criteria

All tests should pass:
```
running 95 tests (unit)
test result: ok. 95 passed; 0 failed

running 17 tests (integration)
test result: ok. 17 passed; 0 failed
```

Total: **112 tests passing**
