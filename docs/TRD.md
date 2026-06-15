<!-- Copyright (c) 2025-Present VeloDB Contributors -->
<!-- SPDX-License-Identifier: MIT -->

# Technical Requirements Document — VeloDB Rewrite

## 1. Technology Stack

| Category | Choice | Rationale |
|----------|--------|-----------|
| Language | Rust 2024 Edition | Memory safety, zero-cost abstractions, fearless concurrency |
| Async Runtime | tokio 1.x | Industry standard, multi-threaded work-stealing |
| RESP Parser | nom 7.x | Zero-copy parser combinators |
| In-Memory Store | dashmap 6.x | Lock-free concurrent hash map |
| Flash Store | rust-rocksdb 0.22 | Mature RocksDB bindings |
| TLS | rustls 0.23 + tokio-rustls | Pure Rust, no OpenSSL |
| Lua Scripting | mlua 0.10 | Safe Lua bindings with sandboxing |
| Serialization | serde + custom binary | RDB format serialization |
| Logging | tracing 0.1 | Structured, async-aware, spans |
| Metrics | prometheus 0.13 | Industry standard metrics |
| CLI | rustyline 14.x | Readline-like CLI with history/completion |
| Hashing | crc16 crate | Redis-compatible CRC16 for hash slots |
| Concurrency | crossbeam 0.8, tokio::sync | Channels, epochs, primitives |

## 2. Architecture Overview

The VeloDB server runs as a single process with N shards, each pinned to an OS thread.
A connection acceptor distributes client connections to shards based on key hash.
Each shard has its own tokio runtime, RESP parser, command dispatcher, and storage backend.
A cross-shard message bus handles multi-key operations spanning slots.

```
velodb-server process:
  Main event loop (connection acceptor)
  Connection router (crc16(key) mod 16384 -> slot -> shard index)
  N shards, each on own tokio runtime (pinned to one OS thread)
  Shard owns private key range (slot subset of 16384)
  RESP parser + command dispatch per shard
  Storage backend per shard (in-memory or RocksDB)
  Cross-shard message bus (tokio::mpsc channels)
  Persistence manager (AOF + RDB snapshots)
  Replication engine (master-replica + multi-master)
  Lua VM (sandboxed per shard)
  Cluster manager (Raft-based membership, slot assignment)
  HTTP metrics endpoint on :9090
```

## 3. Component Specifications

### 3.1 Network Layer

- TCP listener on port 6379 (configurable), Unix socket support
- TLS via rustls, configurable cert/key paths
- SO_REUSEPORT for distributing connections across shard threads
- Connection limits via maxclients config
- Nagle algorithm disabled by default (TCP_NODELAY)
- TCP keepalive with configurable interval

### 3.2 RESP2/RESP3 Protocol

RESP2 types:
- Simple String: `+OK\r\n`
- Error: `-ERR message\r\n`
- Integer: `:42\r\n`
- Bulk String: `$5\r\nhello\r\n`
- Array: `*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n`
- Null Bulk: `$-1\r\n`
- Null Array: `*-1\r\n`

RESP3 additional types:
- Null: `_\r\n`
- Boolean: `#t\r\n` / `#f\r\n`
- Double: `,3.14\r\n`
- Big Number: `(34928903284092385093248...\r\n`
- Bulk Error: `!21\r\nSYNTAX invalid syntax\r\n`
- Verbatim String: `=15\r\ntxt:Some text\r\n`
- Map: `%2\r\n+first\r\n:1\r\n+second\r\n:2\r\n`
- Set: `~5\r\n+orange\r\n...`
- Push: `>4\r\n+pubsub\r\n+message\r\n...`

Streaming parser:
- nom-based combinators reading from BytesMut buffer
- Zero-copy slice references into buffer for bulk strings
- Pipeline detection: process all complete commands in buffer
- Maximum argument size limit: 512MB
- Maximum query buffer: 1GB

### 3.3 Storage Trait

```rust
#[async_trait]
pub trait Storage: Send + Sync + 'static {
    async fn set(&self, key: &[u8], value: Vec<u8>) -> Result<()>;
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    async fn del(&self, key: &[u8]) -> Result<bool>;
    async fn exists(&self, key: &[u8]) -> Result<bool>;
    async fn set_expire(&self, key: &[u8], expire_at_ms: u64) -> Result<bool>;
    async fn get_expire(&self, key: &[u8]) -> Result<Option<u64>>;
    async fn remove_expire(&self, key: &[u8]) -> Result<bool>;
    async fn enumerate_keys(&self, f: &dyn Fn(&[u8]) -> bool) -> Result<()>;
    async fn dbsize(&self) -> Result<usize>;
    async fn flush(&self) -> Result<()>;
    async fn get_type(&self, key: &[u8]) -> Result<Option<ValueType>>;
    async fn rename(&self, old_key: &[u8], new_key: &[u8]) -> Result<()>;
}

pub trait StorageFactory: Send + Sync {
    fn create(&self, db_index: usize) -> Result<Box<dyn Storage>>;
}
```

### 3.4 In-Memory Store

- DashMap<Vec<u8>, StorageValue> for O(1) concurrent access
- BTreeMap<u64, Vec<Vec<u8>>> for expiration index (timestamp to keys)
- Active expiry: background task samples 20 random keys per iteration; repeats if >25% expired
- Passive expiry: checks on GET/DEL/EXISTS; deletes if expired, returns nil
- maxmemory config with LRU, LFU, TTL, random, noeviction policies
- Approximate LRU via per-key access timestamp field

### 3.5 RocksDB Flash Store

- Column families: default (data), expire (timestamps)
- Key format: {u16 slot BE}{key bytes} for slot-prefixed scans
- WriteBatch for atomic multi-operation commits
- StorageCache wrapper: DashMap-based hot key cache with configurable size
- Config: write_buffer_size, max_write_buffer_number, compression (none/snappy/lz4)
- Background compaction with configurable parallelism

### 3.6 Concurrency Model

Thread-per-core sharding:
- N shards = N OS threads (configurable, default = num_cpus)
- Independent tokio runtime per shard, pinned to one OS thread
- 16384 hash slots divided equally among shards
- Connection routed to shard based on crc16(key) mod slot_count
- PUBLISH fan-out: message sent to all shards via broadcast channel

Cross-shard operations:
- Multi-key commands (MGET, MSET, DEL multi-key, RENAME across slots)
- tokio::mpsc channels between shards for message passing
- Two-phase protocol: request -> process -> response
- 2PC for MULTI/EXEC spanning shards
- Timeout-based deadlock prevention

### 3.7 Persistence

AOF (Append-Only File):
- RESP-text format, command-by-command logging
- appendfsync: no (OS decides), everysec (background fsync), always (sync every write)
- AOF buffer per shard, flushed to file
- AOF rewrite: reads current state, generates minimal SET commands
- RDB preamble in AOF file for faster restarts
- AOF checksum for corruption detection

RDB Snapshots:
- Binary format, identical structure to Redis RDB
- BGSAVE: fork process, child writes to temp file, renames on success
- SAVE: block until complete
- Configurable automatic snapshots (save after N changes in M seconds)
- RDB checksum (CRC64)

### 3.8 Clustering (VeloDB Cluster v2)

Core design:
- Raft-based cluster membership (3-5 manager nodes for consensus)
- Slot-to-node mapping: leader maintains authoritative map
- Data nodes report status to managers via heartbeat
- Independent from Redis Cluster gossip protocol

Redis Cluster compatibility layer:
- CLUSTER SLOTS: Redis-compatible array format
- CLUSTER NODES: synthesized gossip-style output from manager state
- MOVED / ASK redirects identical to Redis Cluster
- Smart client libraries (Jedis, go-redis, redis-py-cluster) work without changes

### 3.9 Replication

Master-replica:
- PSYNC: replication ID + offset for partial resynchronization
- Full sync: RDB snapshot + replication backlog replay
- Ring buffer replication backlog (configurable size, default 1MB)

Multi-master (Phase 2):
- CRDT-based conflict resolution
- Last-Write-Wins with hybrid logical clocks for basic types
- Active-active mesh with configurable replication factor

### 3.10 Command Dispatch

```rust
pub struct CommandTable {
    commands: HashMap<&'static str, CommandDef>,
}

pub struct CommandDef {
    pub name: &'static str,
    pub arity: i32,       // negative = minimum args
    pub flags: CommandFlags,
    pub first_key: u8,    // position of first key in argv
    pub last_key: u8,     // position of last key in argv
    pub key_step: u8,     // step between multiple keys
    pub handler: CommandHandler,
}
```

## 4. Data Flow

### Request Lifecycle

1. Client TCP connect -> port 6379
2. Connection accepted by TCP listener
3. First command determines key -> crc16 -> slot -> shard
4. Connection assigned to shard's tokio runtime
5. RESP parser reads from TCP stream into BytesMut buffer
6. Parser extracts complete Command struct(s)
7. Command dispatched to handler function
8. Handler calls Storage trait methods
9. Write commands: append to AOF buffer + replication backlog
10. Response serialized to RESP, written to connection buffer
11. TCP flush sends response to client

### GET Request

1. Parse: GET key
2. route(key) -> Shard N
3. storage.get(key)
4. If Some(value): check expire. If expired, delete, return nil. Else return value
5. If None: return nil
6. Serialize: $-1\r\n or $len\r\nvalue\r\n

### SET Request

1. Parse: SET key value [EX sec|PX ms|EXAT ts|PXAT ts|NX|XX|KEEPTTL|GET]
2. route(key) -> Shard N
3. storage.set(key, value)
4. If expire options: storage.set_expire(key, expire_at_ms)
5. Append to AOF buffer (if AOF enabled)
6. Append to replication backlog (if replicas connected)
7. Serialize: +OK\r\n or old value if GET option

## 5. Error Handling

- Result<T, VeloDBError> for all fallible operations
- VeloDBError enum: WrongType, KeyNotFound, SyntaxError, OOM, IO, Internal
- Errors serialized as Redis protocol errors: -ERR message\r\n
- Panics caught at shard boundary with std::panic::catch_unwind
- Failed shard restarted by supervisor
- debug_assert! for development assertions (removed in release)

## 6. Testing Strategy

Level | Tool | Coverage Target
------|------|----------------
Unit tests | #[test] + proptest | >90% storage, parser
Integration | redis-rs client | Full command suite
RESP protocol | Binary wire tests | 100% RESP2/3 types
Fuzz tests | cargo-fuzz | Parser, serializer
Performance | criterion | Regression benchmarks
Stress tests | custom harness | 10K concurrent clients
Cluster tests | docker-compose | Failover, migration

## 7. Build and Deployment

- Build: cargo build --release -> single static binary
- Docker: multi-stage build, Alpine Linux final image
- Config: velodb.conf (redis.conf compatible) or env vars
- Logging: stderr (default), file, or journald; JSON format option
- Signals: SIGTERM (graceful shutdown), SIGHUP (reload config), SIGUSR1 (reopen logs)
- Target platforms: Linux x86_64, Linux aarch64, macOS (development)

## 8. Performance Targets

Operation | Target (per core) | Baseline
----------|-------------------|----------
SET (in-memory) | 100,000 ops/sec | Redis 6.2 single-thread
GET (in-memory) | 120,000 ops/sec | Redis 6.2 single-thread
Pipeline (16 cmds) | 800,000 ops/sec | Redis 6.2 single-thread
SET (RocksDB) | 30,000 ops/sec | N/A (new feature)
GET (RocksDB cache) | 80,000 ops/sec | N/A (new feature)

## 9. Security

- No unsafe code in protocol parsing, command dispatch, or storage
- Lua sandbox: no filesystem, no network, no os.execute
- ACL: per-user command allowlists + key patterns
- TLS 1.3 minimum, strong cipher suites only
- Client certificate authentication option
- Rate limiting per connection
- Maximum key/value size: 512MB
- Connection timeouts for idle clients
