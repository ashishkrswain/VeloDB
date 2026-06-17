<!-- Copyright (c) 2025-Present VeloDB Contributors -->
<!-- SPDX-License-Identifier: MIT -->

# Changelog

All notable changes to VeloDB are documented in this file.

## [Unreleased]

### Added
- **Lua Scripting**: EVAL, EVALSHA, SCRIPT LOAD/EXISTS/FLUSH commands with mlua sandbox
- **redis.call() bridge**: Lua scripts can call Redis commands (SET, GET, DEL, etc.) through dispatch
- **KEYS/ARGV arrays**: Passed as Lua tables accessible from scripts
- **Script cache**: SHA1-hashed script storage in DashMap on Store
- **INFO command**: Returns proper multi-line bulk string with version, uptime, keyspace stats
- **CONFIG GET/SET**: Runtime configuration introspection (stub implementation)
- **8 new integration tests**: EVAL return int/string/table/error/pipelined, SCRIPT load/exists/flush, INFO, CONFIG GET
- **mlua + sha1 dependencies** for Lua scripting engine

### Changed
- **cmd/server.rs**: Added INFO and CONFIG commands, replaced stub implementations
- **Store struct**: Added `lua_scripts: DashMap<String, String>` for script caching
- **Error enum**: Added `Lua(String)` variant for Lua error reporting

### Added
- **Multi-Threading (Phase 4)**: Thread-per-core sharded server architecture with N tokio runtimes
- **ShardedServer**: Round-robin connection routing to shard runtimes, mpsc channel-based forwarding
- **Slot Router**: CRC16-based key hashing with hashtag extraction support (`{...}`)
- **Shard per core**: Each shard runs on a dedicated OS thread with its own tokio runtime
- **Config field**: `cthreads` (default: available_parallelism cores)
- **New modules**: `src/shard/` (mod, router with hashtag + CRC16)
- **3 slot router unit tests**: hashtag extraction, same-slot verification, SLOT_COUNT bounds

### Changed
- **Server startup**: Replaced single-threaded ServerHandle with ShardedServer 
- **Main event loop**: All share a single TCP listener, connections routed to shard runtimes

### Added
- **Replication framework**: PSYNC protocol support, ReplBacklog ring buffer, master/replica handshake
- **Master-side replication**: PSYNC negotiation (full and partial sync), RDB snapshot transfer, command streaming from backlog
- **Replica-side replication**: Master connection, full RDB sync reception/loading, command stream replay
- **Replication backlog**: Ring buffer storing raw RESP commands, configurable size (default 1MB), offset-based partial sync
- **Replid generation**: 40-char hex random replication ID per server instance
- **Config fields**: `replicaof` (master host:port), `masterauth`, `repl-backlog-size`
- **New modules**: `src/replication/` (backlog, master, replica)
- **`rand` dependency** for replication ID generation

### Changed
- **ServerHandle**: Added `repl_backlog` and `replid` fields for replication support
- **Connection handler**: Commands appended to replication backlog after dispatch
- **Server startup**: Spawns replica connection task if `replicaof` is configured, with auto-reconnect loop

### Added
- **Pub/Sub system**: SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE, PUNSUBSCRIBE, PUBLISH with channel and pattern matching support
- **Transactions**: MULTI, EXEC, DISCARD, WATCH, UNWATCH with atomic execution and WATCH-based optimistic locking
- **PubSubRegistry**: DashMap-based channel/pattern registry with mpsc channels for real-time message delivery
- **Connection pubsub mode**: Non-blocking select loop for pubsub connections, message push via RESP format
- **Key versioning**: Per-key version counter for WATCH/EXEC conflict detection
- **4 new integration tests**: PUBLISH/SUBSCRIBE roundtrip, MULTI/EXEC, WATCH conflict, DISCARD
- **New command modules**: `src/cmd/pubsub.rs` (5 commands), `src/cmd/transaction.rs` (5 commands)
- **New Store methods**: `get_version()`, `pubsub_publish()`, `pubsub_subscribe_channel()`, `pubsub_subscribe_pattern()`

### Changed
- **ClientContext**: Added sub_mode, subscribed_channels, subscribed_patterns, pubsub_rx, multi_mode, multi_queue, watched_keys, watched_versions
- **Entry struct**: Added `version: u64` field for transaction conflict detection
- **Store struct**: Added `pubsub_registry: PubSubRegistry`
- **connection::handle()**: Added pubsub mode select loop and multi-command queueing for MULTI/EXEC
- **cmd/mod.rs**: Registered pubsub and transaction command modules

### Added
- **AOF persistence**: RESP-format append-only file logging with three fsync policies (no, everysec, always), background fsync task, and server startup replay
- **RDB persistence**: Binary snapshot format (VELO magic, CRC64), save/load with full support for all 7 data types (String, List, Set, Hash, ZSet, Stream, NestedHash) and expiry timestamps
- **BGSAVE**: Background RDB save via `tokio::task::spawn_blocking`, writes to temp file then atomically renames
- **Auto-save**: Configurable periodic RDB snapshots via `save <seconds> <changes>` configuration
- **Server startup persistence**: RDB loads on startup (takes priority over AOF), AOF replays if no RDB and `appendonly yes`
- **AOF command logging**: All write commands (SET, LPUSH, SADD, HSET, ZADD, XADD, NHSEt, etc.) automatically append to AOF buffer
- **4 new ServerConfig fields**: `appendonly`, `appendfsync`, `save` (vector of (seconds, changes) tuples)
- **2 new VeloDBError variants**: `AofError`, `RdbError`
- **`Store::iterate_db()`**: Iterates all non-expired entries per database for RDB serialization
- **`Store::set_with_entry()`**: Direct entry insertion for RDB loading
- **4 new integration tests**: RDB save/load roundtrip, AOF append/replay, persistence with expiry, AOF logging on write
- **`tempfile` dev-dependency** for test temporary directories

### Added
- **Comprehensive test suite**: 143 tests covering store, parser, serializer, config, commands, and TCP integration
- **Unit tests**: 116 tests across store/memory (86), RESP parser (15), RESP serializer (8), and config (7)
- **Integration tests**: 27 end-to-end TCP roundtrip tests covering all 6 data types, expiry, pipelining, error responses, SELECT isolation, and WRONGTYPE enforcement
- **`src/lib.rs`**: Library crate exposing all modules for integration test access

### Fixed
- **SET EX/PX expiration**: EX and PX flags now properly calculate absolute timestamps instead of storing relative durations
- **LINDEX out-of-bounds**: LINDEX with index beyond list bounds now correctly returns nil instead of clamping

## [0.2.0] — Phase 2: Data Types

### Added
- **List type** with 12 commands: LPUSH, RPUSH, LPOP, RPOP, LLEN, LRANGE, LINDEX, LSET, LTRIM, LREM, BLPOP, BRPOP
- **Set type** with 10 commands: SADD, SREM, SMEMBERS, SISMEMBER, SCARD, SINTER, SUNION, SDIFF, SRANDMEMBER, SPOP
- **Hash type** with 9 commands: HSET, HGET, HDEL, HEXISTS, HGETALL, HKEYS, HVALS, HLEN, HINCRBY
- **ZSet type** with 8 commands: ZADD, ZREM, ZSCORE, ZRANK, ZRANGE, ZRANGEBYSCORE, ZCARD, ZCOUNT
- **Stream type** with 7 commands: XADD, XRANGE, XREVRANGE, XLEN, XDEL, XTRIM, XREAD (basic, no consumer groups)
- **NestedHash type** with 6 commands: NHSET, NHGET, NHDEL, NHKEYS, NHVALS, NHGETALL
- **StorageValue enum**: String, List (VecDeque), Set (HashSet), Hash (HashMap), ZSet (dual-index HashMap + BTreeMap), Stream (VecDeque with last-ID tracking), NestedHash (HashMap of HashMaps)
- **BlockRegistry**: DashMap-based waiter registry for BLPOP/BRPOP blocking support with tokio::Notify
- **Blocking architecture**: Full BLPOP/BRPOP with timeout support — connection parks on Notify, wakes on LPUSH/RPUSH, returns nil on timeout
- **ZSet dual-index**: members HashMap (O(1) score lookup) + scores BTreeMap (ordered iteration) for efficient ZRANGE/ZRANGEBYSCORE
- **ZSet score bound parsing**: Support for -inf, +inf, (exclusive bounds in ZRANGEBYSCORE and ZCOUNT
- **Stream ID generation**: Auto-generating IDs via *, partial IDs (ms-*), validation against last ID
- **NestedHash hierarchical operations**: Field-level and subfield-level get/set/del/keys/vals/getall

### Changed
- **Entry struct**: Removed `type_name` field — type is now encoded in the `StorageValue` enum variant
- **Store::get_type()**: Returns type name by matching on StorageValue variant instead of reading a String field
- **Store::get()**: WRONGTYPE error when called on non-string keys
- **WRONGTYPE enforcement**: INCR, DECR, APPEND, STRLEN, GETRANGE, SETRANGE now check for string type
- **ClientContext**: Added `block_state: Option<BlockState>` for connection-level blocking support
- **connection::handle()**: Blocking path — detects block_state after dispatch, registers on BlockRegistry, parks, unblocks with result

### Infrastructure
- 6 new command modules: list.rs, set.rs, hash.rs, zset.rs, stream.rs, nested_hash.rs
- 52 new commands registered (total now 34 + 52 = 86)

## [0.1.0] — Phase 1: Core Server

### Added
- Project setup with Cargo.toml (tokio, bytes, nom, dashmap, clap, tracing, thiserror, serde, crc16, parking_lot)
- `velodb-server` binary: tokio-based TCP server with per-connection task spawning
- `velodb-cli` binary: interactive REPL and one-shot command mode with RESP encoding
- RESP2 protocol parser: nom-based streaming parser for SimpleString, Error, Integer, BulkString, Array, Null
- RESP2 serializer: full wire-format serialization of all RESP types
- In-memory store: DashMap-based concurrent hashmap with passive expiry and 16 databases
- Configuration parser: redis.conf-compatible with 9 config fields (port, bind, timeout, tcp-keepalive, databases, maxmemory, loglevel, dbfilename, dir)
- Error type system: VeloDBError with 10 variants (UnknownCommand, WrongNumberOfArgs, SyntaxError, WrongType, KeyNotFound, NotInteger, Overflow, ProtocolError, Io, Internal)
- Graceful shutdown via SIGINT/SIGTERM handlers
- Structured logging: tracing-based with env-filter and JSON format support

### Server Commands
- `PING` — returns PONG or echoes argument
- `ECHO` — returns the given message
- `COMMAND` — returns OK (stub)
- `SELECT` — switches database index (0-15)

### String Commands
- `GET` — get value by key
- `SET` — set key to value with EX/PX/EXAT/PXAT flags
- `MGET` — get multiple keys in one call
- `MSET` — set multiple key-value pairs atomically
- `INCR` — increment integer value by 1
- `INCRBY` — increment integer value by N
- `DECR` — decrement integer value by 1
- `DECRBY` — decrement integer value by N
- `APPEND` — append value to existing string
- `STRLEN` — get string length
- `GETRANGE` — get substring by range (supports negative indices)
- `SETRANGE` — overwrite part of string at offset
- `GETSET` — set new value, return old value

### Generic/Key Commands
- `DEL` — delete one or more keys, returns count
- `EXISTS` — check if keys exist, returns count
- `EXPIRE` — set TTL in seconds
- `EXPIREAT` — set absolute expiry in seconds (Unix timestamp)
- `PEXPIRE` — set TTL in milliseconds
- `PEXPIREAT` — set absolute expiry in milliseconds
- `TTL` — get remaining TTL in seconds (-1: no expire, -2: expired)
- `PTTL` — get remaining TTL in milliseconds
- `PERSIST` — remove expiration from key
- `TYPE` — get value type of key (string or none)
- `RENAME` — rename key (errors if source doesn't exist)
- `RENAMENX` — rename key only if destination doesn't exist
- `KEYS` — find keys matching glob pattern (*, ?)
- `DBSIZE` — count keys in current database (lazy expiry cleanup)
- `FLUSHDB` — delete all keys in current database
- `FLUSHALL` — delete all keys in all databases
- `RANDOMKEY` — return a random key name

### Documentation
- Product Requirements Document (PRD.md)
- Technical Requirements Document (TRD.md)
- Implementation Plan (implementation-plan.md) — 7-phase plan
- Application Flow Diagrams (app-flow.md)
- Backend Schema (backend-schema.md)
- UI/UX Design (ui-ux-design.md)
- Workflow documentation (WORKFLOW.md)
- CHANGELOG.md (this file)

### Infrastructure
- Copyright headers added to all source files and documentation
- MIT license (SPDX-License-Identifier)
