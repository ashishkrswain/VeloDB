<!-- Copyright (c) 2025-Present VeloDB Contributors -->
<!-- SPDX-License-Identifier: MIT -->

# Changelog

All notable changes to VeloDB are documented in this file.

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
