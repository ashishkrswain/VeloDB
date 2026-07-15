<!-- Copyright (c) 2025-Present VeloDB Contributors -->
<!-- SPDX-License-Identifier: MIT -->

# Changelog

All notable changes to VeloDB are documented in this file.

## [Unreleased]

### Added
- **Active expiry (Phase 1.1 production hardening)**: Background expiry cycle in the style of Redis serverCron — samples up to 20 volatile keys per database every 100ms, evicts expired ones, and repeats while >25% of the sample was expired. Fixes memory leak where expired keys were only removed when accessed (passive expiry).
- **`Store::active_expire_cycle(sample_size)`**: Public method returning number of keys evicted; 4 new unit tests
- **`server::start_active_expiry_task`**: Spawned at startup; 1 new async test (167 total tests)
- **maxmemory + eviction policies (Phase 1.2 production hardening)**: `maxmemory` is now enforced. Policies: `noeviction` (writes rejected with `OOM` error when over limit, reads/deletes still allowed), `allkeys-random`, `volatile-random` (only keys with TTLs are candidates). Background memory cycle refreshes the usage estimate every 100ms and evicts back under the limit.
- **Config**: `maxmemory` now parses redis.conf size units (`100mb`, `2gb`, `512kb`); new `maxmemory-policy` field (default `noeviction`)
- **`Store` memory API**: `configure_memory`, `estimated_memory` (full-scan estimate), `refresh_memory_usage`, `over_memory_limit` (cheap cached check on dispatch path), `evict_until_under`
- **15 new tests** covering eviction policies, OOM command rejection, size-unit parsing, and the background cycle (182 total tests)
- **SCAN command family (Phase 1.3 production hardening)**: `SCAN cursor [MATCH pattern] [COUNT n]` with guaranteed-complete cursor semantics (sorted-key traversal; keys present for the whole scan are returned exactly once, even with concurrent writes). `HSCAN`/`SSCAN`/`ZSCAN` return the full container in one page (cursor 0), matching Redis behavior for small collections; MATCH filtering supported. Invalid cursors return `ERR invalid cursor`.
- **`Store` scan API**: `scan_keys`, `hscan`, `sscan`, `zscan`; `simple_match` glob matcher now public
- **14 new tests** (10 unit + 4 integration) covering pagination, MATCH, deletion mid-scan, expired-key filtering, WRONGTYPE (196 total tests)
- **BGREWRITEAOF (Phase 1.4 production hardening)**: AOF rewrite compacts the log to one minimal constructing command per key (SET/RPUSH/SADD/HSET/ZADD/XADD/NHSET) plus PEXPIREAT for TTLs, with SELECT markers for multi-database datasets. Writes to a temp file, fsyncs, atomically swaps, and reopens the writer handle under the file lock so concurrent appends land in the new file. Runs on `spawn_blocking`; returns `ERR AOF is not enabled` when AOF is off.
- **New module** `src/persist/aof_rewrite.rs`; `AofWriter::rewrite(&Store)`
- **6 new tests** (5 unit + 1 integration): shrink verification, all-types coverage, TTL preservation, append-after-rewrite, multi-db SELECT (202 total tests)
- **Replication partial sync (Phase 1.5 production hardening)**: PSYNC is now actually wired into the connection pipeline — previously `handle_replica_connection` existed but was never called, so every PSYNC silently fell through to the normal command dispatcher. A synced replica now stays on the connection and receives live writes as they happen (previously nothing streamed writes to a replica after its initial sync at all). Replicas remember their `(replid, offset)` across reconnects and request `PSYNC <replid> <offset>`; the master serves `+CONTINUE` with only the missed bytes when the offset is still in the backlog window, falling back to `+FULLRESYNC` otherwise.
- **Fixed a data-corruption bug in `ReplBacklog::read_from`**: it computed ring-buffer offsets relative to the window start instead of the absolute stream offset modulo capacity, returning corrupted bytes for any read that required a buffer wraparound. Caught by a new characterization test before partial sync could be built on top of it.
- **Fixed a temp-file collision bug**: full-sync (and `BGSAVE`) temp RDB paths were built from the OS process ID alone, so concurrent full-syncs within the same process (multiple replicas, or concurrent test runs) could collide on the same filename and corrupt each other's transfer. Added a process-wide atomic counter (`persist::unique_temp_id`) alongside the PID.
- **`ReplBacklog`**: now fans out every push to registered live replicas (`register_replica`/`unregister_replica`) under the same lock as the ring-buffer write, so a replica registered mid-sync cannot miss a write in the handoff gap.
- **15 new tests** (10 unit + 5 integration, including two full master+replica-over-TCP end-to-end tests and a concurrency regression test) covering: backlog wraparound correctness, live fan-out, PSYNC full/partial negotiation, reconnect-preserves-data, and concurrent full-syncs (217 total tests)
- **Stream consumer groups (Phase 1.6 production hardening)**: `XGROUP CREATE [MKSTREAM] / DESTROY / CREATECONSUMER / DELCONSUMER / SETID`, `XREADGROUP GROUP g c [COUNT n] [NOACK] STREAMS k1 [k2 ...] id1 [id2 ...]` (supports both `>` for new entries and an explicit ID to replay a consumer's own history), `XACK`, `XPENDING` (both summary and range forms, with optional consumer filter), `XCLAIM` (respects `MIN-IDLE-TIME`, supports `JUSTID`).
- **Consumer group state** lives in a new out-of-band registry on `Store` (`consumer_groups: DashMap<(db, stream_key, group), ConsumerGroup>`), following the same pattern as `BlockRegistry`/`PubSubRegistry` — keeps RDB/AOF-rewrite serialization of stream values untouched, since group membership is process-local like Redis's own replica behavior.
- **`Store` consumer group API**: `xgroup_create/destroy/create_consumer/del_consumer`, `xreadgroup`, `xack`, `xpending_summary`, `xpending_range`, `xclaim`
- **18 new tests** (13 unit + 5 integration) covering group lifecycle, MKSTREAM, delivery/PEL bookkeeping, NOACK, ack, pending summary/range with consumer filtering, claim with and without MIN-IDLE-TIME (235 total tests)
- **AUTH + minimal ACL (Phase 1.7a production hardening)**: `requirepass` config gates all commands except `AUTH`/`HELLO`/`PING`/`QUIT`/`RESET` with a `NOAUTH` error until a connection successfully authenticates; `AUTH password` and `AUTH default password` both supported. `ACL WHOAMI/LIST/USERS/CAT/GETUSER` implemented against a single built-in `default` user (full permissions) — enough for client libraries that probe ACL on connect; multi-user ACL rules are out of scope.
- **Fixed a config-forwarding bug in `ShardedServer`**: `ShardedServer::new` accepted `config: &ServerConfig` but every connection was still handed `ServerConfig::default()`, so `requirepass` (and any other per-connection setting) would silently never take effect on the actual production server path — `ServerHandle` (which does forward config correctly) is only used by tests/library callers. Locked in with a regression test that spins up a real `ShardedServer` with `requirepass` set and confirms `NOAUTH` reaches the client.
- **Config**: new `requirepass`, `tls-port`, `tls-cert-file`, `tls-key-file` fields (TLS fields parsed now, listener wiring lands in Phase 1.7c)
- **14 new tests** (4 config unit + 6 AUTH integration + 3 ACL integration + 1 ShardedServer config-forwarding regression) covering AUTH gating, wrong/right password, pre-auth-exempt commands, no-requirepass passthrough, and ACL introspection (249 total tests)
- **RESP3 protocol support (Phase 1.7b production hardening)**: `HELLO [protover] [AUTH user pass] [SETNAME name]` negotiates RESP2 (default, unchanged wire format) or RESP3 per-connection; combines protocol negotiation with login in one round trip for RESP3-aware client libraries that send `HELLO 3 AUTH ...` on connect. Added RESP3-only `RespValue` variants — `Map` (`%`), `Double` (`,`), `Boolean` (`#`), and a unified `Null` (`_`) — each degrading to its RESP2 equivalent (flattened array, bulk string, integer 0/1, `$-1`) when the connection hasn't upgraded, so RESP2 clients see byte-for-byte identical output to before this change.
- **`resp::serialize_response_proto(value, protocol)`**: protocol-aware serializer; `serialize_response` (used by all existing call sites) is unchanged and always emits RESP2.
- **`ClientContext::protocol`**: per-connection negotiated version, defaults to 2 until HELLO 3 is sent.
- **15 new tests** (9 unit + 6 integration) covering RESP3 vs RESP2 encoding of every new type, HELLO negotiation, HELLO+AUTH combined, and unsupported protocol version rejection (264 total tests)
- **TLS support (Phase 1.7c production hardening)**: optional `tls-port`/`tls-cert-file`/`tls-key-file` config spins up a second, TLS-terminated listener alongside the plaintext port, running the exact same command pipeline. Backed by `rustls`/`tokio-rustls` (ring crypto provider). `net::connection::handle` and `replication::master::handle_psync` are now generic over any `AsyncRead + AsyncWrite + Unpin` stream (previously hardcoded to `TcpStream`), which also let the manual `readable()`/`try_read_buf()` polling loop collapse into a single `read_buf()` call.
- **Fixed a real ordering bug in replica reconnect**: `connect_to_master` loaded RDB data into the store, then awaited a temp-file cleanup, and only *after* that update the shared `ReplicaSyncState.replid`. The await was a genuine yield point: a concurrent observer could see the newly-replicated data while `sync_state.replid` still showed the previous (or default `"?"`) value, which would make a reconnect attempt sent during that window request the wrong resync from the master. Found via a test that flaked only under full-suite load (73 concurrent tasks), not in isolation — reproduced, root-caused to the await ordering, and fixed by updating `sync_state` immediately after `load_rdb` with no intervening await. Verified with 5 consecutive full-suite runs post-fix.
- **New module** `src/net/tls.rs`: `load_tls_config` (PEM cert/key → `rustls::ServerConfig`), `accept_loop` (TLS-terminated accept loop feeding the shared connection handler)
- **5 new tests** (4 unit covering valid/missing/malformed cert-key loading + 1 true end-to-end integration test performing a real TLS 1.3 handshake with a self-signed cert against a running server, executing SET/GET over the encrypted channel) (269 total tests)



### Added
- **Cluster support (Phase 6)**: Redis Cluster API compatibility layer with slot-to-node mapping
- **SlotMap**: 16384-slot allocation with Assigned/Migrating/Importing/Unassigned states
- **CLUSTER commands**: SLOTS, NODES, INFO, MYID, KEYSLOT, MEET, RESET, FORGET, REPLICATE, SETSLOT, GETKEYSINSLOT, COUNTKEYSINSLOT, SAVECONFIG
- **Cluster bus**: Background TCP listener on cluster_port for inter-node communication
- **MOVED/ASK redirect support**: Slot ownership check with proper RESP error responses
- **Cluster config**: `cluster-enabled`, `cluster-port`, `cluster-node-timeout`, `cluster-config-file` fields
- **New modules**: `src/cluster/` (slots, compat), `src/cmd/cluster.rs`

### Changed
- **Server startup**: Starts cluster bus service when `cluster-enabled yes` is configured
- **Command table**: Registered 13 new CLUSTER subcommands

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
