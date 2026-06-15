# Product Requirements Document — VeloDB Rewrite

## Executive Summary

VeloDB is a ground-up Rust rewrite of the KeyDB/Redis-compatible database server.
It provides a high-performance, multi-threaded, 100% RESP2/RESP3 wire-compatible
key-value database with pluggable storage backends (in-memory and RocksDB flash),
active-active replication, and a redesigned clustering mechanism.

## Product Vision

A safe, fast, modern Redis-compatible database that eliminates the memory safety
and concurrency limitations of the C/C++ original while introducing a superior
clustering architecture.

---

## Target Audience

- DevOps / SRE teams running Redis-compatible infrastructure needing better multi-core scaling
- Backend engineers wanting a drop-in Redis replacement with flash storage
- Platform teams deploying multi-region active-active databases
- Developers using standard Redis clients (Jedis, redis-py, go-redis, ioredis, StackExchange.Redis)

---

## User Stories

### Core Database

ID    | Story | Priority
------|-------|----------
US-01 | Connect using any standard Redis client and issue GET/SET/DEL commands | P0
US-02 | Use all Redis data types with identical semantics | P0
US-03 | Set key expiration with EXPIRE/EXPIREAT/TTL | P0
US-04 | Use PING/ECHO/COMMAND/INFO for server introspection | P0
US-05 | Use SELECT to switch databases (0-15) | P0
US-06 | Use transactions (MULTI/EXEC/DISCARD/WATCH) | P1
US-07 | Execute Lua scripts via EVAL/EVALSHA/SCRIPT LOAD | P1
US-08 | Use Pub/Sub (SUBSCRIBE/PUBLISH/PSUBSCRIBE) | P1
US-09 | Use ACL commands to manage user permissions | P1
US-10 | Use client-side caching with RESP3 push notifications | P2

### Persistence

ID    | Story | Priority
------|-------|----------
US-11 | Persist via AOF with configurable fsync | P0
US-12 | Take RDB snapshots (BGSAVE) and restore from them | P0
US-13 | Backup snapshots directly to S3-compatible storage | P1
US-14 | Configure flash storage (RocksDB) for datasets larger than RAM | P1

### Performance

ID    | Story | Priority
------|-------|----------
US-15 | Configure number of worker threads for multi-core scaling | P0
US-16 | Sub-millisecond latency for GET/SET at 100K+ ops/sec per core | P0
US-17 | Use a config file compatible with redis.conf syntax | P0

### Replication and Clustering

ID    | Story | Priority
------|-------|----------
US-18 | Set up master-replica replication (REPLICAOF) | P1
US-19 | Deploy a VeloDB cluster with automatic sharding and failover | P1
US-20 | Run active-active (multi-master) replication across regions | P2
US-21 | Migrate slots between cluster nodes with zero downtime | P2

### Tooling

ID    | Story | Priority
------|-------|----------
US-22 | CLI tool (velodb-cli) with history, tab completion, REPL | P0
US-23 | Benchmark the server (velodb-benchmark) | P1
US-24 | Check/repair RDB files and AOF files | P1

---

## Functional Requirements

### FR-01: RESP2/RESP3 Wire Protocol
- 100% compatible with Redis serialization format
- Support inline, bulk string, array, integer, null, error
- RESP3: maps, sets, booleans, doubles, big numbers, push types

### FR-02: Command Set
- Full Redis 6.2 command set (approximately 200+ commands)
- Identical command names and argument order
- Identical error message format where practical

### FR-03: Storage Abstraction
- Storage trait abstracting get/put/delete/iterate
- In-memory implementation (DashMap + expiry timer wheel)
- RocksDB-based flash implementation
- Cached store wrapper with configurable eviction

### FR-04: Concurrency Model
- Thread-per-core architecture with slot-based key sharding
- Each core owns a private key range (slot mapping)
- Cross-core operations via message passing channels
- Zero global locks on the critical path

### FR-05: Persistence
- AOF: append-only file with no/everysec/always fsync modes
- RDB: binary snapshot format compatible with existing tools
- Background save via BGSAVE

### FR-06: Clustering (VeloDB Cluster v2)
- New cluster protocol (not Redis Cluster gossip)
- Raft-based membership
- Slot-to-node mapping with automatic rebalancing
- Zero-downtime slot migration
- Redis Cluster compatibility layer for client libraries

### FR-07: Configuration
- velodb.conf file compatible with redis.conf syntax
- CONFIG GET/SET/REWRITE for runtime configuration
- Environment variable overrides

### FR-08: Security
- TLS 1.3 via rustls
- ACL system with user/password, command allowlists, key patterns
- Client certificate authentication

---

## Non-Functional Requirements

### NFR-01: Performance
- In-memory GET: less than 0.5ms p99 at 100K ops/sec per core
- In-memory SET: less than 1ms p99 at 100K ops/sec per core
- Linear scaling up to (CPU cores - 1) threads

### NFR-02: Reliability
- No data loss on graceful shutdown
- Crash-safe AOF with checksums
- Automatic recovery from partial writes

### NFR-03: Security
- No unsafe code in core data path
- All network input validated via nom parser
- No command injection via Lua sandboxing

### NFR-04: Observability
- Structured JSON logging via tracing
- Prometheus metrics endpoint
- INFO command with detailed server statistics
- Slow log for query analysis
- Latency monitoring

### NFR-05: Compatibility
- Any Redis client library works without modification
- RDB files readable by redis-check-rdb
- AOF files replayable by standard Redis

---

## Out of Scope (v1)

- Redis Modules API (Rust-native plugin system in v2)
- RedisJSON / RedisSearch / RedisGraph equivalents
- Redis Sentinel (replaced by VeloDB Cluster v2)
- Full RESP3 push-based client caching (partial RESP3 types only)

---

## Success Metrics

- Compatibility: Pass redis-benchmark with identical output format
- Correctness: Pass all GET/SET/DEL/EXPIRE/PING tests against redis-rs
- Performance: Greater than or equal to Redis 6.2 single-threaded on identical hardware
- Safety: Zero unsafe blocks in hot path code
- Build: Single cargo build --release produces a static binary

---

## Open Questions

1. Should CLUSTER SLOTS / CLUSTER NODES be compatibility-mode only, or native?
2. Config file: TOML (Rust-native) or keep redis.conf syntax?
3. Minimum supported Rust version (MSRV): target Stable or require Nightly?
