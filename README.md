<!-- Copyright (c) 2025-Present VeloDB Contributors -->
<!-- SPDX-License-Identifier: MIT -->

# VeloDB

**The next-generation in-memory database engine. Built in Rust. Drop-in Redis compatible. Designed to change the game.**

---

## The Vision

Redis changed how we think about data — sub-millisecond access, rich data structures, dead-simple operations. But it's running on an architecture designed in 2009: single-threaded C, limited multi-core scaling, a clustering protocol held together with gossip and hope.

**VeloDB is the answer to a simple question: what if we rebuilt Redis from scratch, today, with everything we've learned?**

No inherited C bugs. No single-threaded bottleneck. No bolted-on clustering. Just a clean-sheet design for the hardware and workloads of 2025 and beyond.

---

## Why VeloDB Changes the Game

### 1. Rust: Memory Safety Without Compromise

Every line of VeloDB's core data path is safe Rust. No use-after-free. No buffer overflows. No segmentation faults taking down your production database at 3 AM. The compiler catches entire classes of bugs before they ever reach production.

This isn't about being trendy — it's about a database that *cannot* corrupt itself under load. That changes the reliability calculus for every team running Redis today.

### 2. True Multi-Core Scaling (Coming Phase 4)

Redis 6 introduced I/O threading, but commands still execute on a single thread. KeyDB forked to add threading, but at the cost of complexity and C's memory model.

VeloDB's **thread-per-core architecture** gives each core its own tokio runtime, its own key shard, and its own storage backend. CRC16-based slot routing distributes connections without coordination. Cross-shard operations flow through typed message channels — no global locks, no contention, linear scaling.

### 3. Pluggable Storage

In-memory when you need speed. RocksDB flash storage when your dataset outgrows RAM. Same wire protocol, same commands, no application changes. The `Storage` trait abstracts it all.

### 4. Next-Gen Clustering (Coming Phase 6)

No gossip. No split-brain. **Raft-based consensus** for cluster membership. A dedicated manager tier maintains the authoritative slot-to-node map. Data nodes report heartbeats — no voting, no confusion. Redis Cluster API compatibility means every existing client library works without changes.

### 5. Active-Active Replication (Coming Phase 5)

Multi-master with CRDT-based conflict resolution. Write to any node, read from any node. Global applications don't need complex primary-election logic.

### 6. 100% Redis Wire Compatible

You don't need new client libraries. You don't need to retrain your team. `redis-cli`, `redis-py`, `go-redis`, `ioredis`, Jedis — they all work. VeloDB speaks RESP2 today, RESP3 coming.

---

## What's Built So Far

| Phase | Feature | Status |
|-------|---------|--------|
| 1 | RESP2 protocol, TCP server, config parser | ✅ Complete |
| 2 | 6 data types, 52 commands, blocking operations | ✅ Complete |
| Test | 151 tests (unit + integration) | ✅ Complete |
| 3 | AOF + RDB persistence, BGSAVE | ✅ Complete |
| 7a | Pub/Sub, MULTI/EXEC transactions | ✅ Complete |

### 96 Commands Across 13 Modules

**Server:** `PING` `ECHO` `COMMAND` `SELECT`

**String:** `GET` `SET` `MGET` `MSET` `INCR` `INCRBY` `DECR` `DECRBY` `APPEND` `STRLEN` `GETRANGE` `SETRANGE` `GETSET`

**Generic:** `DEL` `EXISTS` `EXPIRE` `EXPIREAT` `PEXPIRE` `PEXPIREAT` `TTL` `PTTL` `PERSIST` `TYPE` `RENAME` `RENAMENX` `KEYS` `DBSIZE` `FLUSHDB` `FLUSHALL` `RANDOMKEY`

**List:** `LPUSH` `RPUSH` `LPOP` `RPOP` `LLEN` `LRANGE` `LINDEX` `LSET` `LTRIM` `LREM` `BLPOP` `BRPOP`

**Set:** `SADD` `SREM` `SMEMBERS` `SISMEMBER` `SCARD` `SINTER` `SUNION` `SDIFF` `SRANDMEMBER` `SPOP`

**Hash:** `HSET` `HGET` `HDEL` `HEXISTS` `HGETALL` `HKEYS` `HVALS` `HLEN` `HINCRBY`

**ZSet:** `ZADD` `ZREM` `ZSCORE` `ZRANK` `ZRANGE` `ZRANGEBYSCORE` `ZCARD` `ZCOUNT`

**Stream:** `XADD` `XRANGE` `XREVRANGE` `XLEN` `XDEL` `XTRIM` `XREAD`

**NestedHash:** `NHSET` `NHGET` `NHDEL` `NHKEYS` `NHVALS` `NHGETALL`

**Pub/Sub:** `SUBSCRIBE` `UNSUBSCRIBE` `PSUBSCRIBE` `PUNSUBSCRIBE` `PUBLISH`

**Transactions:** `MULTI` `EXEC` `DISCARD` `WATCH` `UNWATCH`

### Architecture

```
┌─────────────────────────────────────────────────┐
│  TCP Layer (tokio, per-connection tasks)        │
├─────────────────────────────────────────────────┤
│  RESP2 Parser (nom, streaming, zero-copy)       │
├─────────────────────────────────────────────────┤
│  Command Dispatch (HashMap, 96 commands)        │
├──────────┬──────────┬──────────┬────────────────┤
│  String  │  List    │  Set     │  Hash          │
│  ZSet    │  Stream  │  NHash   │  Server/Gen    │
├──────────┴──────────┴──────────┴────────────────┤
│  In-Memory Store (DashMap, lock-free)           │
│  BlockRegistry (tokio::Notify)                  │
│  PubSubRegistry (mpsc channels)                 │
├─────────────────────────────────────────────────┤
│  Persistence: AOF (RESP logging) + RDB (binary) │
└─────────────────────────────────────────────────┘
```

---

## Quick Start

```bash
# Build
cargo build --release

# Start the server
./target/release/velodb-server

# In another terminal — use any Redis client
redis-cli PING
# → PONG

redis-cli SET mykey "hello world"
# → OK

# Or use the bundled CLI
./target/release/velodb-cli
velodb> LPUSH mylist a b c
velodb> LRANGE mylist 0 -1
```

### Configuration

Create `velodb.conf`:

```
port 6379
bind 127.0.0.1
databases 16
dir ./
dbfilename dump.rdb
appendonly yes
appendfsync everysec
save 3600 1
save 300 100
```

### Features In Action

```bash
# Blocking operations
redis-cli BLPOP myqueue 5    # waits 5 seconds for an element

# Transactions with optimistic locking
redis-cli WATCH balance
redis-cli MULTI
redis-cli SET balance 100
redis-cli EXEC

# Pub/Sub
# Terminal 1: redis-cli SUBSCRIBE news
# Terminal 2: redis-cli PUBLISH news "Breaking story!"

# Persistence
redis-cli BGSAVE              # background snapshot
redis-cli SET key val         # automatically appended to AOF if enabled
```

---

## Roadmap

| Phase | Feature | Status |
|-------|---------|--------|
| 4 | Multi-Threading — thread-per-core sharding, slot routing | 🔜 Next |
| 5 | Replication — PSYNC, replication backlog, multi-master | 📋 Planned |
| 6 | Clustering — Raft-based, Redis Cluster API compatible | 📋 Planned |
| 7b | Lua scripting, ACL, TLS, RESP3 | 📋 Planned |

---

## Development

```bash
# Run all 151 tests
cargo test

# Build release
cargo build --release

# Run with config
cargo run --release -- --config velodb.conf --port 6380
```

### Project Structure

```
src/
├── main.rs              # Server binary entry point
├── bin/cli.rs           # CLI client binary
├── lib.rs               # Library root
├── config.rs            # redis.conf compatible parser
├── server.rs            # Startup orchestration
├── server_info.rs       # Runtime metrics
├── error.rs             # VeloDBError types (15 variants)
├── resp/                # RESP2 protocol (parser, serializer, types)
├── net/                 # TCP listener, connection handler
├── cmd/                 # 13 command modules (96 commands)
├── store/               # In-memory store (DashMap + BlockRegistry + PubSubRegistry)
└── persist/             # AOF writer + RDB save/load
```

---

## License

MIT — [SPDX-License-Identifier: MIT](https://spdx.org/licenses/MIT.html)
