<!-- Copyright (c) 2025-Present VeloDB Contributors -->
<!-- SPDX-License-Identifier: MIT -->

# VeloDB

**The next-generation in-memory database engine. Built in Rust. Drop-in Redis compatible. Designed to change the game.**

---

## What Is VeloDB?

VeloDB is a **high-performance, multi-model, in-memory database server** that speaks the Redis protocol natively. Fire up any Redis client, point it at VeloDB, and everything just works — but underneath, everything is different.

Think of it as Redis rebuilt from the metal up: same familiar commands and wire format, but running on a modern Rust engine with lock-free concurrency, pluggable storage backends, and a clean-room implementation that inherits none of the original C codebase's limitations.

### How It Works: A Request's Journey

When a client connects and sends `SET mykey "hello"`, here's exactly what happens inside VeloDB:

```
1. TCP ACCEPT
   tokio's async runtime accepts the connection on port 6379
   and spawns a dedicated green thread (task) for this client.
   No thread pools, no blocking — pure async I/O.

2. STREAMING RESP2 PARSER
   Bytes arrive into a BytesMut buffer (4KB initial, grows dynamically).
   VeloDB's nom-based parser reads the wire protocol byte-by-byte:
     *3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$5\r\nhello\r\n
   The parser is streaming — it handles partial reads naturally,
   returning "Incomplete, give me more data" when a command is
   split across TCP packets. No wasted CPU spinning on incomplete frames.

3. COMMAND DISPATCH
   The parser extracts the command name ("SET") and arguments
   (["mykey", "hello"]). A HashMap<String, CommandDef> lookup finds
   the handler function in O(1). Arity validation runs — wrong number
   of arguments returns an error immediately, before touching any data.

4. COMMAND EXECUTION
   The SET handler processes optional flags (EX, PX, NX, XX, etc.),
   calculates absolute expiry timestamps if needed, then calls into
   the storage layer.

5. STORAGE: LOCK-FREE CONCURRENT HASHMAP
   The Store holds 16 databases (configurable), each backed by a
   DashMap — a sharded, lock-free concurrent hashmap. Each database
   shard owns its stripe of keys independently. No global mutex.
   No contention between clients writing to different keys.

   Every entry stores:
   - A StorageValue enum (String, List, Set, Hash, ZSet, Stream, NestedHash)
   - An optional expire_at timestamp (passive expiry: checked on access)
   - A version counter (for WATCH/MULTI/EXEC optimistic locking)

   The ZSet uses a dual-index: HashMap<member, score> for O(1) lookups
   and BTreeMap<score, members> for ordered range queries.
   Zero-copy where possible — DashMap references avoid cloning until write.

6. AOF PERSISTENCE (if enabled)
   Before responding to the client, the raw command bytes are appended
   to the Append-Only File. Fsync policy controls durability:
   - "no": write to OS buffer, let the kernel decide
   - "everysec": background tokio task fsyncs every 1 second
   - "always": fsync before responding (slowest, safest)

7. BLOCKING OPERATIONS
   If the command was BLPOP on an empty list, VeloDB doesn't spin.
   The connection registers a tokio::Notify on the key via the
   BlockRegistry, then parks. When another connection LPUSHes to
   that key, the notify fires, the blocked connection wakes up,
   pops the value, and responds. Timeouts use tokio::select! for
   race-free cancellation.

8. PUB/SUB MESSAGE DELIVERY
   When a client runs SUBSCRIBE news, VeloDB registers an mpsc
   unbounded sender in the PubSubRegistry. The connection enters
   "pubsub mode" — a select loop monitoring both the TCP socket
   and the message channel. PUBLISH fans out through exact channel
   matching AND glob pattern matching (PSUBSCRIBE), using the same
   glob engine as KEYS.

9. RESPONSE SERIALIZATION
   The command handler returns a RespValue enum. The serializer
   walks the tree and emits wire-format bytes with proper \r\n
   termination. The response is written to the TCP socket via
   write_all — tokio handles the async I/O under the hood.

10. CONNECTION LIFECYCLE
    The connection loop continues: read → parse → dispatch → write.
    Pipelining is supported — multiple commands in one TCP packet
    are processed sequentially before the next read.
    The entire system is single-threaded per connection but
    multi-connection via tokio's work-stealing scheduler.
```

### What Makes It Different

**Zero unsafe code in the hot path.** The RESP parser, command dispatch, storage engine, and network layer are all safe Rust. The compiler guarantees no data races, no use-after-free, no buffer overruns at the type level. Your database doesn't segfault.

**Lock-free everywhere.** DashMap shards keys internally. BlockRegistry uses atomic operations and tokio::Notify. PubSubRegistry uses mpsc channels. There is not a single Mutex on the critical path from client request to response.

**Batteries included.** Persistence (AOF + RDB snapshot), blocking operations (BLPOP with timeouts), transactions (MULTI/EXEC with WATCH), and Pub/Sub — all built in from the start, not bolted on later.

**Extensible by design.** The StorageValue enum makes adding a new data type a matter of defining the variant, adding Store methods, and writing command handlers. The CommandTable registry turns adding a new command into a three-line registration.

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
