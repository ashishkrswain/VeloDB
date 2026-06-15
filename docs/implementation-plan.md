# Implementation Plan — VeloDB Rewrite

## Phase Overview

| Phase | Name | Scope | Deliverable |
|-------|------|-------|-------------|
| 1 | Core Server | RESP parser, network, Storage trait, MemoryStore, basic commands | Working velodb-server with GET/SET/DEL/PING |
| 2 | Data Types | Lists, Sets, Hashes, ZSets, Streams, NestedHash | Full Redis data type support |
| 3 | Persistence | AOF + RDB save/load, BGSAVE | Durable storage |
| 4 | Multi-Threading | Thread-per-core sharding, cross-shard ops | Linear multi-core scaling |
| 5 | Replication | Master-replica, PSYNC, multi-master | High availability |
| 6 | Clustering | Raft-based cluster, slot migration, compatibility layer | Distributed deployment |
| 7 | Extras | Lua scripting, Pub/Sub, ACL, Transactions, TLS | Feature complete |

---

## Phase 1: Core Server

### Goal
A working velodb-server binary that:
- Binds to TCP port 6379
- Parses RESP2 protocol
- Responds to PING, ECHO, COMMAND
- Supports GET, SET, DEL, EXISTS, EXPIRE, TTL, SELECT, DBSIZE, FLUSHDB, FLUSHALL, KEYS, RANDOMKEY, TYPE, RENAME
- Config file parsing (redis.conf compatible)
- Graceful shutdown on SIGTERM

### Components

#### 1.1 Project Setup
```
Cargo.toml:
  [dependencies]
  tokio = { version = "1", features = ["full"] }
  bytes = "1"
  nom = "7"
  dashmap = "6"
  anyhow = "1"
  thiserror = "2"
  tracing = "0.1"
  tracing-subscriber = "0.3"
  serde = { version = "1", features = ["derive"] }
  toml = "0.8"
  clap = { version = "4", features = ["derive"] }
  parking_lot = "0.12"
  crc16 = "0.4"
  chrono = "0.4"

src/
  main.rs              # CLI args, signal handling, bootstrap
  config.rs            # Config parsing (redis.conf compatible)
  error.rs             # VeloDBError types
  resp/
    mod.rs             # RESP module re-exports
    parser.rs          # nom-based RESP2/RESP3 streaming parser
    types.rs           # RESP value types (enum)
    serializer.rs      # RESP serializer (values -> bytes)
  net/
    mod.rs             # Network module re-exports
    listener.rs        # TCP listener, TLS setup
    connection.rs      # Per-connection read/write state machine
  cmd/
    mod.rs             # Command table, dispatch logic
    string.rs          # GET, SET, MGET, MSET, INCR, DECR, APPEND, etc.
    generic.rs         # DEL, EXISTS, EXPIRE, TTL, PERSIST, TYPE, RENAME, etc.
    server.rs          # PING, ECHO, COMMAND, SELECT, FLUSHDB, FLUSHALL, etc.
  store/
    mod.rs             # Storage trait definition
    memory.rs          # InMemoryStore (DashMap + BTreeMap for expiry)
    factory.rs         # StorageFactory implementation
  server.rs            # Server struct, startup sequence, graceful shutdown
```

#### 1.2 RESP Parser

```rust
// resp/parser.rs
pub fn parse_resp(input: &[u8]) -> IResult<&[u8], RespValue> {
    alt((
        parse_simple_string,
        parse_error,
        parse_integer,
        parse_bulk_string,
        parse_array,
        parse_null_bulk,
        parse_null_array,
    ))(input)
}

// resp/types.rs
pub enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Option<Vec<u8>>),
    Array(Option<Vec<RespValue>>),
    // RESP3 additions (Phase 7)
    Null,
    Boolean(bool),
    Double(f64),
    Map(Vec<(RespValue, RespValue)>),
    Set(Vec<RespValue>),
    Push(Vec<RespValue>),
}
```

#### 1.3 Storage Trait

```rust
// store/mod.rs
#[async_trait]
pub trait Storage: Send + Sync + 'static {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    async fn set(&self, key: &[u8], value: Vec<u8>) -> Result<()>;
    async fn del(&self, key: &[u8]) -> Result<bool>;
    async fn exists(&self, key: &[u8]) -> Result<bool>;
    async fn expire(&self, key: &[u8], at_ms: u64) -> Result<bool>;
    async fn ttl(&self, key: &[u8]) -> Result<Option<i64>>;
    async fn keys(&self, pattern: &str) -> Result<Vec<Vec<u8>>>;
    async fn random_key(&self) -> Result<Option<Vec<u8>>>;
    async fn dbsize(&self) -> Result<usize>;
    async fn get_type(&self, key: &[u8]) -> Result<Option<String>>;
    async fn rename(&self, old: &[u8], new: &[u8]) -> Result<()>;
    async fn flush(&self) -> Result<()>;
}

// store/memory.rs
pub struct MemoryStore {
    data: DashMap<Vec<u8>, Entry>,
    expires: RwLock<BTreeMap<u64, Vec<Vec<u8>>>>,
}

struct Entry {
    value: Vec<u8>,
    type_name: String,
}
```

#### 1.4 Command Dispatch

```rust
// cmd/mod.rs
pub type CommandHandler = fn(&ServerContext, &Client, &[RespValue]) -> Result<RespValue>;

pub struct CommandDef {
    pub name: &'static str,
    pub arity: i32,
    pub handler: CommandHandler,
}

pub struct CommandTable {
    commands: HashMap<String, CommandDef>,
}

impl CommandTable {
    pub fn new() -> Self {
        let mut table = HashMap::new();
        table.insert("PING", CommandDef { name: "PING", arity: -1, handler: cmd_server::ping });
        table.insert("GET", CommandDef { name: "GET", arity: 2, handler: cmd_string::get });
        table.insert("SET", CommandDef { name: "SET", arity: -3, handler: cmd_string::set });
        // ... all other commands
        Self { commands: table }
    }

    pub fn dispatch(&self, name: &str, ctx: &ServerContext, client: &Client, args: &[RespValue]) -> Result<RespValue> {
        match self.commands.get(name.to_uppercase().as_str()) {
            Some(cmd) => {
                if cmd.arity > 0 && args.len() != cmd.arity as usize {
                    return Err(VeloDBError::wrong_number_of_args(name));
                }
                (cmd.handler)(ctx, client, args)
            }
            None => Err(VeloDBError::unknown_command(name)),
        }
    }
}
```

#### 1.5 Server Main Loop

```rust
// server.rs
pub struct Server {
    config: ServerConfig,
    databases: Vec<Box<dyn Storage>>,
    cmd_table: CommandTable,
}

impl Server {
    pub async fn run(config: ServerConfig) -> Result<()> {
        let mut server = Self::new(config)?;
        let listener = TcpListener::bind(("127.0.0.1", server.config.port)).await?;
        tracing::info!("velodb-server listening on port {}", server.config.port);

        loop {
            tokio::select! {
                result = listener.accept() => {
                    let (socket, addr) = result?;
                    let server_ctx = server.create_context();
                    tokio::spawn(handle_connection(socket, addr, server_ctx));
                }
                _ = shutdown_signal() => {
                    tracing::info!("shutting down");
                    break;
                }
            }
        }
        Ok(())
    }
}

async fn handle_connection(socket: TcpStream, addr: SocketAddr, ctx: ServerContext) -> Result<()> {
    let mut buf = BytesMut::with_capacity(4096);
    loop {
        socket.readable().await?;
        match socket.try_read_buf(&mut buf) {
            Ok(0) => break, // EOF
            Ok(n) => {
                while let Some((cmd, remaining)) = resp::parse_command(&buf)? {
                    let args = resp::extract_args(cmd)?;
                    let name = args[0].as_bulk_string().unwrap();
                    let response = ctx.dispatch(name, &args[1..]);
                    let bytes = resp::serialize(&response);
                    socket.write_all(&bytes).await?;
                    buf = remaining;
                }
            }
            Err(ref e) if e.kind() == WouldBlock => continue,
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}
```

### Tests
- RESP parser unit tests (all RESP types, edge cases, malformed input)
- GET/SET/DEL roundtrip integration test via redis-rs
- Expiry test (set with EX, verify nil after TTL)
- Multi-client concurrent access test
- Config file parsing test

### Deliverables
- `velodb-server` binary (cargo build --release)
- Passes: `redis-cli PING` -> PONG, `SET key val` -> OK, `GET key` -> val
- All Phase 1 commands functional
- Integration test suite passes

---

## Phase 2: Data Types

### Goal
Full support for all Redis data structure commands.

### Components
- List: LPUSH, RPUSH, LPOP, RPOP, LLEN, LRANGE, LINDEX, LSET, LTRIM, LREM, BLPOP, BRPOP
- Set: SADD, SREM, SMEMBERS, SISMEMBER, SCARD, SINTER, SUNION, SDIFF, SRANDMEMBER, SPOP
- Hash: HSET, HGET, HDEL, HEXISTS, HGETALL, HKEYS, HVALS, HLEN, HINCRBY
- ZSet: ZADD, ZREM, ZSCORE, ZRANK, ZRANGE, ZRANGEBYSCORE, ZCARD, ZCOUNT
- Stream: XADD, XREAD, XRANGE, XREVRANGE, XLEN, XDEL, XTRIM, XGROUP
- NestedHash: NHSET, NHGET, NHDEL, NHKEYS, NHVALS, NHGETALL

### Internal encoding optimizations
- Int encoding for small integers in strings
- Ziplist/Listpack encoding for small hashes/lists/zsets
- Intset for small integer-only sets

### Tests
- Command-level tests for each data type
- Encoding boundary tests (how data converts between encodings)
- Type mismatch error tests

---

## Phase 3: Persistence

### Goal
Durable storage with AOF and RDB.

### Components
- AOF writer: append commands to file, fsync policy
- AOF rewriter: background task produces minimal command set
- AOF loader: replay AOF on startup
- RDB serializer: binary format, all data types
- RDB loader: parse RDB, populate storage
- BGSAVE: fork child, write RDB in child process
- S3 backup: optional upload to S3-compatible storage

---

## Phase 4: Multi-Threading

### Goal
Thread-per-core sharding with linear scaling.

### Components
- Shard abstraction: owns key range, tokio runtime
- Slot router: crc16(key) -> slot -> shard
- Cross-shard message bus: tokio::mpsc per shard pair
- Multi-key commands: scatter-gather pattern
- 2PC for transactions spanning shards
- Connection assignment: route to shard by first command's key

---

## Phase 5: Replication

### Goal
High availability with master-replica replication.

### Components
- PSYNC handshake: replid + offset
- Replication backlog: ring buffer
- Full sync: RDB snapshot transfer
- Command streaming: real-time replication
- Multi-master (later): CRDT-based conflict resolution

---

## Phase 6: Clustering

### Goal
Distributed deployment with automatic failover.

### Components
- Raft-based membership (manager nodes)
- Slot-to-node mapping
- Slot migration with zero downtime
- Redis Cluster API compatibility (CLUSTER SLOTS, CLUSTER NODES, MOVED, ASK)
- Automatic failover on node failure

---

## Phase 7: Extras

### Goal
Feature complete Redis replacement.

### Components
- Lua scripting: EVAL, EVALSHA, SCRIPT LOAD/FLUSH/EXISTS
- Pub/Sub: SUBSCRIBE, PUBLISH, PSUBSCRIBE
- Transactions: MULTI, EXEC, DISCARD, WATCH, UNWATCH
- ACL: user management, command allowlists, key patterns
- TLS: rustls integration
- Slow log
- Latency monitor
- Client-side caching (RESP3 push notifications)

---

## Timeline Estimate (1 developer, full-time)

| Phase | Estimated Duration | Cumulative |
|-------|-------------------|------------|
| 1. Core Server | 4-6 weeks | 6 weeks |
| 2. Data Types | 4-6 weeks | 12 weeks |
| 3. Persistence | 3-4 weeks | 16 weeks |
| 4. Multi-Threading | 4-6 weeks | 22 weeks |
| 5. Replication | 3-4 weeks | 26 weeks |
| 6. Clustering | 3-4 weeks | 30 weeks |
| 7. Extras | 4-6 weeks | 36 weeks |

**Total: approximately 9 months to feature complete**

---

## Development Workflow

1. Each phase begins with: review phase design doc, create feature branch
2. Implementation: write code, write tests concurrently
3. All tests must pass before phase complete
4. Each phase ends with: merge to main, tag release, update changelog
5. CI: GitHub Actions — cargo test, cargo clippy, cargo fmt --check, cargo build --release
6. Benchmarks: criterion benchmarks run on each PR, regression alerts
