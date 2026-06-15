# App Flow — VeloDB Rewrite

## 1. Server Startup Flow

```
start
  |
  v
[Parse CLI args] -----> --help, --version, --port, --config
  |
  v
[Load config file] ----> velodb.conf (redis.conf syntax)
  |                      Environment variable overrides
  v
[Initialize logging] ---> tracing subscriber (stderr/file/json)
  |
  v
[Initialize storage] ---> MemoryStore (default) or RocksDBStore
  |                      via StorageFactory
  v
[Load persistence] -----> Replay AOF if exists
  |                      Load RDB if exists (takes priority)
  v
[Setup networking] -----> Bind TCP port 6379 (or configured)
  |                      Optional Unix socket
  |                      Optional TLS setup
  v
[Create shards] --------> N = cthreads (default: num_cpus)
  |                      Each shard = tokio runtime + Storage
  v
[Spawn admin server] ---> HTTP :9090 for metrics/health
  |
  v
[Enter event loop] -----> Accept connections
                         Route to shards
                         Process until SIGTERM
```

## 2. Connection Lifecycle

```
[Client connects]
  |
  v
[TCP handshake] --------> TLS handshake (if enabled)
  |
  v
[Read initial bytes] ---> Parse RESP command
  |
  v
[Extract key] ----------> crc16(key) % 16384 = slot
  |                      slot % shard_count = shard_index
  v
[Route to shard] -------> Connection handed to shard runtime
  |
  v
[Command loop] ---------> Read -> Parse -> Dispatch -> Reply
  |                      (repeats until connection closed)
  v
[Client disconnects] ---> Cleanup client state
                         Unsubscribe from pub/sub channels
                         Release watched keys
```

## 3. Single Command Execution Flow

```
[Bytes arrive on TCP stream]
  |
  v
[RESP Parser] ----------> nom combinator reads from BytesMut
  |                      Buffer pipeline: multiple commands?
  |                      Parse: *3\r\n$3\r\nSET\r\n...
  |                      Result: Vec<Command> (one or more)
  v
[For each Command]
  |
  v
[Command validation] ---> Check arity (min/max args)
  |                      ACL check (user permissions)
  |                      OOM check (if DENYOOM flag)
  v
[Key extraction] -------> first_key, last_key, key_step
  |                      Validate key types match expected
  v
[Cluster check] --------> Does this node own the slot?
  |                      If no: return -MOVED slot ip:port
  v
[Execute handler] ------> match command name:
  |                        "SET" -> cmd_string::set()
  |                        "GET" -> cmd_string::get()
  |                        etc.
  v
[Storage interaction] --> storage.set(key, val)
  |                      storage.get(key)
  v
[Post-execution] -------> If write command:
  |                        - Increment dirty counter
  |                        - Add to AOF buffer
  |                        - Add to replication backlog
  |                        - Notify pub/sub if applicable
  v
[Serialize response] ---> RESP serializer
  |                      +OK\r\n  or  $5\r\nvalue\r\n
  v
[Write to client] ------> append to TcpStream write buffer
  |                      Flush when buffer full or idle
```

## 4. Cross-Shard Operation Flow (MGET across slots)

```
[Client sends: MGET key1 key2 key3]
  |
  v
[Shard A (where client lives)]
  |
  v
[Group keys by slot] ---> key1 -> slot 400  -> shard A (local)
  |                      key2 -> slot 8001 -> shard B (remote)
  |                      key3 -> slot 12000-> shard C (remote)
  v
[Send remote requests] -> tokio::mpsc to Shard B: GET key2
  |                      tokio::mpsc to Shard C: GET key3
  v
[Wait for all responses] (timeout: 5s)
  |
  v
[Local: storage.get(key1)]
  |
  v
[Assemble result] ------> [value1, value2, value3]
  |
  v
[Serialize and reply] --> *3\r\n$6\r\nvalue1\r\n...
```

## 5. Transaction Flow (MULTI/EXEC)

```
[Client sends: MULTI]
  |
  v
[Enter MULTI mode] -----> flag client as multi-mode
  |                      Reply: +OK
  v
[Client sends: SET key1 val1]
  |
  v
[Queue command] --------> push to client->multi_queue
  |                      Reply: +QUEUED
  v
[Client sends: GET key2]
  |
  v
[Queue command] --------> Reply: +QUEUED
  |
  v
[Client sends: EXEC]
  |
  v
[Determine affected shards] -> keys in queue span shards A, B
  |
  v
[2PC Phase 1: PREPARE] --> Send prepare to Shard B
  |                       Shard B acquires locks on key2
  |                       Shard B replies: READY
  v
[2PC Phase 2: COMMIT] ---> Shard A executes: SET key1 val1
  |                       Shard B executes: GET key2
  v
[Assemble responses] ----> [OK, value2]
  |
  v
[Reply to client] -------> *2\r\n+OK\r\n$6\r\nvalue2\r\n
```

## 6. Replication Flow (Full Sync)

```
[Replica connects to Master]
  |
  v
[Send PING] ------------> Master replies: +PONG
  |
  v
[Send AUTH] (if needed) -> Master authenticates
  |
  v
[Send REPLCONF] --------> listening-port, capabilities
  |
  v
[Send PSYNC ? -1] ------> Request full resync
  |
  v
[Master forks BGSAVE] --> Child process writes RDB
  |                      Parent accumulates replication backlog
  v
[Master sends RDB] -----> Full RDB file over socket
  |
  v
[Replica loads RDB] ----> Populates in-memory store
  |                      Flushes to RocksDB if flash enabled
  v
[Master sends backlog] -> Commands accumulated during RDB transfer
  |
  v
[Replica replays] ------> Executes backlog commands in order
  |
  v
[Steady state] ---------> Master streams commands in real-time
                         Replica applies each command
```

## 7. Shutdown Flow

```
[SIGTERM received]
  |
  v
[Stop accepting] -------> Close TCP listener
  |                      Stop accepting new connections
  v
[Drain connections] ----> Finish in-flight commands
  |                      Timeout: shutdown-timeout (default 10s)
  v
[Save persistence] -----> Flush AOF buffer to disk
  |                      Final BGSAVE if configured
  v
[Stop shards] ----------> Cancel tokio runtimes
  |                      Join threads
  v
[Exit process] ---------> Exit code 0
```
