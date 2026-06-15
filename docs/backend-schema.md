<!-- Copyright (c) 2025-Present VeloDB Contributors -->
<!-- SPDX-License-Identifier: MIT -->

# Backend Schema — VeloDB Rewrite

## 1. Data Model

### 1.1 Core Data Structures

```rust
/// A value stored in the database
pub enum StorageValue {
    String(Vec<u8>),
    List(QuickList),
    Set(HashSet<Vec<u8>>),
    Hash(HashMap<Vec<u8>, Vec<u8>>),
    ZSet(SkipList),
    Stream(RadixTree),
    NestedHash(HashMap<Vec<u8>, HashMap<Vec<u8>, Vec<u8>>>),
}

/// Value type enum for TYPE command
pub enum ValueType {
    String = 0,
    List = 1,
    Set = 2,
    ZSet = 3,
    Hash = 4,
    Stream = 5,
    NestedHash = 6,
}
```

### 1.2 Key-Value Entry

```rust
pub struct KeyEntry {
    pub value: StorageValue,
    pub expire_at: Option<u64>,     // milliseconds timestamp
    pub access_time: AtomicU64,     // approximate LRU
    pub lfu_counter: AtomicU8,      // LFU access frequency
    pub encoding: EncodingType,     // internal encoding optimization
}
```

### 1.3 Database Instance

```rust
pub struct Database {
    pub id: usize,
    pub store: Box<dyn Storage>,
    pub expires: BTreeMap<u64, Vec<Vec<u8>>>,  // timestamp -> keys
    pub key_count: AtomicU64,
    pub avg_ttl: AtomicU64,
}

pub struct Shard {
    pub index: usize,
    pub runtime: tokio::runtime::Runtime,
    pub databases: Vec<Database>,       // one per db index (0-15)
    pub clients: DashMap<u64, ClientState>,
    pub slot_range: Range<u16>,         // which hash slots this shard owns
    pub cross_shard_rx: mpsc::Receiver<CrossShardOp>,
    pub cross_shard_txs: HashMap<u64, mpsc::Sender<CrossShardOp>>, // peer shard senders
}
```

### 1.4 Client Connection

```rust
pub struct ClientState {
    pub id: u64,
    pub db: usize,                          // selected database
    pub socket: TcpStream,
    pub read_buffer: BytesMut,
    pub write_buffer: BytesMut,
    pub authenticated: bool,
    pub user: Option<Arc<User>>,
    pub flags: ClientFlags,
    pub multi_queue: Vec<Command>,          // MULTI transaction queue
    pub watched_keys: Vec<Vec<u8>>,         // WATCH keys
    pub subscribed_channels: HashSet<Vec<u8>>,
    pub subscribed_patterns: Vec<GlobPattern>,
    pub created_at: Instant,
    pub last_active: Instant,
}
```

---

## 2. Persistence Schema

### 2.1 AOF Format

AOF is a text file in RESP format:

```
*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n
*2\r\n$3\r\nDEL\r\n$7\r\nold_key\r\n
*3\r\n$6\r\nEXPIRE\r\n$3\r\nkey\r\n$4\r\n1000\r\n
```

Optional RDB preamble at start of file.

### 2.2 RDB Format

Binary format, key sections:

```
RDB Header:
  Magic: "REDIS" (5 bytes)
  Version: "0009" (4 bytes)  [or "0010" for VeloDB extensions]

Aux Fields:
  FE 0x00                    database selector
  FB 0x00 0x00               database 0, 0 expired keys
  FD 0x00 0x00 0x00 0x00 0x00 0x00 0x00 0x00  [expiry timestamp, 8 bytes LE]
  0x00                        string type
  key_len key_bytes
  value_len value_bytes

Checksum:
  FF 0x00 0x00 0x00 0x00 0x00 0x00 0x00 0x00  [CRC64, 8 bytes]
```

### 2.3 RocksDB Key Format

```
column family: default
  key:   {slot:2 bytes BE}{key bytes}
  value: {type:1 byte}{encoded_value}

column family: expire
  key:   {expire_at_ms:8 bytes BE}{slot:2 bytes BE}{key bytes}
  value: empty (used for scanning expired keys)
```

---

## 3. Cluster Schema

### 3.1 Slot Assignment

```rust
pub struct SlotMap {
    // slot -> node mapping
    pub slots: [SlotState; 16384],
}

pub enum SlotState {
    Local,              // owned by this node
    Remote(NodeId),     // owned by remote node
    Migrating(NodeId),  // being migrated to remote node
    Importing(NodeId),  // being imported from remote node
}
```

### 3.2 Cluster Manager State (Raft)

```rust
pub struct ClusterState {
    pub members: HashMap<NodeId, NodeInfo>,
    pub epoch: u64,
    pub slot_map: Arc<RwLock<SlotMap>>,
    pub leader_id: Option<NodeId>,
    pub raft_state: RaftState,
}

pub struct NodeInfo {
    pub id: NodeId,
    pub addr: SocketAddr,
    pub cluster_port: u16,      // default: base_port + 10000
    pub flags: NodeFlags,
    pub slots: Vec<u16>,        // slots owned by this node
    pub last_seen: Instant,
    pub repl_offset: u64,
}
```

---

## 4. Replication Schema

### 4.1 Replication State

```rust
pub struct ReplicationState {
    pub role: ReplRole,
    pub replid: String,             // 40-char hex replication ID
    pub replid2: Option<String>,    // previous replid (after failover)
    pub master_repl_offset: u64,
    pub backlog: ReplBacklog,
    pub masters: Vec<MasterInfo>,   // for multi-master
    pub replicas: Vec<ReplicaInfo>,
}

pub struct ReplBacklog {
    pub buffer: Vec<u8>,            // ring buffer
    pub capacity: usize,            // configurable size
    pub offset: u64,                // global replication offset
    pub replid: String,
}

pub struct MasterInfo {
    pub host: String,
    pub port: u16,
    pub state: MasterState,         // CONNECTING, CONNECTED, SYNC, etc.
    pub repl_offset: u64,
    pub link: Option<TcpStream>,
}

pub struct ReplicaInfo {
    pub id: u64,
    pub addr: SocketAddr,
    pub state: ReplicaState,        // WAIT_BGSAVE_START, SEND_BULK, ONLINE
    pub repl_offset: u64,
    pub repl_ack_time: Instant,
}
```

---

## 5. ACL Schema

```rust
pub struct User {
    pub name: String,
    pub flags: UserFlags,
    pub passwords: Vec<HashedPassword>,
    pub commands: String,           // +@all -@dangerous +set +get
    pub keys: Vec<KeyPattern>,      // ~* or ~prefix:*
    pub channels: Vec<ChannelPattern>,
    pub enabled: bool,
}

pub struct UserFlags {
    pub enabled: bool,
    pub nopass: bool,
    pub nocommands: bool,
    pub allcommands: bool,
    pub allkeys: bool,
    pub allchannels: bool,
    pub sanitize_payload: bool,
}

pub struct KeyPattern {
    pub pattern: String,
    pub allow: bool,
}
```

---

## 6. Metrics Schema

```rust
pub struct ServerMetrics {
    pub uptime_seconds: u64,
    pub connected_clients: u64,
    pub blocked_clients: u64,
    pub used_memory_bytes: u64,
    pub used_memory_rss_bytes: u64,
    pub mem_fragmentation_ratio: f64,
    pub total_connections_received: u64,
    pub total_commands_processed: u64,
    pub instantaneous_ops_per_sec: u64,
    pub total_net_input_bytes: u64,
    pub total_net_output_bytes: u64,
    pub keyspace_hits: u64,
    pub keyspace_misses: u64,
    pub pubsub_channels: u64,
    pub pubsub_patterns: u64,
    pub expired_keys: u64,
    pub evicted_keys: u64,
    pub rejected_connections: u64,
    pub sync_full: u64,
    pub sync_partial_ok: u64,
    pub sync_partial_err: u64,
    pub per_db: Vec<DbMetrics>,
    pub per_cmd: HashMap<String, CmdMetrics>,
}

pub struct DbMetrics {
    pub keys: u64,
    pub expires: u64,
    pub avg_ttl: u64,
}

pub struct CmdMetrics {
    pub calls: u64,
    pub total_time_us: u64,
}
```

---

## 7. Configuration Schema

```rust
pub struct ServerConfig {
    // Network
    pub port: u16,                    // default: 6379
    pub bind_addresses: Vec<String>,  // default: ["127.0.0.1"]
    pub tcp_backlog: i32,             // default: 511
    pub timeout: u64,                 // default: 0 (no timeout)
    pub tcp_keepalive: u64,           // default: 300

    // General
    pub daemonize: bool,              // default: false
    pub pidfile: Option<String>,
    pub loglevel: LogLevel,           // debug, verbose, notice, warning
    pub logfile: Option<String>,
    pub databases: usize,             // default: 16

    // Sharding
    pub shard_count: usize,           // default: num_cpus
    pub shard_threads_per_core: usize,// default: 1
    pub active_client_balancing: bool,

    // Memory
    pub maxmemory: ByteSize,          // e.g., "32gb"
    pub maxmemory_policy: EvictPolicy,
    pub maxmemory_samples: usize,     // default: 5

    // Persistence
    pub save_rules: Vec<SaveRule>,    // e.g., [(900,1), (300,10)]
    pub dbfilename: String,           // default: "dump.rdb"
    pub dir: PathBuf,                 // default: "./"
    pub appendonly: bool,
    pub appendfsync: FsyncMode,       // no, everysec, always
    pub aof_rewrite_percentage: u64,  // default: 100
    pub aof_rewrite_min_size: ByteSize,
    pub aof_use_rdb_preamble: bool,   // default: true

    // Storage
    pub storage_provider: StorageConfig,

    // Security
    pub requirepass: Option<String>,
    pub aclfile: Option<PathBuf>,

    // TLS
    pub tls_port: Option<u16>,
    pub tls_cert_file: Option<PathBuf>,
    pub tls_key_file: Option<PathBuf>,
    pub tls_ca_cert_file: Option<PathBuf>,

    // Cluster
    pub cluster_enabled: bool,
    pub cluster_config_file: Option<PathBuf>,

    // Replication
    pub replicaof: Option<(String, u16)>,
    pub masterauth: Option<String>,
    pub repl_backlog_size: ByteSize,   // default: 1MB

    // Slow log
    pub slowlog_log_slower_than: u64,  // microseconds, default: 10000
    pub slowlog_max_len: usize,        // default: 128

    // Advanced
    pub hash_max_ziplist_entries: usize,
    pub hash_max_ziplist_value: usize,
    pub list_max_ziplist_size: i32,
    pub set_max_intset_entries: usize,
    pub zset_max_ziplist_entries: usize,
    pub zset_max_ziplist_value: usize,
    pub stream_node_max_bytes: usize,
    pub stream_node_max_entries: usize,
    pub hz: usize,                     // server cron frequency, default: 10
    pub lazyfree_lazy_eviction: bool,
    pub lazyfree_lazy_expire: bool,
    pub lazyfree_lazy_server_del: bool,
    pub replica_lazy_flush: bool,
    pub activerehashing: bool,
    pub active_defrag: bool,
}

pub enum StorageConfig {
    Memory,
    Flash { path: PathBuf, cache_size: ByteSize },
}

pub enum EvictPolicy {
    NoEviction,
    AllKeysLru,
    AllKeysLfu,
    AllKeysRandom,
    VolatileLru,
    VolatileLfu,
    VolatileRandom,
    VolatileTtl,
}

pub enum FsyncMode {
    No,
    EverySec,
    Always,
}
```
