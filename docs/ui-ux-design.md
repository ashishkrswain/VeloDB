<!-- Copyright (c) 2025-Present VeloDB Contributors -->
<!-- SPDX-License-Identifier: MIT -->

# UI/UX Design — VeloDB Rewrite

## 1. Overview

VeloDB provides three user interfaces:
1. **velodb-cli** — Interactive REPL terminal client (replacing redis-cli)
2. **HTTP Admin API** — Metrics, health, cluster status (replaces INFO command for monitoring)
3. **Server Logs** — Structured output for operators

---

## 2. velodb-cli — Interactive CLI

### 2.1 Design Philosophy

- Familiar to Redis CLI users (identical interaction model)
- Enhanced with modern terminal features (syntax coloring, autocomplete, inline help)
- Tab completion for commands and keys
- Built-in latency and performance hints

### 2.2 Startup Screen

```
╔══════════════════════════════════════════════════╗
║                VeloDB CLI v0.1.0                 ║
║         Connected to localhost:6379              ║
║         RESP3 mode | TLS: disabled               ║
╚══════════════════════════════════════════════════╝

127.0.0.1:6379>
```

### 2.3 Command Input

- Prompt shows `host:port[db]>` (e.g., `127.0.0.1:6379[0]>`)
- Syntax coloring: commands in cyan, keys in yellow, values in green, errors in red
- Tab completion for Redis commands (types "GE" + tab -> "GET")
- Up/down arrow for command history (persisted to ~/.velodb_history)
- Ctrl+C: cancel current input, does not exit
- Ctrl+D: exit (or "exit" / "quit" command)

### 2.4 Response Display

GET response:
```
127.0.0.1:6379[0]> GET mykey
"myvalue"
```

SET response:
```
127.0.0.1:6379[0]> SET mykey "hello world"
OK
```

Array response (MGET):
```
127.0.0.1:6379[0]> MGET key1 key2 key3
1) "value1"
2) "value2"
3) (nil)
```

Error response:
```
127.0.0.1:6379[0]> GET nonexistent
(nil)
127.0.0.1:6379[0]> SET
(error) ERR wrong number of arguments for 'set' command
```

### 2.5 Special Modes

**Monitor Mode** (`velodb-cli monitor`):
```
OK
127.0.0.1:6379[0] 12:34:56.789 [0 127.0.0.1:54321] "SET" "key" "value"
127.0.0.1:6379[0] 12:34:57.012 [0 127.0.0.1:54321] "GET" "key"
127.0.0.1:6379[0] 12:34:58.345 [1 10.0.0.5:12345] "INCR" "counter"
```

**Latency Mode** (`velodb-cli --latency`):
```
min: 0, max: 1, avg: 0.12 (312 samples)
```

**Cluster Mode** (`velodb-cli -c`):
- Automatically follows MOVED/ASK redirects
- Prompt shows cluster info

---

## 3. HTTP Admin API — Prometheus Metrics

### 3.1 Endpoints

| Path | Method | Description |
|------|--------|-------------|
| `/metrics` | GET | Prometheus text format metrics |
| `/health` | GET | Health check (200 OK or 503) |
| `/health/ready` | GET | Readiness check |
| `/cluster/status` | GET | Cluster membership JSON |

### 3.2 Prometheus Metrics

```
# HELP velodb_connected_clients Number of client connections
# TYPE velodb_connected_clients gauge
velodb_connected_clients 42

# HELP velodb_commands_total Total commands processed
# TYPE velodb_commands_total counter
velodb_commands_total{cmd="get"} 15234
velodb_commands_total{cmd="set"} 8912

# HELP velodb_keyspace_keys Number of keys per database
# TYPE velodb_keyspace_keys gauge
velodb_keyspace_keys{db="0"} 1048576

# HELP velodb_used_memory_bytes Total memory used
# TYPE velodb_used_memory_bytes gauge
velodb_used_memory_bytes 2147483648

# HELP velodb_instantaneous_ops_per_sec Operations per second
# TYPE velodb_instantaneous_ops_per_sec gauge
velodb_instantaneous_ops_per_sec 45231
```

### 3.3 Health Endpoint

```json
{
  "status": "healthy",
  "uptime_seconds": 86400,
  "version": "0.1.0",
  "shards": {
    "total": 8,
    "healthy": 8,
    "degraded": 0,
    "down": 0
  },
  "storage": {
    "backend": "memory",
    "used_bytes": 2147483648,
    "total_bytes": 34359738368
  },
  "replication": {
    "role": "master",
    "connected_replicas": 3
  }
}
```

---

## 4. Server Log Output

### 4.1 Default Text Format

```
2026-06-15T10:30:00.123Z  INFO velodb: VeloDB 0.1.0 starting
2026-06-15T10:30:00.456Z  INFO velodb: Storage backend: memory (maxmemory: 32GB)
2026-06-15T10:30:00.789Z  INFO velodb: 8 shards initialized
2026-06-15T10:30:00.890Z  INFO velodb: TCP listener on 0.0.0.0:6379
2026-06-15T10:30:00.891Z  INFO velodb: HTTP metrics on 0.0.0.0:9090
2026-06-15T10:30:00.892Z  INFO velodb: Ready to accept connections

2026-06-15T10:31:15.234Z  WARN velodb::net: Client 127.0.0.1:54321 exceeded max query buffer (512MB), closing connection
2026-06-15T10:35:00.567Z  INFO velodb::persist: RDB snapshot saved: dump.rdb (2.1GB, 45s)

2026-06-15T11:00:00.001Z  INFO velodb: SIGTERM received, shutting down
2026-06-15T11:00:05.123Z  INFO velodb: All connections drained
2026-06-15T11:00:05.456Z  INFO velodb: AOF flushed to disk
2026-06-15T11:00:05.789Z  INFO velodb: Goodbye
```

### 4.2 JSON Format (--log-format json)

```json
{"timestamp":"2026-06-15T10:30:00.123Z","level":"INFO","target":"velodb","message":"VeloDB 0.1.0 starting"}
{"timestamp":"2026-06-15T10:30:00.456Z","level":"INFO","target":"velodb","message":"Storage backend: memory","maxmemory_bytes":34359738368}
{"timestamp":"2026-06-15T10:30:00.891Z","level":"INFO","target":"velodb::net","message":"Ready to accept connections","port":6379}
```

---

## 5. Configuration File UX

velodb.conf follows redis.conf conventions:

```conf
# VeloDB configuration file example

# Network
port 6379
bind 127.0.0.1
tcp-backlog 511
timeout 0
tcp-keepalive 300

# General
daemonize no
loglevel notice
logfile ""
databases 16

# Sharding
shard-count 8
shard-threads-per-core 1

# Memory
maxmemory 32gb
maxmemory-policy allkeys-lru

# Persistence
save 900 1
save 300 10
save 60 10000
dbfilename dump.rdb
dir /var/lib/velodb
appendonly no
appendfsync everysec

# Storage
storage-provider memory
# storage-provider flash /mnt/flash/db

# Security
requirepass ""
# aclfile /etc/velodb/users.acl

# TLS
# tls-port 6380
# tls-cert-file /etc/velodb/server.crt
# tls-key-file /etc/velodb/server.key

# Cluster
# cluster-enabled yes
# cluster-config-file nodes.conf

# Replication
# replicaof 192.168.1.100 6379

# Slow log
slowlog-log-slower-than 10000
slowlog-max-len 128
```
