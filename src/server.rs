// Copyright (c) 2025-Present VeloDB Contributors
// SPDX-License-Identifier: MIT

use std::path::PathBuf;
use std::sync::Arc;
use tokio::net::TcpListener;
use crate::config::ServerConfig;
use crate::store::Store;
use crate::cmd::CommandTable;
use crate::persist::aof::{AofWriter, FsyncPolicy, start_fsync_task};
use crate::persist::rdb;
use crate::replication::backlog::ReplBacklog;

pub async fn run(config: ServerConfig) -> anyhow::Result<()> {
    crate::cmd::mark_start_time();
    let store = Arc::new(Store::new(config.databases));
    let eviction_policy = crate::store::EvictionPolicy::parse(&config.maxmemory_policy)
        .unwrap_or_else(|| {
            tracing::warn!("Unknown maxmemory-policy '{}', falling back to noeviction", config.maxmemory_policy);
            crate::store::EvictionPolicy::NoEviction
        });
    store.configure_memory(config.maxmemory, eviction_policy);
    if config.maxmemory > 0 {
        tracing::info!("maxmemory: {} bytes, policy: {:?}", config.maxmemory, eviction_policy);
    }
    let replid = crate::replication::backlog::ReplicationState::new().replid;
    let repl_backlog = Arc::new(std::sync::Mutex::new(ReplBacklog::new(config.repl_backlog_size)));

    // Try to load from persistence
    let rdb_path = PathBuf::from(format!("{}/{}", config.dir, config.dbfilename));
    let aof_path = PathBuf::from(format!("{}/{}", config.dir, "appendonly.aof"));

    if rdb_path.exists() {
        tracing::info!("Loading RDB from {:?}", rdb_path);
        match rdb::load_rdb(&store, &rdb_path) {
            Ok(count) => tracing::info!("Loaded {} keys from RDB", count),
            Err(e) => tracing::warn!("Failed to load RDB: {}, starting fresh", e),
        }
    } else if config.appendonly && aof_path.exists() {
        tracing::info!("Replaying AOF from {:?}", aof_path);
        replay_aof(&store, &aof_path).await?;
    }

    // Setup AOF writer if enabled
    let aof_writer = if config.appendonly {
        let policy = match config.appendfsync.as_str() {
            "always" => FsyncPolicy::Always,
            "no" => FsyncPolicy::No,
            _ => FsyncPolicy::EverySec,
        };
        let is_everysec = matches!(policy, FsyncPolicy::EverySec);
        let writer = Arc::new(AofWriter::open(aof_path, policy)?);
        if is_everysec {
            start_fsync_task(writer.clone());
        }
        Some(writer)
    } else { None };

    // Save initial RDB if empty and AOF is off
    if !config.appendonly && !rdb_path.exists() {
        let store_clone = store.clone();
        let config_clone = config.clone();
        tokio::spawn(async move {
            let _ = rdb::bgsave(store_clone, &config_clone).await;
        });
    }

    // Active expiry background task (Redis serverCron equivalent)
    start_active_expiry_task(store.clone(), std::time::Duration::from_millis(100));

    // Memory accounting + eviction background task
    if config.maxmemory > 0 {
        start_memory_cycle_task(store.clone(), std::time::Duration::from_millis(100));
    }

    // Auto-save background task
    if !config.save.is_empty() {
        let store_save = store.clone();
        let config_save = config.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(60)).await;
                let _ = rdb::bgsave(store_save.clone(), &config_save).await;
            }
        });
    }

    // Replication: start as replica if configured
    if let Some(ref replicaof) = config.replicaof {
        let parts: Vec<&str> = replicaof.splitn(2, ' ').collect();
        let master_host = parts[0].to_string();
        let master_port: u16 = parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(6379);
        let repl_store = store.clone();
        let cmd_table = Arc::new(CommandTable::new());
        let host = master_host.clone();
        let sync_state = Arc::new(std::sync::Mutex::new(crate::replication::replica::ReplicaSyncState::default()));
        tokio::spawn(async move {
            tracing::info!("Replica connecting to master at {}:{}", host, master_port);
            loop {
                match crate::replication::replica::connect_to_master(repl_store.clone(), cmd_table.clone(), &host, master_port, sync_state.clone()).await {
                    Ok(()) => tracing::info!("Replication connection closed"),
                    Err(e) => {
                        tracing::warn!("Replication connection failed: {}, retrying in 5s", e);
                        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    }
                }
            }
        });
    }

    // Bind listener
    let addr = format!("{}:{}", config.bind_address, config.port);
    let listener = TcpListener::bind(&addr).await?;
    tracing::info!("VeloDB server listening on {}", addr);
    tracing::info!("Master replication ID: {}", replid);
    tracing::info!("Starting with {} worker threads", config.cthreads);

    // Create sharded server
    let cmd_table = Arc::new(CommandTable::new());
    let sharded = crate::shard::ShardedServer::new(
        config.cthreads,
        store.clone(),
        cmd_table,
        &config,
        aof_writer,
        repl_backlog.clone(),
        &replid,
    );
    tracing::info!("{} shard runtimes started", config.cthreads);

    // TLS listener (optional, alongside the plaintext port)
    if config.tls_port > 0 {
        match (&config.tls_cert_file, &config.tls_key_file) {
            (Some(cert), Some(key)) => {
                let tls_config = crate::net::tls::load_tls_config(std::path::Path::new(cert), std::path::Path::new(key))?;
                let tls_addr = format!("{}:{}", config.bind_address, config.tls_port);
                let tls_listener = TcpListener::bind(&tls_addr).await?;
                tracing::info!("VeloDB TLS listener on {}", tls_addr);

                let tls_store = store.clone();
                let tls_cmd_table = Arc::new(CommandTable::new());
                let tls_config_clone = config.clone();
                let tls_backlog = repl_backlog.clone();
                let tls_replid = replid.clone();
                tokio::spawn(async move {
                    if let Err(e) = crate::net::tls::accept_loop(
                        tls_listener, tls_config, tls_store, tls_cmd_table, tls_config_clone, None, Some(tls_backlog), tls_replid,
                    ).await {
                        tracing::warn!("TLS accept loop stopped: {}", e);
                    }
                });
            }
            _ => tracing::warn!("tls-port set but tls-cert-file/tls-key-file missing; TLS listener not started"),
        }
    }

    // Cluster: start cluster bus if enabled
    if config.cluster_enabled {
        let node_id = format!("{:040x}", rand::random::<u128>());
        let node_addr = format!("{}:{}", config.bind_address, config.port);
        let cluster_state = Arc::new(std::sync::RwLock::new(
            crate::cluster::slots::ClusterState::new(node_id.clone(), node_addr, config.cluster_port)
        ));
        crate::cluster::slots::start_cluster_service(cluster_state);
        tracing::info!("Cluster mode enabled, node ID: {}", node_id);
    }

    // Accept loop
    tokio::select! {
        result = sharded.accept_loop(listener) => result?,
        _ = tokio::signal::ctrl_c() => tracing::info!("SIGINT received, shutting down"),
    }

    // Final save on shutdown
    tracing::info!("Saving final RDB snapshot");
    let _ = rdb::bgsave(store, &config).await;

    tracing::info!("VeloDB server stopped");
    Ok(())
}

/// Spawns the memory cycle: refreshes the cached usage estimate every
/// `interval` and, when over maxmemory with an eviction policy set,
/// evicts keys back under the limit.
pub fn start_memory_cycle_task(store: Arc<Store>, interval: std::time::Duration) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(interval).await;
            let usage = store.refresh_memory_usage();
            let limit = store.maxmemory();
            if limit > 0 && usage > limit {
                let policy = store.eviction_policy();
                let evicted = store.evict_until_under(limit, policy);
                if evicted > 0 {
                    tracing::info!("Evicted {} keys under {:?} policy ({} -> under {} bytes)", evicted, policy, usage, limit);
                }
            }
        }
    });
}

/// Spawns the active expiry background task: every `interval`, sample
/// volatile keys and evict expired ones (Redis serverCron equivalent).
pub fn start_active_expiry_task(store: Arc<Store>, interval: std::time::Duration) {
    tokio::spawn(async move {
        const SAMPLE_SIZE: usize = 20;
        loop {
            tokio::time::sleep(interval).await;
            let removed = store.active_expire_cycle(SAMPLE_SIZE);
            if removed > 0 {
                tracing::debug!("Active expiry removed {} keys", removed);
            }
        }
    });
}

async fn replay_aof(store: &Arc<Store>, path: &std::path::Path) -> anyhow::Result<()> {
    use crate::cmd::CommandTable;
    use crate::net::connection::ClientContext;
    use crate::resp;
    use bytes::Buf;
    use tokio::fs::File;
    use tokio::io::AsyncReadExt;

    let mut file = File::open(path).await?;
    let mut data = vec![];
    file.read_to_end(&mut data).await?;

    let cmd_table = Arc::new(CommandTable::new());
    let mut buf = bytes::BytesMut::from(data.as_slice());
    let mut ctx = ClientContext::new();
    let mut count = 0;

    while !buf.is_empty() {
        match resp::parse_command(&buf) {
            Ok((remaining, args)) => {
                if args.is_empty() { break; }
                let consumed = buf.len() - remaining.len();
                buf.advance(consumed);
                let cmd_name = String::from_utf8_lossy(&args[0]).to_uppercase();
                let _ = cmd_table.dispatch(&cmd_name, store, &mut ctx, &args[1..]);
                count += 1;
            }
            Err(nom::Err::Incomplete(_)) => break,
            Err(_) => {
                tracing::warn!("AOF parse error, stopping replay");
                break;
            }
        }
    }
    tracing::info!("AOF replay complete: {} commands", count);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_memory_cycle_task_evicts_over_limit() {
        let store = Arc::new(Store::new(1));
        store.configure_memory(2_000, crate::store::EvictionPolicy::AllKeysRandom);
        for i in 0..50u32 {
            store.set(0, format!("k{}", i).as_bytes(), &[0u8; 100], None).unwrap();
        }
        assert!(store.estimated_memory() > 2_000);

        start_memory_cycle_task(store.clone(), std::time::Duration::from_millis(10));

        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        assert!(store.estimated_memory() <= 2_000, "memory should be evicted under limit");
        assert!(store.databases[0].len() > 0, "some keys should survive");
    }

    #[tokio::test]
    async fn test_active_expiry_task_evicts_in_background() {
        let store = Arc::new(Store::new(1));
        store.set(0, b"dead", b"v", Some(1)).unwrap();
        store.set(0, b"live", b"v", None).unwrap();
        assert_eq!(store.databases[0].len(), 2);

        start_active_expiry_task(store.clone(), std::time::Duration::from_millis(10));

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert_eq!(store.databases[0].len(), 1);
        assert!(store.exists(0, b"live").unwrap());
    }
}
