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
    let store = Arc::new(Store::new(config.databases));
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
        tokio::spawn(async move {
            tracing::info!("Replica connecting to master at {}:{}", host, master_port);
            loop {
                match crate::replication::replica::connect_to_master(repl_store.clone(), cmd_table.clone(), &host, master_port).await {
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
