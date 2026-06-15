// Copyright (c) 2025-Present VeloDB Contributors
// SPDX-License-Identifier: MIT

use std::path::PathBuf;
use std::sync::Arc;
use crate::config::ServerConfig;
use crate::net::listener::ServerHandle;
use crate::store::Store;
use crate::persist::aof::{AofWriter, FsyncPolicy, start_fsync_task};
use crate::persist::rdb;

pub async fn run(config: ServerConfig) -> anyhow::Result<()> {
    let store = Arc::new(Store::new(config.databases));

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

    let handle = ServerHandle::new(&config, store.clone(), aof_writer).await?;
    tracing::info!("VeloDB server listening on {}:{}", config.bind_address, config.port);
    tracing::info!("Ready to accept connections");

    tokio::select! {
        result = handle.accept_loop() => result?,
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
