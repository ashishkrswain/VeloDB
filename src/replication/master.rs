// Copyright (c) 2025-Present VeloDB Contributors
// SPDX-License-Identifier: MIT

use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use crate::store::Store;
use crate::persist::rdb;
use crate::config::ServerConfig;

pub async fn handle_replica_connection(
    mut socket: TcpStream,
    store: Arc<Store>,
    backlog: Arc<std::sync::Mutex<crate::replication::backlog::ReplBacklog>>,
    replid: String,
    config: ServerConfig,
) -> anyhow::Result<()> {
    let mut buf = [0u8; 4096];
    let n = socket.read(&mut buf).await?;
    if n == 0 { return Ok(()); }

    let cmd = String::from_utf8_lossy(&buf[..n]);
    let lines: Vec<&str> = cmd.split("\r\n").filter(|s| !s.is_empty() && !s.starts_with('*') && !s.starts_with('$')).collect();

    if lines.len() >= 2 && lines[0].eq_ignore_ascii_case("PSYNC") {
        let parts: Vec<&str> = lines[1].splitn(2, ' ').collect();
        let req_replid = parts[0];
        let offset: u64 = parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(0);

        let backlog_lock = backlog.lock().unwrap();
        if backlog_lock.can_serve_partial(req_replid, &replid, offset) {
            drop(backlog_lock);
            // Partial sync
            socket.write_all(b"+CONTINUE\r\n").await?;
            let data = backlog.lock().unwrap().read_from(offset);
            if !data.is_empty() {
                socket.write_all(&data).await?;
            }
        } else {
            let current_offset = backlog_lock.global_offset;
            drop(backlog_lock);
            // Full sync
            socket.write_all(format!("+FULLRESYNC {} {}\r\n", replid, current_offset).as_bytes()).await?;

            let tmp_config = ServerConfig {
                dbfilename: format!("temp-replica-{}.rdb", std::process::id()),
                dir: config.dir.clone(),
                ..config.clone()
            };
            let tmp_path = std::path::PathBuf::from(format!("{}/{}", tmp_config.dir, tmp_config.dbfilename));
            rdb::save_rdb(&store, &tmp_path, store.databases.len())?;
            let rdb_data = tokio::fs::read(&tmp_path).await?;
            let _ = tokio::fs::remove_file(&tmp_path).await;

            let len_str = format!("${}\r\n", rdb_data.len());
            socket.write_all(len_str.as_bytes()).await?;
            socket.write_all(&rdb_data).await?;

            // Stream backlog
            let to_send = backlog.lock().unwrap().read_from(current_offset);
            if !to_send.is_empty() {
                socket.write_all(&to_send).await?;
            }
        }

        tracing::info!("Replication handshake complete with replica");
    }

    Ok(())
}

pub async fn broadcast_to_replicas(
    replicas: &[tokio::sync::mpsc::UnboundedSender<Vec<u8>>],
    data: &[u8],
) {
    let mut dead = vec![];
    for (i, tx) in replicas.iter().enumerate() {
        if tx.send(data.to_vec()).is_err() {
            dead.push(i);
        }
    }
    // Note: dead replicas would be cleaned up by the caller
}
