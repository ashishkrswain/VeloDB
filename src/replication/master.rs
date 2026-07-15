// Copyright (c) 2025-Present VeloDB Contributors
// SPDX-License-Identifier: MIT

use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite, AsyncReadExt, AsyncWriteExt};
use crate::store::Store;
use crate::persist::rdb;
use crate::replication::backlog::ReplBacklog;

/// Handles a PSYNC request on an already-accepted connection: performs
/// full or partial resync, then blocks streaming live writes to the
/// replica until the connection closes. `args` is `[replid, offset]`
/// as sent by the replica (replid "?" and offset "-1" request full sync).
pub async fn handle_psync<S>(
    socket: &mut S,
    store: &Arc<Store>,
    backlog: &Arc<std::sync::Mutex<ReplBacklog>>,
    replid: &str,
    args: &[Vec<u8>],
) -> anyhow::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let req_replid = args.first().map(|a| String::from_utf8_lossy(a).to_string()).unwrap_or_else(|| "?".to_string());
    let offset: i64 = args.get(1)
        .map(|a| String::from_utf8_lossy(a).to_string())
        .and_then(|s| s.parse().ok())
        .unwrap_or(-1);

    // Register the replica's live-stream channel and read any catch-up
    // data under the SAME lock acquisition, so no write pushed after
    // this point can be missed regardless of which path (full/partial)
    // we take below.
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();
    let (is_partial, catch_up, current_offset, replica_id) = {
        let mut bl = backlog.lock().unwrap();
        let is_partial = offset >= 0 && bl.can_serve_partial(&req_replid, replid, offset as u64);
        let catch_up = if is_partial { bl.read_from(offset as u64) } else { Vec::new() };
        let current_offset = bl.global_offset;
        let replica_id = bl.register_replica(tx);
        (is_partial, catch_up, current_offset, replica_id)
    };

    if is_partial {
        socket.write_all(b"+CONTINUE\r\n").await?;
        if !catch_up.is_empty() {
            socket.write_all(&catch_up).await?;
        }
        tracing::info!("Partial resync served from offset {}", offset);
    } else {
        socket.write_all(format!("+FULLRESYNC {} {}\r\n", replid, current_offset).as_bytes()).await?;

        let tmp_path = std::env::temp_dir().join(format!("velodb-fullsync-{}-{}.rdb", std::process::id(), crate::persist::unique_temp_id()));
        rdb::save_rdb(store, &tmp_path, store.databases.len())?;
        let rdb_data = tokio::fs::read(&tmp_path).await?;
        let _ = tokio::fs::remove_file(&tmp_path).await;

        socket.write_all(format!("${}\r\n", rdb_data.len()).as_bytes()).await?;
        socket.write_all(&rdb_data).await?;

        // Anything written to the backlog between reading current_offset
        // above and now must still reach the replica; read_from covers
        // exactly that gap since the replica is already registered.
        let gap = backlog.lock().unwrap().read_from(current_offset);
        if !gap.is_empty() {
            socket.write_all(&gap).await?;
        }
        tracing::info!("Full resync served, {} bytes RDB", rdb_data.len());
    }

    // Live streaming: forward every subsequent write until the replica
    // disconnects. REPLCONF ACK pings from the replica are drained and
    // ignored (no separate offset tracking needed yet).
    let mut ignore_buf = [0u8; 4096];
    loop {
        tokio::select! {
            data = rx.recv() => {
                match data {
                    Some(bytes) => {
                        if socket.write_all(&bytes).await.is_err() { break; }
                    }
                    None => break,
                }
            }
            result = socket.read(&mut ignore_buf) => {
                match result {
                    Ok(0) | Err(_) => break,
                    Ok(_) => continue,
                }
            }
        }
    }

    backlog.lock().unwrap().unregister_replica(replica_id);
    tracing::info!("Replica disconnected");
    Ok(())
}
