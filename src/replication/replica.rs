// Copyright (c) 2025-Present VeloDB Contributors
// SPDX-License-Identifier: MIT

use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use crate::store::Store;
use crate::cmd::CommandTable;
use crate::net::connection::ClientContext;
use crate::resp;
use bytes::Buf;

/// Replid/offset remembered across reconnects so a dropped replica
/// link can request partial resync instead of a full RDB transfer.
pub struct ReplicaSyncState {
    pub replid: String,
    pub offset: i64,
}

impl Default for ReplicaSyncState {
    fn default() -> Self {
        Self { replid: "?".to_string(), offset: -1 }
    }
}

fn encode_psync(replid: &str, offset: i64) -> Vec<u8> {
    crate::persist::aof::encode_command_for_aof(&[
        b"PSYNC".to_vec(), replid.as_bytes().to_vec(), offset.to_string().into_bytes(),
    ])
}

/// Reads exactly one `\r\n`-terminated line from the socket into `carry`,
/// consuming any bytes already buffered from a previous over-read.
async fn read_line(socket: &mut TcpStream, carry: &mut Vec<u8>) -> anyhow::Result<String> {
    loop {
        if let Some(pos) = carry.iter().position(|&b| b == b'\n') {
            let line: Vec<u8> = carry.drain(..=pos).collect();
            return Ok(String::from_utf8_lossy(&line).trim_end().to_string());
        }
        let mut chunk = [0u8; 4096];
        let n = socket.read(&mut chunk).await?;
        if n == 0 { anyhow::bail!("master closed connection during handshake"); }
        carry.extend_from_slice(&chunk[..n]);
    }
}

/// Connects to `master_host:master_port`, performs PSYNC (full or
/// partial depending on `sync_state`), and streams replicated writes
/// into `store` until the connection drops. Updates `sync_state` with
/// the negotiated replid and the offset reached, so the caller's retry
/// loop can request a partial resync on the next reconnect attempt.
pub async fn connect_to_master(
    store: Arc<Store>,
    cmd_table: Arc<CommandTable>,
    master_host: &str,
    master_port: u16,
    sync_state: Arc<Mutex<ReplicaSyncState>>,
) -> anyhow::Result<()> {
    let addr = format!("{}:{}", master_host, master_port);
    let mut socket = TcpStream::connect(&addr).await?;

    let (req_replid, req_offset) = {
        let s = sync_state.lock().unwrap();
        (s.replid.clone(), s.offset)
    };
    socket.write_all(&encode_psync(&req_replid, req_offset)).await?;

    let mut carry = Vec::new();
    let response = read_line(&mut socket, &mut carry).await?;

    if let Some(rest) = response.strip_prefix("+FULLRESYNC") {
        let parts: Vec<&str> = rest.trim().split(' ').collect();
        let new_replid = parts.first().unwrap_or(&"").to_string();
        let mut new_offset: i64 = parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(0);

        let len_line = read_line(&mut socket, &mut carry).await?;
        let rdb_len: usize = len_line.trim_start_matches('$').trim().parse().unwrap_or(0);

        // `carry` may already hold part (or all) of the RDB body from an
        // over-read while hunting for the length line's newline.
        let mut rdb_data = std::mem::take(&mut carry);
        while rdb_data.len() < rdb_len {
            let mut chunk = [0u8; 65536];
            let n = socket.read(&mut chunk).await?;
            if n == 0 { anyhow::bail!("master closed connection during RDB transfer"); }
            rdb_data.extend_from_slice(&chunk[..n]);
        }
        let trailing = rdb_data.split_off(rdb_len);

        let tmp_path = std::env::temp_dir().join(format!("velodb-replica-load-{}-{}.rdb", std::process::id(), crate::persist::unique_temp_id()));
        tokio::fs::write(&tmp_path, &rdb_data).await?;
        match crate::persist::rdb::load_rdb(&store, &tmp_path) {
            Ok(count) => tracing::info!("Replica loaded {} keys from RDB", count),
            Err(e) => tracing::warn!("Replica RDB load error: {}", e),
        }
        // sync_state must be updated immediately after load_rdb, with no
        // await in between: load_rdb writes replicated data into `store`
        // synchronously, and any yield point before sync_state reflects
        // the new replid lets a concurrent observer see the replicated
        // data while sync_state still shows the old (or default) replid.
        {
            let mut s = sync_state.lock().unwrap();
            s.replid = new_replid.clone();
            s.offset = new_offset;
        }
        tracing::info!("Full sync complete (replid {}), starting command streaming", new_replid);
        let _ = tokio::fs::remove_file(&tmp_path).await;

        let mut buf = bytes::BytesMut::from(trailing.as_slice());
        stream_commands(&mut socket, &mut buf, &store, &cmd_table, &sync_state, &mut new_offset).await
    } else if response.starts_with("+CONTINUE") {
        tracing::info!("Partial sync achieved from offset {}", req_offset);
        let mut offset = req_offset;
        let mut buf = bytes::BytesMut::from(carry.as_slice());
        stream_commands(&mut socket, &mut buf, &store, &cmd_table, &sync_state, &mut offset).await
    } else {
        anyhow::bail!("unexpected PSYNC response: {}", response)
    }
}

/// Applies commands as they arrive from the master, advancing `offset`
/// by the exact byte count of each command (matching the master's
/// backlog accounting) and mirroring it into `sync_state` so a later
/// reconnect can resume from the right point.
async fn stream_commands(
    socket: &mut TcpStream,
    buf: &mut bytes::BytesMut,
    store: &Arc<Store>,
    cmd_table: &Arc<CommandTable>,
    sync_state: &Arc<Mutex<ReplicaSyncState>>,
    offset: &mut i64,
) -> anyhow::Result<()> {
    let mut ctx = ClientContext::new();
    loop {
        while !buf.is_empty() {
            match resp::parse_command(buf) {
                Ok((remaining, args)) => {
                    if args.is_empty() { break; }
                    let consumed = buf.len() - remaining.len();
                    buf.advance(consumed);
                    *offset += consumed as i64;
                    sync_state.lock().unwrap().offset = *offset;
                    let cmd_name = String::from_utf8_lossy(&args[0]).to_uppercase();
                    let _ = cmd_table.dispatch(&cmd_name, store, &mut ctx, &args[1..]);
                }
                Err(nom::Err::Incomplete(_)) => break,
                Err(_) => { buf.clear(); break; }
            }
        }
        socket.readable().await?;
        match socket.try_read_buf(buf) {
            Ok(0) => return Ok(()),
            Ok(_) => continue,
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
            Err(e) => return Err(e.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    /// Minimal fake master: reads the PSYNC request, records what it
    /// asked for, and replies with a canned response.
    async fn fake_master_fullresync(listener: TcpListener, rdb_bytes: &'static [u8], extra_stream: &'static [u8]) -> Vec<u8> {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 4096];
        let n = sock.read(&mut buf).await.unwrap();
        let request = buf[..n].to_vec();
        sock.write_all(format!("+FULLRESYNC replidABC 0\r\n${}\r\n", rdb_bytes.len()).as_bytes()).await.unwrap();
        sock.write_all(rdb_bytes).await.unwrap();
        sock.write_all(extra_stream).await.unwrap();
        // Keep the socket open briefly so the client's read loop can
        // consume the streamed bytes before we drop it.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        request
    }

    #[tokio::test]
    async fn test_first_connect_requests_full_resync() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let set_cmd = crate::persist::aof::encode_command_for_aof(&[b"SET".to_vec(), b"k".to_vec(), b"v".to_vec()]);
        let set_cmd_static: &'static [u8] = Box::leak(set_cmd.into_boxed_slice());

        let server = tokio::spawn(fake_master_fullresync(listener, b"", set_cmd_static));

        let store = Arc::new(Store::new(1));
        let cmd_table = Arc::new(CommandTable::new());
        let sync_state = Arc::new(Mutex::new(ReplicaSyncState::default()));

        let _ = tokio::time::timeout(
            std::time::Duration::from_millis(200),
            connect_to_master(store.clone(), cmd_table, "127.0.0.1", port, sync_state.clone()),
        ).await;

        let request = server.await.unwrap();
        let req_str = String::from_utf8_lossy(&request);
        assert!(req_str.contains("PSYNC"));
        assert!(req_str.contains('?'), "first connect must request full resync with '?': {}", req_str);
        assert!(req_str.contains("-1"));

        assert_eq!(store.get(0, b"k").unwrap().unwrap(), b"v", "streamed command after RDB must be applied");
        let s = sync_state.lock().unwrap();
        assert_eq!(s.replid, "replidABC", "replid from FULLRESYNC must be remembered for future reconnects");
        assert!(s.offset > 0, "offset must advance past the streamed SET command");
    }

    #[tokio::test]
    async fn test_reconnect_requests_partial_resync_with_remembered_state() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 4096];
            let n = sock.read(&mut buf).await.unwrap();
            let request = buf[..n].to_vec();
            sock.write_all(b"+CONTINUE\r\n").await.unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            request
        });

        let store = Arc::new(Store::new(1));
        let cmd_table = Arc::new(CommandTable::new());
        let sync_state = Arc::new(Mutex::new(ReplicaSyncState { replid: "replidABC".to_string(), offset: 42 }));

        let _ = tokio::time::timeout(
            std::time::Duration::from_millis(200),
            connect_to_master(store, cmd_table, "127.0.0.1", port, sync_state),
        ).await;

        let request = server.await.unwrap();
        let req_str = String::from_utf8_lossy(&request);
        assert!(req_str.contains("replidABC"), "reconnect must send the remembered replid: {}", req_str);
        assert!(req_str.contains("42"), "reconnect must send the remembered offset: {}", req_str);
    }
}
