// Copyright (c) 2025-Present VeloDB Contributors
// SPDX-License-Identifier: MIT

use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use crate::store::Store;
use crate::cmd::CommandTable;
use crate::net::connection::ClientContext;
use crate::resp;
use bytes::Buf;

pub async fn connect_to_master(
    store: Arc<Store>,
    cmd_table: Arc<CommandTable>,
    master_host: &str,
    master_port: u16,
) -> anyhow::Result<()> {
    let addr = format!("{}:{}", master_host, master_port);
    let mut socket = TcpStream::connect(&addr).await?;

    // Send PSYNC ? -1 (full resync)
    socket.write_all(b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n").await?;

    // Read response
    let mut resp_buf = vec![0u8; 65536];
    let n = socket.read(&mut resp_buf).await?;
    let response = String::from_utf8_lossy(&resp_buf[..n]);

    let mut _replid = String::new();

    if response.starts_with("+FULLRESYNC") {
        // Parse replid and offset
        let lines: Vec<&str> = response.split("\r\n").collect();
        for line in &lines {
            if line.starts_with('+') {
                let parts: Vec<&str> = line[1..].split(' ').collect();
                if parts.len() >= 3 && parts[0] == "FULLRESYNC" {
                    let _replid_val = parts[1].to_string();
                }
            }
        }

        // Read RDB file length
        let mut len_buf = [0u8; 32];
        let mut len_str = String::new();
        loop {
            let n = socket.read(&mut len_buf).await?;
            if n == 0 { break; }
            len_str.push_str(&String::from_utf8_lossy(&len_buf[..n]));
            if len_str.contains("\r\n") { break; }
        }
        // Expect $<len>\r\n
        let rdb_len: usize = len_str
            .trim_start_matches('$')
            .trim_end_matches("\r\n")
            .trim()
            .parse()
            .unwrap_or(0);

        // Read RDB data
        let mut rdb_data = vec![0u8; rdb_len];
        socket.read_exact(&mut rdb_data).await?;
        let tmp_path = format!("./temp-replica-load-{}.rdb", std::process::id());
        tokio::fs::write(&tmp_path, &rdb_data).await?;
        match crate::persist::rdb::load_rdb(&store, std::path::Path::new(&tmp_path)) {
            Ok(count) => tracing::info!("Replica loaded {} keys from RDB", count),
            Err(e) => tracing::warn!("Replica RDB load error: {}", e),
        }
        let _ = tokio::fs::remove_file(&tmp_path).await;

        tracing::info!("Full sync complete, starting command streaming");

        // Enter streaming mode
        let mut buf = bytes::BytesMut::with_capacity(4096);
        let mut ctx = ClientContext::new();
        loop {
            socket.readable().await?;
            match socket.try_read_buf(&mut buf) {
                Ok(0) => break,
                Ok(_) => {
                    while !buf.is_empty() {
                        match resp::parse_command(&buf) {
                            Ok((remaining, args)) => {
                                if args.is_empty() { break; }
                                buf.advance(buf.len() - remaining.len());
                                let cmd_name = String::from_utf8_lossy(&args[0]).to_uppercase();
                                let _ = cmd_table.dispatch(&cmd_name, &store, &mut ctx, &args[1..]);
                            }
                            Err(nom::Err::Incomplete(_)) => break,
                            Err(_) => { buf.clear(); break; }
                        }
                    }
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
                Err(e) => return Err(e.into()),
            }
        }
    } else if response.starts_with("+CONTINUE") {
        tracing::info!("Partial sync achieved");
        // Enter streaming mode (same as above)
    }

    Ok(())
}
