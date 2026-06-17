// Copyright (c) 2025-Present VeloDB Contributors
// SPDX-License-Identifier: MIT

use tokio::net::TcpStream;
use tokio::io::AsyncWriteExt;
use bytes::Buf;
use std::sync::Arc;
use std::time::Duration;
use crate::store::Store;
use crate::cmd::CommandTable;
use crate::resp::{self, RespValue};
use crate::config::ServerConfig;
use crate::error::VeloDBError;
use crate::persist::aof::{AofWriter, encode_command_for_aof};
use crate::replication::backlog::ReplBacklog;

const MAX_QUERY_BUFFER: usize = 1024 * 1024 * 1024;

pub enum PopDirection { Left, Right }

pub struct BlockState {
    pub keys: Vec<Vec<u8>>,
    pub timeout_ms: i64,
    pub pop_direction: PopDirection,
}

pub struct ClientContext {
    pub db_index: usize,
    pub block_state: Option<BlockState>,
    pub sub_mode: bool,
    pub subscribed_channels: Vec<Vec<u8>>,
    pub subscribed_patterns: Vec<String>,
    pub pubsub_rx: Option<tokio::sync::mpsc::UnboundedReceiver<(Vec<u8>, Vec<u8>)>>,
    pub multi_mode: bool,
    pub multi_queue: Vec<Vec<Vec<u8>>>,
    pub watched_keys: Vec<Vec<u8>>,
    pub watched_versions: Vec<u64>,
}

impl ClientContext {
    pub fn new() -> Self {
        Self {
            db_index: 0, block_state: None, sub_mode: false,
            subscribed_channels: vec![], subscribed_patterns: vec![], pubsub_rx: None,
            multi_mode: false, multi_queue: vec![], watched_keys: vec![], watched_versions: vec![],
        }
    }
}

pub async fn handle(
    mut socket: TcpStream,
    store: Arc<Store>,
    cmd_table: Arc<CommandTable>,
    _config: ServerConfig,
    aof_writer: Option<Arc<AofWriter>>,
    repl_backlog: Option<Arc<std::sync::Mutex<ReplBacklog>>>,
) -> anyhow::Result<()> {
    let mut buf = bytes::BytesMut::with_capacity(4096);
    let mut ctx = ClientContext::new();

    loop {
        if ctx.sub_mode {
            // PubSub mode: read commands OR wait for messages
            tokio::select! {
                _ = socket.readable() => {
                    match socket.try_read_buf(&mut buf) {
                        Ok(0) => break,
                        Ok(_) => {
                            while !buf.is_empty() {
                                match resp::parse_command(&buf) {
                                    Ok((remaining, args)) => {
                                        if args.is_empty() { break; }
                                        buf.advance(buf.len() - remaining.len());
                                        let cmd_name = String::from_utf8_lossy(&args[0]).to_uppercase();
                                        let response = cmd_table.dispatch(&cmd_name, &store, &mut ctx, &args[1..]);
                                        let resp_bytes = resp::serialize_response(&response);
                                        socket.write_all(&resp_bytes).await?;
                                        if !ctx.sub_mode { break; }
                                    }
                                    Err(nom::Err::Incomplete(_)) => break,
                                    Err(_) => { buf.clear(); }
                                }
                            }
                        }
                        Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
                        Err(e) => return Err(e.into()),
                    }
                }
            }
            continue;
        }

        // Normal mode
        socket.readable().await?;

        match socket.try_read_buf(&mut buf) {
            Ok(0) => break,
            Ok(_) => {
                if buf.len() > MAX_QUERY_BUFFER {
                    return Err(VeloDBError::protocol_error("query buffer limit exceeded").into());
                }
                while !buf.is_empty() {
                    match resp::parse_command(&buf) {
                        Ok((remaining, args)) => {
                            if args.is_empty() { break; }
                            let cmd_name = String::from_utf8_lossy(&args[0]).to_uppercase();
                            if ctx.multi_mode && cmd_name != "EXEC" && cmd_name != "DISCARD" && cmd_name != "WATCH" && cmd_name != "UNWATCH" && cmd_name != "MULTI" {
                                ctx.multi_queue.push(args.clone());
                                let resp_bytes = resp::serialize_response(&RespValue::SimpleString("QUEUED".into()));
                                socket.write_all(&resp_bytes).await?;
                                buf.advance(buf.len() - remaining.len());
                                continue;
                            }

                            tracing::trace!("Command: {} with {} args", cmd_name, args.len() - 1);
                            let response = cmd_table.dispatch(&cmd_name, &store, &mut ctx, &args[1..]);
                            let consumed = buf.len() - remaining.len();
                            buf.advance(consumed);

                            if let Some(aof) = &aof_writer {
                                if is_write_command(&cmd_name) {
                                    let aof_entry = encode_command_for_aof(&args);
                                    let _ = aof.append(&aof_entry);
                                }
                            }

                            // Replication backlog
                            if let Some(ref bl) = repl_backlog {
                                if is_write_command(&cmd_name) {
                                    let cmd_bytes = encode_command_for_aof(&args);
                                    bl.lock().unwrap().push(&cmd_bytes);
                                }
                            }

                            if ctx.sub_mode {
                                let resp_bytes = resp::serialize_response(&response);
                                socket.write_all(&resp_bytes).await?;
                            } else if let Some(block) = ctx.block_state.take() {
                                let notify = Arc::new(tokio::sync::Notify::new());
                                let id = store.block_registry.register(&block.keys, notify.clone());

                                let triggered = if block.timeout_ms == 0 {
                                    notify.notified().await;
                                    true
                                } else {
                                    tokio::select! {
                                        _ = notify.notified() => true,
                                        _ = tokio::time::sleep(Duration::from_millis(block.timeout_ms as u64)) => false,
                                    }
                                };

                                store.block_registry.unregister(id, &block.keys);

                                let response = if triggered {
                                    unblock_pop(&store, ctx.db_index, &block.keys, &block.pop_direction)
                                } else {
                                    RespValue::Array(Some(vec![]))
                                };

                                let resp_bytes = resp::serialize_response(&response);
                                socket.write_all(&resp_bytes).await?;
                            } else {
                                let resp_bytes = resp::serialize_response(&response);
                                socket.write_all(&resp_bytes).await?;
                            }
                        }
                        Err(nom::Err::Incomplete(_)) => break,
                        Err(_) => {
                            let err = resp::RespValue::error("ERR protocol error");
                            let bytes = resp::serialize_response(&err);
                            socket.write_all(&bytes).await?;
                            return Err(VeloDBError::protocol_error("parse error").into());
                        }
                    }
                }
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}

fn unblock_pop(store: &Store, db_idx: usize, keys: &[Vec<u8>], dir: &PopDirection) -> resp::RespValue {
    for key in keys {
        let result = match dir {
            PopDirection::Left => store.lpop_one(db_idx, key),
            PopDirection::Right => store.rpop_one(db_idx, key),
        };
        if let Ok(Some(val)) = result {
            return RespValue::Array(Some(vec![
                RespValue::bulk_string(key.clone()),
                RespValue::bulk_string(val),
            ]));
        }
    }
    RespValue::Array(Some(vec![]))
}

fn is_write_command(name: &str) -> bool {
    matches!(name,
        "SET" | "SETRANGE" | "MSET" | "INCR" | "INCRBY" | "DECR" | "DECRBY" | "APPEND" | "GETSET" |
        "LPUSH" | "RPUSH" | "LPOP" | "RPOP" | "LSET" | "LTRIM" | "LREM" |
        "SADD" | "SREM" | "SPOP" |
        "HSET" | "HDEL" | "HINCRBY" |
        "ZADD" | "ZREM" |
        "XADD" | "XDEL" | "XTRIM" |
        "NHSET" | "NHDEL" |
        "DEL" | "RENAME" | "RENAMENX" |
        "EXPIRE" | "EXPIREAT" | "PEXPIRE" | "PEXPIREAT" | "PERSIST" |
        "FLUSHDB" | "FLUSHALL" | "SELECT"
    )
}
