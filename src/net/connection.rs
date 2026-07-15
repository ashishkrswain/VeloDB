// Copyright (c) 2025-Present VeloDB Contributors
// SPDX-License-Identifier: MIT

use tokio::io::{AsyncRead, AsyncWrite, AsyncReadExt, AsyncWriteExt};
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
    /// True once AUTH has succeeded on this connection, or always true
    /// when the server has no requirepass configured.
    pub authenticated: bool,
    /// Negotiated RESP protocol version (2 or 3), set via HELLO.
    pub protocol: u8,
}

impl ClientContext {
    pub fn new() -> Self {
        Self {
            db_index: 0, block_state: None, sub_mode: false,
            subscribed_channels: vec![], subscribed_patterns: vec![], pubsub_rx: None,
            multi_mode: false, multi_queue: vec![], watched_keys: vec![], watched_versions: vec![],
            authenticated: true, protocol: 2,
        }
    }
}

/// Commands allowed before AUTH succeeds, matching Redis: auth itself,
/// connection housekeeping, and HELLO's protocol negotiation.
fn is_auth_exempt(name: &str) -> bool {
    matches!(name, "AUTH" | "HELLO" | "PING" | "QUIT" | "RESET")
}

pub async fn handle<S>(
    mut socket: S,
    store: Arc<Store>,
    cmd_table: Arc<CommandTable>,
    config: ServerConfig,
    aof_writer: Option<Arc<AofWriter>>,
    repl_backlog: Option<Arc<std::sync::Mutex<ReplBacklog>>>,
    replid: String,
) -> anyhow::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut buf = bytes::BytesMut::with_capacity(4096);
    let mut ctx = ClientContext::new();
    ctx.authenticated = config.requirepass.is_none();

    loop {
        if ctx.sub_mode {
            // PubSub mode: read commands OR wait for messages
            tokio::select! {
                read_result = socket.read_buf(&mut buf) => {
                    match read_result {
                        Ok(0) => break,
                        Ok(_) => {
                            while !buf.is_empty() {
                                match resp::parse_command(&buf) {
                                    Ok((remaining, args)) => {
                                        if args.is_empty() { break; }
                                        buf.advance(buf.len() - remaining.len());
                                        let cmd_name = String::from_utf8_lossy(&args[0]).to_uppercase();
                                        let response = cmd_table.dispatch(&cmd_name, &store, &mut ctx, &args[1..]);
                                        let resp_bytes = resp::serialize_response_proto(&response, ctx.protocol);
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
        match socket.read_buf(&mut buf).await {
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

                            if cmd_name == "PSYNC" {
                                let consumed = buf.len() - remaining.len();
                                buf.advance(consumed);
                                if let Some(bl) = repl_backlog {
                                    crate::replication::master::handle_psync(
                                        &mut socket, &store, &bl, &replid, &args[1..],
                                    ).await?;
                                }
                                // The connection is now dedicated to replica
                                // streaming (or the client sent PSYNC with no
                                // backlog configured); either way this
                                // connection's command loop is done.
                                return Ok(());
                            }

                            if cmd_name == "AUTH" {
                                let consumed = buf.len() - remaining.len();
                                buf.advance(consumed);
                                let response = handle_auth(&config, &mut ctx, &args[1..]);
                                let resp_bytes = resp::serialize_response_proto(&response, ctx.protocol);
                                socket.write_all(&resp_bytes).await?;
                                continue;
                            }

                            if cmd_name == "HELLO" {
                                let consumed = buf.len() - remaining.len();
                                buf.advance(consumed);
                                let response = handle_hello(&config, &mut ctx, &args[1..]);
                                let resp_bytes = resp::serialize_response_proto(&response, ctx.protocol);
                                socket.write_all(&resp_bytes).await?;
                                continue;
                            }

                            if !ctx.authenticated && !is_auth_exempt(&cmd_name) {
                                let consumed = buf.len() - remaining.len();
                                buf.advance(consumed);
                                let response = RespValue::error("NOAUTH Authentication required.");
                                let resp_bytes = resp::serialize_response_proto(&response, ctx.protocol);
                                socket.write_all(&resp_bytes).await?;
                                continue;
                            }

                            if ctx.multi_mode && cmd_name != "EXEC" && cmd_name != "DISCARD" && cmd_name != "WATCH" && cmd_name != "UNWATCH" && cmd_name != "MULTI" {
                                ctx.multi_queue.push(args.clone());
                                let resp_bytes = resp::serialize_response_proto(&RespValue::SimpleString("QUEUED".into()), ctx.protocol);
                                socket.write_all(&resp_bytes).await?;
                                buf.advance(buf.len() - remaining.len());
                                continue;
                            }

                            tracing::trace!("Command: {} with {} args", cmd_name, args.len() - 1);
                            // BGREWRITEAOF is handled here because only the
                            // connection layer holds the AofWriter.
                            let response = if cmd_name == "BGREWRITEAOF" {
                                match &aof_writer {
                                    Some(aof) => {
                                        let aof = aof.clone();
                                        let st = store.clone();
                                        tokio::task::spawn_blocking(move || {
                                            if let Err(e) = aof.rewrite(&st) {
                                                tracing::warn!("AOF rewrite failed: {}", e);
                                            }
                                        });
                                        RespValue::SimpleString("Background append only file rewriting started".into())
                                    }
                                    None => RespValue::error("ERR AOF is not enabled"),
                                }
                            } else {
                                cmd_table.dispatch(&cmd_name, &store, &mut ctx, &args[1..])
                            };
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
                                let resp_bytes = resp::serialize_response_proto(&response, ctx.protocol);
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

                                let resp_bytes = resp::serialize_response_proto(&response, ctx.protocol);
                                socket.write_all(&resp_bytes).await?;
                            } else {
                                let resp_bytes = resp::serialize_response_proto(&response, ctx.protocol);
                                socket.write_all(&resp_bytes).await?;
                            }
                        }
                        Err(nom::Err::Incomplete(_)) => break,
                        Err(_) => {
                            let err = resp::RespValue::error("ERR protocol error");
                            let bytes = resp::serialize_response_proto(&err, ctx.protocol);
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

/// AUTH [username] password. Only the no-ACL, single-password form is
/// supported: `AUTH password` against `requirepass`. `AUTH default
/// password` (the form RESP3-aware clients send when no ACL users are
/// configured) is accepted with the username ignored, matching Redis's
/// behavior when only the default user exists.
fn handle_auth(config: &ServerConfig, ctx: &mut ClientContext, args: &[Vec<u8>]) -> RespValue {
    let password = match args.len() {
        1 => &args[0],
        2 if args[0].eq_ignore_ascii_case(b"default") => &args[1],
        _ => return RespValue::error("ERR wrong number of arguments for 'auth' command"),
    };
    match &config.requirepass {
        None => RespValue::error("ERR Client sent AUTH, but no password is set. Did you mean AUTH <username> <password>?"),
        Some(expected) if expected.as_bytes() == password.as_slice() => {
            ctx.authenticated = true;
            RespValue::ok()
        }
        Some(_) => RespValue::error("WRONGPASS invalid username-password pair or user is disabled."),
    }
}

/// HELLO [protover [AUTH username password] [SETNAME clientname]].
/// Negotiates the RESP protocol version and, if requested, authenticates
/// in the same round trip (the form most RESP3-aware client libraries
/// send on connect). With no protover, re-describes the server at the
/// currently active protocol version, matching Redis.
fn handle_hello(config: &ServerConfig, ctx: &mut ClientContext, args: &[Vec<u8>]) -> RespValue {
    let mut pos = 0;
    if pos < args.len() {
        let requested = String::from_utf8_lossy(&args[pos]).to_string();
        match requested.parse::<u8>() {
            Ok(v) if v == 2 || v == 3 => { ctx.protocol = v; pos += 1; }
            _ => return RespValue::error("NOPROTO unsupported protocol version"),
        }
    }
    while pos < args.len() {
        let opt = String::from_utf8_lossy(&args[pos]).to_uppercase();
        match opt.as_str() {
            "AUTH" if pos + 2 < args.len() => {
                let auth_resp = handle_auth(config, ctx, &args[pos + 1..pos + 3]);
                if matches!(auth_resp, RespValue::Error(_)) { return auth_resp; }
                pos += 3;
            }
            "SETNAME" if pos + 1 < args.len() => { pos += 2; } // accepted, not tracked
            _ => return RespValue::error("ERR syntax error in HELLO"),
        }
    }
    if !ctx.authenticated {
        return RespValue::error("NOAUTH HELLO must be called with the client already authenticated, otherwise the HELLO <proto> AUTH <user> <pass> option can be used to authenticate the client and select the RESP protocol version at the same time");
    }
    RespValue::Map(vec![
        (RespValue::bulk_string(b"server".to_vec()), RespValue::bulk_string(b"velodb".to_vec())),
        (RespValue::bulk_string(b"version".to_vec()), RespValue::bulk_string(b"0.1.0".to_vec())),
        (RespValue::bulk_string(b"proto".to_vec()), RespValue::Integer(ctx.protocol as i64)),
        (RespValue::bulk_string(b"id".to_vec()), RespValue::Integer(0)),
        (RespValue::bulk_string(b"mode".to_vec()), RespValue::bulk_string(b"standalone".to_vec())),
        (RespValue::bulk_string(b"role".to_vec()), RespValue::bulk_string(b"master".to_vec())),
        (RespValue::bulk_string(b"modules".to_vec()), RespValue::Array(Some(vec![]))),
    ])
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
