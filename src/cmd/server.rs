// Copyright (c) 2025-Present VeloDB Contributors
// SPDX-License-Identifier: MIT

use super::CommandDef;
use crate::store::Store;
use crate::resp::RespValue;
use crate::net::connection::ClientContext;
use std::time::Instant;

static START_TIME: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();

/// Marks process start for INFO's uptime_in_seconds. Must be called once,
/// as early as possible during server startup — a lazily-initialized
/// OnceLock would instead measure "time since first INFO call".
pub fn mark_start_time() {
    START_TIME.get_or_init(Instant::now);
}

fn start_time() -> Instant {
    *START_TIME.get_or_init(Instant::now)
}

pub const COMMANDS: &[CommandDef] = &[
    CommandDef { name: "PING", arity: -1, handler: ping },
    CommandDef { name: "ECHO", arity: 2, handler: echo },
    CommandDef { name: "COMMAND", arity: -1, handler: command },
    CommandDef { name: "SELECT", arity: 2, handler: select },
    CommandDef { name: "INFO", arity: -1, handler: info },
    CommandDef { name: "CONFIG", arity: -2, handler: config_cmd },
    CommandDef { name: "ACL", arity: -2, handler: acl_cmd },
];

fn ping(_s: &Store, _c: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    if args.is_empty() { Ok(RespValue::pong()) }
    else { Ok(RespValue::bulk_string(args[0].clone())) }
}

fn echo(_s: &Store, _c: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    Ok(RespValue::bulk_string(args[0].clone()))
}

fn command(_s: &Store, _c: &mut ClientContext, _args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    Ok(RespValue::ok())
}

fn select(_s: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    ctx.db_index = String::from_utf8_lossy(&args[0]).parse().unwrap_or(0);
    Ok(RespValue::ok())
}

fn info(store: &Store, _ctx: &mut ClientContext, _args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    let mut info = String::new();
    let uptime = start_time().elapsed().as_secs();
    info.push_str("# Server\r\n");
    info.push_str("velodb_version:0.3.0\r\n");
    info.push_str(&format!("uptime_in_seconds:{}\r\n", uptime));
    info.push_str("os:Windows\r\n");
    info.push_str("\r\n# Keyspace\r\n");
    for db_idx in 0..store.databases.len() {
        let keys = store.dbsize(db_idx).unwrap_or(0);
        let expires = store.expires_count(db_idx).unwrap_or(0);
        if keys > 0 {
            info.push_str(&format!("db{}:keys={},expires={}\r\n", db_idx, keys, expires));
        }
    }
    Ok(RespValue::bulk_string(info.as_bytes()))
}

fn config_cmd(_store: &Store, _ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    if args.is_empty() { return Ok(RespValue::ok()); }
    let subcmd = String::from_utf8_lossy(&args[0]).to_uppercase();
    match subcmd.as_str() {
        "GET" => {
            let mut result = Vec::new();
            result.push(RespValue::bulk_string(b"port".to_vec()));
            result.push(RespValue::bulk_string(b"6379".to_vec()));
            result.push(RespValue::bulk_string(b"databases".to_vec()));
            result.push(RespValue::bulk_string(b"16".to_vec()));
            Ok(RespValue::Array(Some(result)))
        }
        "SET" => Ok(RespValue::ok()),
        "REWRITE" => Ok(RespValue::ok()),
        _ => Err(crate::error::VeloDBError::syntax_error()),
    }
}

/// Minimal ACL surface: a single built-in "default" user (full
/// permissions, matching a server with no requirepass/ACL config),
/// enough for client libraries that probe ACL WHOAMI/LIST/CAT/USERS on
/// connect. Multi-user ACL rules are not implemented.
fn acl_cmd(_store: &Store, _ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    if args.is_empty() { return Err(crate::error::VeloDBError::syntax_error()); }
    let subcmd = String::from_utf8_lossy(&args[0]).to_uppercase();
    match subcmd.as_str() {
        "WHOAMI" => Ok(RespValue::bulk_string(b"default".to_vec())),
        "LIST" => Ok(RespValue::Array(Some(vec![
            RespValue::bulk_string(b"user default on nopass sanitize-payload ~* &* +@all".to_vec()),
        ]))),
        "USERS" => Ok(RespValue::Array(Some(vec![RespValue::bulk_string(b"default".to_vec())]))),
        "CAT" => Ok(RespValue::Array(Some(
            ["keyspace", "read", "write", "admin", "pubsub", "scripting", "connection", "transaction", "fast", "slow"]
                .iter().map(|c| RespValue::bulk_string(c.as_bytes().to_vec())).collect(),
        ))),
        "GETUSER" => {
            if args.get(1).map(|a| a.as_slice()) == Some(b"default") {
                Ok(RespValue::Array(Some(vec![
                    RespValue::bulk_string(b"flags".to_vec()),
                    RespValue::Array(Some(vec![RespValue::bulk_string(b"on".to_vec()), RespValue::bulk_string(b"nopass".to_vec())])),
                    RespValue::bulk_string(b"passwords".to_vec()),
                    RespValue::Array(Some(vec![])),
                    RespValue::bulk_string(b"commands".to_vec()),
                    RespValue::bulk_string(b"+@all".to_vec()),
                    RespValue::bulk_string(b"keys".to_vec()),
                    RespValue::bulk_string(b"~*".to_vec()),
                    RespValue::bulk_string(b"channels".to_vec()),
                    RespValue::bulk_string(b"&*".to_vec()),
                ])))
            } else {
                Ok(RespValue::nil())
            }
        }
        _ => Err(crate::error::VeloDBError::syntax_error()),
    }
}
