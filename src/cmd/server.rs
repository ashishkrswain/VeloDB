// Copyright (c) 2025-Present VeloDB Contributors
// SPDX-License-Identifier: MIT

use super::CommandDef;
use crate::store::Store;
use crate::resp::RespValue;
use crate::net::connection::ClientContext;
use std::time::{SystemTime, UNIX_EPOCH};

pub const COMMANDS: &[CommandDef] = &[
    CommandDef { name: "PING", arity: -1, handler: ping },
    CommandDef { name: "ECHO", arity: 2, handler: echo },
    CommandDef { name: "COMMAND", arity: -1, handler: command },
    CommandDef { name: "SELECT", arity: 2, handler: select },
    CommandDef { name: "INFO", arity: -1, handler: info },
    CommandDef { name: "CONFIG", arity: -2, handler: config_cmd },
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
    let uptime = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs().to_string();
    info.push_str("# Server\r\n");
    info.push_str("velodb_version:0.3.0\r\n");
    info.push_str(&format!("uptime_in_seconds:{}\r\n", uptime));
    info.push_str("os:Windows\r\n");
    info.push_str("\r\n# Keyspace\r\n");
    for db_idx in 0..store.databases.len() {
        let keys = store.dbsize(db_idx).unwrap_or(0);
        info.push_str(&format!("db{}:keys={},expires=0\r\n", db_idx, keys));
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
