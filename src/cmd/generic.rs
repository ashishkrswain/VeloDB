// Copyright (c) 2025-Present VeloDB Contributors
// SPDX-License-Identifier: MIT

use super::CommandDef;
use crate::store::Store;
use crate::resp::RespValue;
use crate::net::connection::ClientContext;
use std::time::{SystemTime, UNIX_EPOCH};

pub const COMMANDS: &[CommandDef] = &[
    CommandDef { name: "DEL", arity: -2, handler: del },
    CommandDef { name: "EXISTS", arity: -2, handler: exists },
    CommandDef { name: "EXPIRE", arity: 3, handler: expire },
    CommandDef { name: "EXPIREAT", arity: 3, handler: expireat },
    CommandDef { name: "PEXPIRE", arity: 3, handler: pexpire },
    CommandDef { name: "PEXPIREAT", arity: 3, handler: pexpireat },
    CommandDef { name: "TTL", arity: 2, handler: ttl },
    CommandDef { name: "PTTL", arity: 2, handler: pttl },
    CommandDef { name: "PERSIST", arity: 2, handler: persist },
    CommandDef { name: "TYPE", arity: 2, handler: type_cmd },
    CommandDef { name: "RENAME", arity: 3, handler: rename_cmd },
    CommandDef { name: "RENAMENX", arity: 3, handler: renamenx },
    CommandDef { name: "KEYS", arity: 2, handler: keys_cmd },
    CommandDef { name: "DBSIZE", arity: 1, handler: dbsize_cmd },
    CommandDef { name: "FLUSHDB", arity: 1, handler: flushdb_cmd },
    CommandDef { name: "FLUSHALL", arity: 1, handler: flushall_cmd },
    CommandDef { name: "RANDOMKEY", arity: 1, handler: randomkey_cmd },
];

fn now_ms() -> u64 { SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64 }

fn del(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    let mut count = 0;
    for key in args { if store.del(ctx.db_index, key)? { count += 1; } }
    Ok(RespValue::integer(count))
}

fn exists(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    let mut count = 0;
    for key in args { if store.exists(ctx.db_index, key)? { count += 1; } }
    Ok(RespValue::integer(count))
}

fn expire(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    let seconds: u64 = String::from_utf8_lossy(&args[1]).parse().unwrap_or(0);
    store.set_expire(ctx.db_index, &args[0], now_ms() + seconds * 1000)
        .map(|b| RespValue::integer(b as i64))
        .map_err(Into::into)
}

fn expireat(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    let ts: u64 = String::from_utf8_lossy(&args[1]).parse().unwrap_or(0);
    store.set_expire(ctx.db_index, &args[0], ts * 1000)
        .map(|b| RespValue::integer(b as i64))
        .map_err(Into::into)
}

fn pexpire(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    let ms: u64 = String::from_utf8_lossy(&args[1]).parse().unwrap_or(0);
    store.set_expire(ctx.db_index, &args[0], now_ms() + ms)
        .map(|b| RespValue::integer(b as i64))
        .map_err(Into::into)
}

fn pexpireat(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    let ts: u64 = String::from_utf8_lossy(&args[1]).parse().unwrap_or(0);
    store.set_expire(ctx.db_index, &args[0], ts)
        .map(|b| RespValue::integer(b as i64))
        .map_err(Into::into)
}

fn ttl(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    match store.get_expire(ctx.db_index, &args[0])? {
        Some(at) if at > now_ms() => Ok(RespValue::integer(((at - now_ms()) / 1000) as i64)),
        Some(_) => Ok(RespValue::integer(-2)),
        None => Ok(RespValue::integer(-1)),
    }
}

fn pttl(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    match store.get_expire(ctx.db_index, &args[0])? {
        Some(at) if at > now_ms() => Ok(RespValue::integer((at - now_ms()) as i64)),
        Some(_) => Ok(RespValue::integer(-2)),
        None => Ok(RespValue::integer(-1)),
    }
}

fn persist(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    store.remove_expire(ctx.db_index, &args[0])
        .map(|b| RespValue::integer(b as i64))
        .map_err(Into::into)
}

fn type_cmd(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    match store.get_type(ctx.db_index, &args[0])? {
        Some(t) => Ok(RespValue::SimpleString(t)),
        None => Ok(RespValue::SimpleString("none".into())),
    }
}

fn rename_cmd(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    if !store.exists(ctx.db_index, &args[0])? { return Err(crate::error::VeloDBError::key_not_found()); }
    store.rename(ctx.db_index, &args[0], &args[1])?;
    Ok(RespValue::ok())
}

fn renamenx(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    if store.exists(ctx.db_index, &args[1])? { return Ok(RespValue::integer(0)); }
    if !store.exists(ctx.db_index, &args[0])? { return Err(crate::error::VeloDBError::key_not_found()); }
    store.rename(ctx.db_index, &args[0], &args[1])?;
    Ok(RespValue::integer(1))
}

fn keys_cmd(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    let pattern = String::from_utf8_lossy(&args[0]).to_string();
    let keys = store.keys(ctx.db_index, &pattern)?;
    let resp_keys: Vec<RespValue> = keys.into_iter().map(RespValue::bulk_string).collect();
    Ok(RespValue::Array(Some(resp_keys)))
}

fn dbsize_cmd(store: &Store, ctx: &mut ClientContext, _args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    Ok(RespValue::integer(store.dbsize(ctx.db_index)? as i64))
}

fn flushdb_cmd(store: &Store, ctx: &mut ClientContext, _args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    store.flushdb(ctx.db_index)?; Ok(RespValue::ok())
}

fn flushall_cmd(store: &Store, _ctx: &mut ClientContext, _args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    store.flushall()?; Ok(RespValue::ok())
}

fn randomkey_cmd(store: &Store, ctx: &mut ClientContext, _args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    match store.random_key(ctx.db_index)? {
        Some(key) => Ok(RespValue::bulk_string(key)),
        None => Ok(RespValue::nil()),
    }
}
