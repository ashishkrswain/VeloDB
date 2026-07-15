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
    CommandDef { name: "SCAN", arity: -2, handler: scan_cmd },
    CommandDef { name: "HSCAN", arity: -3, handler: hscan_cmd },
    CommandDef { name: "SSCAN", arity: -3, handler: sscan_cmd },
    CommandDef { name: "ZSCAN", arity: -3, handler: zscan_cmd },
];

fn parse_cursor(raw: &[u8]) -> crate::error::Result<u64> {
    String::from_utf8_lossy(raw).parse()
        .map_err(|_| crate::error::VeloDBError::internal("ERR invalid cursor"))
}

/// Parses trailing `[MATCH pattern] [COUNT n]` options of SCAN commands.
fn parse_scan_opts(args: &[Vec<u8>]) -> crate::error::Result<(Option<String>, usize)> {
    let mut pattern = None;
    let mut count = 10usize;
    let mut i = 0;
    while i < args.len() {
        let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
        match opt.as_str() {
            "MATCH" if i + 1 < args.len() => {
                pattern = Some(String::from_utf8_lossy(&args[i + 1]).to_string());
                i += 2;
            }
            "COUNT" if i + 1 < args.len() => {
                count = String::from_utf8_lossy(&args[i + 1]).parse()
                    .map_err(|_| crate::error::VeloDBError::not_integer())?;
                if count == 0 { return Err(crate::error::VeloDBError::syntax_error()); }
                i += 2;
            }
            _ => return Err(crate::error::VeloDBError::syntax_error()),
        }
    }
    Ok((pattern, count))
}

fn scan_reply(cursor: u64, items: Vec<RespValue>) -> RespValue {
    RespValue::Array(Some(vec![
        RespValue::bulk_string(cursor.to_string().into_bytes()),
        RespValue::Array(Some(items)),
    ]))
}

fn scan_cmd(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    let cursor = parse_cursor(&args[0])?;
    let (pattern, count) = parse_scan_opts(&args[1..])?;
    let (next, keys) = store.scan_keys(ctx.db_index, cursor, pattern.as_deref(), count)?;
    Ok(scan_reply(next, keys.into_iter().map(RespValue::bulk_string).collect()))
}

fn hscan_cmd(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    let cursor = parse_cursor(&args[1])?;
    let (pattern, _count) = parse_scan_opts(&args[2..])?;
    let _ = cursor;
    let (next, pairs) = store.hscan(ctx.db_index, &args[0])?;
    let mut items = Vec::with_capacity(pairs.len() * 2);
    for (f, v) in pairs {
        if pattern.as_deref().map_or(true, |p| crate::store::simple_match(&String::from_utf8_lossy(&f), p)) {
            items.push(RespValue::bulk_string(f));
            items.push(RespValue::bulk_string(v));
        }
    }
    Ok(scan_reply(next, items))
}

fn sscan_cmd(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    let cursor = parse_cursor(&args[1])?;
    let (pattern, _count) = parse_scan_opts(&args[2..])?;
    let _ = cursor;
    let (next, members) = store.sscan(ctx.db_index, &args[0])?;
    let items = members.into_iter()
        .filter(|m| pattern.as_deref().map_or(true, |p| crate::store::simple_match(&String::from_utf8_lossy(m), p)))
        .map(RespValue::bulk_string)
        .collect();
    Ok(scan_reply(next, items))
}

fn zscan_cmd(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    let cursor = parse_cursor(&args[1])?;
    let (pattern, _count) = parse_scan_opts(&args[2..])?;
    let _ = cursor;
    let (next, pairs) = store.zscan(ctx.db_index, &args[0])?;
    let mut items = Vec::with_capacity(pairs.len() * 2);
    for (m, score) in pairs {
        if pattern.as_deref().map_or(true, |p| crate::store::simple_match(&String::from_utf8_lossy(&m), p)) {
            items.push(RespValue::bulk_string(m));
            items.push(RespValue::bulk_string(format_score(score).into_bytes()));
        }
    }
    Ok(scan_reply(next, items))
}

/// Formats a score the way Redis does: integers without a decimal point.
fn format_score(score: f64) -> String {
    if score == score.trunc() && score.abs() < 1e17 {
        format!("{}", score as i64)
    } else {
        format!("{}", score)
    }
}

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
