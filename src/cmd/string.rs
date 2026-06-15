// Copyright (c) 2025-Present VeloDB Contributors
// SPDX-License-Identifier: MIT

use super::CommandDef;
use crate::store::Store;
use crate::resp::RespValue;
use crate::net::connection::ClientContext;

pub const COMMANDS: &[CommandDef] = &[
    CommandDef { name: "GET", arity: 2, handler: get },
    CommandDef { name: "SET", arity: -3, handler: set },
    CommandDef { name: "MGET", arity: -2, handler: mget },
    CommandDef { name: "MSET", arity: -3, handler: mset },
    CommandDef { name: "INCR", arity: 2, handler: incr },
    CommandDef { name: "INCRBY", arity: 3, handler: incrby },
    CommandDef { name: "DECR", arity: 2, handler: decr },
    CommandDef { name: "DECRBY", arity: 3, handler: decrby },
    CommandDef { name: "APPEND", arity: 3, handler: append },
    CommandDef { name: "STRLEN", arity: 2, handler: strlen },
    CommandDef { name: "GETRANGE", arity: 4, handler: getrange },
    CommandDef { name: "SETRANGE", arity: 4, handler: setrange },
    CommandDef { name: "GETSET", arity: 3, handler: getset },
];

fn get(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    match store.get(ctx.db_index, &args[0])? {
        Some(val) => Ok(RespValue::bulk_string(val)),
        None => Ok(RespValue::nil()),
    }
}

fn set(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    let mut expire_ms: Option<u64> = None;
    let flag_args = &args[2..];
    let mut i = 0;
    while i < flag_args.len() {
        let flag = String::from_utf8_lossy(&flag_args[i]).to_uppercase();
        match flag.as_str() {
            "EX" | "PX" | "EXAT" | "PXAT" => {
                if let Some(v) = flag_args.get(i + 1) {
                    if let Ok(n) = String::from_utf8_lossy(v).parse::<i64>() {
                        expire_ms = match flag.as_str() {
                            "EX" | "EXAT" => Some((n * 1000) as u64),
                            _ => Some(n as u64),
                        };
                    }
                }
                i += 2;
            }
            _ => i += 1,
        }
    }
    store.set(ctx.db_index, &args[0], &args[1], expire_ms)?;
    Ok(RespValue::ok())
}

fn mget(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    let mut results = Vec::new();
    for key in args {
        match store.get(ctx.db_index, key)? {
            Some(val) => results.push(RespValue::bulk_string(val)),
            None => results.push(RespValue::nil()),
        }
    }
    Ok(RespValue::Array(Some(results)))
}

fn mset(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    if args.len() % 2 != 0 {
        return Err(crate::error::VeloDBError::wrong_number_of_args("MSET"));
    }
    for chunk in args.chunks(2) {
        store.set(ctx.db_index, &chunk[0], &chunk[1], None)?;
    }
    Ok(RespValue::ok())
}

fn incr_fn(store: &Store, ctx: &mut ClientContext, key: &[u8], by: i64) -> crate::error::Result<RespValue> {
    let current = store.get(ctx.db_index, key)?
        .and_then(|v| String::from_utf8_lossy(&v).parse::<i64>().ok())
        .unwrap_or(0);
    let new_val = current.checked_add(by).ok_or(crate::error::VeloDBError::overflow())?;
    store.set(ctx.db_index, key, new_val.to_string().as_bytes(), None)?;
    Ok(RespValue::integer(new_val))
}

fn incr(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    incr_fn(store, ctx, &args[0], 1)
}

fn incrby(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    let by = String::from_utf8_lossy(&args[1]).parse::<i64>().map_err(|_| crate::error::VeloDBError::not_integer())?;
    incr_fn(store, ctx, &args[0], by)
}

fn decr(store: &Store, ctx: &mut ClientContext, _args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    incr_fn(store, ctx, &_args[0], -1)
}

fn decrby(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    let by = String::from_utf8_lossy(&args[1]).parse::<i64>().map_err(|_| crate::error::VeloDBError::not_integer())?;
    incr_fn(store, ctx, &args[0], -by)
}

fn append(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    let mut current = store.get(ctx.db_index, &args[0])?.unwrap_or_default();
    current.extend_from_slice(&args[1]);
    let len = current.len();
    store.set(ctx.db_index, &args[0], &current, None)?;
    Ok(RespValue::integer(len as i64))
}

fn strlen(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    match store.get(ctx.db_index, &args[0])? {
        Some(val) => Ok(RespValue::integer(val.len() as i64)),
        None => Ok(RespValue::integer(0)),
    }
}

fn getrange(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    let start: i64 = String::from_utf8_lossy(&args[1]).parse().unwrap_or(0);
    let end: i64 = String::from_utf8_lossy(&args[2]).parse().unwrap_or(-1);
    match store.get(ctx.db_index, &args[0])? {
        Some(val) => {
            let len = val.len() as i64;
            let s = (if start < 0 { (len + start).max(0) } else { start.min(len - 1) }) as usize;
            let e = (if end < 0 { (len + end).max(-1) } else { end.min(len - 1) }) as usize;
            if s > e || s >= val.len() { Ok(RespValue::bulk_string("")) }
            else { Ok(RespValue::bulk_string(&val[s..=e.min(val.len() - 1)])) }
        }
        None => Ok(RespValue::bulk_string("")),
    }
}

fn setrange(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    let offset: usize = String::from_utf8_lossy(&args[1]).parse().unwrap_or(0);
    let mut current = store.get(ctx.db_index, &args[0])?.unwrap_or_default();
    if offset + args[2].len() > current.len() { current.resize(offset + args[2].len(), 0); }
    current[offset..offset + args[2].len()].copy_from_slice(&args[2]);
    let len = current.len();
    store.set(ctx.db_index, &args[0], &current, None)?;
    Ok(RespValue::integer(len as i64))
}

fn getset(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    let old = store.get(ctx.db_index, &args[0])?;
    store.set(ctx.db_index, &args[0], &args[1], None)?;
    match old { Some(val) => Ok(RespValue::bulk_string(val)), None => Ok(RespValue::nil()) }
}
