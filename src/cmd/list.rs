// Copyright (c) 2025-Present VeloDB Contributors
// SPDX-License-Identifier: MIT

use super::CommandDef;
use crate::store::Store;
use crate::resp::RespValue;
use crate::net::connection::{ClientContext, BlockState, PopDirection};

pub const COMMANDS: &[CommandDef] = &[
    CommandDef { name: "LPUSH", arity: -3, handler: lpush },
    CommandDef { name: "RPUSH", arity: -3, handler: rpush },
    CommandDef { name: "LPOP", arity: -2, handler: lpop },
    CommandDef { name: "RPOP", arity: -2, handler: rpop },
    CommandDef { name: "LLEN", arity: 2, handler: llen },
    CommandDef { name: "LRANGE", arity: 4, handler: lrange },
    CommandDef { name: "LINDEX", arity: 3, handler: lindex },
    CommandDef { name: "LSET", arity: 4, handler: lset },
    CommandDef { name: "LTRIM", arity: 4, handler: ltrim },
    CommandDef { name: "LREM", arity: 4, handler: lrem },
    CommandDef { name: "BLPOP", arity: -3, handler: blpop },
    CommandDef { name: "BRPOP", arity: -3, handler: brpop },
];

fn lpush(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    let len = store.lpush(ctx.db_index, &args[0], &args[1..])?;
    Ok(RespValue::integer(len as i64))
}

fn rpush(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    let len = store.rpush(ctx.db_index, &args[0], &args[1..])?;
    Ok(RespValue::integer(len as i64))
}

fn lpop(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    match store.lpop_one(ctx.db_index, &args[0])? {
        Some(val) => Ok(RespValue::bulk_string(val)),
        None => Ok(RespValue::nil()),
    }
}

fn rpop(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    match store.rpop_one(ctx.db_index, &args[0])? {
        Some(val) => Ok(RespValue::bulk_string(val)),
        None => Ok(RespValue::nil()),
    }
}

fn llen(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    Ok(RespValue::integer(store.llen(ctx.db_index, &args[0])? as i64))
}

fn lrange(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    let start: i64 = String::from_utf8_lossy(&args[1]).parse().unwrap_or(0);
    let stop: i64 = String::from_utf8_lossy(&args[2]).parse().unwrap_or(-1);
    let vals = store.lrange(ctx.db_index, &args[0], start, stop)?;
    Ok(RespValue::Array(Some(vals.into_iter().map(RespValue::bulk_string).collect())))
}

fn lindex(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    let idx: i64 = String::from_utf8_lossy(&args[1]).parse().unwrap_or(0);
    match store.lindex(ctx.db_index, &args[0], idx)? {
        Some(val) => Ok(RespValue::bulk_string(val)),
        None => Ok(RespValue::nil()),
    }
}

fn lset(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    let idx: i64 = String::from_utf8_lossy(&args[1]).parse().unwrap_or(0);
    store.lset(ctx.db_index, &args[0], idx, &args[2])?;
    Ok(RespValue::ok())
}

fn ltrim(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    let start: i64 = String::from_utf8_lossy(&args[1]).parse().unwrap_or(0);
    let stop: i64 = String::from_utf8_lossy(&args[2]).parse().unwrap_or(-1);
    store.ltrim(ctx.db_index, &args[0], start, stop)?;
    Ok(RespValue::ok())
}

fn lrem(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    let count: i64 = String::from_utf8_lossy(&args[1]).parse().unwrap_or(0);
    let removed = store.lrem(ctx.db_index, &args[0], count, &args[2])?;
    Ok(RespValue::integer(removed as i64))
}

fn blpop(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    let keys = &args[..args.len() - 1];
    let timeout_str = String::from_utf8_lossy(&args[args.len() - 1]);
    let timeout: i64 = timeout_str.parse().unwrap_or(0);

    for key in keys {
        if let Some(val) = store.lpop_one(ctx.db_index, key)? {
            return Ok(RespValue::Array(Some(vec![
                RespValue::bulk_string(key.clone()),
                RespValue::bulk_string(val),
            ])));
        }
    }
    if timeout == 0 { return Ok(RespValue::nil()); }
    ctx.block_state = Some(BlockState {
        keys: keys.to_vec(),
        timeout_ms: timeout,
        pop_direction: PopDirection::Left,
    });
    Ok(RespValue::ok())
}

fn brpop(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    let keys = &args[..args.len() - 1];
    let timeout_str = String::from_utf8_lossy(&args[args.len() - 1]);
    let timeout: i64 = timeout_str.parse().unwrap_or(0);

    for key in keys {
        if let Some(val) = store.rpop_one(ctx.db_index, key)? {
            return Ok(RespValue::Array(Some(vec![
                RespValue::bulk_string(key.clone()),
                RespValue::bulk_string(val),
            ])));
        }
    }
    if timeout == 0 { return Ok(RespValue::nil()); }
    ctx.block_state = Some(BlockState {
        keys: keys.to_vec(),
        timeout_ms: timeout,
        pop_direction: PopDirection::Right,
    });
    Ok(RespValue::ok())
}
