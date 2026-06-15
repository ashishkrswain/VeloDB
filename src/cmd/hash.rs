// Copyright (c) 2025-Present VeloDB Contributors
// SPDX-License-Identifier: MIT

use super::CommandDef;
use crate::store::Store;
use crate::resp::RespValue;
use crate::net::connection::ClientContext;

pub const COMMANDS: &[CommandDef] = &[
    CommandDef { name: "HSET", arity: -4, handler: hset },
    CommandDef { name: "HGET", arity: 3, handler: hget },
    CommandDef { name: "HDEL", arity: -3, handler: hdel },
    CommandDef { name: "HEXISTS", arity: 3, handler: hexists },
    CommandDef { name: "HGETALL", arity: 2, handler: hgetall },
    CommandDef { name: "HKEYS", arity: 2, handler: hkeys },
    CommandDef { name: "HVALS", arity: 2, handler: hvals },
    CommandDef { name: "HLEN", arity: 2, handler: hlen },
    CommandDef { name: "HINCRBY", arity: 4, handler: hincrby },
];

fn parse_pairs(args: &[Vec<u8>]) -> Vec<(Vec<u8>, Vec<u8>)> {
    args.chunks(2).filter(|c| c.len() == 2).map(|c| (c[0].clone(), c[1].clone())).collect()
}

fn hset(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    let pairs = parse_pairs(&args[1..]);
    if pairs.is_empty() {
        return Err(crate::error::VeloDBError::wrong_number_of_args("HSET"));
    }
    let added = store.hset(ctx.db_index, &args[0], &pairs)?;
    Ok(RespValue::integer(added as i64))
}

fn hget(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    match store.hget(ctx.db_index, &args[0], &args[1])? {
        Some(val) => Ok(RespValue::bulk_string(val)),
        None => Ok(RespValue::nil()),
    }
}

fn hdel(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    let removed = store.hdel(ctx.db_index, &args[0], &args[1..])?;
    Ok(RespValue::integer(removed as i64))
}

fn hexists(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    Ok(RespValue::integer(store.hexists(ctx.db_index, &args[0], &args[1])? as i64))
}

fn hgetall(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    let pairs = store.hgetall(ctx.db_index, &args[0])?;
    let mut resp: Vec<RespValue> = Vec::with_capacity(pairs.len() * 2);
    for (f, v) in pairs {
        resp.push(RespValue::bulk_string(f));
        resp.push(RespValue::bulk_string(v));
    }
    Ok(RespValue::Array(Some(resp)))
}

fn hkeys(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    let keys = store.hkeys(ctx.db_index, &args[0])?;
    Ok(RespValue::Array(Some(keys.into_iter().map(RespValue::bulk_string).collect())))
}

fn hvals(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    let vals = store.hvals(ctx.db_index, &args[0])?;
    Ok(RespValue::Array(Some(vals.into_iter().map(RespValue::bulk_string).collect())))
}

fn hlen(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    Ok(RespValue::integer(store.hlen(ctx.db_index, &args[0])? as i64))
}

fn hincrby(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    let incr: i64 = String::from_utf8_lossy(&args[2]).parse().map_err(|_| crate::error::VeloDBError::not_integer())?;
    let new_val = store.hincrby(ctx.db_index, &args[0], &args[1], incr)?;
    Ok(RespValue::integer(new_val))
}
