// Copyright (c) 2025-Present VeloDB Contributors
// SPDX-License-Identifier: MIT

use super::CommandDef;
use crate::store::Store;
use crate::resp::RespValue;
use crate::net::connection::ClientContext;

pub const COMMANDS: &[CommandDef] = &[
    CommandDef { name: "SADD", arity: -3, handler: sadd },
    CommandDef { name: "SREM", arity: -3, handler: srem },
    CommandDef { name: "SMEMBERS", arity: 2, handler: smembers },
    CommandDef { name: "SISMEMBER", arity: 3, handler: sismember },
    CommandDef { name: "SCARD", arity: 2, handler: scard },
    CommandDef { name: "SINTER", arity: -2, handler: sinter },
    CommandDef { name: "SUNION", arity: -2, handler: sunion },
    CommandDef { name: "SDIFF", arity: -2, handler: sdiff },
    CommandDef { name: "SRANDMEMBER", arity: -2, handler: srandmember },
    CommandDef { name: "SPOP", arity: -2, handler: spop },
];

fn sadd(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    let added = store.sadd(ctx.db_index, &args[0], &args[1..])?;
    Ok(RespValue::integer(added as i64))
}

fn srem(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    let removed = store.srem(ctx.db_index, &args[0], &args[1..])?;
    Ok(RespValue::integer(removed as i64))
}

fn smembers(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    let members = store.smembers(ctx.db_index, &args[0])?;
    Ok(RespValue::Array(Some(members.into_iter().map(RespValue::bulk_string).collect())))
}

fn sismember(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    Ok(RespValue::integer(store.sismember(ctx.db_index, &args[0], &args[1])? as i64))
}

fn scard(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    Ok(RespValue::integer(store.scard(ctx.db_index, &args[0])? as i64))
}

fn sinter(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    let result = store.sinter(ctx.db_index, args)?;
    Ok(RespValue::Array(Some(result.into_iter().map(RespValue::bulk_string).collect())))
}

fn sunion(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    let result = store.sunion(ctx.db_index, args)?;
    Ok(RespValue::Array(Some(result.into_iter().map(RespValue::bulk_string).collect())))
}

fn sdiff(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    let result = store.sdiff(ctx.db_index, args)?;
    Ok(RespValue::Array(Some(result.into_iter().map(RespValue::bulk_string).collect())))
}

fn srandmember(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    let count = if args.len() > 1 {
        String::from_utf8_lossy(&args[1]).parse::<i64>().ok()
    } else { None };
    let result = store.srandmember(ctx.db_index, &args[0], count)?;
    if count.is_none() || count == Some(1) {
        Ok(result.into_iter().next().map(RespValue::bulk_string).unwrap_or(RespValue::nil()))
    } else {
        Ok(RespValue::Array(Some(result.into_iter().map(RespValue::bulk_string).collect())))
    }
}

fn spop(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    let count = if args.len() > 1 {
        Some(String::from_utf8_lossy(&args[1]).parse::<usize>().unwrap_or(1))
    } else { None };
    let result = store.spop(ctx.db_index, &args[0], count)?;
    if count.is_none() || count == Some(1) {
        Ok(result.into_iter().next().map(RespValue::bulk_string).unwrap_or(RespValue::nil()))
    } else {
        Ok(RespValue::Array(Some(result.into_iter().map(RespValue::bulk_string).collect())))
    }
}
