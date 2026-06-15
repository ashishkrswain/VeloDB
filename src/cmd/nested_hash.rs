// Copyright (c) 2025-Present VeloDB Contributors
// SPDX-License-Identifier: MIT

use super::CommandDef;
use crate::store::Store;
use crate::resp::RespValue;
use crate::net::connection::ClientContext;

pub const COMMANDS: &[CommandDef] = &[
    CommandDef { name: "NHSET", arity: 5, handler: nhset },
    CommandDef { name: "NHGET", arity: 4, handler: nhget },
    CommandDef { name: "NHDEL", arity: -3, handler: nhdel },
    CommandDef { name: "NHKEYS", arity: -2, handler: nhkeys },
    CommandDef { name: "NHVALS", arity: -2, handler: nhvals },
    CommandDef { name: "NHGETALL", arity: -2, handler: nhgetall },
];

fn nhset(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    let added = store.nhset(ctx.db_index, &args[0], &args[1], &args[2], &args[3])?;
    Ok(RespValue::integer(added as i64))
}

fn nhget(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    match store.nhget(ctx.db_index, &args[0], &args[1], &args[2])? {
        Some(val) => Ok(RespValue::bulk_string(val)),
        None => Ok(RespValue::nil()),
    }
}

fn nhdel(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    let subfield = if args.len() >= 3 { Some(args[2].as_slice()) } else { None };
    let removed = store.nhdel(ctx.db_index, &args[0], &args[1], subfield)?;
    Ok(RespValue::integer(removed as i64))
}

fn nhkeys(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    let field = if args.len() >= 2 { Some(args[1].as_slice()) } else { None };
    let keys = store.nhkeys(ctx.db_index, &args[0], field)?;
    Ok(RespValue::Array(Some(keys.into_iter().map(RespValue::bulk_string).collect())))
}

fn nhvals(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    let field = if args.len() >= 2 { Some(args[1].as_slice()) } else { None };
    let vals = store.nhvals(ctx.db_index, &args[0], field)?;
    Ok(RespValue::Array(Some(vals.into_iter().map(RespValue::bulk_string).collect())))
}

fn nhgetall(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    let field = if args.len() >= 2 { Some(args[1].as_slice()) } else { None };
    let pairs = store.nhgetall(ctx.db_index, &args[0], field)?;
    let mut resp: Vec<RespValue> = Vec::with_capacity(pairs.len() * 2);
    for (k, v) in pairs {
        resp.push(RespValue::bulk_string(k));
        resp.push(RespValue::bulk_string(v));
    }
    Ok(RespValue::Array(Some(resp)))
}
