// Copyright (c) 2025-Present VeloDB Contributors
// SPDX-License-Identifier: MIT

use super::CommandDef;
use crate::store::Store;
use crate::resp::RespValue;
use crate::net::connection::ClientContext;

pub const COMMANDS: &[CommandDef] = &[
    CommandDef { name: "MULTI", arity: 1, handler: multi },
    CommandDef { name: "EXEC", arity: 1, handler: exec },
    CommandDef { name: "DISCARD", arity: 1, handler: discard },
    CommandDef { name: "WATCH", arity: -2, handler: watch },
    CommandDef { name: "UNWATCH", arity: 1, handler: unwatch },
];

fn multi(_store: &Store, ctx: &mut ClientContext, _args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    ctx.multi_mode = true;
    ctx.multi_queue.clear();
    Ok(RespValue::ok())
}

fn exec(store: &Store, ctx: &mut ClientContext, _args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    if !ctx.multi_mode {
        return Err(crate::error::VeloDBError::Internal("EXEC without MULTI".into()));
    }
    ctx.multi_mode = false;
    let queue = std::mem::take(&mut ctx.multi_queue);
    let watched = std::mem::take(&mut ctx.watched_keys);
    let versions: Vec<u64> = std::mem::take(&mut ctx.watched_versions);

    // Check watch violations
    for (i, key) in watched.iter().enumerate() {
        match store.get_version(ctx.db_index, key) {
            Ok(current) => {
                if current != versions[i] {
                    return Ok(RespValue::Array(None));
                }
            }
            Err(_) => return Ok(RespValue::Array(None)),
        }
    }

    // Execute
    let mut results = Vec::with_capacity(queue.len());
    for args in queue {
        let cmd_name = String::from_utf8_lossy(&args[0]).to_uppercase();
        // Use a temporary CommandTable just for dispatch
        let cmd_table = crate::cmd::CommandTable::new();
        let resp = cmd_table.dispatch(&cmd_name, store, ctx, &args[1..]);
        results.push(resp);
    }
    Ok(RespValue::Array(Some(results)))
}

fn discard(_store: &Store, ctx: &mut ClientContext, _args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    ctx.multi_mode = false;
    ctx.multi_queue.clear();
    ctx.watched_keys.clear();
    ctx.watched_versions.clear();
    Ok(RespValue::ok())
}

fn watch(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    if ctx.multi_mode { return Ok(RespValue::ok()); }
    for key in args {
        match store.get_version(ctx.db_index, key) {
            Ok(v) => {
                ctx.watched_keys.push(key.clone());
                ctx.watched_versions.push(v);
            }
            Err(_) => {
                ctx.watched_keys.push(key.clone());
                ctx.watched_versions.push(0);
            }
        }
    }
    Ok(RespValue::ok())
}

fn unwatch(_store: &Store, ctx: &mut ClientContext, _args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    ctx.watched_keys.clear();
    ctx.watched_versions.clear();
    Ok(RespValue::ok())
}
