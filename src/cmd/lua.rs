// Copyright (c) 2025-Present VeloDB Contributors
// SPDX-License-Identifier: MIT

use sha1::{Digest, Sha1};
use mlua::{Lua, Value as LuaValue};
use crate::store::Store;
use crate::resp::RespValue;
use crate::net::connection::ClientContext;
use super::CommandDef;

pub const COMMANDS: &[CommandDef] = &[
    CommandDef { name: "EVAL", arity: -3, handler: eval_cmd },
    CommandDef { name: "EVALSHA", arity: -3, handler: evalsha_cmd },
    CommandDef { name: "SCRIPT", arity: -2, handler: script_cmd },
];

fn sha1_hex(data: &str) -> String {
    let mut hasher = Sha1::new();
    hasher.update(data.as_bytes());
    format!("{:x}", hasher.finalize())
}

fn lua_error(e: mlua::Error) -> crate::error::VeloDBError {
    crate::error::VeloDBError::internal(format!("lua: {}", e))
}

fn eval_cmd(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    if args.len() < 2 {
        return Err(crate::error::VeloDBError::wrong_number_of_args("EVAL"));
    }

    let script = String::from_utf8_lossy(&args[0]).to_string();
    let num_keys: usize = String::from_utf8_lossy(&args[1]).parse().unwrap_or(0);

    let keys: Vec<Vec<u8>> = args[2..std::cmp::min(2 + num_keys, args.len())].to_vec();
    let script_args: Vec<Vec<u8>> = args[std::cmp::min(2 + num_keys, args.len())..].to_vec();

    let lua = Lua::new();
    let cmd_table = crate::cmd::CommandTable::new();
    let store_ptr = store as *const Store;
    let db_idx = ctx.db_index;

    let call_fn = lua.create_function(move |lua_ctx, (_name, targs): (String, LuaValue)| {
        let cmd_args = match targs {
            LuaValue::Table(t) => {
                let mut v = Vec::new();
                let len = t.len().unwrap_or(0);
                for i in 1..=len {
                    if let Ok(s) = t.get::<String>(i) { v.push(s.as_bytes().to_vec()); }
                }
                v
            }
            _ => return Ok(LuaValue::Nil),
        };
        if cmd_args.is_empty() { return Ok(LuaValue::Nil); }
        let cmd_name = String::from_utf8_lossy(&cmd_args[0]).to_uppercase();
        let store_ref = unsafe { &*store_ptr };
        let mut call_ctx = ClientContext { db_index: db_idx, ..ClientContext::new() };
        let result = cmd_table.dispatch(&cmd_name, store_ref, &mut call_ctx, &cmd_args[1..]);

        resp_to_lua(lua_ctx, &result)
    }).map_err(lua_error)?;

    lua.globals().set("redis", lua.create_table_from(vec![("call", call_fn)]).map_err(lua_error)?).map_err(lua_error)?;
    lua.globals().set("KEYS", lua.create_sequence_from(
        keys.iter().map(|k| String::from_utf8_lossy(k).to_string()).collect::<Vec<_>>()
    ).map_err(lua_error)?).map_err(lua_error)?;
    lua.globals().set("ARGV", lua.create_sequence_from(
        script_args.iter().map(|a| String::from_utf8_lossy(a).to_string()).collect::<Vec<_>>()
    ).map_err(lua_error)?).map_err(lua_error)?;

    match lua.load(&script).eval::<LuaValue>() {
        Ok(val) => lua_to_resp(val),
        Err(e) => Ok(RespValue::error(format!("ERR {}", e))),
    }
}

fn resp_to_lua(lua_ctx: &Lua, val: &RespValue) -> mlua::Result<LuaValue> {
    match val {
        RespValue::Integer(i) => Ok(LuaValue::Integer(*i)),
        RespValue::SimpleString(s) => Ok(LuaValue::String(lua_ctx.create_string(s.as_bytes())?)),
        RespValue::BulkString(Some(b)) => Ok(LuaValue::String(lua_ctx.create_string(b)?)),
        RespValue::BulkString(None) => Ok(LuaValue::Boolean(false)),
        RespValue::Error(e) => Ok(LuaValue::String(lua_ctx.create_string(e.as_bytes())?)),
        RespValue::Array(Some(items)) => {
            let tbl = lua_ctx.create_table()?;
            for (i, item) in items.iter().enumerate() {
                tbl.set(i + 1, resp_to_lua(lua_ctx, item)?)?;
            }
            Ok(LuaValue::Table(tbl))
        }
        RespValue::Array(None) => Ok(LuaValue::Boolean(false)),
    }
}

fn lua_to_resp(val: LuaValue) -> crate::error::Result<RespValue> {
    match val {
        LuaValue::Integer(i) => Ok(RespValue::integer(i)),
        LuaValue::String(s) => Ok(RespValue::bulk_string(s.as_bytes().to_vec())),
        LuaValue::Boolean(b) => Ok(RespValue::integer(if b { 1 } else { 0 })),
        LuaValue::Table(_) => Ok(RespValue::ok()),
        _ => Ok(RespValue::ok()),
    }
}

fn evalsha_cmd(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    if args.len() < 2 {
        return Err(crate::error::VeloDBError::wrong_number_of_args("EVALSHA"));
    }
    let sha = String::from_utf8_lossy(&args[0]).to_string();
    if let Some(script) = store.lua_scripts.get(&sha) {
        let mut new_args = vec![script.as_bytes().to_vec()];
        new_args.extend_from_slice(&args[1..]);
        eval_cmd(store, ctx, &new_args)
    } else {
        Ok(RespValue::error("NOSCRIPT No matching script. Please use EVAL."))
    }
}

fn script_cmd(store: &Store, _ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    if args.is_empty() {
        return Err(crate::error::VeloDBError::wrong_number_of_args("SCRIPT"));
    }
    let subcmd = String::from_utf8_lossy(&args[0]).to_uppercase();
    match subcmd.as_str() {
        "LOAD" => {
            if args.len() < 2 {
                return Err(crate::error::VeloDBError::wrong_number_of_args("SCRIPT LOAD"));
            }
            let script = String::from_utf8_lossy(&args[1]).to_string();
            let sha = sha1_hex(&script);
            store.lua_scripts.insert(sha.clone(), script);
            Ok(RespValue::bulk_string(sha.as_bytes()))
        }
        "EXISTS" => {
            let mut results: Vec<RespValue> = Vec::new();
            for arg in &args[1..] {
                let sha_str = String::from_utf8_lossy(arg);
                results.push(RespValue::integer(if store.lua_scripts.contains_key(sha_str.as_ref()) { 1 } else { 0 }));
            }
            Ok(RespValue::Array(Some(results)))
        }
        "FLUSH" => {
            store.lua_scripts.clear();
            Ok(RespValue::ok())
        }
        _ => Err(crate::error::VeloDBError::syntax_error()),
    }
}
