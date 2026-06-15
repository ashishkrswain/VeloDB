// Copyright (c) 2025-Present VeloDB Contributors
// SPDX-License-Identifier: MIT

use super::CommandDef;
use crate::store::{Store, StreamEntry};
use crate::resp::RespValue;
use crate::net::connection::ClientContext;

pub const COMMANDS: &[CommandDef] = &[
    CommandDef { name: "XADD", arity: -4, handler: xadd },
    CommandDef { name: "XRANGE", arity: -4, handler: xrange },
    CommandDef { name: "XREVRANGE", arity: -4, handler: xrevrange },
    CommandDef { name: "XLEN", arity: 2, handler: xlen },
    CommandDef { name: "XDEL", arity: -3, handler: xdel },
    CommandDef { name: "XTRIM", arity: 4, handler: xtrim },
    CommandDef { name: "XREAD", arity: -4, handler: xread },
];

fn parse_kv_pairs(args: &[Vec<u8>]) -> Vec<(Vec<u8>, Vec<u8>)> {
    args.chunks(2).filter(|c| c.len() == 2).map(|c| (c[0].clone(), c[1].clone())).collect()
}

fn xadd(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    let mut pos = 1;
    let mut maxlen: Option<usize> = None;
    while pos < args.len() {
        let flag = String::from_utf8_lossy(&args[pos]).to_uppercase();
        if flag == "MAXLEN" && pos + 2 < args.len() && String::from_utf8_lossy(&args[pos + 1]) == "~" {
            maxlen = String::from_utf8_lossy(&args[pos + 2]).parse::<usize>().ok();
            pos += 3;
        } else { break; }
    }
    if pos >= args.len() { return Err(crate::error::VeloDBError::syntax_error()); }
    let id = &args[pos];
    pos += 1;
    let fields = parse_kv_pairs(&args[pos..]);
    if fields.is_empty() { return Err(crate::error::VeloDBError::syntax_error()); }
    let generated = store.xadd(ctx.db_index, &args[0], id, &fields, maxlen)?;
    Ok(RespValue::bulk_string(generated))
}

fn serialize_entry(entry: &StreamEntry) -> RespValue {
    let mut items = Vec::with_capacity(2);
    items.push(RespValue::bulk_string(format!("{}-{}", entry.id_ms, entry.id_seq)));
    let mut fields = Vec::new();
    for (k, v) in &entry.fields {
        fields.push(RespValue::bulk_string(k.clone()));
        fields.push(RespValue::bulk_string(v.clone()));
    }
    items.push(RespValue::Array(Some(fields)));
    RespValue::Array(Some(items))
}

fn xrange(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    let mut count: Option<usize> = None;
    let arg_count = args.len();
    if arg_count > 3 {
        let mut i = 3;
        while i < arg_count {
            if String::from_utf8_lossy(&args[i]).to_uppercase() == "COUNT" && i + 1 < arg_count {
                count = String::from_utf8_lossy(&args[i + 1]).parse::<usize>().ok();
                break;
            }
            i += 1;
        }
    }
    let entries = store.xrange(ctx.db_index, &args[0], &args[1], &args[2], count)?;
    Ok(RespValue::Array(Some(entries.iter().map(serialize_entry).collect())))
}

fn xrevrange(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    let mut count: Option<usize> = None;
    let arg_count = args.len();
    if arg_count > 3 {
        let mut i = 3;
        while i < arg_count {
            if String::from_utf8_lossy(&args[i]).to_uppercase() == "COUNT" && i + 1 < arg_count {
                count = String::from_utf8_lossy(&args[i + 1]).parse::<usize>().ok();
                break;
            }
            i += 1;
        }
    }
    let entries = store.xrevrange(ctx.db_index, &args[0], &args[1], &args[2], count)?;
    Ok(RespValue::Array(Some(entries.iter().map(serialize_entry).collect())))
}

fn xlen(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    Ok(RespValue::integer(store.xlen(ctx.db_index, &args[0])? as i64))
}

fn xdel(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    let ids: Vec<String> = args[1..].iter().map(|a| String::from_utf8_lossy(a).to_string()).collect();
    Ok(RespValue::integer(store.xdel(ctx.db_index, &args[0], &ids)? as i64))
}

fn xtrim(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    // XTRIM key MAXLEN ~ count
    if args.len() < 4 || String::from_utf8_lossy(&args[1]).to_uppercase() != "MAXLEN" {
        return Err(crate::error::VeloDBError::syntax_error());
    }
    let maxlen: usize = String::from_utf8_lossy(&args[3]).parse().unwrap_or(0);
    let removed = store.xtrim(ctx.db_index, &args[0], maxlen)?;
    Ok(RespValue::integer(removed as i64))
}

fn parse_stream_id(id_str: &[u8]) -> (u64, u64) {
    let s = String::from_utf8_lossy(id_str);
    if s == "0" || s == "0-0" { return (0, 0); }
    let parts: Vec<&str> = s.splitn(2, '-').collect();
    let ms: u64 = parts[0].parse().unwrap_or(0);
    let seq: u64 = parts.get(1).and_then(|p| p.parse().ok()).unwrap_or(0);
    (ms, seq)
}

fn xread(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    let mut count: Option<usize> = None;
    let mut block_ms: Option<i64> = None;
    let mut pos = 0;
    while pos < args.len() && args[pos].len() > 5 {
        let flag = String::from_utf8_lossy(&args[pos]).to_uppercase();
        match flag.as_str() {
            "COUNT" if pos + 1 < args.len() => {
                count = String::from_utf8_lossy(&args[pos + 1]).parse::<usize>().ok();
                pos += 2;
            }
            "BLOCK" if pos + 1 < args.len() => {
                block_ms = String::from_utf8_lossy(&args[pos + 1]).parse::<i64>().ok();
                pos += 2;
            }
            "STREAMS" => { pos += 1; break; }
            _ => pos += 1,
        }
    }
    if pos >= args.len() { return Err(crate::error::VeloDBError::syntax_error()); }

    let half = (args.len() - pos) / 2;
    let keys = &args[pos..pos + half];
    let ids: Vec<(u64, u64)> = args[pos + half..].iter().map(|a| parse_stream_id(a)).collect();

    let result = store.xread(ctx.db_index, keys, &ids, count, block_ms)?;

    if result.is_empty() && block_ms.is_some() && block_ms != Some(0) {
        return Ok(RespValue::SimpleString("__BLOCK__".into()));
    }

    if result.is_empty() {
        return Ok(RespValue::nil());
    }

    let resp: Vec<RespValue> = result.into_iter().map(|(key, entries)| {
        let key_resp = RespValue::bulk_string(key);
        let entries_resp = RespValue::Array(Some(entries.iter().map(serialize_entry).collect()));
        RespValue::Array(Some(vec![key_resp, entries_resp]))
    }).collect();
    Ok(RespValue::Array(Some(resp)))
}
