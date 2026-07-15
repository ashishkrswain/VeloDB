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
    CommandDef { name: "XGROUP", arity: -2, handler: xgroup },
    CommandDef { name: "XREADGROUP", arity: -7, handler: xreadgroup },
    CommandDef { name: "XACK", arity: -4, handler: xack },
    CommandDef { name: "XPENDING", arity: -3, handler: xpending },
    CommandDef { name: "XCLAIM", arity: -6, handler: xclaim },
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

fn xgroup(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    if args.is_empty() { return Err(crate::error::VeloDBError::syntax_error()); }
    let sub = String::from_utf8_lossy(&args[0]).to_uppercase();
    match sub.as_str() {
        "CREATE" => {
            if args.len() < 4 { return Err(crate::error::VeloDBError::syntax_error()); }
            let mkstream = args.iter().skip(4).any(|a| String::from_utf8_lossy(a).to_uppercase() == "MKSTREAM");
            let group = String::from_utf8_lossy(&args[2]).to_string();
            store.xgroup_create(ctx.db_index, &args[1], &group, &args[3], mkstream)?;
            Ok(RespValue::ok())
        }
        "DESTROY" => {
            if args.len() < 3 { return Err(crate::error::VeloDBError::syntax_error()); }
            let group = String::from_utf8_lossy(&args[2]).to_string();
            let destroyed = store.xgroup_destroy(ctx.db_index, &args[1], &group)?;
            Ok(RespValue::integer(destroyed as i64))
        }
        "CREATECONSUMER" => {
            if args.len() < 4 { return Err(crate::error::VeloDBError::syntax_error()); }
            let group = String::from_utf8_lossy(&args[2]).to_string();
            let consumer = String::from_utf8_lossy(&args[3]).to_string();
            let created = store.xgroup_create_consumer(ctx.db_index, &args[1], &group, &consumer)?;
            Ok(RespValue::integer(created as i64))
        }
        "DELCONSUMER" => {
            if args.len() < 4 { return Err(crate::error::VeloDBError::syntax_error()); }
            let group = String::from_utf8_lossy(&args[2]).to_string();
            let consumer = String::from_utf8_lossy(&args[3]).to_string();
            let pending = store.xgroup_del_consumer(ctx.db_index, &args[1], &group, &consumer)?;
            Ok(RespValue::integer(pending as i64))
        }
        "SETID" => {
            if args.len() < 4 { return Err(crate::error::VeloDBError::syntax_error()); }
            let group = String::from_utf8_lossy(&args[2]).to_string();
            // Re-create with a new cursor if the group already exists;
            // matches XGROUP SETID's "move the cursor" semantics without
            // touching consumers/PEL (destroy+recreate would lose those,
            // so this is a deliberately narrower implementation).
            store.xgroup_create(ctx.db_index, &args[1], &group, &args[3], false)?;
            Ok(RespValue::ok())
        }
        _ => Err(crate::error::VeloDBError::syntax_error()),
    }
}

fn xreadgroup(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    if args.len() < 6 || String::from_utf8_lossy(&args[0]).to_uppercase() != "GROUP" {
        return Err(crate::error::VeloDBError::syntax_error());
    }
    let group = String::from_utf8_lossy(&args[1]).to_string();
    let consumer = String::from_utf8_lossy(&args[2]).to_string();

    let mut count: Option<usize> = None;
    let mut noack = false;
    let mut pos = 3;
    while pos < args.len() {
        let flag = String::from_utf8_lossy(&args[pos]).to_uppercase();
        match flag.as_str() {
            "COUNT" if pos + 1 < args.len() => {
                count = String::from_utf8_lossy(&args[pos + 1]).parse::<usize>().ok();
                pos += 2;
            }
            "NOACK" => { noack = true; pos += 1; }
            "BLOCK" if pos + 1 < args.len() => { pos += 2; } // blocking XREADGROUP not implemented; treat as non-blocking
            "STREAMS" => { pos += 1; break; }
            _ => return Err(crate::error::VeloDBError::syntax_error()),
        }
    }
    if pos >= args.len() { return Err(crate::error::VeloDBError::syntax_error()); }

    let half = (args.len() - pos) / 2;
    if half == 0 { return Err(crate::error::VeloDBError::syntax_error()); }
    let keys = &args[pos..pos + half];
    let id_args = &args[pos + half..];

    let mut resp = Vec::new();
    for (key, id_arg) in keys.iter().zip(id_args.iter()) {
        let start = if id_arg.as_slice() == b">" {
            crate::store::memory::ReadGroupStart::New
        } else {
            let (ms, seq) = parse_stream_id(id_arg);
            crate::store::memory::ReadGroupStart::History { after_ms: ms, after_seq: seq }
        };
        let entries = store.xreadgroup(ctx.db_index, key, &group, &consumer, start, count, noack)?;
        if !entries.is_empty() || id_arg.as_slice() != b">" {
            resp.push(RespValue::Array(Some(vec![
                RespValue::bulk_string(key.clone()),
                RespValue::Array(Some(entries.iter().map(serialize_entry).collect())),
            ])));
        }
    }
    if resp.is_empty() { return Ok(RespValue::nil()); }
    Ok(RespValue::Array(Some(resp)))
}

fn xack(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    let group = String::from_utf8_lossy(&args[1]).to_string();
    let ids: Vec<(u64, u64)> = args[2..].iter().map(|a| parse_stream_id(a)).collect();
    let acked = store.xack(ctx.db_index, &args[0], &group, &ids)?;
    Ok(RespValue::integer(acked as i64))
}

fn xpending(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    let group = String::from_utf8_lossy(&args[1]).to_string();
    if args.len() == 2 {
        let summary = store.xpending_summary(ctx.db_index, &args[0], &group)?;
        if summary.count == 0 {
            return Ok(RespValue::Array(Some(vec![
                RespValue::integer(0), RespValue::nil(), RespValue::nil(), RespValue::nil(),
            ])));
        }
        let per_consumer: Vec<RespValue> = summary.per_consumer.into_iter().map(|(c, n)| {
            RespValue::Array(Some(vec![RespValue::bulk_string(c.into_bytes()), RespValue::bulk_string(n.to_string().into_bytes())]))
        }).collect();
        let fmt_id = |id: Option<(u64, u64)>| id.map_or(RespValue::nil(), |(ms, seq)| RespValue::bulk_string(format!("{}-{}", ms, seq).into_bytes()));
        return Ok(RespValue::Array(Some(vec![
            RespValue::integer(summary.count as i64),
            fmt_id(summary.min_id),
            fmt_id(summary.max_id),
            RespValue::Array(Some(per_consumer)),
        ])));
    }

    if args.len() < 5 { return Err(crate::error::VeloDBError::syntax_error()); }
    let start = parse_range_id(&args[2], true);
    let end = parse_range_id(&args[3], false);
    let count: usize = String::from_utf8_lossy(&args[4]).parse().map_err(|_| crate::error::VeloDBError::not_integer())?;
    let consumer_filter = args.get(5).map(|a| String::from_utf8_lossy(a).to_string());

    let details = store.xpending_range(ctx.db_index, &args[0], &group, start, end, count, consumer_filter.as_deref())?;
    let resp: Vec<RespValue> = details.into_iter().map(|d| RespValue::Array(Some(vec![
        RespValue::bulk_string(format!("{}-{}", d.id_ms, d.id_seq).into_bytes()),
        RespValue::bulk_string(d.consumer.into_bytes()),
        RespValue::integer(d.idle_ms as i64),
        RespValue::integer(d.delivery_count as i64),
    ]))).collect();
    Ok(RespValue::Array(Some(resp)))
}

fn xclaim(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    let group = String::from_utf8_lossy(&args[1]).to_string();
    let consumer = String::from_utf8_lossy(&args[2]).to_string();
    let min_idle_ms: u64 = String::from_utf8_lossy(&args[3]).parse().map_err(|_| crate::error::VeloDBError::not_integer())?;

    let mut ids = Vec::new();
    let mut pos = 4;
    let mut force = false;
    let mut justid = false;
    while pos < args.len() {
        let a = String::from_utf8_lossy(&args[pos]).to_uppercase();
        match a.as_str() {
            "FORCE" => { force = true; pos += 1; }
            "JUSTID" => { justid = true; pos += 1; }
            "IDLE" | "TIME" | "RETRYCOUNT" | "LASTID" if pos + 1 < args.len() => { pos += 2; } // accepted, not tracked
            _ => { ids.push(parse_stream_id(&args[pos])); pos += 1; }
        }
    }
    if ids.is_empty() { return Err(crate::error::VeloDBError::syntax_error()); }

    let claimed = store.xclaim(ctx.db_index, &args[0], &group, &consumer, min_idle_ms, &ids, force, justid)?;
    if justid {
        return Ok(RespValue::Array(Some(claimed.iter().map(|e| RespValue::bulk_string(format!("{}-{}", e.id_ms, e.id_seq).into_bytes())).collect())));
    }
    Ok(RespValue::Array(Some(claimed.iter().map(serialize_entry).collect())))
}

fn parse_range_id(id_str: &[u8], is_start: bool) -> (u64, u64) {
    let s = String::from_utf8_lossy(id_str);
    if s == "-" { return (0, 0); }
    if s == "+" { return (u64::MAX, u64::MAX); }
    let (ms, seq) = parse_stream_id(id_str);
    if id_str.iter().any(|&b| b == b'-') { (ms, seq) } else { (ms, if is_start { 0 } else { u64::MAX }) }
}
