// Copyright (c) 2025-Present VeloDB Contributors
// SPDX-License-Identifier: MIT

use super::CommandDef;
use crate::store::Store;
use crate::resp::RespValue;
use crate::net::connection::ClientContext;

pub const COMMANDS: &[CommandDef] = &[
    CommandDef { name: "ZADD", arity: -4, handler: zadd },
    CommandDef { name: "ZREM", arity: -3, handler: zrem },
    CommandDef { name: "ZSCORE", arity: 3, handler: zscore },
    CommandDef { name: "ZRANK", arity: 3, handler: zrank },
    CommandDef { name: "ZRANGE", arity: -4, handler: zrange },
    CommandDef { name: "ZRANGEBYSCORE", arity: -4, handler: zrangebyscore },
    CommandDef { name: "ZCARD", arity: 2, handler: zcard },
    CommandDef { name: "ZCOUNT", arity: 4, handler: zcount },
];

fn parse_score_member_pairs(args: &[Vec<u8>]) -> crate::error::Result<Vec<(f64, Vec<u8>)>> {
    let mut pairs = Vec::new();
    let mut i = 0;
    while i + 1 < args.len() {
        let score: f64 = String::from_utf8_lossy(&args[i]).parse().map_err(|_| crate::error::VeloDBError::min_max_not_valid_float())?;
        pairs.push((score, args[i + 1].clone()));
        i += 2;
    }
    Ok(pairs)
}

fn zadd(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    let pairs = parse_score_member_pairs(&args[1..])?;
    let added = store.zadd(ctx.db_index, &args[0], &pairs)?;
    Ok(RespValue::integer(added as i64))
}

fn zrem(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    let removed = store.zrem(ctx.db_index, &args[0], &args[1..])?;
    Ok(RespValue::integer(removed as i64))
}

fn zscore(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    match store.zscore(ctx.db_index, &args[0], &args[1])? {
        Some(s) => Ok(RespValue::bulk_string(format!("{}", s))),
        None => Ok(RespValue::nil()),
    }
}

fn zrank(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    match store.zrank(ctx.db_index, &args[0], &args[1])? {
        Some(rank) => Ok(RespValue::integer(rank as i64)),
        None => Ok(RespValue::nil()),
    }
}

fn zrange(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    let start: i64 = String::from_utf8_lossy(&args[1]).parse().unwrap_or(0);
    let stop: i64 = String::from_utf8_lossy(&args[2]).parse().unwrap_or(-1);
    let withscores = args.len() > 3 && String::from_utf8_lossy(&args[3]).to_uppercase() == "WITHSCORES";
    let items = store.zrange(ctx.db_index, &args[0], start, stop, withscores)?;
    let mut resp = Vec::new();
    for (member, score) in items {
        resp.push(RespValue::bulk_string(member));
        if let Some(s) = score { resp.push(RespValue::bulk_string(format!("{}", s))); }
    }
    Ok(RespValue::Array(Some(resp)))
}

fn parse_score_bound(s: &[u8]) -> crate::error::Result<(f64, bool)> {
    let s = String::from_utf8_lossy(s);
    if s == "-inf" || s == "-infinity" { return Ok((f64::NEG_INFINITY, false)); }
    if s == "+inf" || s == "+infinity" || s == "inf" { return Ok((f64::INFINITY, false)); }
    if s.starts_with('(') {
        let val: f64 = s[1..].parse().map_err(|_| crate::error::VeloDBError::min_max_not_valid_float())?;
        return Ok((val, true));
    }
    let val: f64 = s.parse().map_err(|_| crate::error::VeloDBError::min_max_not_valid_float())?;
    Ok((val, false))
}

fn zrangebyscore(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    let (min, min_excl) = parse_score_bound(&args[1])?;
    let (max, max_excl) = parse_score_bound(&args[2])?;
    let mut withscores = false;
    let mut limit: Option<(usize, usize)> = None;
    let mut i = 3;
    while i < args.len() {
        match String::from_utf8_lossy(&args[i]).to_uppercase().as_str() {
            "WITHSCORES" => { withscores = true; i += 1; }
            "LIMIT" if i + 2 < args.len() => {
                let offset: usize = String::from_utf8_lossy(&args[i + 1]).parse().unwrap_or(0);
                let count: usize = String::from_utf8_lossy(&args[i + 2]).parse().unwrap_or(0);
                limit = Some((offset, count));
                i += 3;
            }
            _ => i += 1,
        }
    }
    let items = store.zrange_by_score(ctx.db_index, &args[0], min, min_excl, max, max_excl, withscores, limit)?;
    let mut resp = Vec::new();
    for (member, score) in items {
        resp.push(RespValue::bulk_string(member));
        if let Some(s) = score { resp.push(RespValue::bulk_string(format!("{}", s))); }
    }
    Ok(RespValue::Array(Some(resp)))
}

fn zcard(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    Ok(RespValue::integer(store.zcard(ctx.db_index, &args[0])? as i64))
}

fn zcount(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    let (min, min_excl) = parse_score_bound(&args[1])?;
    let (max, max_excl) = parse_score_bound(&args[2])?;
    Ok(RespValue::integer(store.zcount(ctx.db_index, &args[0], min, min_excl, max, max_excl)? as i64))
}
