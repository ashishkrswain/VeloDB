// Copyright (c) 2025-Present VeloDB Contributors
// SPDX-License-Identifier: MIT

use super::CommandDef;
use crate::store::Store;
use crate::resp::RespValue;
use crate::net::connection::ClientContext;
use tokio::sync::mpsc;

pub const COMMANDS: &[CommandDef] = &[
    CommandDef { name: "SUBSCRIBE", arity: -2, handler: subscribe },
    CommandDef { name: "UNSUBSCRIBE", arity: -1, handler: unsubscribe },
    CommandDef { name: "PSUBSCRIBE", arity: -2, handler: psubscribe },
    CommandDef { name: "PUNSUBSCRIBE", arity: -1, handler: punsubscribe },
    CommandDef { name: "PUBLISH", arity: 3, handler: publish },
];

fn subscribe(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    let mut result: Vec<RespValue> = Vec::new();
    let channels = if args.is_empty() { return Ok(RespValue::ok()); } else { args };
    for ch in channels {
        ctx.subscribed_channels.push(ch.clone());
        let (tx, rx) = mpsc::unbounded_channel();
        store.pubsub_subscribe_channel(ch, tx);
        ctx.pubsub_rx = Some(rx);
        result.push(RespValue::bulk_string(b"subscribe".to_vec()));
        result.push(RespValue::bulk_string(ch.clone()));
        result.push(RespValue::integer(ctx.subscribed_channels.len() as i64));
    }
    ctx.sub_mode = true;
    Ok(RespValue::Array(Some(result)))
}

fn unsubscribe(_store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    let mut result: Vec<RespValue> = Vec::new();
    if args.is_empty() {
        ctx.subscribed_channels.clear();
        ctx.pubsub_rx = None;
    } else {
        for ch in args {
            ctx.subscribed_channels.retain(|c| c != ch);
            result.push(RespValue::bulk_string(b"unsubscribe".to_vec()));
            result.push(RespValue::bulk_string(ch.clone()));
            result.push(RespValue::integer(ctx.subscribed_channels.len() as i64));
        }
    }
    if result.is_empty() {
        result.push(RespValue::bulk_string(b"unsubscribe".to_vec()));
        result.push(RespValue::bulk_string(b"".to_vec()));
        result.push(RespValue::integer(0));
    }
    if ctx.subscribed_channels.is_empty() && ctx.subscribed_patterns.is_empty() {
        ctx.sub_mode = false;
        ctx.pubsub_rx = None;
    }
    Ok(RespValue::Array(Some(result)))
}

fn psubscribe(store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    let mut result: Vec<RespValue> = Vec::new();
    let patterns = if args.is_empty() { return Ok(RespValue::ok()); } else { args };
    for p in patterns {
        let p_str = String::from_utf8_lossy(p).to_string();
        ctx.subscribed_patterns.push(p_str.clone());
        let (tx, rx) = mpsc::unbounded_channel();
        store.pubsub_subscribe_pattern(&p_str, tx);
        ctx.pubsub_rx = Some(rx);
        result.push(RespValue::bulk_string(b"psubscribe".to_vec()));
        result.push(RespValue::bulk_string(p.clone()));
        result.push(RespValue::integer(ctx.subscribed_patterns.len() as i64));
    }
    ctx.sub_mode = true;
    Ok(RespValue::Array(Some(result)))
}

fn punsubscribe(_store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    let mut result: Vec<RespValue> = Vec::new();
    if args.is_empty() {
        ctx.subscribed_patterns.clear();
        ctx.pubsub_rx = None;
    } else {
        for p in args {
            let p_str = String::from_utf8_lossy(p);
            ctx.subscribed_patterns.retain(|sp| sp != p_str.as_ref());
        }
    }
    result.push(RespValue::bulk_string(b"punsubscribe".to_vec()));
    result.push(RespValue::bulk_string(b"".to_vec()));
    result.push(RespValue::integer(0));
    if ctx.subscribed_channels.is_empty() && ctx.subscribed_patterns.is_empty() {
        ctx.sub_mode = false;
        ctx.pubsub_rx = None;
    }
    Ok(RespValue::Array(Some(result)))
}

fn publish(store: &Store, _ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    let count = store.pubsub_publish(&args[0], &args[1]);
    Ok(RespValue::integer(count as i64))
}
