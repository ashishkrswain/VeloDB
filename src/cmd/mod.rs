// Copyright (c) 2025-Present VeloDB Contributors
// SPDX-License-Identifier: MIT

mod server;
pub use server::mark_start_time;
mod string;
mod generic;
mod list;
mod set;
mod hash;
mod zset;
mod stream;
mod nested_hash;
mod pubsub;
mod transaction;
mod lua;
mod cluster;

use std::collections::HashMap;
use crate::store::Store;
use crate::resp::RespValue;
use crate::net::connection::ClientContext;

pub type CommandFn = fn(&Store, &mut ClientContext, &[Vec<u8>]) -> crate::error::Result<RespValue>;

pub struct CommandDef {
    pub name: &'static str,
    pub arity: i32,
    pub handler: CommandFn,
}

pub struct CommandTable { commands: HashMap<String, CommandDef> }

impl CommandTable {
    pub fn new() -> Self {
        let mut table = Self { commands: HashMap::new() };
        table.register(server::COMMANDS);
        table.register(string::COMMANDS);
        table.register(generic::COMMANDS);
        table.register(list::COMMANDS);
        table.register(set::COMMANDS);
        table.register(hash::COMMANDS);
        table.register(zset::COMMANDS);
        table.register(stream::COMMANDS);
        table.register(nested_hash::COMMANDS);
        table.register(pubsub::COMMANDS);
        table.register(transaction::COMMANDS);
        table.register(lua::COMMANDS);
        table.register(cluster::COMMANDS);
        table
    }

    fn register(&mut self, commands: &[CommandDef]) {
        for cmd in commands {
            self.commands.insert(cmd.name.to_string(), CommandDef {
                name: cmd.name, arity: cmd.arity, handler: cmd.handler,
            });
        }
    }

    pub fn dispatch(&self, name: &str, store: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> RespValue {
        if store.over_memory_limit()
            && store.eviction_policy() == crate::store::EvictionPolicy::NoEviction
            && is_deny_oom_command(name)
        {
            return RespValue::error("OOM command not allowed when used memory > 'maxmemory'.");
        }
        match self.commands.get(name) {
            Some(cmd) => {
                if cmd.arity > 0 && args.len() as i32 + 1 != cmd.arity {
                    return RespValue::error(format!(
                        "ERR wrong number of arguments for '{}' command", cmd.name.to_lowercase()
                    ));
                }
                match (cmd.handler)(store, ctx, args) {
                    Ok(resp) => resp,
                    Err(e) => RespValue::Error(format!("{}", e)),
                }
            }
            None => RespValue::error(format!("ERR unknown command '{}'", name.to_lowercase())),
        }
    }
}

/// Commands rejected under OOM with the noeviction policy — commands
/// that can grow memory. Reads and deletes always pass (like Redis).
fn is_deny_oom_command(name: &str) -> bool {
    matches!(name,
        "SET" | "SETRANGE" | "MSET" | "INCR" | "INCRBY" | "DECR" | "DECRBY" | "APPEND" | "GETSET" |
        "LPUSH" | "RPUSH" | "LSET" |
        "SADD" |
        "HSET" | "HINCRBY" |
        "ZADD" |
        "XADD" |
        "NHSET" |
        "RENAME" | "RENAMENX" |
        "EVAL" | "EVALSHA"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::EvictionPolicy;

    #[test]
    fn test_oom_rejects_writes_under_noeviction() {
        let store = Store::new(1);
        store.configure_memory(1, EvictionPolicy::NoEviction);
        store.set(0, b"k", &[0u8; 1000], None).unwrap();
        store.refresh_memory_usage();

        let table = CommandTable::new();
        let mut ctx = ClientContext::new();
        let resp = table.dispatch("SET", &store, &mut ctx, &[b"k2".to_vec(), b"v".to_vec()]);
        match resp {
            RespValue::Error(e) => assert!(e.starts_with("OOM"), "expected OOM error, got: {}", e),
            other => panic!("expected OOM error, got {:?}", other),
        }
    }

    #[test]
    fn test_oom_allows_reads_and_deletes() {
        let store = Store::new(1);
        store.configure_memory(1, EvictionPolicy::NoEviction);
        store.set(0, b"k", &[0u8; 1000], None).unwrap();
        store.refresh_memory_usage();

        let table = CommandTable::new();
        let mut ctx = ClientContext::new();
        assert!(!matches!(table.dispatch("GET", &store, &mut ctx, &[b"k".to_vec()]), RespValue::Error(_)));
        assert!(!matches!(table.dispatch("DEL", &store, &mut ctx, &[b"k".to_vec()]), RespValue::Error(_)));
    }

    #[test]
    fn test_no_oom_when_under_limit() {
        let store = Store::new(1);
        store.configure_memory(1_000_000, EvictionPolicy::NoEviction);
        store.refresh_memory_usage();

        let table = CommandTable::new();
        let mut ctx = ClientContext::new();
        let resp = table.dispatch("SET", &store, &mut ctx, &[b"k".to_vec(), b"v".to_vec()]);
        assert!(!matches!(resp, RespValue::Error(_)));
    }
}
