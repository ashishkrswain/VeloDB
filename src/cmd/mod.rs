// Copyright (c) 2025-Present VeloDB Contributors
// SPDX-License-Identifier: MIT

mod server;
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
