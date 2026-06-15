use super::CommandDef;
use crate::store::Store;
use crate::resp::RespValue;
use crate::net::connection::ClientContext;

pub const COMMANDS: &[CommandDef] = &[
    CommandDef { name: "PING", arity: -1, handler: ping },
    CommandDef { name: "ECHO", arity: 2, handler: echo },
    CommandDef { name: "COMMAND", arity: -1, handler: command },
    CommandDef { name: "SELECT", arity: 2, handler: select },
];

fn ping(_s: &Store, _c: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    if args.is_empty() { Ok(RespValue::pong()) }
    else { Ok(RespValue::bulk_string(args[0].clone())) }
}

fn echo(_s: &Store, _c: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    Ok(RespValue::bulk_string(args[0].clone()))
}

fn command(_s: &Store, _c: &mut ClientContext, _args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    Ok(RespValue::ok())
}

fn select(_s: &Store, ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    ctx.db_index = String::from_utf8_lossy(&args[0]).parse().unwrap_or(0);
    Ok(RespValue::ok())
}
