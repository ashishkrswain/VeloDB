// Copyright (c) 2025-Present VeloDB Contributors
// SPDX-License-Identifier: MIT

use thiserror::Error;

#[derive(Error, Debug)]
#[allow(dead_code)]
pub enum VeloDBError {
    #[error("ERR unknown command '{0}'")]
    UnknownCommand(String),
    #[error("ERR wrong number of arguments for '{0}' command")]
    WrongNumberOfArgs(String),
    #[error("ERR syntax error")]
    SyntaxError,
    #[error("WRONGTYPE Operation against a key holding the wrong kind of value")]
    WrongType,
    #[error("ERR no such key")]
    KeyNotFound,
    #[error("ERR value is not an integer or out of range")]
    NotInteger,
    #[error("ERR increment or decrement would overflow")]
    Overflow,
    #[error("ERR index out of range")]
    IndexOutOfRange,
    #[error("ERR min or max not a valid float")]
    MinOrMaxNotValidFloat,
    #[error("ERR The ID specified in XADD is equal or smaller than the target stream top item")]
    StreamIDTooSmall,
    #[error("ERR AOF error: {0}")]
    AofError(String),
    #[error("ERR RDB error: {0}")]
    RdbError(String),
    #[error("ERR replication error: {0}")]
    ReplError(String),
    #[error("ERR protocol error: {0}")]
    ProtocolError(String),
    #[error("{0}")]
    Io(#[from] std::io::Error),
    #[error("Lua error: {0}")]
    Lua(String),
    #[error("{0}")]
    Internal(String),
}

impl VeloDBError {
    pub fn unknown_command(cmd: impl Into<String>) -> Self { Self::UnknownCommand(cmd.into().to_uppercase()) }
    pub fn wrong_number_of_args(cmd: impl Into<String>) -> Self { Self::WrongNumberOfArgs(cmd.into()) }
    pub fn protocol_error(msg: impl Into<String>) -> Self { Self::ProtocolError(msg.into()) }
    pub fn internal(msg: impl Into<String>) -> Self { Self::Internal(msg.into()) }
    pub fn syntax_error() -> Self { Self::SyntaxError }
    pub fn key_not_found() -> Self { Self::KeyNotFound }
    pub fn not_integer() -> Self { Self::NotInteger }
    pub fn overflow() -> Self { Self::Overflow }
    pub fn wrong_type() -> Self { Self::WrongType }
    pub fn index_out_of_range() -> Self { Self::IndexOutOfRange }
    pub fn min_max_not_valid_float() -> Self { Self::MinOrMaxNotValidFloat }
    pub fn stream_id_too_small() -> Self { Self::StreamIDTooSmall }
}

pub type Result<T> = std::result::Result<T, VeloDBError>;
