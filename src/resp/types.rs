// Copyright (c) 2025-Present VeloDB Contributors
// SPDX-License-Identifier: MIT

#[derive(Debug, Clone, PartialEq)]
pub enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Option<Vec<u8>>),
    Array(Option<Vec<RespValue>>),
}

impl RespValue {
    pub fn ok() -> Self { RespValue::SimpleString("OK".into()) }
    pub fn pong() -> Self { RespValue::SimpleString("PONG".into()) }
    pub fn nil() -> Self { RespValue::BulkString(None) }
    pub fn bulk_string(s: impl Into<Vec<u8>>) -> Self { RespValue::BulkString(Some(s.into())) }
    pub fn integer(i: i64) -> Self { RespValue::Integer(i) }
    pub fn error(msg: impl Into<String>) -> Self { RespValue::Error(msg.into()) }

    pub fn as_bulk_string(&self) -> Option<&[u8]> {
        match self {
            RespValue::BulkString(Some(s)) => Some(s.as_slice()),
            RespValue::SimpleString(s) => Some(s.as_bytes()),
            _ => None,
        }
    }

    pub fn as_bulk_string_owned(&self) -> Option<Vec<u8>> { self.as_bulk_string().map(|s| s.to_vec()) }

    pub fn as_int(&self) -> Option<i64> {
        match self {
            RespValue::Integer(i) => Some(*i),
            RespValue::BulkString(Some(s)) => String::from_utf8_lossy(s).parse().ok(),
            _ => None,
        }
    }
}
