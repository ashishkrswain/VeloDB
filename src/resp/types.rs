// Copyright (c) 2025-Present VeloDB Contributors
// SPDX-License-Identifier: MIT

#[derive(Debug, Clone, PartialEq)]
pub enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Option<Vec<u8>>),
    Array(Option<Vec<RespValue>>),
    /// RESP3 map (`%`). Serializes as a flat interleaved Array under
    /// RESP2, since RESP2 has no dedicated map type.
    Map(Vec<(RespValue, RespValue)>),
    /// RESP3 double (`,`). Serializes as a bulk string under RESP2.
    Double(f64),
    /// RESP3 boolean (`#`). Serializes as integer 0/1 under RESP2.
    Boolean(bool),
    /// RESP3's dedicated null type (`_`). Distinct from `BulkString(None)`
    /// / `Array(None)` because RESP2 has no unified null — it serializes
    /// as `$-1\r\n` there, matching Redis's own RESP2 fallback for `_`.
    Null,
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
