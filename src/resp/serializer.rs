// Copyright (c) 2025-Present VeloDB Contributors
// SPDX-License-Identifier: MIT

use super::RespValue;

pub fn serialize_response(value: &RespValue) -> Vec<u8> {
    let mut out = Vec::with_capacity(64);
    serialize_value(value, &mut out);
    out
}

fn serialize_value(value: &RespValue, out: &mut Vec<u8>) {
    match value {
        RespValue::SimpleString(s) => { out.push(b'+'); out.extend_from_slice(s.as_bytes()); out.extend_from_slice(b"\r\n"); }
        RespValue::Error(s) => { out.push(b'-'); out.extend_from_slice(s.as_bytes()); out.extend_from_slice(b"\r\n"); }
        RespValue::Integer(i) => { out.push(b':'); out.extend_from_slice(i.to_string().as_bytes()); out.extend_from_slice(b"\r\n"); }
        RespValue::BulkString(None) => { out.extend_from_slice(b"$-1\r\n"); }
        RespValue::BulkString(Some(s)) => {
            out.push(b'$'); out.extend_from_slice(s.len().to_string().as_bytes());
            out.extend_from_slice(b"\r\n"); out.extend_from_slice(s); out.extend_from_slice(b"\r\n");
        }
        RespValue::Array(None) => { out.extend_from_slice(b"*-1\r\n"); }
        RespValue::Array(Some(items)) => {
            out.push(b'*'); out.extend_from_slice(items.len().to_string().as_bytes()); out.extend_from_slice(b"\r\n");
            for item in items { serialize_value(item, out); }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resp::parser;

    #[test]
    fn test_serialize_simple_string() {
        let bytes = serialize_response(&RespValue::SimpleString("OK".into()));
        assert_eq!(bytes, b"+OK\r\n");
    }

    #[test]
    fn test_serialize_error() {
        let bytes = serialize_response(&RespValue::Error("ERR msg".into()));
        assert_eq!(bytes, b"-ERR msg\r\n");
    }

    #[test]
    fn test_serialize_integer() {
        let bytes = serialize_response(&RespValue::Integer(42));
        assert_eq!(bytes, b":42\r\n");
    }

    #[test]
    fn test_serialize_bulk_string() {
        let bytes = serialize_response(&RespValue::BulkString(Some(b"hello".to_vec())));
        assert_eq!(bytes, b"$5\r\nhello\r\n");
    }

    #[test]
    fn test_serialize_null() {
        let bytes = serialize_response(&RespValue::BulkString(None));
        assert_eq!(bytes, b"$-1\r\n");
    }

    #[test]
    fn test_serialize_array() {
        let val = RespValue::Array(Some(vec![
            RespValue::Integer(1),
            RespValue::BulkString(Some(b"foo".to_vec())),
        ]));
        let bytes = serialize_response(&val);
        assert_eq!(bytes, b"*2\r\n:1\r\n$3\r\nfoo\r\n");
    }

    #[test]
    fn test_serialize_null_array() {
        let bytes = serialize_response(&RespValue::Array(None));
        assert_eq!(bytes, b"*-1\r\n");
    }

    #[test]
    fn test_roundtrip() {
        let original = RespValue::Array(Some(vec![
            RespValue::SimpleString("OK".into()),
            RespValue::Integer(42),
            RespValue::BulkString(Some(b"data".to_vec())),
        ]));
        let bytes = serialize_response(&original);
        let (_, parsed) = parser::resp_value(&bytes).unwrap();
        assert_eq!(parsed, original);
    }
}
