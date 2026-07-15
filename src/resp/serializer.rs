// Copyright (c) 2025-Present VeloDB Contributors
// SPDX-License-Identifier: MIT

use super::RespValue;

/// Serializes using RESP2 — the default wire format, and what every
/// command handler emits before RESP3 (HELLO 3) is negotiated.
pub fn serialize_response(value: &RespValue) -> Vec<u8> {
    let mut out = Vec::with_capacity(64);
    serialize_value(value, &mut out, 2);
    out
}

/// Serializes for the negotiated protocol version (2 or 3). RESP3-only
/// types (Map/Double/Boolean/Null) degrade to their RESP2 equivalents
/// when `protocol` is 2, since RESP2 clients don't understand `%`/`,`/`#`/`_`.
pub fn serialize_response_proto(value: &RespValue, protocol: u8) -> Vec<u8> {
    let mut out = Vec::with_capacity(64);
    serialize_value(value, &mut out, protocol);
    out
}

fn serialize_value(value: &RespValue, out: &mut Vec<u8>, protocol: u8) {
    match value {
        RespValue::SimpleString(s) => { out.push(b'+'); out.extend_from_slice(s.as_bytes()); out.extend_from_slice(b"\r\n"); }
        RespValue::Error(s) => { out.push(b'-'); out.extend_from_slice(s.as_bytes()); out.extend_from_slice(b"\r\n"); }
        RespValue::Integer(i) => { out.push(b':'); out.extend_from_slice(i.to_string().as_bytes()); out.extend_from_slice(b"\r\n"); }
        RespValue::BulkString(None) => {
            if protocol >= 3 { out.extend_from_slice(b"_\r\n"); } else { out.extend_from_slice(b"$-1\r\n"); }
        }
        RespValue::BulkString(Some(s)) => {
            out.push(b'$'); out.extend_from_slice(s.len().to_string().as_bytes());
            out.extend_from_slice(b"\r\n"); out.extend_from_slice(s); out.extend_from_slice(b"\r\n");
        }
        RespValue::Array(None) => {
            if protocol >= 3 { out.extend_from_slice(b"_\r\n"); } else { out.extend_from_slice(b"*-1\r\n"); }
        }
        RespValue::Array(Some(items)) => {
            out.push(b'*'); out.extend_from_slice(items.len().to_string().as_bytes()); out.extend_from_slice(b"\r\n");
            for item in items { serialize_value(item, out, protocol); }
        }
        RespValue::Map(pairs) => {
            if protocol >= 3 {
                out.push(b'%'); out.extend_from_slice(pairs.len().to_string().as_bytes()); out.extend_from_slice(b"\r\n");
                for (k, v) in pairs { serialize_value(k, out, protocol); serialize_value(v, out, protocol); }
            } else {
                out.push(b'*'); out.extend_from_slice((pairs.len() * 2).to_string().as_bytes()); out.extend_from_slice(b"\r\n");
                for (k, v) in pairs { serialize_value(k, out, protocol); serialize_value(v, out, protocol); }
            }
        }
        RespValue::Double(d) => {
            if protocol >= 3 {
                out.push(b','); out.extend_from_slice(format_double(*d).as_bytes()); out.extend_from_slice(b"\r\n");
            } else {
                let s = format_double(*d);
                out.push(b'$'); out.extend_from_slice(s.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n"); out.extend_from_slice(s.as_bytes()); out.extend_from_slice(b"\r\n");
            }
        }
        RespValue::Boolean(b) => {
            if protocol >= 3 {
                out.push(b'#'); out.push(if *b { b't' } else { b'f' }); out.extend_from_slice(b"\r\n");
            } else {
                out.push(b':'); out.push(if *b { b'1' } else { b'0' }); out.extend_from_slice(b"\r\n");
            }
        }
        RespValue::Null => {
            if protocol >= 3 { out.extend_from_slice(b"_\r\n"); } else { out.extend_from_slice(b"$-1\r\n"); }
        }
    }
}

fn format_double(d: f64) -> String {
    if d.is_infinite() { return if d > 0.0 { "inf".to_string() } else { "-inf".to_string() }; }
    if d.is_nan() { return "nan".to_string(); }
    if d == d.trunc() && d.abs() < 1e17 { format!("{}", d as i64) } else { format!("{}", d) }
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

    // ========= RESP3 =========
    #[test]
    fn test_resp3_null_uses_underscore() {
        assert_eq!(serialize_response_proto(&RespValue::BulkString(None), 3), b"_\r\n");
        assert_eq!(serialize_response_proto(&RespValue::Array(None), 3), b"_\r\n");
        assert_eq!(serialize_response_proto(&RespValue::Null, 3), b"_\r\n");
    }

    #[test]
    fn test_resp2_null_stays_dollar_minus_one() {
        assert_eq!(serialize_response_proto(&RespValue::BulkString(None), 2), b"$-1\r\n");
        assert_eq!(serialize_response_proto(&RespValue::Null, 2), b"$-1\r\n");
    }

    #[test]
    fn test_resp3_map_uses_percent() {
        let val = RespValue::Map(vec![(RespValue::bulk_string(b"a".to_vec()), RespValue::Integer(1))]);
        assert_eq!(serialize_response_proto(&val, 3), b"%1\r\n$1\r\na\r\n:1\r\n");
    }

    #[test]
    fn test_resp2_map_flattens_to_array() {
        let val = RespValue::Map(vec![(RespValue::bulk_string(b"a".to_vec()), RespValue::Integer(1))]);
        assert_eq!(serialize_response_proto(&val, 2), b"*2\r\n$1\r\na\r\n:1\r\n");
    }

    #[test]
    fn test_resp3_double_uses_comma() {
        assert_eq!(serialize_response_proto(&RespValue::Double(3.5), 3), b",3.5\r\n");
        assert_eq!(serialize_response_proto(&RespValue::Double(3.0), 3), b",3\r\n");
    }

    #[test]
    fn test_resp2_double_becomes_bulk_string() {
        assert_eq!(serialize_response_proto(&RespValue::Double(3.5), 2), b"$3\r\n3.5\r\n");
    }

    #[test]
    fn test_resp3_boolean_uses_hash() {
        assert_eq!(serialize_response_proto(&RespValue::Boolean(true), 3), b"#t\r\n");
        assert_eq!(serialize_response_proto(&RespValue::Boolean(false), 3), b"#f\r\n");
    }

    #[test]
    fn test_resp2_boolean_becomes_integer() {
        assert_eq!(serialize_response_proto(&RespValue::Boolean(true), 2), b":1\r\n");
        assert_eq!(serialize_response_proto(&RespValue::Boolean(false), 2), b":0\r\n");
    }

    #[test]
    fn test_default_serialize_response_is_resp2() {
        // serialize_response (no explicit protocol) must match RESP2 output.
        assert_eq!(serialize_response(&RespValue::BulkString(None)), serialize_response_proto(&RespValue::BulkString(None), 2));
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
