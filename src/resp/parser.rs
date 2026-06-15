// Copyright (c) 2025-Present VeloDB Contributors
// SPDX-License-Identifier: MIT

use nom::IResult;
use nom::branch::alt;
use nom::bytes::complete::{tag, take, take_while_m_n};
use nom::character::complete::{crlf, i64 as nom_i64};
use nom::multi::many_m_n;
use nom::sequence::terminated;
use nom::Parser;
use super::RespValue;

fn simple_string(input: &[u8]) -> IResult<&[u8], RespValue> {
    let (input, _) = tag("+").parse(input)?;
    let (input, bytes) = take_while_m_n(0, usize::MAX, |b| b != b'\r').parse(input)?;
    let (input, _) = crlf.parse(input)?;
    Ok((input, RespValue::SimpleString(String::from_utf8_lossy(bytes).into())))
}

fn parse_error(input: &[u8]) -> IResult<&[u8], RespValue> {
    let (input, _) = tag("-").parse(input)?;
    let (input, bytes) = take_while_m_n(0, usize::MAX, |b| b != b'\r').parse(input)?;
    let (input, _) = crlf.parse(input)?;
    Ok((input, RespValue::Error(String::from_utf8_lossy(bytes).into())))
}

fn integer(input: &[u8]) -> IResult<&[u8], RespValue> {
    let (input, _) = tag(":").parse(input)?;
    let (input, num) = nom_i64.parse(input)?;
    let (input, _) = crlf.parse(input)?;
    Ok((input, RespValue::Integer(num)))
}

fn bulk_string(input: &[u8]) -> IResult<&[u8], RespValue> {
    let (input, _) = tag("$").parse(input)?;
    let (input, len) = terminated(nom_i64, crlf).parse(input)?;
    if len < 0 { return Ok((input, RespValue::BulkString(None))); }
    let (input, bytes) = take(len as usize).parse(input)?;
    let (input, _) = crlf.parse(input)?;
    Ok((input, RespValue::BulkString(Some(bytes.to_vec()))))
}

fn array(input: &[u8]) -> IResult<&[u8], RespValue> {
    let (input, _) = tag("*").parse(input)?;
    let (input, len) = terminated(nom_i64, crlf).parse(input)?;
    if len < 0 { return Ok((input, RespValue::Array(None))); }
    let (input, items) = many_m_n(len as usize, len as usize, resp_value).parse(input)?;
    Ok((input, RespValue::Array(Some(items))))
}

pub(crate) fn resp_value(input: &[u8]) -> IResult<&[u8], RespValue> {
    alt((simple_string, parse_error, integer, bulk_string, array)).parse(input)
}

pub fn parse_command(input: &[u8]) -> IResult<&[u8], Vec<Vec<u8>>> {
    let (remaining, value) = resp_value(input)?;
    match value {
        RespValue::Array(Some(items)) => {
            let args: Vec<Vec<u8>> = items.into_iter()
                .filter_map(|v| v.as_bulk_string_owned())
                .collect();
            Ok((remaining, args))
        }
        _ => Ok((remaining, vec![])),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_string() {
        let (rem, val) = simple_string(b"+OK\r\n").unwrap();
        assert!(rem.is_empty());
        assert_eq!(val, RespValue::SimpleString("OK".into()));
    }

    #[test]
    fn test_parse_error() {
        let (rem, val) = parse_error(b"-ERR msg\r\n").unwrap();
        assert!(rem.is_empty());
        assert_eq!(val, RespValue::Error("ERR msg".into()));
    }

    #[test]
    fn test_parse_integer() {
        let (rem, val) = integer(b":42\r\n").unwrap();
        assert!(rem.is_empty());
        assert_eq!(val, RespValue::Integer(42));
    }

    #[test]
    fn test_parse_integer_negative() {
        let (rem, val) = integer(b":-1\r\n").unwrap();
        assert!(rem.is_empty());
        assert_eq!(val, RespValue::Integer(-1));
    }

    #[test]
    fn test_parse_bulk_string() {
        let (rem, val) = bulk_string(b"$5\r\nhello\r\n").unwrap();
        assert!(rem.is_empty());
        assert_eq!(val, RespValue::BulkString(Some(b"hello".to_vec())));
    }

    #[test]
    fn test_parse_bulk_string_empty() {
        let (rem, val) = bulk_string(b"$0\r\n\r\n").unwrap();
        assert!(rem.is_empty());
        assert_eq!(val, RespValue::BulkString(Some(vec![])));
    }

    #[test]
    fn test_parse_null_bulk_string() {
        let (rem, val) = bulk_string(b"$-1\r\n").unwrap();
        assert!(rem.is_empty());
        assert_eq!(val, RespValue::BulkString(None));
    }

    #[test]
    fn test_parse_array() {
        let (rem, val) = array(b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n").unwrap();
        assert!(rem.is_empty());
        assert_eq!(val, RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"foo".to_vec())),
            RespValue::BulkString(Some(b"bar".to_vec())),
        ])));
    }

    #[test]
    fn test_parse_empty_array() {
        let (rem, val) = array(b"*0\r\n").unwrap();
        assert!(rem.is_empty());
        assert_eq!(val, RespValue::Array(Some(vec![])));
    }

    #[test]
    fn test_parse_null_array() {
        let (rem, val) = array(b"*-1\r\n").unwrap();
        assert!(rem.is_empty());
        assert_eq!(val, RespValue::Array(None));
    }

    #[test]
    fn test_parse_nested_array() {
        let input = b"*2\r\n*1\r\n$1\r\na\r\n*1\r\n$1\r\nb\r\n";
        let (rem, val) = array(input).unwrap();
        assert!(rem.is_empty());
        match val {
            RespValue::Array(Some(items)) => {
                assert_eq!(items.len(), 2);
                assert!(matches!(&items[0], RespValue::Array(Some(_))));
            }
            _ => panic!("expected nested array"),
        }
    }

    #[test]
    fn test_parse_command() {
        let input = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
        let (rem, args) = parse_command(input).unwrap();
        assert!(rem.is_empty());
        assert_eq!(args, vec![b"GET".to_vec(), b"key".to_vec()]);
    }

    #[test]
    fn test_parse_incomplete() {
        // Partial data without CRLF returns Error (not Incomplete)
        let result = resp_value(b"+OK\r");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_incomplete_bulk() {
        // Partial bulk — not enough bytes for the body
        let result = bulk_string(b"$5\r\nhel");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_malformed() {
        let result = resp_value(b"garbage\r\n");
        assert!(result.is_err());
    }
}
