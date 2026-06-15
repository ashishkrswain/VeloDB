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

fn resp_value(input: &[u8]) -> IResult<&[u8], RespValue> {
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
