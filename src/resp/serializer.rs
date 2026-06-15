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
