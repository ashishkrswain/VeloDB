// Copyright (c) 2025-Present VeloDB Contributors
// SPDX-License-Identifier: MIT

use std::collections::{VecDeque, HashSet, HashMap, BTreeMap};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use crate::store::{Store, StreamEntry};
use crate::config::ServerConfig;

use crate::store::memory::{StorageValue, OrderedF64};

const TYPE_STRING: u8 = 0x00;
const TYPE_LIST: u8 = 0x01;
const TYPE_SET: u8 = 0x02;
const TYPE_HASH: u8 = 0x03;
const TYPE_ZSET: u8 = 0x04;
const TYPE_STREAM: u8 = 0x05;
const TYPE_NH: u8 = 0x06;

const DB_SELECT: u8 = 0xFE;
const EXPIRETIME: u8 = 0xFD;
const DB_END: u8 = 0xFF;

fn write_u32_be(buf: &mut Vec<u8>, v: u32) { buf.extend_from_slice(&v.to_be_bytes()); }
fn write_u64_le(buf: &mut Vec<u8>, v: u64) { buf.extend_from_slice(&v.to_le_bytes()); }
fn write_f64_le(buf: &mut Vec<u8>, v: f64) { buf.extend_from_slice(&v.to_le_bytes()); }
fn write_bytes_len(buf: &mut Vec<u8>, data: &[u8]) { write_u32_be(buf, data.len() as u32); buf.extend_from_slice(data); }

fn read_u32_be(data: &[u8], pos: &mut usize) -> u32 {
    let bytes: [u8; 4] = data[*pos..*pos+4].try_into().unwrap();
    *pos += 4;
    u32::from_be_bytes(bytes)
}
fn read_u64_le(data: &[u8], pos: &mut usize) -> u64 {
    let bytes: [u8; 8] = data[*pos..*pos+8].try_into().unwrap();
    *pos += 8;
    u64::from_le_bytes(bytes)
}
fn read_f64_le(data: &[u8], pos: &mut usize) -> f64 {
    let bytes: [u8; 8] = data[*pos..*pos+8].try_into().unwrap();
    *pos += 8;
    f64::from_le_bytes(bytes)
}
fn read_bytes_len(data: &[u8], pos: &mut usize) -> Vec<u8> {
    let len = read_u32_be(data, pos) as usize;
    let bytes = data[*pos..*pos+len].to_vec();
    *pos += len;
    bytes
}

fn crc64(data: &[u8]) -> u64 {
    let mut crc: u64 = 0;
    for &byte in data {
        crc = crc.wrapping_mul(0x1000000000000001).wrapping_add(byte as u64);
    }
    crc
}

pub fn save_rdb(store: &Store, path: &Path, num_dbs: usize) -> std::io::Result<()> {
    let mut buf = Vec::new();
    buf.extend_from_slice(b"VELO");
    buf.extend_from_slice(&[0, 1, 0, 0]);

    let crc_offset = buf.len();
    buf.extend_from_slice(&[0u8; 8]);

    for db_idx in 0..num_dbs {
        buf.push(DB_SELECT);
        buf.push(db_idx as u8);

        let entries = store.iterate_db(db_idx).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("{}", e)))?;
        for (key, entry) in entries {
            if let Some(at) = entry.expire_at {
                buf.push(EXPIRETIME);
                write_u64_le(&mut buf, at);
            }
            encode_entry(&key, &entry.value, &mut buf);
        }
        buf.push(DB_END);
    }

    let crc = crc64(&buf[crc_offset + 8..]);
    buf[crc_offset..crc_offset + 8].copy_from_slice(&crc.to_le_bytes());

    fs::write(path, &buf)
}

fn encode_entry(key: &[u8], value: &StorageValue, buf: &mut Vec<u8>) {
    match value {
        StorageValue::String(val) => {
            buf.push(TYPE_STRING);
            write_bytes_len(buf, key);
            write_bytes_len(buf, val);
        }
        StorageValue::List(list) => {
            buf.push(TYPE_LIST);
            write_bytes_len(buf, key);
            write_u32_be(buf, list.len() as u32);
            for item in list.iter() { write_bytes_len(buf, item); }
        }
        StorageValue::Set(set) => {
            buf.push(TYPE_SET);
            write_bytes_len(buf, key);
            write_u32_be(buf, set.len() as u32);
            for member in set.iter() { write_bytes_len(buf, member); }
        }
        StorageValue::Hash(map) => {
            buf.push(TYPE_HASH);
            write_bytes_len(buf, key);
            write_u32_be(buf, map.len() as u32);
            for (f, v) in map.iter() { write_bytes_len(buf, f); write_bytes_len(buf, v); }
        }
        StorageValue::ZSet { members, .. } => {
            buf.push(TYPE_ZSET);
            write_bytes_len(buf, key);
            write_u32_be(buf, members.len() as u32);
            let mut sorted: Vec<(&Vec<u8>, f64)> = members.iter().map(|(k, v)| (k, *v)).collect();
            sorted.sort_by(|(_, s1), (_, s2)| s1.partial_cmp(s2).unwrap_or(std::cmp::Ordering::Equal));
            for (member, score) in sorted {
                write_f64_le(buf, score);
                write_bytes_len(buf, member);
            }
        }
        StorageValue::Stream { entries, last_id_ms, last_id_seq } => {
            buf.push(TYPE_STREAM);
            write_bytes_len(buf, key);
            write_u32_be(buf, entries.len() as u32);
            write_u64_le(buf, *last_id_ms);
            write_u64_le(buf, *last_id_seq);
            for entry in entries.iter() {
                write_u64_le(buf, entry.id_ms);
                write_u64_le(buf, entry.id_seq);
                write_u32_be(buf, entry.fields.len() as u32);
                for (f, v) in entry.fields.iter() { write_bytes_len(buf, f); write_bytes_len(buf, v); }
            }
        }
        StorageValue::NestedHash(map) => {
            buf.push(TYPE_NH);
            write_bytes_len(buf, key);
            write_u32_be(buf, map.len() as u32);
            for (field, inner) in map.iter() {
                write_bytes_len(buf, field);
                let sf_count: u32 = inner.len() as u32;
                write_u32_be(buf, sf_count);
                for (sf, val) in inner.iter() { write_bytes_len(buf, sf); write_bytes_len(buf, val); }
            }
        }
    }
}

pub fn load_rdb(store: &Store, path: &Path) -> std::io::Result<usize> {
    let data = fs::read(path)?;
    if data.len() < 16 { return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "RDB file too short")); }

    let magic = &data[0..4];
    if magic != b"VELO" { return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid RDB magic")); }

    let version = &data[4..8];
    tracing::info!("Loading RDB version {}.{}.{}.{}", version[0], version[1], version[2], version[3]);

    let stored_crc = u64::from_le_bytes(data[8..16].try_into().unwrap());
    let computed_crc = crc64(&data[16..]);
    if stored_crc != 0 && stored_crc != computed_crc {
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "RDB CRC mismatch"));
    }

    let mut pos = 16;
    let mut count = 0;
    let mut current_db: usize = 0;

    while pos < data.len() {
        match data[pos] {
            DB_SELECT => {
                pos += 1;
                current_db = data[pos] as usize;
                pos += 1;
            }
            DB_END => { pos += 1; }
            EXPIRETIME => {
                pos += 1;
                let expire_at = read_u64_le(&data, &mut pos);
                let (key_bytes, entry) = decode_entry(&data, &mut pos)?;
                if !key_bytes.is_empty() {
                    store.set_with_entry(current_db, key_bytes, entry, Some(expire_at)).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("{}", e)))?;
                    count += 1;
                }
            }
            TYPE_STRING | TYPE_LIST | TYPE_SET | TYPE_HASH | TYPE_ZSET | TYPE_STREAM | TYPE_NH => {
                let (key_bytes, entry) = decode_entry(&data, &mut pos)?;
                if !key_bytes.is_empty() {
                    store.set_with_entry(current_db, key_bytes, entry, None).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("{}", e)))?;
                    count += 1;
                }
            }
            _ => { return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, format!("Unknown RDB opcode: {}", data[pos]))); }
        }
    }
    Ok(count)
}

fn decode_entry(data: &[u8], pos: &mut usize) -> std::io::Result<(Vec<u8>, StorageValue)> {
    if *pos >= data.len() { return Ok((vec![], StorageValue::String(vec![]))); }
    let type_byte = data[*pos];
    *pos += 1;

    let key = read_bytes_len(data, pos);

    let value = match type_byte {
        TYPE_STRING => StorageValue::String(read_bytes_len(data, pos)),
        TYPE_LIST => {
            let len = read_u32_be(data, pos) as usize;
            let mut list = VecDeque::new();
            for _ in 0..len { list.push_back(read_bytes_len(data, pos)); }
            StorageValue::List(list)
        }
        TYPE_SET => {
            let len = read_u32_be(data, pos) as usize;
            let mut set = HashSet::new();
            for _ in 0..len { set.insert(read_bytes_len(data, pos)); }
            StorageValue::Set(set)
        }
        TYPE_HASH => {
            let len = read_u32_be(data, pos) as usize;
            let mut map = HashMap::new();
            for _ in 0..len {
                let f = read_bytes_len(data, pos);
                let v = read_bytes_len(data, pos);
                map.insert(f, v);
            }
            StorageValue::Hash(map)
        }
        TYPE_ZSET => {
            let len = read_u32_be(data, pos) as usize;
            let mut members = HashMap::new();
            let mut scores = BTreeMap::new();
            for _ in 0..len {
                let score = read_f64_le(data, pos);
                let member = read_bytes_len(data, pos);
                members.insert(member.clone(), score);
                scores.entry(OrderedF64(score)).or_insert_with(HashSet::new).insert(member);
            }
            StorageValue::ZSet { members, scores }
        }
        TYPE_STREAM => {
            let len = read_u32_be(data, pos) as usize;
            let last_id_ms = read_u64_le(data, pos);
            let last_id_seq = read_u64_le(data, pos);
            let mut entries = VecDeque::new();
            for _ in 0..len {
                let id_ms = read_u64_le(data, pos);
                let id_seq = read_u64_le(data, pos);
                let f_len = read_u32_be(data, pos) as usize;
                let mut fields = Vec::new();
                for _ in 0..f_len {
                    let f = read_bytes_len(data, pos);
                    let v = read_bytes_len(data, pos);
                    fields.push((f, v));
                }
                entries.push_back(StreamEntry { id_ms, id_seq, fields });
            }
            StorageValue::Stream { entries, last_id_ms, last_id_seq }
        }
        TYPE_NH => {
            let len = read_u32_be(data, pos) as usize;
            let mut map = HashMap::new();
            for _ in 0..len {
                let field = read_bytes_len(data, pos);
                let sf_len = read_u32_be(data, pos) as usize;
                let mut inner = HashMap::new();
                for _ in 0..sf_len {
                    let sf = read_bytes_len(data, pos);
                    let val = read_bytes_len(data, pos);
                    inner.insert(sf, val);
                }
                map.insert(field, inner);
            }
            StorageValue::NestedHash(map)
        }
        _ => return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, format!("Unknown type byte: {}", type_byte))),
    };

    Ok((key, value))
}

pub async fn bgsave(store: Arc<Store>, config: &ServerConfig) -> std::io::Result<()> {
    let dir = config.dir.clone();
    let filename = config.dbfilename.clone();
    let num_dbs = store.databases.len();
    let temp_path = PathBuf::from(format!("{}/temp-{}-{}.rdb", dir, std::process::id(), crate::persist::unique_temp_id()));
    let final_path = PathBuf::from(format!("{}/{}", dir, filename));

    tokio::task::spawn_blocking(move || {
        save_rdb(&store, &temp_path, num_dbs)?;
        fs::rename(&temp_path, &final_path)
    }).await.unwrap()?;
    Ok(())
}
