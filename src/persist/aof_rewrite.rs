// Copyright (c) 2025-Present VeloDB Contributors
// SPDX-License-Identifier: MIT

//! Serializes the live dataset as a minimal RESP command stream for
//! AOF rewrite: one constructing command per key plus PEXPIREAT for TTLs.

use std::io::Write;
use crate::store::Store;
use super::aof::encode_command_for_aof;

pub(crate) fn write_dataset(out: &mut impl Write, store: &Store) -> std::io::Result<()> {
    for db_idx in 0..store.databases.len() {
        let entries = match store.iterate_db(db_idx) {
            Ok(e) => e,
            Err(_) => continue,
        };
        if entries.is_empty() { continue; }
        if store.databases.len() > 1 {
            out.write_all(&encode_command_for_aof(&[b"SELECT".to_vec(), db_idx.to_string().into_bytes()]))?;
        }
        for (key, entry) in entries {
            for cmd in commands_for_value(&key, &entry.value) {
                out.write_all(&encode_command_for_aof(&cmd))?;
            }
            if let Some(at_ms) = entry.expire_at {
                out.write_all(&encode_command_for_aof(&[
                    b"PEXPIREAT".to_vec(), key.clone(), at_ms.to_string().into_bytes(),
                ]))?;
            }
        }
    }
    Ok(())
}

fn commands_for_value(key: &[u8], value: &crate::store::memory::StorageValue) -> Vec<Vec<Vec<u8>>> {
    use crate::store::memory::StorageValue;
    match value {
        StorageValue::String(v) => vec![vec![b"SET".to_vec(), key.to_vec(), v.clone()]],
        StorageValue::List(l) => {
            if l.is_empty() { return vec![]; }
            let mut cmd = vec![b"RPUSH".to_vec(), key.to_vec()];
            cmd.extend(l.iter().cloned());
            vec![cmd]
        }
        StorageValue::Set(s) => {
            if s.is_empty() { return vec![]; }
            let mut cmd = vec![b"SADD".to_vec(), key.to_vec()];
            cmd.extend(s.iter().cloned());
            vec![cmd]
        }
        StorageValue::Hash(h) => {
            if h.is_empty() { return vec![]; }
            let mut cmd = vec![b"HSET".to_vec(), key.to_vec()];
            for (f, v) in h { cmd.push(f.clone()); cmd.push(v.clone()); }
            vec![cmd]
        }
        StorageValue::ZSet { members, .. } => {
            if members.is_empty() { return vec![]; }
            let mut cmd = vec![b"ZADD".to_vec(), key.to_vec()];
            for (m, score) in members {
                cmd.push(score.to_string().into_bytes());
                cmd.push(m.clone());
            }
            vec![cmd]
        }
        StorageValue::Stream { entries, .. } => {
            entries.iter().map(|e| {
                let mut cmd = vec![
                    b"XADD".to_vec(), key.to_vec(),
                    format!("{}-{}", e.id_ms, e.id_seq).into_bytes(),
                ];
                for (f, v) in &e.fields { cmd.push(f.clone()); cmd.push(v.clone()); }
                cmd
            }).collect()
        }
        StorageValue::NestedHash(nh) => {
            let mut cmds = vec![];
            for (field, inner) in nh {
                for (sub, v) in inner {
                    cmds.push(vec![b"NHSET".to_vec(), key.to_vec(), field.clone(), sub.clone(), v.clone()]);
                }
            }
            cmds
        }
    }
}
