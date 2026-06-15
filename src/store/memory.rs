// Copyright (c) 2025-Present VeloDB Contributors
// SPDX-License-Identifier: MIT

use dashmap::DashMap;
use std::collections::{VecDeque, HashSet, HashMap, BTreeMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Notify;
use crate::error::{Result, VeloDBError};

#[derive(Clone)]
pub struct StreamEntry {
    pub id_ms: u64,
    pub id_seq: u64,
    pub fields: Vec<(Vec<u8>, Vec<u8>)>,
}

#[derive(Clone, PartialEq, PartialOrd)]
struct OrderedF64(f64);

impl Eq for OrderedF64 {}
impl Ord for OrderedF64 {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap_or(std::cmp::Ordering::Equal)
    }
}

#[derive(Clone)]
enum StorageValue {
    String(Vec<u8>),
    List(VecDeque<Vec<u8>>),
    Set(HashSet<Vec<u8>>),
    Hash(HashMap<Vec<u8>, Vec<u8>>),
    ZSet {
        members: HashMap<Vec<u8>, f64>,
        scores: BTreeMap<OrderedF64, HashSet<Vec<u8>>>,
    },
    Stream {
        entries: VecDeque<StreamEntry>,
        last_id_ms: u64,
        last_id_seq: u64,
    },
    NestedHash(HashMap<Vec<u8>, HashMap<Vec<u8>, Vec<u8>>>),
}

impl StorageValue {
    fn type_name(&self) -> &'static str {
        match self {
            StorageValue::String(_) => "string",
            StorageValue::List(_) => "list",
            StorageValue::Set(_) => "set",
            StorageValue::Hash(_) => "hash",
            StorageValue::ZSet { .. } => "zset",
            StorageValue::Stream { .. } => "stream",
            StorageValue::NestedHash(_) => "nh",
        }
    }
}

#[derive(Clone)]
struct Entry {
    value: StorageValue,
    expire_at: Option<u64>,
}

pub struct BlockRegistry {
    waiters: DashMap<Vec<u8>, Vec<(u64, Arc<Notify>)>>,
    next_id: AtomicU64,
}

impl BlockRegistry {
    fn new() -> Self {
        Self { waiters: DashMap::new(), next_id: AtomicU64::new(1) }
    }

    pub fn register(&self, keys: &[Vec<u8>], notify: Arc<Notify>) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        for key in keys {
            self.waiters.entry(key.clone()).or_default().push((id, notify.clone()));
        }
        id
    }

    pub fn unregister(&self, id: u64, keys: &[Vec<u8>]) {
        for key in keys {
            if let Some(mut waiters) = self.waiters.get_mut(key) {
                waiters.retain(|(wid, _)| *wid != id);
            }
        }
    }

    pub fn notify(&self, key: &[u8]) {
        if let Some(mut waiters) = self.waiters.get_mut(key) {
            let to_wake: Vec<Arc<Notify>> = waiters.iter().map(|(_, n)| n.clone()).collect();
            waiters.clear();
            drop(waiters);
            for n in to_wake {
                n.notify_waiters();
            }
        }
    }
}

#[allow(private_interfaces)]
pub struct Store {
    pub databases: Vec<DashMap<Vec<u8>, Entry>>,
    pub block_registry: BlockRegistry,
}

impl Store {
    pub fn new(num: usize) -> Self {
        Self { databases: (0..num).map(|_| DashMap::new()).collect(), block_registry: BlockRegistry::new() }
    }

    fn db(&self, idx: usize) -> Result<&DashMap<Vec<u8>, Entry>> {
        self.databases.get(idx).ok_or_else(|| VeloDBError::internal(format!("db {} out of range", idx)))
    }

    fn expired(entry: &Entry) -> bool {
        entry.expire_at.map_or(false, |at| at <= SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64)
    }

    fn entry(&self, db_idx: usize, key: &[u8]) -> Result<Option<dashmap::mapref::one::Ref<'_, Vec<u8>, Entry>>> {
        match self.db(db_idx)?.get(key) {
            Some(e) if !Self::expired(&e) => Ok(Some(e)),
            _ => Ok(None),
        }
    }

    fn entry_mut(&self, db_idx: usize, key: &[u8]) -> Result<Option<dashmap::mapref::one::RefMut<'_, Vec<u8>, Entry>>> {
        match self.db(db_idx)?.get_mut(key) {
            Some(e) if !Self::expired(&e) => Ok(Some(e)),
            _ => Ok(None),
        }
    }

    // ========= existing generic methods =========

    pub fn get(&self, db_idx: usize, key: &[u8]) -> Result<Option<Vec<u8>>> {
        match self.entry(db_idx, key)? {
            Some(e) => match &e.value {
                StorageValue::String(v) => Ok(Some(v.clone())),
                _ => Err(VeloDBError::wrong_type()),
            },
            None => Ok(None),
        }
    }

    pub fn set(&self, db_idx: usize, key: &[u8], value: &[u8], expire_ms: Option<u64>) -> Result<()> {
        self.db(db_idx)?.insert(key.to_vec(), Entry { value: StorageValue::String(value.to_vec()), expire_at: expire_ms });
        Ok(())
    }

    pub fn del(&self, db_idx: usize, key: &[u8]) -> Result<bool> {
        Ok(self.db(db_idx)?.remove(key).is_some())
    }

    pub fn exists(&self, db_idx: usize, key: &[u8]) -> Result<bool> {
        self.entry(db_idx, key).map(|e| e.is_some())
    }

    pub fn set_expire(&self, db_idx: usize, key: &[u8], at_ms: u64) -> Result<bool> {
        match self.db(db_idx)?.get_mut(key) {
            Some(mut e) => { e.expire_at = Some(at_ms); Ok(true) }
            None => Ok(false),
        }
    }

    pub fn get_expire(&self, db_idx: usize, key: &[u8]) -> Result<Option<u64>> {
        match self.entry(db_idx, key)? {
            Some(e) => Ok(e.expire_at),
            None => Ok(None),
        }
    }

    pub fn remove_expire(&self, db_idx: usize, key: &[u8]) -> Result<bool> {
        match self.db(db_idx)?.get_mut(key) {
            Some(mut e) => { e.expire_at = None; Ok(true) }
            None => Ok(false),
        }
    }

    pub fn get_type(&self, db_idx: usize, key: &[u8]) -> Result<Option<String>> {
        match self.entry(db_idx, key)? {
            Some(e) => Ok(Some(e.value.type_name().to_string())),
            None => Ok(None),
        }
    }

    pub fn rename(&self, db_idx: usize, old: &[u8], new: &[u8]) -> Result<()> {
        let (_, entry) = self.db(db_idx)?.remove(old).ok_or_else(|| VeloDBError::key_not_found())?;
        self.db(db_idx)?.insert(new.to_vec(), entry);
        Ok(())
    }

    pub fn keys(&self, db_idx: usize, pattern: &str) -> Result<Vec<Vec<u8>>> {
        let db = self.db(db_idx)?;
        Ok(db.iter().filter(|e| !Self::expired(&e) && simple_match(&String::from_utf8_lossy(&e.key()), pattern)).map(|e| e.key().clone()).collect())
    }

    pub fn dbsize(&self, db_idx: usize) -> Result<usize> {
        let db = self.db(db_idx)?;
        let mut count = 0;
        let mut to_remove = Vec::new();
        for e in db.iter() { if Self::expired(&e) { to_remove.push(e.key().clone()); } else { count += 1; } }
        for k in to_remove { self.db(db_idx)?.remove(&k); }
        Ok(count)
    }

    pub fn flushdb(&self, db_idx: usize) -> Result<()> { self.db(db_idx)?.clear(); Ok(()) }
    pub fn flushall(&self) -> Result<()> { for db in &self.databases { db.clear(); } Ok(()) }

    pub fn random_key(&self, db_idx: usize) -> Result<Option<Vec<u8>>> {
        let db = self.db(db_idx)?;
        let keys: Vec<Vec<u8>> = db.iter().filter(|e| !Self::expired(&e)).map(|e| e.key().clone()).collect();
        if keys.is_empty() { return Ok(None); }
        let idx = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().subsec_nanos() as usize % keys.len();
        Ok(Some(keys[idx].clone()))
    }

    // ========= List methods =========

    pub fn lpush(&self, db_idx: usize, key: &[u8], values: &[Vec<u8>]) -> Result<usize> {
        let db = self.db(db_idx)?;
        let mut entry = db.entry(key.to_vec()).or_insert_with(|| Entry { value: StorageValue::List(VecDeque::new()), expire_at: None });
        match &mut entry.value {
            StorageValue::List(list) => {
                for v in values.iter().rev() { list.push_front(v.clone()); }
                let len = list.len();
                drop(entry);
                self.block_registry.notify(key);
                Ok(len)
            }
            _ => Err(VeloDBError::wrong_type()),
        }
    }

    pub fn rpush(&self, db_idx: usize, key: &[u8], values: &[Vec<u8>]) -> Result<usize> {
        let db = self.db(db_idx)?;
        let mut entry = db.entry(key.to_vec()).or_insert_with(|| Entry { value: StorageValue::List(VecDeque::new()), expire_at: None });
        match &mut entry.value {
            StorageValue::List(list) => {
                for v in values { list.push_back(v.clone()); }
                let len = list.len();
                drop(entry);
                self.block_registry.notify(key);
                Ok(len)
            }
            _ => Err(VeloDBError::wrong_type()),
        }
    }

    pub fn lpop_one(&self, db_idx: usize, key: &[u8]) -> Result<Option<Vec<u8>>> {
        match self.entry_mut(db_idx, key)? {
            Some(mut e) => match &mut e.value {
                StorageValue::List(list) => Ok(list.pop_front()),
                _ => Err(VeloDBError::wrong_type()),
            },
            None => Ok(None),
        }
    }

    pub fn rpop_one(&self, db_idx: usize, key: &[u8]) -> Result<Option<Vec<u8>>> {
        match self.entry_mut(db_idx, key)? {
            Some(mut e) => match &mut e.value {
                StorageValue::List(list) => Ok(list.pop_back()),
                _ => Err(VeloDBError::wrong_type()),
            },
            None => Ok(None),
        }
    }

    pub fn llen(&self, db_idx: usize, key: &[u8]) -> Result<usize> {
        match self.entry(db_idx, key)? {
            Some(e) => match &e.value {
                StorageValue::List(list) => Ok(list.len()),
                _ => Err(VeloDBError::wrong_type()),
            },
            None => Ok(0),
        }
    }

    pub fn lrange(&self, db_idx: usize, key: &[u8], start: i64, stop: i64) -> Result<Vec<Vec<u8>>> {
        match self.entry(db_idx, key)? {
            Some(e) => match &e.value {
                StorageValue::List(list) => {
                    let len = list.len() as i64;
                    let s = if start < 0 { (len + start).max(0) } else { start.min(len) } as usize;
                    let e = if stop < 0 { (len + stop).max(0) } else { stop.min(len - 1) } as usize;
                    if s > e || s >= list.len() { return Ok(vec![]); }
                    Ok(list.iter().skip(s).take(e - s + 1).cloned().collect())
                }
                _ => Err(VeloDBError::wrong_type()),
            },
            None => Ok(vec![]),
        }
    }

    pub fn lindex(&self, db_idx: usize, key: &[u8], idx: i64) -> Result<Option<Vec<u8>>> {
        match self.entry(db_idx, key)? {
            Some(e) => match &e.value {
                StorageValue::List(list) => {
                    let len = list.len() as i64;
                    let i = if idx < 0 { (len + idx).max(0) } else { idx.min(len - 1) } as usize;
                    Ok(list.get(i).cloned())
                }
                _ => Err(VeloDBError::wrong_type()),
            },
            None => Ok(None),
        }
    }

    pub fn lset(&self, db_idx: usize, key: &[u8], idx: i64, value: &[u8]) -> Result<()> {
        match self.entry_mut(db_idx, key)? {
            Some(mut e) => match &mut e.value {
                StorageValue::List(list) => {
                    let len = list.len() as i64;
                    let i = if idx < 0 { len + idx } else { idx };
                    if i < 0 || i >= len { return Err(VeloDBError::index_out_of_range()); }
                    list[i as usize] = value.to_vec();
                    Ok(())
                }
                _ => Err(VeloDBError::wrong_type()),
            },
            None => Err(VeloDBError::key_not_found()),
        }
    }

    pub fn ltrim(&self, db_idx: usize, key: &[u8], start: i64, stop: i64) -> Result<()> {
        match self.entry_mut(db_idx, key)? {
            Some(mut e) => match &mut e.value {
                StorageValue::List(list) => {
                    let len = list.len() as i64;
                    let s = if start < 0 { (len + start).max(0) } else { start.min(len) } as usize;
                    let e = if stop < 0 { (len + stop).max(0) } else { stop.min(len - 1) } as usize;
                    if s > e || s >= list.len() { list.clear(); return Ok(()); }
                    let new_len = e - s + 1;
                    list.drain(..s);
                    list.truncate(new_len);
                    Ok(())
                }
                _ => Err(VeloDBError::wrong_type()),
            },
            None => Ok(()),
        }
    }

    pub fn lrem(&self, db_idx: usize, key: &[u8], count: i64, value: &[u8]) -> Result<usize> {
        match self.entry_mut(db_idx, key)? {
            Some(mut e) => match &mut e.value {
                StorageValue::List(list) => {
                    let mut removed = 0usize;
                    if count >= 0 {
                        for _ in 0..(if count == 0 { list.len() } else { count as usize }) {
                            if let Some(pos) = list.iter().position(|v| v == value) {
                                list.remove(pos);
                                removed += 1;
                            }
                        }
                    } else {
                        let limit = (-count) as usize;
                        let len = list.len();
                        for i in 0..len {
                            let idx = len - 1 - i;
                            if list[idx] == value {
                                list.remove(idx);
                                removed += 1;
                                if removed >= limit { break; }
                            }
                        }
                    }
                    Ok(removed)
                }
                _ => Err(VeloDBError::wrong_type()),
            },
            None => Ok(0),
        }
    }

    // ========= Set methods =========

    pub fn sadd(&self, db_idx: usize, key: &[u8], members: &[Vec<u8>]) -> Result<usize> {
        let db = self.db(db_idx)?;
        let mut entry = db.entry(key.to_vec()).or_insert_with(|| Entry { value: StorageValue::Set(HashSet::new()), expire_at: None });
        match &mut entry.value {
            StorageValue::Set(set) => {
                let mut added = 0;
                for m in members { if set.insert(m.clone()) { added += 1; } }
                Ok(added)
            }
            _ => Err(VeloDBError::wrong_type()),
        }
    }

    pub fn srem(&self, db_idx: usize, key: &[u8], members: &[Vec<u8>]) -> Result<usize> {
        match self.entry_mut(db_idx, key)? {
            Some(mut e) => match &mut e.value {
                StorageValue::Set(set) => {
                    let mut removed = 0;
                    for m in members { if set.remove(m) { removed += 1; } }
                    Ok(removed)
                }
                _ => Err(VeloDBError::wrong_type()),
            },
            None => Ok(0),
        }
    }

    pub fn smembers(&self, db_idx: usize, key: &[u8]) -> Result<Vec<Vec<u8>>> {
        match self.entry(db_idx, key)? {
            Some(e) => match &e.value {
                StorageValue::Set(set) => Ok(set.iter().cloned().collect()),
                _ => Err(VeloDBError::wrong_type()),
            },
            None => Ok(vec![]),
        }
    }

    pub fn sismember(&self, db_idx: usize, key: &[u8], member: &[u8]) -> Result<bool> {
        match self.entry(db_idx, key)? {
            Some(e) => match &e.value {
                StorageValue::Set(set) => Ok(set.contains(member)),
                _ => Err(VeloDBError::wrong_type()),
            },
            None => Ok(false),
        }
    }

    pub fn scard(&self, db_idx: usize, key: &[u8]) -> Result<usize> {
        match self.entry(db_idx, key)? {
            Some(e) => match &e.value {
                StorageValue::Set(set) => Ok(set.len()),
                _ => Err(VeloDBError::wrong_type()),
            },
            None => Ok(0),
        }
    }

    fn get_set(&self, db_idx: usize, key: &[u8]) -> Result<Option<HashSet<Vec<u8>>>> {
        match self.entry(db_idx, key)? {
            Some(e) => match &e.value {
                StorageValue::Set(set) => Ok(Some(set.clone())),
                _ => Err(VeloDBError::wrong_type()),
            },
            None => Ok(None),
        }
    }

    pub fn sinter(&self, db_idx: usize, keys: &[Vec<u8>]) -> Result<Vec<Vec<u8>>> {
        if keys.is_empty() { return Ok(vec![]); }
        let first = match self.get_set(db_idx, &keys[0])? { Some(s) => s, None => return Ok(vec![]) };
        let mut result: HashSet<Vec<u8>> = first;
        for key in &keys[1..] {
            let set = match self.get_set(db_idx, key)? { Some(s) => s, None => return Ok(vec![]) };
            result.retain(|v| set.contains(v));
        }
        Ok(result.into_iter().collect())
    }

    pub fn sunion(&self, db_idx: usize, keys: &[Vec<u8>]) -> Result<Vec<Vec<u8>>> {
        let mut result = HashSet::new();
        for key in keys {
            if let Some(set) = self.get_set(db_idx, key)? {
                for v in set { result.insert(v); }
            }
        }
        Ok(result.into_iter().collect())
    }

    pub fn sdiff(&self, db_idx: usize, keys: &[Vec<u8>]) -> Result<Vec<Vec<u8>>> {
        if keys.is_empty() { return Ok(vec![]); }
        let mut result = self.get_set(db_idx, &keys[0])?.unwrap_or_default();
        for key in &keys[1..] {
            if let Some(set) = self.get_set(db_idx, key)? {
                result.retain(|v| !set.contains(v));
            }
        }
        Ok(result.into_iter().collect())
    }

    pub fn srandmember(&self, db_idx: usize, key: &[u8], count: Option<i64>) -> Result<Vec<Vec<u8>>> {
        match self.entry(db_idx, key)? {
            Some(e) => match &e.value {
                StorageValue::Set(set) => {
                    let members: Vec<&Vec<u8>> = set.iter().collect();
                    if members.is_empty() { return Ok(vec![]); }
                    let nano = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().subsec_nanos() as usize;
                    match count {
                        None | Some(1) => {
                            Ok(vec![members[nano % members.len()].clone()])
                        }
                        Some(n) if n >= 0 => {
                            let n = (n as usize).min(members.len());
                            let mut result = Vec::new();
                            let mut used = HashSet::new();
                            while result.len() < n {
                                let idx = (nano + result.len() * 31) % members.len();
                                if used.insert(idx) { result.push(members[idx].clone()); }
                            }
                            Ok(result)
                        }
                        Some(n) => {
                            let n = (-n) as usize;
                            let mut result = Vec::new();
                            for i in 0..n {
                                let idx = (nano + i * 31) % members.len();
                                result.push(members[idx].clone());
                            }
                            Ok(result)
                        }
                    }
                }
                _ => Err(VeloDBError::wrong_type()),
            },
            None => Ok(vec![]),
        }
    }

    pub fn spop(&self, db_idx: usize, key: &[u8], count: Option<usize>) -> Result<Vec<Vec<u8>>> {
        match self.entry_mut(db_idx, key)? {
            Some(mut e) => match &mut e.value {
                StorageValue::Set(set) => {
                    let count = count.unwrap_or(1).min(set.len());
                    let members: Vec<Vec<u8>> = set.iter().take(count).cloned().collect();
                    for m in &members { set.remove(m); }
                    Ok(members)
                }
                _ => Err(VeloDBError::wrong_type()),
            },
            None => Ok(vec![]),
        }
    }

    // ========= Hash methods =========

    pub fn hset(&self, db_idx: usize, key: &[u8], pairs: &[(Vec<u8>, Vec<u8>)]) -> Result<usize> {
        let db = self.db(db_idx)?;
        let mut entry = db.entry(key.to_vec()).or_insert_with(|| Entry { value: StorageValue::Hash(HashMap::new()), expire_at: None });
        match &mut entry.value {
            StorageValue::Hash(map) => {
                let mut added = 0usize;
                for (field, val) in pairs {
                    if map.insert(field.clone(), val.clone()).is_none() { added += 1; }
                }
                Ok(added)
            }
            _ => Err(VeloDBError::wrong_type()),
        }
    }

    pub fn hget(&self, db_idx: usize, key: &[u8], field: &[u8]) -> Result<Option<Vec<u8>>> {
        match self.entry(db_idx, key)? {
            Some(e) => match &e.value {
                StorageValue::Hash(map) => Ok(map.get(field).cloned()),
                _ => Err(VeloDBError::wrong_type()),
            },
            None => Ok(None),
        }
    }

    pub fn hdel(&self, db_idx: usize, key: &[u8], fields: &[Vec<u8>]) -> Result<usize> {
        match self.entry_mut(db_idx, key)? {
            Some(mut e) => match &mut e.value {
                StorageValue::Hash(map) => {
                    let mut removed = 0;
                    for f in fields { if map.remove(f).is_some() { removed += 1; } }
                    Ok(removed)
                }
                _ => Err(VeloDBError::wrong_type()),
            },
            None => Ok(0),
        }
    }

    pub fn hexists(&self, db_idx: usize, key: &[u8], field: &[u8]) -> Result<bool> {
        match self.entry(db_idx, key)? {
            Some(e) => match &e.value {
                StorageValue::Hash(map) => Ok(map.contains_key(field)),
                _ => Err(VeloDBError::wrong_type()),
            },
            None => Ok(false),
        }
    }

    pub fn hgetall(&self, db_idx: usize, key: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        match self.entry(db_idx, key)? {
            Some(e) => match &e.value {
                StorageValue::Hash(map) => Ok(map.iter().map(|(k, v)| (k.clone(), v.clone())).collect()),
                _ => Err(VeloDBError::wrong_type()),
            },
            None => Ok(vec![]),
        }
    }

    pub fn hkeys(&self, db_idx: usize, key: &[u8]) -> Result<Vec<Vec<u8>>> {
        match self.entry(db_idx, key)? {
            Some(e) => match &e.value {
                StorageValue::Hash(map) => Ok(map.keys().cloned().collect()),
                _ => Err(VeloDBError::wrong_type()),
            },
            None => Ok(vec![]),
        }
    }

    pub fn hvals(&self, db_idx: usize, key: &[u8]) -> Result<Vec<Vec<u8>>> {
        match self.entry(db_idx, key)? {
            Some(e) => match &e.value {
                StorageValue::Hash(map) => Ok(map.values().cloned().collect()),
                _ => Err(VeloDBError::wrong_type()),
            },
            None => Ok(vec![]),
        }
    }

    pub fn hlen(&self, db_idx: usize, key: &[u8]) -> Result<usize> {
        match self.entry(db_idx, key)? {
            Some(e) => match &e.value {
                StorageValue::Hash(map) => Ok(map.len()),
                _ => Err(VeloDBError::wrong_type()),
            },
            None => Ok(0),
        }
    }

    pub fn hincrby(&self, db_idx: usize, key: &[u8], field: &[u8], increment: i64) -> Result<i64> {
        let db = self.db(db_idx)?;
        let mut entry = db.entry(key.to_vec()).or_insert_with(|| Entry { value: StorageValue::Hash(HashMap::new()), expire_at: None });
        match &mut entry.value {
            StorageValue::Hash(map) => {
                let current = map.get(field).and_then(|v| String::from_utf8_lossy(v).parse::<i64>().ok()).unwrap_or(0);
                let new_val = current.checked_add(increment).ok_or(VeloDBError::overflow())?;
                map.insert(field.to_vec(), new_val.to_string().into_bytes());
                Ok(new_val)
            }
            _ => Err(VeloDBError::wrong_type()),
        }
    }

    // ========= ZSet methods =========

    pub fn zadd(&self, db_idx: usize, key: &[u8], items: &[(f64, Vec<u8>)]) -> Result<usize> {
        let db = self.db(db_idx)?;
        let mut entry = db.entry(key.to_vec()).or_insert_with(|| Entry { value: StorageValue::ZSet { members: HashMap::new(), scores: BTreeMap::new() }, expire_at: None });
        match &mut entry.value {
            StorageValue::ZSet { members, scores } => {
                let mut added = 0usize;
                for (score, member) in items {
                    let old_score = members.insert(member.clone(), *score);
                    if let Some(os) = old_score {
                        if let Some(set) = scores.get_mut(&OrderedF64(os)) {
                            set.remove(member);
                            if set.is_empty() { scores.remove(&OrderedF64(os)); }
                        }
                    }
                    scores.entry(OrderedF64(*score)).or_default().insert(member.clone());
                    if old_score.is_none() { added += 1; }
                }
                Ok(added)
            }
            _ => Err(VeloDBError::wrong_type()),
        }
    }

    pub fn zrem(&self, db_idx: usize, key: &[u8], members: &[Vec<u8>]) -> Result<usize> {
        match self.entry_mut(db_idx, key)? {
            Some(mut e) => match &mut e.value {
                StorageValue::ZSet { members: zmembers, scores } => {
                    let mut removed = 0;
                    for m in members {
                        if let Some(score) = zmembers.remove(m) {
                            if let Some(set) = scores.get_mut(&OrderedF64(score)) {
                                set.remove(m);
                                if set.is_empty() { scores.remove(&OrderedF64(score)); }
                            }
                            removed += 1;
                        }
                    }
                    Ok(removed)
                }
                _ => Err(VeloDBError::wrong_type()),
            },
            None => Ok(0),
        }
    }

    pub fn zscore(&self, db_idx: usize, key: &[u8], member: &[u8]) -> Result<Option<f64>> {
        match self.entry(db_idx, key)? {
            Some(e) => match &e.value {
                StorageValue::ZSet { members, .. } => Ok(members.get(member).copied()),
                _ => Err(VeloDBError::wrong_type()),
            },
            None => Ok(None),
        }
    }

    pub fn zrank(&self, db_idx: usize, key: &[u8], member: &[u8]) -> Result<Option<usize>> {
        match self.entry(db_idx, key)? {
            Some(e) => match &e.value {
                StorageValue::ZSet { members, scores } => {
                    let target_score = match members.get(member) { Some(s) => *s, None => return Ok(None) };
                    let mut rank = 0usize;
                    for (score, set) in scores.iter() {
                        if score.0 < target_score { rank += set.len(); }
                        else if score.0 == target_score {
                            let members_before = set.iter().take_while(|m| **m != *member).count();
                            return Ok(Some(rank + members_before));
                        }
                    }
                    Ok(None)
                }
                _ => Err(VeloDBError::wrong_type()),
            },
            None => Ok(None),
        }
    }

    pub fn zrange(&self, db_idx: usize, key: &[u8], start: i64, stop: i64, withscores: bool) -> Result<Vec<(Vec<u8>, Option<f64>)>> {
        let all = self.zrange_by_score(db_idx, key, f64::NEG_INFINITY, true, f64::INFINITY, true, withscores, None)?;
        let len = all.len() as i64;
        let s = if start < 0 { (len + start).max(0) } else { start.min(len) } as usize;
        let e = if stop < 0 { (len + stop).max(0) } else { stop.min(len - 1) } as usize;
        if s > e || s >= all.len() { return Ok(vec![]); }
        Ok(all.into_iter().skip(s).take(e - s + 1).collect())
    }

    pub fn zrange_by_score(&self, db_idx: usize, key: &[u8], min: f64, min_exclusive: bool, max: f64, max_exclusive: bool, withscores: bool, limit: Option<(usize, usize)>) -> Result<Vec<(Vec<u8>, Option<f64>)>> {
        match self.entry(db_idx, key)? {
            Some(e) => match &e.value {
                StorageValue::ZSet { scores, .. } => {
                    let mut result = Vec::new();
                    let mut skipped = 0usize;
                    let (offset, count) = limit.unwrap_or((0, usize::MAX));
                    for (score, set) in scores.range(OrderedF64(min)..=OrderedF64(max)) {
                        let ss = score.0;
                        if min_exclusive && ss == min { continue; }
                        if max_exclusive && ss == max { continue; }
                        for member in set {
                            if skipped < offset { skipped += 1; continue; }
                            if result.len() >= count { break; }
                            result.push((member.clone(), if withscores { Some(ss) } else { None }));
                        }
                        if result.len() >= count { break; }
                    }
                    Ok(result)
                }
                _ => Err(VeloDBError::wrong_type()),
            },
            None => Ok(vec![]),
        }
    }

    pub fn zcard(&self, db_idx: usize, key: &[u8]) -> Result<usize> {
        match self.entry(db_idx, key)? {
            Some(e) => match &e.value {
                StorageValue::ZSet { members, .. } => Ok(members.len()),
                _ => Err(VeloDBError::wrong_type()),
            },
            None => Ok(0),
        }
    }

    pub fn zcount(&self, db_idx: usize, key: &[u8], min: f64, min_exclusive: bool, max: f64, max_exclusive: bool) -> Result<usize> {
        match self.entry(db_idx, key)? {
            Some(e) => match &e.value {
                StorageValue::ZSet { members, .. } => {
                    let count = members.iter().filter(|(_, score)| {
                        let s = **score;
                        let above_min = if min_exclusive { s > min } else { s >= min };
                        let below_max = if max_exclusive { s < max } else { s <= max };
                        above_min && below_max
                    }).count();
                    Ok(count)
                }
                _ => Err(VeloDBError::wrong_type()),
            },
            None => Ok(0),
        }
    }

    // ========= Stream methods =========

    pub fn xadd(&self, db_idx: usize, key: &[u8], id_str: &[u8], fields: &[(Vec<u8>, Vec<u8>)], maxlen: Option<usize>) -> Result<Vec<u8>> {
        let (id_ms, id_seq) = self.generate_stream_id(db_idx, key, id_str)?;
        // Validate ID is greater than last
        if let Ok(Some(e)) = self.entry(db_idx, key) {
            if let StorageValue::Stream { last_id_ms, last_id_seq, .. } = &e.value {
                if id_ms < *last_id_ms || (id_ms == *last_id_ms && id_seq <= *last_id_seq) {
                    return Err(VeloDBError::stream_id_too_small());
                }
            }
        }
        let db = self.db(db_idx)?;
        let mut entry = db.entry(key.to_vec()).or_insert_with(|| Entry { value: StorageValue::Stream { entries: VecDeque::new(), last_id_ms: 0, last_id_seq: 0 }, expire_at: None });
        let generated_id = format!("{}-{}", id_ms, id_seq).into_bytes();
        match &mut entry.value {
            StorageValue::Stream { entries, last_id_ms, last_id_seq } => {
                entries.push_back(StreamEntry { id_ms, id_seq, fields: fields.to_vec() });
                *last_id_ms = id_ms;
                *last_id_seq = id_seq;
                if let Some(max) = maxlen {
                    while entries.len() > max { entries.pop_front(); }
                }
                let gid = generated_id.clone();
                drop(entry);
                self.block_registry.notify(key);
                Ok(gid)
            }
            _ => Err(VeloDBError::wrong_type()),
        }
    }

    fn generate_stream_id(&self, db_idx: usize, key: &[u8], id_str: &[u8]) -> Result<(u64, u64)> {
        let id = String::from_utf8_lossy(id_str);
        if id == "*" {
            let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
            let (last_ms, last_seq) = self.get_stream_last_id(db_idx, key);
            if now_ms > last_ms { return Ok((now_ms, 0)); }
            return Ok((last_ms, last_seq + 1));
        }
        let parts: Vec<&str> = id.splitn(2, '-').collect();
        if parts.len() != 2 { return Err(VeloDBError::not_integer()); }
        let ms: u64 = parts[0].parse().map_err(|_| VeloDBError::not_integer())?;
        if parts[1] == "*" {
            let (last_ms, last_seq) = self.get_stream_last_id(db_idx, key);
            if ms == 0 && last_seq == 0 { return Ok((if ms == 0 { 1 } else { ms }, 0)); }
            if ms > last_ms { return Ok((ms, 0)); }
            return Ok((ms, last_seq + 1));
        }
        let seq: u64 = parts[1].parse().map_err(|_| VeloDBError::not_integer())?;
        if ms == 0 && seq == 0 { return Err(VeloDBError::stream_id_too_small()); }
        Ok((ms, seq))
    }

    fn get_stream_last_id(&self, db_idx: usize, key: &[u8]) -> (u64, u64) {
        match self.entry(db_idx, key) {
            Ok(Some(e)) => match &e.value {
                StorageValue::Stream { last_id_ms, last_id_seq, .. } => (*last_id_ms, *last_id_seq),
                _ => (0, 0),
            },
            _ => (0, 0),
        }
    }

    pub fn xrange(&self, db_idx: usize, key: &[u8], start_str: &[u8], end_str: &[u8], count: Option<usize>) -> Result<Vec<StreamEntry>> {
        let all = self.stream_entries(db_idx, key)?;
        let (start_ms, start_seq) = self.parse_stream_id(start_str, true);
        let (end_ms, end_seq) = self.parse_stream_id(end_str, false);
        let result: Vec<StreamEntry> = all.into_iter()
            .filter(|e| {
                let after_start = e.id_ms > start_ms || (e.id_ms == start_ms && e.id_seq >= start_seq);
                let before_end = e.id_ms < end_ms || (e.id_ms == end_ms && e.id_seq <= end_seq);
                after_start && before_end
            })
            .take(count.unwrap_or(usize::MAX))
            .collect();
        Ok(result)
    }

    pub fn xrevrange(&self, db_idx: usize, key: &[u8], end_str: &[u8], start_str: &[u8], count: Option<usize>) -> Result<Vec<StreamEntry>> {
        let all = self.stream_entries(db_idx, key)?;
        let (start_ms, start_seq) = self.parse_stream_id(start_str, true);
        let (end_ms, end_seq) = self.parse_stream_id(end_str, false);
        let mut result: Vec<StreamEntry> = all.into_iter()
            .filter(|e| {
                let after_start = e.id_ms > start_ms || (e.id_ms == start_ms && e.id_seq >= start_seq);
                let before_end = e.id_ms < end_ms || (e.id_ms == end_ms && e.id_seq <= end_seq);
                after_start && before_end
            })
            .collect();
        result.reverse();
        if let Some(c) = count { result.truncate(c); }
        Ok(result)
    }

    fn stream_entries(&self, db_idx: usize, key: &[u8]) -> Result<Vec<StreamEntry>> {
        match self.entry(db_idx, key)? {
            Some(e) => match &e.value {
                StorageValue::Stream { entries, .. } => Ok(entries.iter().map(|e| StreamEntry { id_ms: e.id_ms, id_seq: e.id_seq, fields: e.fields.clone() }).collect()),
                _ => Err(VeloDBError::wrong_type()),
            },
            None => Ok(vec![]),
        }
    }

    fn parse_stream_id(&self, s: &[u8], is_start: bool) -> (u64, u64) {
        let s = String::from_utf8_lossy(s);
        if s == "-" { return (0, 0); }
        if s == "+" { return (u64::MAX, u64::MAX); }
        if s == "0" || s == "0-0" { return (0, 0); }
        let parts: Vec<&str> = s.splitn(2, '-').collect();
        let ms: u64 = parts[0].parse().unwrap_or(0);
        let seq: u64 = parts.get(1).and_then(|p| p.parse().ok()).unwrap_or(if is_start { 0 } else { u64::MAX });
        (ms, seq)
    }

    pub fn xlen(&self, db_idx: usize, key: &[u8]) -> Result<usize> {
        match self.entry(db_idx, key)? {
            Some(e) => match &e.value {
                StorageValue::Stream { entries, .. } => Ok(entries.len()),
                _ => Err(VeloDBError::wrong_type()),
            },
            None => Ok(0),
        }
    }

    pub fn xdel(&self, db_idx: usize, key: &[u8], ids: &[String]) -> Result<usize> {
        match self.entry_mut(db_idx, key)? {
            Some(mut e) => match &mut e.value {
                StorageValue::Stream { entries, .. } => {
                    let mut removed = 0;
                    for id_str in ids {
                        let (ms, seq) = self.parse_stream_id(id_str.as_bytes(), true);
                        if let Some(pos) = entries.iter().position(|e| e.id_ms == ms && e.id_seq == seq) {
                            entries.remove(pos);
                            removed += 1;
                        }
                    }
                    Ok(removed)
                }
                _ => Err(VeloDBError::wrong_type()),
            },
            None => Ok(0),
        }
    }

    pub fn xtrim(&self, db_idx: usize, key: &[u8], maxlen: usize) -> Result<usize> {
        match self.entry_mut(db_idx, key)? {
            Some(mut e) => match &mut e.value {
                StorageValue::Stream { entries, .. } => {
                    let old_len = entries.len();
                    let to_remove = if old_len > maxlen { old_len - maxlen } else { 0 };
                    for _ in 0..to_remove { entries.pop_front(); }
                    Ok(to_remove)
                }
                _ => Err(VeloDBError::wrong_type()),
            },
            None => Ok(0),
        }
    }

    pub fn xread(&self, db_idx: usize, keys: &[Vec<u8>], ids: &[(u64, u64)], count: Option<usize>, _block_ms: Option<i64>) -> Result<Vec<(Vec<u8>, Vec<StreamEntry>)>> {
        let mut result = Vec::new();
        for (i, key) in keys.iter().enumerate() {
            let (after_ms, after_seq) = ids.get(i).copied().unwrap_or((0, 0));
            let entries = self.stream_entries_after(db_idx, key, after_ms, after_seq, count)?;
            if !entries.is_empty() { result.push((key.clone(), entries)); }
        }
        Ok(result)
    }

    pub fn stream_entries_after(&self, db_idx: usize, key: &[u8], after_ms: u64, after_seq: u64, count: Option<usize>) -> Result<Vec<StreamEntry>> {
        let all = self.stream_entries(db_idx, key)?;
        let result: Vec<StreamEntry> = all.into_iter()
            .filter(|e| e.id_ms > after_ms || (e.id_ms == after_ms && e.id_seq > after_seq))
            .take(count.unwrap_or(usize::MAX))
            .collect();
        Ok(result)
    }

    // ========= NestedHash methods =========

    pub fn nhset(&self, db_idx: usize, key: &[u8], field: &[u8], subfield: &[u8], value: &[u8]) -> Result<usize> {
        let db = self.db(db_idx)?;
        let mut entry = db.entry(key.to_vec()).or_insert_with(|| Entry { value: StorageValue::NestedHash(HashMap::new()), expire_at: None });
        match &mut entry.value {
            StorageValue::NestedHash(map) => {
                let inner = map.entry(field.to_vec()).or_default();
                if inner.insert(subfield.to_vec(), value.to_vec()).is_none() { Ok(1) } else { Ok(0) }
            }
            _ => Err(VeloDBError::wrong_type()),
        }
    }

    pub fn nhget(&self, db_idx: usize, key: &[u8], field: &[u8], subfield: &[u8]) -> Result<Option<Vec<u8>>> {
        match self.entry(db_idx, key)? {
            Some(e) => match &e.value {
                StorageValue::NestedHash(map) => {
                    Ok(map.get(field).and_then(|inner| inner.get(subfield).cloned()))
                }
                _ => Err(VeloDBError::wrong_type()),
            },
            None => Ok(None),
        }
    }

    pub fn nhdel(&self, db_idx: usize, key: &[u8], field: &[u8], subfield: Option<&[u8]>) -> Result<usize> {
        match self.entry_mut(db_idx, key)? {
            Some(mut e) => match &mut e.value {
                StorageValue::NestedHash(map) => {
                    match subfield {
                        Some(sf) => {
                            let removed = map.get_mut(field).map(|inner| inner.remove(sf).is_some()).unwrap_or(false);
                            if removed {
                                if let Some(inner) = map.get(field) { if inner.is_empty() { map.remove(field); } }
                                Ok(1)
                            } else { Ok(0) }
                        }
                        None => {
                            if map.remove(field).is_some() { Ok(1) } else { Ok(0) }
                        }
                    }
                }
                _ => Err(VeloDBError::wrong_type()),
            },
            None => Ok(0),
        }
    }

    pub fn nhkeys(&self, db_idx: usize, key: &[u8], field: Option<&[u8]>) -> Result<Vec<Vec<u8>>> {
        match self.entry(db_idx, key)? {
            Some(e) => match &e.value {
                StorageValue::NestedHash(map) => {
                    match field {
                        Some(f) => Ok(map.get(f).map(|inner| inner.keys().cloned().collect()).unwrap_or_default()),
                        None => Ok(map.keys().cloned().collect()),
                    }
                }
                _ => Err(VeloDBError::wrong_type()),
            },
            None => Ok(vec![]),
        }
    }

    pub fn nhvals(&self, db_idx: usize, key: &[u8], field: Option<&[u8]>) -> Result<Vec<Vec<u8>>> {
        match self.entry(db_idx, key)? {
            Some(e) => match &e.value {
                StorageValue::NestedHash(map) => {
                    match field {
                        Some(f) => Ok(map.get(f).map(|inner| inner.values().cloned().collect()).unwrap_or_default()),
                        None => {
                            let mut all = Vec::new();
                            for inner in map.values() { all.extend(inner.values().cloned()); }
                            Ok(all)
                        }
                    }
                }
                _ => Err(VeloDBError::wrong_type()),
            },
            None => Ok(vec![]),
        }
    }

    pub fn nhgetall(&self, db_idx: usize, key: &[u8], field: Option<&[u8]>) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        match self.entry(db_idx, key)? {
            Some(e) => match &e.value {
                StorageValue::NestedHash(map) => {
                    match field {
                        Some(f) => Ok(map.get(f).map(|inner| inner.iter().map(|(k, v)| (k.clone(), v.clone())).collect()).unwrap_or_default()),
                        None => {
                            let mut all = Vec::new();
                            for (f, inner) in map {
                                for (sf, val) in inner {
                                    let combined = format!("{}:{}", String::from_utf8_lossy(f), String::from_utf8_lossy(sf)).into_bytes();
                                    all.push((combined, val.clone()));
                                }
                            }
                            Ok(all)
                        }
                    }
                }
                _ => Err(VeloDBError::wrong_type()),
            },
            None => Ok(vec![]),
        }
    }
}

fn simple_match(s: &str, p: &str) -> bool {
    let s = s.as_bytes();
    let p = p.as_bytes();
    let (mut si, mut pi) = (0, 0);
    let (mut star, mut match_idx) = (None, 0);
    while si < s.len() {
        if pi < p.len() && (p[pi] == b'?' || p[pi] == s[si]) { si += 1; pi += 1; }
        else if pi < p.len() && p[pi] == b'*' { star = Some(pi); match_idx = si; pi += 1; }
        else if let Some(idx) = star { pi = idx + 1; match_idx += 1; si = match_idx; }
        else { return false; }
    }
    while pi < p.len() && p[pi] == b'*' { pi += 1; }
    pi == p.len()
}
