use dashmap::DashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use crate::error::{Result, VeloDBError};

#[derive(Clone)]
struct Entry { value: Vec<u8>, type_name: String, expire_at: Option<u64> }

pub struct Store { databases: Vec<DashMap<Vec<u8>, Entry>> }

impl Store {
    pub fn new(num: usize) -> Self { Self { databases: (0..num).map(|_| DashMap::new()).collect() } }

    fn db(&self, idx: usize) -> Result<&DashMap<Vec<u8>, Entry>> {
        self.databases.get(idx).ok_or_else(|| VeloDBError::internal(format!("db {} out of range", idx)))
    }

    fn expired(entry: &Entry) -> bool {
        entry.expire_at.map_or(false, |at| at <= SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64)
    }

    pub fn get(&self, db_idx: usize, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let db = self.db(db_idx)?;
        match db.get(key) { Some(e) if !Self::expired(&e) => Ok(Some(e.value.clone())), _ => Ok(None) }
    }

    pub fn set(&self, db_idx: usize, key: &[u8], value: &[u8], expire_ms: Option<u64>) -> Result<()> {
        let db = self.db(db_idx)?;
        db.insert(key.to_vec(), Entry { value: value.to_vec(), type_name: "string".into(), expire_at: expire_ms });
        Ok(())
    }

    pub fn del(&self, db_idx: usize, key: &[u8]) -> Result<bool> { Ok(self.db(db_idx)?.remove(key).is_some()) }

    pub fn exists(&self, db_idx: usize, key: &[u8]) -> Result<bool> {
        match self.db(db_idx)?.get(key) { Some(e) => Ok(!Self::expired(&e)), None => Ok(false) }
    }

    pub fn set_expire(&self, db_idx: usize, key: &[u8], at_ms: u64) -> Result<bool> {
        match self.db(db_idx)?.get_mut(key) { Some(mut e) => { e.expire_at = Some(at_ms); Ok(true) }, None => Ok(false) }
    }

    pub fn get_expire(&self, db_idx: usize, key: &[u8]) -> Result<Option<u64>> {
        match self.db(db_idx)?.get(key) { Some(e) if !Self::expired(&e) => Ok(e.expire_at), _ => Ok(None) }
    }

    pub fn remove_expire(&self, db_idx: usize, key: &[u8]) -> Result<bool> {
        match self.db(db_idx)?.get_mut(key) { Some(mut e) => { e.expire_at = None; Ok(true) }, None => Ok(false) }
    }

    pub fn get_type(&self, db_idx: usize, key: &[u8]) -> Result<Option<String>> {
        match self.db(db_idx)?.get(key) { Some(e) if !Self::expired(&e) => Ok(Some(e.type_name.clone())), _ => Ok(None) }
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
        drop(db);
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
