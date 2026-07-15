// Copyright (c) 2025-Present VeloDB Contributors
// SPDX-License-Identifier: MIT

use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Duration;

#[derive(Clone)]
pub enum FsyncPolicy {
    No,
    EverySec,
    Always,
}

pub struct AofWriter {
    file: Mutex<File>,
    policy: FsyncPolicy,
    path: PathBuf,
}

impl AofWriter {
    pub fn open(path: PathBuf, policy: FsyncPolicy) -> std::io::Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        Ok(Self { file: Mutex::new(file), policy, path })
    }

    /// AOF rewrite (BGREWRITEAOF): serializes the current dataset as a
    /// minimal command stream into a temp file, atomically replaces the
    /// AOF, and swaps the writer's handle to the new file. The file lock
    /// is held throughout so concurrent appends can't land in the old
    /// unlinked file.
    pub fn rewrite(&self, store: &crate::store::Store) -> std::io::Result<()> {
        let mut locked = self.file.lock().unwrap();
        let tmp_path = self.path.with_extension("aof.rewrite");
        {
            let mut tmp = File::create(&tmp_path)?;
            crate::persist::aof_rewrite::write_dataset(&mut tmp, store)?;
            tmp.flush()?;
            tmp.sync_all()?;
        }
        // Windows rename-over-existing requires removing the target first;
        // the lock makes the non-atomic window invisible to appenders.
        let _ = std::fs::remove_file(&self.path);
        std::fs::rename(&tmp_path, &self.path)?;
        *locked = OpenOptions::new().create(true).append(true).open(&self.path)?;
        Ok(())
    }

    pub fn append(&self, data: &[u8]) -> std::io::Result<()> {
        let mut f = self.file.lock().unwrap();
        f.write_all(data)?;
        match &self.policy {
            FsyncPolicy::Always => f.flush()?,
            _ => {}
        }
        Ok(())
    }

    pub fn sync(&self) -> std::io::Result<()> {
        self.file.lock().unwrap().flush()
    }

    pub fn fsync_policy(&self) -> &FsyncPolicy { &self.policy }
}

pub fn start_fsync_task(aof: std::sync::Arc<AofWriter>) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let _ = aof.sync();
        }
    });
}

pub fn load_aof(path: &Path) -> std::io::Result<Vec<String>> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let mut commands = Vec::new();
    let mut buf = String::new();
    loop {
        buf.clear();
        let n = reader.read_line(&mut buf)?;
        if n == 0 { break; }
        let trimmed = buf.trim();
        if trimmed.is_empty() { continue; }
        commands.push(trimmed.to_string());
    }
    Ok(commands)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::Store;
    use std::sync::Arc;

    fn read_all(path: &Path) -> String {
        String::from_utf8_lossy(&std::fs::read(path).unwrap()).to_string()
    }

    #[test]
    fn test_rewrite_produces_minimal_commands() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.aof");
        let aof = AofWriter::open(path.clone(), FsyncPolicy::Always).unwrap();

        // Simulate a bloated AOF: 100 INCRs on one counter
        for _ in 0..100 {
            aof.append(&encode_command_for_aof(&[b"INCR".to_vec(), b"counter".to_vec()])).unwrap();
        }
        let bloated_size = std::fs::metadata(&path).unwrap().len();

        let store = Arc::new(Store::new(1));
        store.set(0, b"counter", b"100", None).unwrap();
        aof.rewrite(&store).unwrap();

        let rewritten_size = std::fs::metadata(&path).unwrap().len();
        assert!(rewritten_size < bloated_size, "rewrite should shrink AOF: {} -> {}", bloated_size, rewritten_size);
        let content = read_all(&path);
        assert!(content.contains("SET"), "rewritten AOF should contain SET: {}", content);
        assert!(content.contains("counter"));
        assert!(content.contains("100"));
        assert!(!content.contains("INCR"));
    }

    #[test]
    fn test_rewrite_covers_all_types_and_replays() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.aof");
        let aof = AofWriter::open(path.clone(), FsyncPolicy::Always).unwrap();

        let store = Arc::new(Store::new(1));
        store.set(0, b"str", b"v", None).unwrap();
        store.rpush(0, b"list", &[b"a".to_vec(), b"b".to_vec()]).unwrap();
        store.sadd(0, b"set", &[b"m".to_vec()]).unwrap();
        store.hset(0, b"hash", &[(b"f".to_vec(), b"v".to_vec())]).unwrap();
        store.zadd(0, b"zset", &[(1.5, b"m".to_vec())]).unwrap();
        store.nhset(0, b"nh", b"f", b"sf", b"v").unwrap();

        aof.rewrite(&store).unwrap();

        let content = read_all(&path);
        for cmd in ["SET", "RPUSH", "SADD", "HSET", "ZADD", "NHSET"] {
            assert!(content.contains(cmd), "missing {} in rewritten AOF:\n{}", cmd, content);
        }
    }

    #[test]
    fn test_rewrite_preserves_ttls() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.aof");
        let aof = AofWriter::open(path.clone(), FsyncPolicy::Always).unwrap();

        let store = Arc::new(Store::new(1));
        let future = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64 + 5_000_000;
        store.set(0, b"k", b"v", Some(future)).unwrap();
        aof.rewrite(&store).unwrap();

        let content = read_all(&path);
        assert!(content.contains("PEXPIREAT"), "TTL must survive rewrite: {}", content);
        assert!(content.contains(&future.to_string()));
    }

    #[test]
    fn test_append_works_after_rewrite() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.aof");
        let aof = AofWriter::open(path.clone(), FsyncPolicy::Always).unwrap();

        let store = Arc::new(Store::new(1));
        store.set(0, b"k", b"v", None).unwrap();
        aof.rewrite(&store).unwrap();

        // The writer must keep appending to the NEW file, not the unlinked old handle
        aof.append(&encode_command_for_aof(&[b"SET".to_vec(), b"after".to_vec(), b"1".to_vec()])).unwrap();
        aof.sync().unwrap();
        let content = read_all(&path);
        assert!(content.contains("after"), "appends after rewrite must land in the new file: {}", content);
    }

    #[test]
    fn test_rewrite_multiple_databases() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.aof");
        let aof = AofWriter::open(path.clone(), FsyncPolicy::Always).unwrap();

        let store = Arc::new(Store::new(2));
        store.set(0, b"db0key", b"v", None).unwrap();
        store.set(1, b"db1key", b"v", None).unwrap();
        aof.rewrite(&store).unwrap();

        let content = read_all(&path);
        assert!(content.contains("SELECT"), "multi-db rewrite needs SELECT: {}", content);
        assert!(content.contains("db0key"));
        assert!(content.contains("db1key"));
    }
}

pub fn encode_command_for_aof(args: &[Vec<u8>]) -> Vec<u8> {
    let mut out = Vec::new();
    out.push(b'*');
    out.extend(args.len().to_string().as_bytes());
    out.extend(b"\r\n");
    for arg in args {
        out.push(b'$');
        out.extend(arg.len().to_string().as_bytes());
        out.extend(b"\r\n");
        out.extend_from_slice(arg);
        out.extend(b"\r\n");
    }
    out
}
