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
    pub path: PathBuf,
}

impl AofWriter {
    pub fn open(path: PathBuf, policy: FsyncPolicy) -> std::io::Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        Ok(Self { file: Mutex::new(file), policy, path })
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
