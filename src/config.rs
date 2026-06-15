// Copyright (c) 2025-Present VeloDB Contributors
// SPDX-License-Identifier: MIT

use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub port: u16,
    pub bind_address: String,
    pub timeout: u64,
    pub tcp_keepalive: u64,
    pub databases: usize,
    pub maxmemory: u64,
    pub loglevel: String,
    pub dbfilename: String,
    pub dir: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            port: 6379, bind_address: "127.0.0.1".into(), timeout: 0,
            tcp_keepalive: 300, databases: 16, maxmemory: 0,
            loglevel: "notice".into(), dbfilename: "dump.rdb".into(), dir: "./".into(),
        }
    }
}

pub fn load(path: impl AsRef<Path>) -> anyhow::Result<ServerConfig> {
    let path = path.as_ref();
    if !path.exists() {
        tracing::warn!("Config file {:?} not found, using defaults", path);
        return Ok(ServerConfig::default());
    }
    let content = std::fs::read_to_string(path)?;
    parse_redis_config(&content)
}

fn parse_redis_config(content: &str) -> anyhow::Result<ServerConfig> {
    let mut config = ServerConfig::default();
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') { continue; }
        let parts: Vec<&str> = line.splitn(2, ' ').collect();
        if parts.len() != 2 { continue; }
        let key = parts[0].to_lowercase();
        let value = parts[1].trim().trim_matches('"');
        match key.as_str() {
            "port" => config.port = value.parse()?,
            "bind" => config.bind_address = value.to_string(),
            "timeout" => config.timeout = value.parse()?,
            "tcp-keepalive" => config.tcp_keepalive = value.parse()?,
            "databases" => config.databases = value.parse()?,
            "loglevel" => config.loglevel = value.to_string(),
            "dbfilename" => config.dbfilename = value.to_string(),
            "dir" => config.dir = value.to_string(),
            _ => {}
        }
    }
    Ok(config)
}
