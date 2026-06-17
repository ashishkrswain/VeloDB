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
    pub appendonly: bool,
    pub appendfsync: String,
    pub save: Vec<(u64, u64)>,
    pub replicaof: Option<String>,
    pub masterauth: Option<String>,
    pub repl_backlog_size: usize,
    pub cthreads: usize,
    pub cluster_enabled: bool,
    pub cluster_port: u16,
    pub cluster_node_timeout: u64,
    pub cluster_config_file: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            port: 6379, bind_address: "127.0.0.1".into(), timeout: 0,
            tcp_keepalive: 300, databases: 16, maxmemory: 0,
            loglevel: "notice".into(), dbfilename: "dump.rdb".into(), dir: "./".into(),
            appendonly: false, appendfsync: "everysec".into(), save: vec![],
            replicaof: None, masterauth: None, repl_backlog_size: 1048576,
            cthreads: std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4),
            cluster_enabled: false, cluster_port: 16379, cluster_node_timeout: 15000, cluster_config_file: "nodes.conf".into(),
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
            "appendonly" => config.appendonly = value.eq_ignore_ascii_case("yes"),
            "appendfsync" => config.appendfsync = value.to_string(),
            "save" => {
                let parts: Vec<&str> = value.splitn(2, ' ').collect();
                if parts.len() == 2 {
                    if let (Ok(secs), Ok(changes)) = (parts[0].parse(), parts[1].parse()) {
                        config.save.push((secs, changes));
                    }
                }
            }
            "replicaof" => config.replicaof = Some(value.to_string()),
            "masterauth" => config.masterauth = Some(value.to_string()),
            "repl-backlog-size" => { if let Ok(sz) = value.parse() { config.repl_backlog_size = sz; } }
            _ => {}
        }
    }
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = ServerConfig::default();
        assert_eq!(config.port, 6379);
        assert_eq!(config.databases, 16);
    }

    #[test]
    fn test_parse_port() {
        let config = parse_redis_config("port 6380").unwrap();
        assert_eq!(config.port, 6380);
    }

    #[test]
    fn test_parse_bind_address() {
        let config = parse_redis_config("bind 0.0.0.0").unwrap();
        assert_eq!(config.bind_address, "0.0.0.0");
    }

    #[test]
    fn test_parse_databases() {
        let config = parse_redis_config("databases 32").unwrap();
        assert_eq!(config.databases, 32);
    }

    #[test]
    fn test_parse_tcp_keepalive() {
        let config = parse_redis_config("tcp-keepalive 60").unwrap();
        assert_eq!(config.tcp_keepalive, 60);
    }

    #[test]
    fn test_parse_comments_and_blanks() {
        let input = "# this is a comment\n\nport 6390\n\n# another comment";
        let config = parse_redis_config(input).unwrap();
        assert_eq!(config.port, 6390);
    }

    #[test]
    fn test_parse_unknown_keywords_ignored() {
        let config = parse_redis_config("unknown-key value\nport 6399").unwrap();
        assert_eq!(config.port, 6399);
    }
}
