// Copyright (c) 2025-Present VeloDB Contributors
// SPDX-License-Identifier: MIT

use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::sync::Arc;
use velodb::net::listener::ServerHandle;
use velodb::config::ServerConfig;

fn encode_command(args: &[&str]) -> Vec<u8> {
    let mut out = Vec::new();
    out.push(b'*');
    out.extend(args.len().to_string().as_bytes());
    out.extend(b"\r\n");
    for arg in args {
        out.push(b'$');
        out.extend(arg.len().to_string().as_bytes());
        out.extend(b"\r\n");
        out.extend(arg.as_bytes());
        out.extend(b"\r\n");
    }
    out
}

async fn send_cmd(stream: &mut TcpStream, cmd: &[&str]) -> Vec<u8> {
    stream.write_all(&encode_command(cmd)).await.unwrap();
    let mut buf = vec![0u8; 65536];
    // Wait briefly then try-read; if nothing, do a small async read
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    let n = stream.try_read(&mut buf).unwrap_or(0);
    if n > 0 {
        buf.truncate(n);
        return buf;
    }
    // Fallback: proper async read
    let n = stream.read(&mut buf).await.unwrap_or(0);
    buf.truncate(n);
    buf
}

fn find_free_port() -> u16 {
    use std::net::TcpListener;
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

/// Polls `check` until it returns Some or `timeout` elapses; avoids
/// flaky fixed-duration sleeps when many tests run in parallel and
/// compete for CPU (replication propagation has no other signal to
/// wait on from the test side).
async fn poll_until<T>(timeout: std::time::Duration, mut check: impl FnMut() -> Option<T>) -> Option<T> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(v) = check() { return Some(v); }
        if tokio::time::Instant::now() >= deadline { return None; }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

async fn spawn_server(port: u16) -> ServerHandle {
    let mut config = ServerConfig::default();
    config.port = port;
    config.databases = 4;
    let store = std::sync::Arc::new(velodb::store::Store::new(config.databases));
    let replid = velodb::replication::backlog::ReplicationState::new().replid;
    let repl_backlog = Arc::new(std::sync::Mutex::new(velodb::replication::backlog::ReplBacklog::new(1048576)));
    ServerHandle::new(&config, store, None, replid, Some(repl_backlog)).await.unwrap()
}

async fn spawn_server_with_requirepass(port: u16, password: &str) -> ServerHandle {
    let mut config = ServerConfig::default();
    config.port = port;
    config.databases = 4;
    config.requirepass = Some(password.to_string());
    let store = std::sync::Arc::new(velodb::store::Store::new(config.databases));
    let replid = velodb::replication::backlog::ReplicationState::new().replid;
    let repl_backlog = Arc::new(std::sync::Mutex::new(velodb::replication::backlog::ReplBacklog::new(1048576)));
    ServerHandle::new(&config, store, None, replid, Some(repl_backlog)).await.unwrap()
}

// ========= String Commands =========
#[tokio::test]
async fn test_ping() {
    let port = find_free_port();
    let handle = spawn_server(port).await;
    tokio::spawn(async move { handle.accept_loop().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    let resp = send_cmd(&mut stream, &["PING"]).await;
    assert!(String::from_utf8_lossy(&resp).contains("PONG"));
}

#[tokio::test]
async fn test_set_get_roundtrip() {
    let port = find_free_port();
    let handle = spawn_server(port).await;
    tokio::spawn(async move { handle.accept_loop().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    let resp = send_cmd(&mut stream, &["SET", "key", "value"]).await;
    assert!(String::from_utf8_lossy(&resp).contains("OK"));

    let resp = send_cmd(&mut stream, &["GET", "key"]).await;
    assert!(String::from_utf8_lossy(&resp).contains("value"));
}

#[tokio::test]
async fn test_get_nonexistent() {
    let port = find_free_port();
    let handle = spawn_server(port).await;
    tokio::spawn(async move { handle.accept_loop().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    let resp = send_cmd(&mut stream, &["GET", "nx"]).await;
    assert!(String::from_utf8_lossy(&resp).contains("$-1"));
}

#[tokio::test]
async fn test_mget_mset() {
    let port = find_free_port();
    let handle = spawn_server(port).await;
    tokio::spawn(async move { handle.accept_loop().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    send_cmd(&mut stream, &["MSET", "k1", "v1", "k2", "v2"]).await;
    let resp = send_cmd(&mut stream, &["MGET", "k1", "k2", "k3"]).await;
    let s = String::from_utf8_lossy(&resp);
    assert!(s.contains("v1"));
    assert!(s.contains("v2"));
    assert!(s.contains("$-1"));
}

#[tokio::test]
async fn test_incr_decr() {
    let port = find_free_port();
    let handle = spawn_server(port).await;
    tokio::spawn(async move { handle.accept_loop().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    let resp = send_cmd(&mut stream, &["INCR", "counter"]).await;
    assert!(String::from_utf8_lossy(&resp).contains(":1"));

    let resp = send_cmd(&mut stream, &["INCRBY", "counter", "4"]).await;
    assert!(String::from_utf8_lossy(&resp).contains(":5"));

    let resp = send_cmd(&mut stream, &["DECR", "counter"]).await;
    assert!(String::from_utf8_lossy(&resp).contains(":4"));

    let resp = send_cmd(&mut stream, &["DECRBY", "counter", "2"]).await;
    assert!(String::from_utf8_lossy(&resp).contains(":2"));
}

#[tokio::test]
async fn test_append_strlen() {
    let port = find_free_port();
    let handle = spawn_server(port).await;
    tokio::spawn(async move { handle.accept_loop().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    send_cmd(&mut stream, &["SET", "s", "hello"]).await;
    let resp = send_cmd(&mut stream, &["APPEND", "s", " world"]).await;
    assert!(String::from_utf8_lossy(&resp).contains(":11"));

    let resp = send_cmd(&mut stream, &["STRLEN", "s"]).await;
    assert!(String::from_utf8_lossy(&resp).contains(":11"));
}

#[tokio::test]
async fn test_getset() {
    let port = find_free_port();
    let handle = spawn_server(port).await;
    tokio::spawn(async move { handle.accept_loop().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    let resp = send_cmd(&mut stream, &["GETSET", "key", "new"]).await;
    assert!(String::from_utf8_lossy(&resp).contains("$-1"));

    let resp = send_cmd(&mut stream, &["GET", "key"]).await;
    assert!(String::from_utf8_lossy(&resp).contains("new"));
}

#[tokio::test]
async fn test_getrange_setrange() {
    let port = find_free_port();
    let handle = spawn_server(port).await;
    tokio::spawn(async move { handle.accept_loop().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    send_cmd(&mut stream, &["SET", "s", "hello world"]).await;
    let resp = send_cmd(&mut stream, &["GETRANGE", "s", "0", "4"]).await;
    assert!(String::from_utf8_lossy(&resp).contains("hello"));

    send_cmd(&mut stream, &["SETRANGE", "s", "6", "velo"]).await;
    let resp = send_cmd(&mut stream, &["GET", "s"]).await;
    assert!(String::from_utf8_lossy(&resp).contains("hello velod"));
}

#[tokio::test]
async fn test_set_with_expire() {
    let port = find_free_port();
    let handle = spawn_server(port).await;
    tokio::spawn(async move { handle.accept_loop().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    // Use PX for millisecond precision
    send_cmd(&mut stream, &["SET", "ek", "val", "PX", "500"]).await;
    let resp = send_cmd(&mut stream, &["GET", "ek"]).await;
    assert!(String::from_utf8_lossy(&resp).contains("val"));

    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
    let resp = send_cmd(&mut stream, &["GET", "ek"]).await;
    assert!(String::from_utf8_lossy(&resp).contains("$-1"));
}

// ========= Generic Commands =========
#[tokio::test]
async fn test_del_exists() {
    let port = find_free_port();
    let handle = spawn_server(port).await;
    tokio::spawn(async move { handle.accept_loop().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    send_cmd(&mut stream, &["SET", "a", "1"]).await;
    send_cmd(&mut stream, &["SET", "b", "2"]).await;

    let resp = send_cmd(&mut stream, &["EXISTS", "a", "b", "c"]).await;
    assert!(String::from_utf8_lossy(&resp).contains(":2"));

    let resp = send_cmd(&mut stream, &["DEL", "a", "b"]).await;
    assert!(String::from_utf8_lossy(&resp).contains(":2"));

    let resp = send_cmd(&mut stream, &["EXISTS", "a"]).await;
    assert!(String::from_utf8_lossy(&resp).contains(":0"));
}

#[tokio::test]
async fn test_expire_ttl() {
    let port = find_free_port();
    let handle = spawn_server(port).await;
    tokio::spawn(async move { handle.accept_loop().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    send_cmd(&mut stream, &["SET", "k", "v"]).await;

    let resp = send_cmd(&mut stream, &["TTL", "k"]).await;
    assert!(String::from_utf8_lossy(&resp).contains(":-1"));

    send_cmd(&mut stream, &["EXPIRE", "k", "100"]).await;
    let resp = send_cmd(&mut stream, &["TTL", "k"]).await;
    assert!(!String::from_utf8_lossy(&resp).contains(":-1"));

    send_cmd(&mut stream, &["PERSIST", "k"]).await;
    let resp = send_cmd(&mut stream, &["TTL", "k"]).await;
    assert!(String::from_utf8_lossy(&resp).contains(":-1"));
}

#[tokio::test]
async fn test_keys_and_dbsize() {
    let port = find_free_port();
    let handle = spawn_server(port).await;
    tokio::spawn(async move { handle.accept_loop().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    send_cmd(&mut stream, &["SET", "aa", "1"]).await;
    send_cmd(&mut stream, &["SET", "ab", "2"]).await;
    send_cmd(&mut stream, &["SET", "ba", "3"]).await;

    let resp = send_cmd(&mut stream, &["DBSIZE"]).await;
    assert!(String::from_utf8_lossy(&resp).contains(":3"));

    let resp = send_cmd(&mut stream, &["KEYS", "a*"]).await;
    let s = String::from_utf8_lossy(&resp);
    assert!(s.contains("aa") && s.contains("ab") && !s.contains("ba"));
}

#[tokio::test]
async fn test_rename() {
    let port = find_free_port();
    let handle = spawn_server(port).await;
    tokio::spawn(async move { handle.accept_loop().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    send_cmd(&mut stream, &["SET", "old", "val"]).await;
    send_cmd(&mut stream, &["RENAME", "old", "new"]).await;
    let resp = send_cmd(&mut stream, &["GET", "new"]).await;
    assert!(String::from_utf8_lossy(&resp).contains("val"));
    let resp = send_cmd(&mut stream, &["GET", "old"]).await;
    assert!(String::from_utf8_lossy(&resp).contains("$-1"));
}

#[tokio::test]
async fn test_type_cmd() {
    let port = find_free_port();
    let handle = spawn_server(port).await;
    tokio::spawn(async move { handle.accept_loop().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    send_cmd(&mut stream, &["SET", "s", "v"]).await;
    let resp = send_cmd(&mut stream, &["TYPE", "s"]).await;
    assert!(String::from_utf8_lossy(&resp).contains("string"));

    let resp = send_cmd(&mut stream, &["TYPE", "nx"]).await;
    assert!(String::from_utf8_lossy(&resp).contains("none"));
}

#[tokio::test]
async fn test_select_isolation() {
    let port = find_free_port();
    let handle = spawn_server(port).await;
    tokio::spawn(async move { handle.accept_loop().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut s1 = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    let mut s2 = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();

    send_cmd(&mut s1, &["SELECT", "0"]).await;
    send_cmd(&mut s2, &["SELECT", "1"]).await;
    send_cmd(&mut s1, &["SET", "k", "db0"]).await;
    send_cmd(&mut s2, &["SET", "k", "db1"]).await;

    let resp = send_cmd(&mut s1, &["GET", "k"]).await;
    assert!(String::from_utf8_lossy(&resp).contains("db0"));
    let resp = send_cmd(&mut s2, &["GET", "k"]).await;
    assert!(String::from_utf8_lossy(&resp).contains("db1"));
}

#[tokio::test]
async fn test_wrongtype_error() {
    let port = find_free_port();
    let handle = spawn_server(port).await;
    tokio::spawn(async move { handle.accept_loop().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    send_cmd(&mut stream, &["LPUSH", "l", "a"]).await;
    let resp = send_cmd(&mut stream, &["GET", "l"]).await;
    assert!(String::from_utf8_lossy(&resp).contains("WRONGTYPE"));
}

// ========= List Commands =========
#[tokio::test]
async fn test_list_operations() {
    let port = find_free_port();
    let handle = spawn_server(port).await;
    tokio::spawn(async move { handle.accept_loop().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    send_cmd(&mut stream, &["LPUSH", "l", "b"]).await;
    send_cmd(&mut stream, &["LPUSH", "l", "a"]).await;
    send_cmd(&mut stream, &["RPUSH", "l", "c"]).await;

    let resp = send_cmd(&mut stream, &["LRANGE", "l", "0", "-1"]).await;
    let s = String::from_utf8_lossy(&resp);
    assert!(s.contains("a") && s.contains("b") && s.contains("c"));

    let resp = send_cmd(&mut stream, &["LLEN", "l"]).await;
    assert!(String::from_utf8_lossy(&resp).contains(":3"));

    let resp = send_cmd(&mut stream, &["LPOP", "l"]).await;
    assert!(String::from_utf8_lossy(&resp).contains("a"));
    let resp = send_cmd(&mut stream, &["RPOP", "l"]).await;
    assert!(String::from_utf8_lossy(&resp).contains("c"));
}

#[tokio::test]
async fn test_lindex_lset_ltrim() {
    let port = find_free_port();
    let handle = spawn_server(port).await;
    tokio::spawn(async move { handle.accept_loop().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    send_cmd(&mut stream, &["RPUSH", "l", "a", "b", "c", "d"]).await;

    let resp = send_cmd(&mut stream, &["LINDEX", "l", "1"]).await;
    assert!(String::from_utf8_lossy(&resp).contains("b"));

    send_cmd(&mut stream, &["LSET", "l", "1", "x"]).await;
    let resp = send_cmd(&mut stream, &["LINDEX", "l", "1"]).await;
    assert!(String::from_utf8_lossy(&resp).contains("x"));

    send_cmd(&mut stream, &["LTRIM", "l", "1", "2"]).await;
    let resp = send_cmd(&mut stream, &["LLEN", "l"]).await;
    assert!(String::from_utf8_lossy(&resp).contains(":2"));
}

// ========= Set Commands =========
#[tokio::test]
async fn test_set_operations() {
    let port = find_free_port();
    let handle = spawn_server(port).await;
    tokio::spawn(async move { handle.accept_loop().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    let resp = send_cmd(&mut stream, &["SADD", "s", "a", "b", "c"]).await;
    assert!(String::from_utf8_lossy(&resp).contains(":3"));

    let resp = send_cmd(&mut stream, &["SADD", "s", "a"]).await;
    assert!(String::from_utf8_lossy(&resp).contains(":0"));

    let resp = send_cmd(&mut stream, &["SCARD", "s"]).await;
    assert!(String::from_utf8_lossy(&resp).contains(":3"));

    let resp = send_cmd(&mut stream, &["SISMEMBER", "s", "a"]).await;
    assert!(String::from_utf8_lossy(&resp).contains(":1"));
    let resp = send_cmd(&mut stream, &["SISMEMBER", "s", "x"]).await;
    assert!(String::from_utf8_lossy(&resp).contains(":0"));

    send_cmd(&mut stream, &["SADD", "s2", "b", "c", "d"]).await;
    let resp = send_cmd(&mut stream, &["SINTER", "s", "s2"]).await;
    let s = String::from_utf8_lossy(&resp);
    assert!(s.contains("b") && s.contains("c") && !s.contains("a"));

    let resp = send_cmd(&mut stream, &["SUNION", "s", "s2"]).await;
    let s = String::from_utf8_lossy(&resp);
    assert!(s.contains("a") && s.contains("d"));

    let resp = send_cmd(&mut stream, &["SDIFF", "s", "s2"]).await;
    let s = String::from_utf8_lossy(&resp);
    assert!(s.contains("a") && !s.contains("b"));
}

// ========= Hash Commands =========
#[tokio::test]
async fn test_hash_operations() {
    let port = find_free_port();
    let handle = spawn_server(port).await;
    tokio::spawn(async move { handle.accept_loop().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    let resp = send_cmd(&mut stream, &["HSET", "h", "f1", "v1", "f2", "v2"]).await;
    assert!(String::from_utf8_lossy(&resp).contains(":2"));

    let resp = send_cmd(&mut stream, &["HGET", "h", "f1"]).await;
    assert!(String::from_utf8_lossy(&resp).contains("v1"));

    let resp = send_cmd(&mut stream, &["HEXISTS", "h", "f1"]).await;
    assert!(String::from_utf8_lossy(&resp).contains(":1"));

    let resp = send_cmd(&mut stream, &["HLEN", "h"]).await;
    assert!(String::from_utf8_lossy(&resp).contains(":2"));

    let resp = send_cmd(&mut stream, &["HINCRBY", "h", "counter", "5"]).await;
    assert!(String::from_utf8_lossy(&resp).contains(":5"));
}

// ========= ZSet Commands =========
#[tokio::test]
async fn test_zset_operations() {
    let port = find_free_port();
    let handle = spawn_server(port).await;
    tokio::spawn(async move { handle.accept_loop().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    let resp = send_cmd(&mut stream, &["ZADD", "z", "1.0", "a", "2.0", "b", "0.5", "c"]).await;
    assert!(String::from_utf8_lossy(&resp).contains(":3"));

    let resp = send_cmd(&mut stream, &["ZCARD", "z"]).await;
    assert!(String::from_utf8_lossy(&resp).contains(":3"));

    let resp = send_cmd(&mut stream, &["ZSCORE", "z", "a"]).await;
    assert!(String::from_utf8_lossy(&resp).contains("1"));

    let resp = send_cmd(&mut stream, &["ZRANK", "z", "a"]).await;
    assert!(String::from_utf8_lossy(&resp).contains(":1"));
    let resp = send_cmd(&mut stream, &["ZRANK", "z", "c"]).await;
    assert!(String::from_utf8_lossy(&resp).contains(":0"));

    let resp = send_cmd(&mut stream, &["ZRANGE", "z", "0", "-1"]).await;
    let s = String::from_utf8_lossy(&resp);
    assert!(s.find("c").unwrap() < s.find("a").unwrap());
    assert!(s.find("a").unwrap() < s.find("b").unwrap());

    let resp = send_cmd(&mut stream, &["ZRANGEBYSCORE", "z", "1", "2"]).await;
    let s = String::from_utf8_lossy(&resp);
    assert!(s.contains("a") && s.contains("b"));
    assert!(!s.contains("c"));

    let resp = send_cmd(&mut stream, &["ZCOUNT", "z", "1", "2"]).await;
    assert!(String::from_utf8_lossy(&resp).contains(":2"));
}

// ========= Stream Commands =========
#[tokio::test]
async fn test_stream_operations() {
    let port = find_free_port();
    let handle = spawn_server(port).await;
    tokio::spawn(async move { handle.accept_loop().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    let resp = send_cmd(&mut stream, &["XADD", "s", "*", "f1", "v1"]).await;
    let s = String::from_utf8_lossy(&resp);
    assert!(s.contains('-'));

    let resp = send_cmd(&mut stream, &["XLEN", "s"]).await;
    assert!(String::from_utf8_lossy(&resp).contains(":1"));

    let resp = send_cmd(&mut stream, &["XRANGE", "s", "-", "+"]).await;
    assert!(String::from_utf8_lossy(&resp).contains("v1"));
}

// ========= NestedHash Commands =========
#[tokio::test]
async fn test_nested_hash_operations() {
    let port = find_free_port();
    let handle = spawn_server(port).await;
    tokio::spawn(async move { handle.accept_loop().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    let resp = send_cmd(&mut stream, &["NHSET", "nh", "f1", "sf1", "v1"]).await;
    assert!(String::from_utf8_lossy(&resp).contains(":1"));

    let resp = send_cmd(&mut stream, &["NHGET", "nh", "f1", "sf1"]).await;
    assert!(String::from_utf8_lossy(&resp).contains("v1"));

    let resp = send_cmd(&mut stream, &["NHKEYS", "nh"]).await;
    assert!(String::from_utf8_lossy(&resp).contains("f1"));

    let resp = send_cmd(&mut stream, &["NHKEYS", "nh", "f1"]).await;
    assert!(String::from_utf8_lossy(&resp).contains("sf1"));
}

// ========= Pipeline & Protocol =========
#[tokio::test]
async fn test_pipeline() {
    let port = find_free_port();
    let handle = spawn_server(port).await;
    tokio::spawn(async move { handle.accept_loop().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    // Send 3 commands at once
    let pipelined = encode_command(&["SET", "a", "1"])
        .into_iter()
        .chain(encode_command(&["SET", "b", "2"]))
        .chain(encode_command(&["GET", "a"]));
    stream.write_all(&pipelined.collect::<Vec<_>>()).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let mut buf = vec![0u8; 65536];
    let n = stream.try_read(&mut buf).unwrap_or(0);
    let all = String::from_utf8_lossy(&buf[..n]);
    assert!(all.contains("OK"));
    assert!(all.contains("OK"));
    assert!(all.contains("1"));
}

#[tokio::test]
async fn test_echo() {
    let port = find_free_port();
    let handle = spawn_server(port).await;
    tokio::spawn(async move { handle.accept_loop().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    let resp = send_cmd(&mut stream, &["ECHO", "hello world"]).await;
    assert!(String::from_utf8_lossy(&resp).contains("hello world"));
}

#[tokio::test]
async fn test_unknown_command() {
    let port = find_free_port();
    let handle = spawn_server(port).await;
    tokio::spawn(async move { handle.accept_loop().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    let resp = send_cmd(&mut stream, &["BOGUS", "arg"]).await;
    assert!(String::from_utf8_lossy(&resp).contains("ERR unknown command"));
}

#[tokio::test]
async fn test_wrong_number_of_args() {
    let port = find_free_port();
    let handle = spawn_server(port).await;
    tokio::spawn(async move { handle.accept_loop().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    let resp = send_cmd(&mut stream, &["GET"]).await;
    assert!(String::from_utf8_lossy(&resp).contains("ERR wrong number of arguments"));
}

// ========= PubSub tests =========
#[tokio::test]
async fn test_pubsub_subscribe_publish() {
    let port = find_free_port();
    let handle = spawn_server(port).await;
    tokio::spawn(async move { handle.accept_loop().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut pubber = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    let mut subber = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();

    let resp = send_cmd(&mut subber, &["SUBSCRIBE", "news"]).await;
    assert!(String::from_utf8_lossy(&resp).contains("subscribe"));

    let resp = send_cmd(&mut pubber, &["PUBLISH", "news", "hello"]).await;
    assert!(String::from_utf8_lossy(&resp).contains(":1"));
}

#[tokio::test]
async fn test_multi_exec() {
    let port = find_free_port();
    let handle = spawn_server(port).await;
    tokio::spawn(async move { handle.accept_loop().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    send_cmd(&mut stream, &["MULTI"]).await;
    send_cmd(&mut stream, &["SET", "a", "1"]).await;
    send_cmd(&mut stream, &["SET", "b", "2"]).await;
    let resp = send_cmd(&mut stream, &["EXEC"]).await;
    let s = String::from_utf8_lossy(&resp);
    assert!(s.contains("OK"));
}

#[tokio::test]
async fn test_watch_exec_conflict() {
    let port = find_free_port();
    let handle = spawn_server(port).await;
    tokio::spawn(async move { handle.accept_loop().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut s1 = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    let mut s2 = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();

    send_cmd(&mut s1, &["SET", "k", "1"]).await;
    send_cmd(&mut s1, &["WATCH", "k"]).await;
    send_cmd(&mut s1, &["MULTI"]).await;
    send_cmd(&mut s1, &["SET", "k", "2"]).await;
    // s2 modifies watched key
    send_cmd(&mut s2, &["SET", "k", "3"]).await;
    let resp = send_cmd(&mut s1, &["EXEC"]).await;
    assert!(String::from_utf8_lossy(&resp).contains("*-1"));
}

#[tokio::test]
async fn test_discard() {
    let port = find_free_port();
    let handle = spawn_server(port).await;
    tokio::spawn(async move { handle.accept_loop().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    send_cmd(&mut stream, &["MULTI"]).await;
    send_cmd(&mut stream, &["SET", "x", "1"]).await;
    let resp = send_cmd(&mut stream, &["DISCARD"]).await;
    assert!(String::from_utf8_lossy(&resp).contains("OK"));
    let resp = send_cmd(&mut stream, &["GET", "x"]).await;
    assert!(String::from_utf8_lossy(&resp).contains("$-1"));
}

// ========= Lua Scripting tests =========
#[tokio::test]
async fn test_eval_return_int() {
    let port = find_free_port();
    let handle = spawn_server(port).await;
    tokio::spawn(async move { handle.accept_loop().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    let resp = send_cmd(&mut stream, &["EVAL", "return 42", "0"]).await;
    assert!(String::from_utf8_lossy(&resp).contains(":42"));
}

#[tokio::test]
async fn test_eval_return_string() {
    let port = find_free_port();
    let handle = spawn_server(port).await;
    tokio::spawn(async move { handle.accept_loop().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    let resp = send_cmd(&mut stream, &["EVAL", "return 'hello'", "0"]).await;
    assert!(String::from_utf8_lossy(&resp).contains("hello"));
}

#[tokio::test]
async fn test_eval_with_keys_and_args() {
    let port = find_free_port();
    let handle = spawn_server(port).await;
    tokio::spawn(async move { handle.accept_loop().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    // Lua with KEYS array access (index is 1-based in Lua)
    let resp = send_cmd(&mut stream, &["EVAL", "return KEYS[1] .. ARGV[1]", "1", "key1", "arg1"]).await;
    assert!(String::from_utf8_lossy(&resp).contains("key1arg1"));
}

#[tokio::test]
async fn test_eval_return_table() {
    let port = find_free_port();
    let handle = spawn_server(port).await;
    tokio::spawn(async move { handle.accept_loop().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    let resp = send_cmd(&mut stream, &["EVAL", "return {1, 2, 3}", "0"]).await;
    // Table responses come back as +OK from lua_to_resp
    assert!(String::from_utf8_lossy(&resp).contains("+OK"));
}

#[tokio::test]
async fn test_eval_error() {
    let port = find_free_port();
    let handle = spawn_server(port).await;
    tokio::spawn(async move { handle.accept_loop().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    let resp = send_cmd(&mut stream, &["EVAL", "error('oops')", "0"]).await;
    assert!(String::from_utf8_lossy(&resp).contains("ERR"));
}

#[tokio::test]
async fn test_script_load_exists_flush() {
    let port = find_free_port();
    let handle = spawn_server(port).await;
    tokio::spawn(async move { handle.accept_loop().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    let resp = send_cmd(&mut stream, &["SCRIPT", "LOAD", "return 42"]).await;
    let s = String::from_utf8_lossy(&resp);
    // Extract SHA from bulk string response: $40\r\n<sha>\r\n
    let sha: String = s.lines()
        .filter(|l| !l.starts_with('$') && !l.is_empty())
        .next()
        .unwrap_or("")
        .to_string();
    assert!(!sha.is_empty());

    let resp = send_cmd(&mut stream, &["SCRIPT", "EXISTS", &sha]).await;
    assert!(String::from_utf8_lossy(&resp).contains(":1"));

    let resp = send_cmd(&mut stream, &["SCRIPT", "FLUSH"]).await;
    assert!(String::from_utf8_lossy(&resp).contains("OK"));

    let resp = send_cmd(&mut stream, &["SCRIPT", "EXISTS", &sha]).await;
    assert!(String::from_utf8_lossy(&resp).contains(":0"));
}

#[tokio::test]
async fn test_info_command() {
    let port = find_free_port();
    let handle = spawn_server(port).await;
    tokio::spawn(async move { handle.accept_loop().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    let resp = send_cmd(&mut stream, &["INFO"]).await;
    let s = String::from_utf8_lossy(&resp);
    assert!(s.contains("velodb_version"));
    assert!(s.contains("uptime_in_seconds"));
}

#[tokio::test]
async fn test_config_get() {
    let port = find_free_port();
    let handle = spawn_server(port).await;
    tokio::spawn(async move { handle.accept_loop().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    let resp = send_cmd(&mut stream, &["CONFIG", "GET", "*"]).await;
    let s = String::from_utf8_lossy(&resp);
    assert!(s.contains("port"));
    assert!(s.contains("databases"));
}
#[tokio::test]
async fn test_rdb_save_and_load() {
    use std::fs;
    use velodb::store::Store;
    use velodb::persist::rdb;

    let dir = tempfile::tempdir().unwrap();
    let rdb_path = dir.path().join("dump.rdb");
    let store = std::sync::Arc::new(Store::new(2));

    store.set(0, b"str", b"value", None).unwrap();
    store.lpush(0, b"lst", &[b"a".to_vec(), b"b".to_vec()]).unwrap();
    store.sadd(0, b"set", &[b"x".to_vec()]).unwrap();
    store.hset(0, b"hash", &[(b"f".to_vec(), b"v".to_vec())]).unwrap();
    store.zadd(0, b"zset", &[(1.0, b"m".to_vec())]).unwrap();

    rdb::save_rdb(&store, &rdb_path, 2).unwrap();
    assert!(rdb_path.exists());

    let fresh_store = std::sync::Arc::new(Store::new(2));
    let count = rdb::load_rdb(&fresh_store, &rdb_path).unwrap();
    assert!(count >= 5);

    assert_eq!(fresh_store.get(0, b"str").unwrap().unwrap(), b"value");
    assert!(!fresh_store.smembers(0, b"set").unwrap().is_empty());
    assert_eq!(fresh_store.hget(0, b"hash", b"f").unwrap(), Some(b"v".to_vec()));
}

#[tokio::test]
async fn test_aof_append_and_replay() {
    use std::fs;
    use std::sync::Arc;
    use velodb::store::Store;
    use velodb::persist::aof::{AofWriter, FsyncPolicy, load_aof, encode_command_for_aof};

    let dir = tempfile::tempdir().unwrap();
    let aof_path = dir.path().join("test.aof");
    let aof = Arc::new(AofWriter::open(aof_path.clone(), FsyncPolicy::Always).unwrap());

    aof.append(&encode_command_for_aof(&[b"SET".to_vec(), b"k".to_vec(), b"v".to_vec()])).unwrap();
    aof.append(&encode_command_for_aof(&[b"EXPIRE".to_vec(), b"k".to_vec(), b"1000".to_vec()])).unwrap();
    aof.sync().unwrap();

    let commands = load_aof(&aof_path).unwrap();
    assert!(commands.len() >= 2);
}

#[tokio::test]
async fn test_persistence_with_expire() {
    use std::fs;
    use std::sync::Arc;
    use velodb::store::Store;
    use velodb::persist::rdb;
    use std::time::{SystemTime, UNIX_EPOCH};

    let dir = tempfile::tempdir().unwrap();
    let rdb_path = dir.path().join("dump.rdb");
    let store = Arc::new(Store::new(1));

    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
    store.set(0, b"k1", b"v1", Some(now + 100000)).unwrap();

    rdb::save_rdb(&store, &rdb_path, 1).unwrap();

    let fresh = Arc::new(Store::new(1));
    rdb::load_rdb(&fresh, &rdb_path).unwrap();
    assert!(fresh.exists(0, b"k1").unwrap());
}

#[tokio::test]
async fn test_aof_logging_on_write() {
    use std::fs;
    use std::sync::Arc;
    use velodb::store::Store;
    use velodb::persist::aof::{AofWriter, FsyncPolicy, encode_command_for_aof};

    let dir = tempfile::tempdir().unwrap();
    let aof_path = dir.path().join("write_test.aof");
    let aof = Arc::new(AofWriter::open(aof_path.clone(), FsyncPolicy::Always).unwrap());

    let store = Arc::new(Store::new(1));
    store.set(0, b"key", b"val", None).unwrap();
    aof.append(&encode_command_for_aof(&[b"SET".to_vec(), b"key".to_vec(), b"val".to_vec()])).unwrap();
    aof.sync().unwrap();

    let content = fs::read_to_string(&aof_path).unwrap();
    assert!(content.contains("SET"));
    assert!(content.contains("key"));
    assert!(content.contains("val"));
}


// ========= SCAN family =========
#[tokio::test]
async fn test_scan_full_iteration() {
    let port = find_free_port();
    let handle = spawn_server(port).await;
    tokio::spawn(async move { handle.accept_loop().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    for i in 0..15 {
        send_cmd(&mut stream, &["SET", &format!("key{:02}", i), "v"]).await;
    }

    let mut seen = std::collections::HashSet::new();
    let mut cursor = "0".to_string();
    loop {
        let resp = send_cmd(&mut stream, &["SCAN", &cursor, "COUNT", "5"]).await;
        let s = String::from_utf8_lossy(&resp).to_string();
        // First bulk string is the next cursor
        let mut lines = s.split("\r\n");
        let mut next = String::new();
        let mut expecting_cursor = false;
        for line in lines.by_ref() {
            if expecting_cursor { next = line.to_string(); break; }
            if line.starts_with('$') { expecting_cursor = true; }
        }
        for line in lines {
            if line.starts_with("key") { seen.insert(line.to_string()); }
        }
        if next == "0" { break; }
        cursor = next;
    }
    assert_eq!(seen.len(), 15, "SCAN must return all 15 keys, got {:?}", seen);
}

#[tokio::test]
async fn test_scan_match() {
    let port = find_free_port();
    let handle = spawn_server(port).await;
    tokio::spawn(async move { handle.accept_loop().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    send_cmd(&mut stream, &["SET", "user:1", "a"]).await;
    send_cmd(&mut stream, &["SET", "order:1", "b"]).await;

    let resp = send_cmd(&mut stream, &["SCAN", "0", "MATCH", "user:*"]).await;
    let s = String::from_utf8_lossy(&resp);
    assert!(s.contains("user:1"));
    assert!(!s.contains("order:1"));
}

#[tokio::test]
async fn test_hscan_sscan_zscan() {
    let port = find_free_port();
    let handle = spawn_server(port).await;
    tokio::spawn(async move { handle.accept_loop().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    send_cmd(&mut stream, &["HSET", "h", "f1", "v1"]).await;
    let resp = send_cmd(&mut stream, &["HSCAN", "h", "0"]).await;
    let s = String::from_utf8_lossy(&resp);
    assert!(s.contains("f1") && s.contains("v1"), "HSCAN response: {}", s);

    send_cmd(&mut stream, &["SADD", "s", "m1"]).await;
    let resp = send_cmd(&mut stream, &["SSCAN", "s", "0"]).await;
    assert!(String::from_utf8_lossy(&resp).contains("m1"));

    send_cmd(&mut stream, &["ZADD", "z", "1.5", "mem"]).await;
    let resp = send_cmd(&mut stream, &["ZSCAN", "z", "0"]).await;
    let s = String::from_utf8_lossy(&resp);
    assert!(s.contains("mem") && s.contains("1.5"), "ZSCAN response: {}", s);
}

#[tokio::test]
async fn test_scan_invalid_cursor_errors() {
    let port = find_free_port();
    let handle = spawn_server(port).await;
    tokio::spawn(async move { handle.accept_loop().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    let resp = send_cmd(&mut stream, &["SCAN", "notanumber"]).await;
    assert!(String::from_utf8_lossy(&resp).contains("ERR invalid cursor"));
}

// ========= BGREWRITEAOF =========
#[tokio::test]
async fn test_bgrewriteaof_errors_without_aof() {
    let port = find_free_port();
    let handle = spawn_server(port).await;
    tokio::spawn(async move { handle.accept_loop().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    let resp = send_cmd(&mut stream, &["BGREWRITEAOF"]).await;
    assert!(String::from_utf8_lossy(&resp).contains("ERR AOF is not enabled"));
}

// ========= Replication (master/replica end-to-end) =========
#[tokio::test]
async fn test_replica_full_sync_then_live_streaming() {
    use velodb::store::Store;
    use velodb::cmd::CommandTable;
    use velodb::replication::replica::{connect_to_master, ReplicaSyncState};

    let master_port = find_free_port();
    let master = spawn_server(master_port).await;
    let master_store = master.store.clone();
    tokio::spawn(async move { master.accept_loop().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Seed data on the master BEFORE the replica connects, so the
    // initial RDB transfer must carry it.
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", master_port)).await.unwrap();
    send_cmd(&mut stream, &["SET", "preexisting", "value1"]).await;

    let replica_store = Arc::new(Store::new(4));
    let cmd_table = Arc::new(CommandTable::new());
    let sync_state = Arc::new(std::sync::Mutex::new(ReplicaSyncState::default()));

    let rs = replica_store.clone();
    let ss = sync_state.clone();
    tokio::spawn(async move {
        let _ = connect_to_master(rs, cmd_table, "127.0.0.1", master_port, ss).await;
    });

    // Wait for full sync to land.
    let got = poll_until(std::time::Duration::from_secs(3), || replica_store.get(0, b"preexisting").ok().flatten()).await;
    assert_eq!(got.unwrap(), b"value1");

    // Now a live write on the master must propagate to the replica
    // without a reconnect.
    send_cmd(&mut stream, &["SET", "livewrite", "value2"]).await;
    let got = poll_until(std::time::Duration::from_secs(3), || replica_store.get(0, b"livewrite").ok().flatten()).await;
    assert_eq!(got.unwrap(), b"value2", "live write must stream to replica");

    let _ = master_store; // keep master_store alive reference for clarity
    let s = sync_state.lock().unwrap();
    assert_eq!(s.replid.len(), 40, "replid must be captured from FULLRESYNC");
    assert!(s.offset > 0);
}

#[tokio::test]
async fn test_replica_reconnect_partial_sync_no_data_loss() {
    use velodb::store::Store;
    use velodb::cmd::CommandTable;
    use velodb::replication::replica::{connect_to_master, ReplicaSyncState};

    let master_port = find_free_port();
    let master = spawn_server(master_port).await;
    tokio::spawn(async move { master.accept_loop().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", master_port)).await.unwrap();

    let replica_store = Arc::new(Store::new(4));
    let cmd_table = Arc::new(CommandTable::new());
    let sync_state = Arc::new(std::sync::Mutex::new(ReplicaSyncState::default()));

    // First connection: full sync.
    {
        let rs = replica_store.clone();
        let ct = cmd_table.clone();
        let ss = sync_state.clone();
        tokio::spawn(async move {
            let _ = connect_to_master(rs, ct, "127.0.0.1", master_port, ss).await;
        });
    }
    send_cmd(&mut stream, &["SET", "beforereconnect", "a"]).await;
    let got = poll_until(std::time::Duration::from_secs(3), || replica_store.get(0, b"beforereconnect").ok().flatten()).await;
    assert_eq!(got.unwrap(), b"a");

    let replid_after_first = sync_state.lock().unwrap().replid.clone();

    // Second connection with the remembered sync_state: must be served
    // as a partial resync (+CONTINUE), not a fresh FULLRESYNC.
    {
        let rs = replica_store.clone();
        let ct = cmd_table.clone();
        let ss = sync_state.clone();
        tokio::spawn(async move {
            let _ = connect_to_master(rs, ct, "127.0.0.1", master_port, ss).await;
        });
    }
    // Give the reconnect a moment to complete its handshake before the
    // next write, purely to keep the scenario deterministic; correctness
    // is still verified via poll_until below, not by this delay.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    send_cmd(&mut stream, &["SET", "afterreconnect", "b"]).await;
    let got = poll_until(std::time::Duration::from_secs(3), || replica_store.get(0, b"afterreconnect").ok().flatten()).await;

    assert_eq!(got.unwrap(), b"b", "writes after reconnect must still replicate");
    assert_eq!(replica_store.get(0, b"beforereconnect").unwrap().unwrap(), b"a", "data from before reconnect must not be lost");
    assert_eq!(sync_state.lock().unwrap().replid, replid_after_first, "replid must be stable across reconnects (same master)");
}

#[tokio::test]
async fn test_concurrent_full_syncs_do_not_corrupt_each_other() {
    // Regression test: full-sync temp RDB paths must be unique per
    // operation, not just per-process, or concurrent syncs against the
    // same master (or concurrently running test cases) collide on the
    // same file and corrupt each other's transfer.
    use velodb::store::Store;
    use velodb::cmd::CommandTable;
    use velodb::replication::replica::{connect_to_master, ReplicaSyncState};

    let master_port = find_free_port();
    let master = spawn_server(master_port).await;
    tokio::spawn(async move { master.accept_loop().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", master_port)).await.unwrap();
    for i in 0..20 {
        send_cmd(&mut stream, &[&format!("SET"), &format!("k{}", i), &format!("v{}", i)]).await;
    }

    let mut handles = vec![];
    let mut replica_stores = vec![];
    for _ in 0..5 {
        let replica_store = Arc::new(Store::new(4));
        let cmd_table = Arc::new(CommandTable::new());
        let sync_state = Arc::new(std::sync::Mutex::new(ReplicaSyncState::default()));
        let rs = replica_store.clone();
        handles.push(tokio::spawn(async move {
            let _ = connect_to_master(rs, cmd_table, "127.0.0.1", master_port, sync_state).await;
        }));
        replica_stores.push(replica_store);
    }

    for rs in &replica_stores {
        let store = rs.clone();
        let got = poll_until(std::time::Duration::from_secs(5), || store.get(0, b"k19").ok().flatten()).await;
        assert_eq!(got, Some(b"v19".to_vec()), "each concurrent replica must receive an uncorrupted full sync");
        for i in 0..20 {
            assert_eq!(store.get(0, format!("k{}", i).as_bytes()).unwrap().unwrap(), format!("v{}", i).as_bytes());
        }
    }
    for h in handles { h.abort(); }
}

// ========= Stream consumer groups =========
#[tokio::test]
async fn test_xgroup_create_and_xreadgroup() {
    let port = find_free_port();
    let handle = spawn_server(port).await;
    tokio::spawn(async move { handle.accept_loop().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    send_cmd(&mut stream, &["XADD", "s", "*", "f", "v1"]).await;
    let resp = send_cmd(&mut stream, &["XGROUP", "CREATE", "s", "g1", "0"]).await;
    assert!(String::from_utf8_lossy(&resp).contains("OK"));

    let resp = send_cmd(&mut stream, &["XREADGROUP", "GROUP", "g1", "c1", "STREAMS", "s", ">"]).await;
    let s = String::from_utf8_lossy(&resp);
    assert!(s.contains("f") && s.contains("v1"), "XREADGROUP response: {}", s);
}

#[tokio::test]
async fn test_xgroup_create_mkstream() {
    let port = find_free_port();
    let handle = spawn_server(port).await;
    tokio::spawn(async move { handle.accept_loop().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    let resp = send_cmd(&mut stream, &["XGROUP", "CREATE", "newstream", "g1", "0"]).await;
    assert!(String::from_utf8_lossy(&resp).contains("ERR") || String::from_utf8_lossy(&resp).to_uppercase().contains("NOGROUP") || String::from_utf8_lossy(&resp).contains("requires"), "expected error without MKSTREAM: {}", String::from_utf8_lossy(&resp));

    let resp = send_cmd(&mut stream, &["XGROUP", "CREATE", "newstream", "g1", "0", "MKSTREAM"]).await;
    assert!(String::from_utf8_lossy(&resp).contains("OK"));
}

#[tokio::test]
async fn test_xack_and_xpending() {
    let port = find_free_port();
    let handle = spawn_server(port).await;
    tokio::spawn(async move { handle.accept_loop().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    send_cmd(&mut stream, &["XADD", "s", "1-1", "f", "v1"]).await;
    send_cmd(&mut stream, &["XGROUP", "CREATE", "s", "g1", "0"]).await;
    send_cmd(&mut stream, &["XREADGROUP", "GROUP", "g1", "c1", "STREAMS", "s", ">"]).await;

    let resp = send_cmd(&mut stream, &["XPENDING", "s", "g1"]).await;
    let s = String::from_utf8_lossy(&resp);
    assert!(s.contains(":1"), "expected pending count 1: {}", s);

    let resp = send_cmd(&mut stream, &["XACK", "s", "g1", "1-1"]).await;
    assert!(String::from_utf8_lossy(&resp).contains(":1"));

    let resp = send_cmd(&mut stream, &["XPENDING", "s", "g1"]).await;
    let s = String::from_utf8_lossy(&resp);
    assert!(s.contains(":0") || s.contains("*0") || s.contains("*-1"), "expected empty PEL after ack: {}", s);
}

#[tokio::test]
async fn test_xclaim_transfers_to_new_consumer() {
    let port = find_free_port();
    let handle = spawn_server(port).await;
    tokio::spawn(async move { handle.accept_loop().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    send_cmd(&mut stream, &["XADD", "s", "1-1", "f", "v1"]).await;
    send_cmd(&mut stream, &["XGROUP", "CREATE", "s", "g1", "0"]).await;
    send_cmd(&mut stream, &["XREADGROUP", "GROUP", "g1", "c1", "STREAMS", "s", ">"]).await;

    let resp = send_cmd(&mut stream, &["XCLAIM", "s", "g1", "c2", "0", "1-1"]).await;
    let s = String::from_utf8_lossy(&resp);
    assert!(s.contains("v1"), "XCLAIM should return the claimed entry: {}", s);

    let resp = send_cmd(&mut stream, &["XPENDING", "s", "g1", "-", "+", "10"]).await;
    let s = String::from_utf8_lossy(&resp);
    assert!(s.contains("c2"), "ownership must show consumer c2 after claim: {}", s);
}

#[tokio::test]
async fn test_xgroup_delconsumer_and_setid() {
    let port = find_free_port();
    let handle = spawn_server(port).await;
    tokio::spawn(async move { handle.accept_loop().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    send_cmd(&mut stream, &["XADD", "s", "1-1", "f", "v1"]).await;
    send_cmd(&mut stream, &["XGROUP", "CREATE", "s", "g1", "0"]).await;
    send_cmd(&mut stream, &["XGROUP", "CREATECONSUMER", "s", "g1", "c1"]).await;

    let resp = send_cmd(&mut stream, &["XGROUP", "DELCONSUMER", "s", "g1", "c1"]).await;
    assert!(String::from_utf8_lossy(&resp).contains(":0"));

    let resp = send_cmd(&mut stream, &["XGROUP", "DESTROY", "s", "g1"]).await;
    assert!(String::from_utf8_lossy(&resp).contains(":1"));
}

// ========= AUTH =========
#[tokio::test]
async fn test_auth_required_when_requirepass_set() {
    let port = find_free_port();
    let handle = spawn_server_with_requirepass(port, "secret").await;
    tokio::spawn(async move { handle.accept_loop().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    let resp = send_cmd(&mut stream, &["GET", "k"]).await;
    assert!(String::from_utf8_lossy(&resp).contains("NOAUTH"), "expected NOAUTH, got: {}", String::from_utf8_lossy(&resp));
}

#[tokio::test]
async fn test_auth_with_correct_password_unlocks_commands() {
    let port = find_free_port();
    let handle = spawn_server_with_requirepass(port, "secret").await;
    tokio::spawn(async move { handle.accept_loop().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    let resp = send_cmd(&mut stream, &["AUTH", "secret"]).await;
    assert!(String::from_utf8_lossy(&resp).contains("OK"));

    let resp = send_cmd(&mut stream, &["SET", "k", "v"]).await;
    assert!(String::from_utf8_lossy(&resp).contains("OK"), "authenticated connection should be able to write: {}", String::from_utf8_lossy(&resp));
}

#[tokio::test]
async fn test_auth_with_wrong_password_rejected() {
    let port = find_free_port();
    let handle = spawn_server_with_requirepass(port, "secret").await;
    tokio::spawn(async move { handle.accept_loop().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    let resp = send_cmd(&mut stream, &["AUTH", "wrong"]).await;
    assert!(String::from_utf8_lossy(&resp).contains("WRONGPASS"), "expected WRONGPASS, got: {}", String::from_utf8_lossy(&resp));

    let resp = send_cmd(&mut stream, &["GET", "k"]).await;
    assert!(String::from_utf8_lossy(&resp).contains("NOAUTH"), "failed AUTH must not unlock the connection");
}

#[tokio::test]
async fn test_auth_without_requirepass_errors() {
    let port = find_free_port();
    let handle = spawn_server(port).await;
    tokio::spawn(async move { handle.accept_loop().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    let resp = send_cmd(&mut stream, &["AUTH", "anything"]).await;
    assert!(String::from_utf8_lossy(&resp).contains("ERR"), "AUTH with no requirepass set must error: {}", String::from_utf8_lossy(&resp));
}

#[tokio::test]
async fn test_ping_allowed_before_auth() {
    let port = find_free_port();
    let handle = spawn_server_with_requirepass(port, "secret").await;
    tokio::spawn(async move { handle.accept_loop().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    let resp = send_cmd(&mut stream, &["PING"]).await;
    assert!(String::from_utf8_lossy(&resp).contains("PONG"), "PING must be allowed pre-auth: {}", String::from_utf8_lossy(&resp));
}

#[tokio::test]
async fn test_no_requirepass_means_no_auth_needed() {
    let port = find_free_port();
    let handle = spawn_server(port).await;
    tokio::spawn(async move { handle.accept_loop().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    let resp = send_cmd(&mut stream, &["SET", "k", "v"]).await;
    assert!(String::from_utf8_lossy(&resp).contains("OK"), "no requirepass configured means commands work without AUTH");
}

#[tokio::test]
async fn test_sharded_server_forwards_requirepass_to_connections() {
    // Regression test: ShardedServer::new took `config: &ServerConfig`
    // but every connection was previously handed `ServerConfig::default()`
    // instead of the real config, so requirepass (and any other
    // per-connection setting) would silently never apply on the actual
    // production server path (ServerHandle is test/library-only).
    use velodb::shard::ShardedServer;
    use velodb::store::Store;
    use velodb::cmd::CommandTable;
    use velodb::replication::backlog::ReplBacklog;

    let port = find_free_port();
    let mut config = ServerConfig::default();
    config.port = port;
    config.databases = 4;
    config.cthreads = 1;
    config.requirepass = Some("shardsecret".to_string());

    let store = Arc::new(Store::new(config.databases));
    let cmd_table = Arc::new(CommandTable::new());
    let repl_backlog = Arc::new(std::sync::Mutex::new(ReplBacklog::new(1048576)));
    let replid = "0".repeat(40);

    let sharded = ShardedServer::new(1, store, cmd_table, &config, None, repl_backlog, &replid);
    let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{}", port)).await.unwrap();
    tokio::spawn(async move { sharded.accept_loop(listener).await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    let resp = send_cmd(&mut stream, &["GET", "k"]).await;
    assert!(String::from_utf8_lossy(&resp).contains("NOAUTH"), "requirepass must reach connections routed through ShardedServer: {}", String::from_utf8_lossy(&resp));
}

// ========= ACL =========
#[tokio::test]
async fn test_acl_whoami_default_user() {
    let port = find_free_port();
    let handle = spawn_server(port).await;
    tokio::spawn(async move { handle.accept_loop().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    let resp = send_cmd(&mut stream, &["ACL", "WHOAMI"]).await;
    assert!(String::from_utf8_lossy(&resp).contains("default"));
}

#[tokio::test]
async fn test_acl_list_and_users() {
    let port = find_free_port();
    let handle = spawn_server(port).await;
    tokio::spawn(async move { handle.accept_loop().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    let resp = send_cmd(&mut stream, &["ACL", "LIST"]).await;
    assert!(String::from_utf8_lossy(&resp).contains("default"));

    let resp = send_cmd(&mut stream, &["ACL", "USERS"]).await;
    assert!(String::from_utf8_lossy(&resp).contains("default"));
}

#[tokio::test]
async fn test_acl_cat_returns_categories() {
    let port = find_free_port();
    let handle = spawn_server(port).await;
    tokio::spawn(async move { handle.accept_loop().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    let resp = send_cmd(&mut stream, &["ACL", "CAT"]).await;
    let s = String::from_utf8_lossy(&resp);
    assert!(s.contains("read") || s.contains("write") || s.contains("keyspace"), "expected known ACL categories: {}", s);
}

// ========= RESP3 / HELLO =========
#[tokio::test]
async fn test_hello_no_args_returns_map() {
    let port = find_free_port();
    let handle = spawn_server(port).await;
    tokio::spawn(async move { handle.accept_loop().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    let resp = send_cmd(&mut stream, &["HELLO"]).await;
    let s = String::from_utf8_lossy(&resp);
    assert!(s.contains("velodb"), "HELLO should describe the server: {}", s);
    assert!(s.starts_with("*"), "HELLO with no protover stays on RESP2 (array), got: {}", s);
}

#[tokio::test]
async fn test_hello_3_switches_to_resp3_map_encoding() {
    let port = find_free_port();
    let handle = spawn_server(port).await;
    tokio::spawn(async move { handle.accept_loop().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    let resp = send_cmd(&mut stream, &["HELLO", "3"]).await;
    let s = String::from_utf8_lossy(&resp);
    assert!(s.starts_with("%"), "HELLO 3 response itself must use RESP3 map encoding: {}", s);
}

#[tokio::test]
async fn test_after_hello_3_nil_uses_underscore() {
    let port = find_free_port();
    let handle = spawn_server(port).await;
    tokio::spawn(async move { handle.accept_loop().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    send_cmd(&mut stream, &["HELLO", "3"]).await;
    let resp = send_cmd(&mut stream, &["GET", "missing"]).await;
    assert_eq!(resp, b"_\r\n", "RESP3 nil must be the underscore type, got: {}", String::from_utf8_lossy(&resp));
}

#[tokio::test]
async fn test_hello_unsupported_protocol_errors() {
    let port = find_free_port();
    let handle = spawn_server(port).await;
    tokio::spawn(async move { handle.accept_loop().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    let resp = send_cmd(&mut stream, &["HELLO", "99"]).await;
    assert!(String::from_utf8_lossy(&resp).contains("NOPROTO"));
}

#[tokio::test]
async fn test_hello_with_auth_combines_negotiation_and_login() {
    let port = find_free_port();
    let handle = spawn_server_with_requirepass(port, "secret").await;
    tokio::spawn(async move { handle.accept_loop().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    let resp = send_cmd(&mut stream, &["HELLO", "3", "AUTH", "default", "secret"]).await;
    let s = String::from_utf8_lossy(&resp);
    assert!(s.starts_with("%"), "successful HELLO+AUTH must still return the RESP3 map: {}", s);

    let resp = send_cmd(&mut stream, &["SET", "k", "v"]).await;
    assert!(String::from_utf8_lossy(&resp).contains("OK"), "HELLO AUTH should have authenticated the connection");
}

#[tokio::test]
async fn test_hello_without_auth_when_required_errors() {
    let port = find_free_port();
    let handle = spawn_server_with_requirepass(port, "secret").await;
    tokio::spawn(async move { handle.accept_loop().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    let resp = send_cmd(&mut stream, &["HELLO", "3"]).await;
    assert!(String::from_utf8_lossy(&resp).contains("NOAUTH"), "HELLO without AUTH must not bypass requirepass: {}", String::from_utf8_lossy(&resp));
}

// ========= TLS =========
#[tokio::test]
async fn test_tls_listener_accepts_and_serves_commands() {
    use std::sync::Arc as StdArc;
    use tokio_rustls::TlsConnector;
    use tokio_rustls::rustls::pki_types::{CertificateDer, ServerName};

    // Same self-signed test cert/key used by src/net/tls.rs's own unit tests.
    const TEST_CERT: &str = "-----BEGIN CERTIFICATE-----\nMIIDCTCCAfGgAwIBAgIUaKQtzb4t5ZrldEcPL3Tzf0Lyz3swDQYJKoZIhvcNAQEL\nBQAwFDESMBAGA1UEAwwJbG9jYWxob3N0MB4XDTI2MDcwMjE5Mjc0N1oXDTI3MDcw\nMjE5Mjc0N1owFDESMBAGA1UEAwwJbG9jYWxob3N0MIIBIjANBgkqhkiG9w0BAQEF\nAAOCAQ8AMIIBCgKCAQEApN5QQTb2cA2dth9I8QPrlu/PVkPZZWZPyH6nwpP/jHw/\naxzc9vijg1wKO2AzuGPJ8oeE+IkRfheh/0OVWK6hgCA1zbW1uaGHJDoqlI1M7vzd\nUfs/96w8xaqWxKERTZ2Ob349oZ3nsw9BRMXte9kOM406459lRTpw9zxHxSzmfvNP\nrfd2K/K22FwDYW8zFmbpkAAB0lLuu4jbs1PbGTj25MZ6cQvH7ZlyYLohdcFLzp6Y\nQq3YsVZ9ABWZFWgqa5VTCNC2aXeRTGhgBh+EIUn5WJ8G0QiWoGwkZC5t/52wZ1v9\ncCaSnG8SHooOKoTXCZkv6hdLncCY7MrgWPPJKvPW7wIDAQABo1MwUTAdBgNVHQ4E\nFgQU+EhiTCWobfhjnYrZN2nw3TrzpHAwHwYDVR0jBBgwFoAU+EhiTCWobfhjnYrZ\nN2nw3TrzpHAwDwYDVR0TAQH/BAUwAwEB/zANBgkqhkiG9w0BAQsFAAOCAQEApMkO\n1EpbxJW7MFQTH67CzimvBo6vUjkOG8+qkgE7KPr/wBtbZPlYaCp1V9Lr5NJ8gyFh\n85eTwWWZwNdNJaLdVhztjzLyxo0WmHMMvINDDxyOEPsyzg3YLImif4/uptE7Rw23\n5S+dfRtOGhmkUN5suGyP6KYLk2LxXHhGh2L4mg+7kL2eMd8gRHJv+DKs65pdAJ/N\ntlZ/KOFjAgGxZ5Bc4cZWcbqD/chumCq+/kST8C04rPG02w8ebsXb1IH6Q5Kichf4\neLQEhHNS6FCcikNs4r/ClPSX2ccHO2wSCw6srUzwqmN1njT69XQJjZ5ws49TI0o/\ng9Bf25NFGWIi7HpMmQ==\n-----END CERTIFICATE-----\n";
    const TEST_KEY: &str = "-----BEGIN PRIVATE KEY-----\nMIIEugIBADANBgkqhkiG9w0BAQEFAASCBKQwggSgAgEAAoIBAQCk3lBBNvZwDZ22\nH0jxA+uW789WQ9llZk/IfqfCk/+MfD9rHNz2+KODXAo7YDO4Y8nyh4T4iRF+F6H/\nQ5VYrqGAIDXNtbW5oYckOiqUjUzu/N1R+z/3rDzFqpbEoRFNnY5vfj2hneezD0FE\nxe172Q4zjTrjn2VFOnD3PEfFLOZ+80+t93Yr8rbYXANhbzMWZumQAAHSUu67iNuz\nU9sZOPbkxnpxC8ftmXJguiF1wUvOnphCrdixVn0AFZkVaCprlVMI0LZpd5FMaGAG\nH4QhSflYnwbRCJagbCRkLm3/nbBnW/1wJpKcbxIeig4qhNcJmS/qF0udwJjsyuBY\n88kq89bvAgMBAAECgf8TYggAAMED4Wyy3Q/TDJOZlpYkzIefDAhip7QnfK0iU8An\nsFjSQw/xdcTlfwctG3J4c8jIqAzUsR+oOl7yxFMBw/S9aZEyNGLaKkdGf1RcOnoi\n5EGtdghdjRVnIJb3iBiR7QdEfUofLYeFmOqnGNvJN56evKyODoaQstgJ9KAvPh6n\n6HHz1ZRh2v7ghrKC6Y9Db2kX52tLD7F1twgGHpS0+S3KlZ4XO7jZD75CitAB4TBX\n7IPZv1A7pIJkNGTJRQPkRj9cGB4q/OAlDDrgFS26Q+kU3X/t8xezWH0qom9oR0u8\n5eMAkNEsTUKdc3RiqfkyD8EqJtWObZBwXkPSMQECgYEA2BAZqqeQi+Nsvrsx8faZ\nePPLv3UUcjek1Fjx1k8ZY6A+ABhF4LdfD4M2mH/u+EqsnogpXCJJGzn+7tieWaQV\nOlc9gB1vLhGSOEaf+ANGTEt+r4geMFGQurpl8585SdlmE7SwcsNeqMC5dCyfnekM\nThquWCdlX/Nptnbwcm1cx48CgYEAw1e8qjwtUL6dJTA7dYcWvk+Run3VlnUqEdRU\nqpAKT8ZZeSFxu37jOcfQD27Obb3mBGHrymxkAcxK1v5D5u7/38uv6OAQJOsoEjNo\nWrFV6r8HcwNecoju0R1j+7C8ZkMkwA1G649Sjc17C3CVLhg28GKLjsA7oWYK25id\n2IHlSqECgYA7joyUpuXIOaNTG+STjucVGRazqsE9DquHwRDAg0M7XANbIVW5sLIq\nY3/cH3+uv16/wEauV+EQ+TaVfe6ARSN41m1kcDiiWUOV8ZnM0pJBG5pLJlkz9nfP\nkOvjcKNpAN0LV4Y/zCy+lYlJRbel3oR+zwn50Lo37a/ZFQYIdK7bbwKBgBw/Yrvn\nSdJETisjh2WebE6G6RbjvXAtbzD22Gt1utgAYc3fZTfsyGUBnPeuWVGLRWja1CMI\ne4m7BhOC30TUyNGO/dgaFpuQGdJP9sYuoLL6ftRF53F+lbJNorixvPy4tubCxL+p\nkRGKZkGoPRpWTBOE3JN+/uB/BhDtR94YIpUhAoGAQme18NkvWTGrm0ujR87ELTjV\ntJBae1Ke7GWL30XloF014kO3h3w7p6cTAeHepU/KCMiUmA6pXSM3186/CHQEi23c\nOyf643XTnK1JOjargk/3vlZE8PyiFs3TV2b2qZnYlnV3uFrnkNtf5TwaxxMy5/RQ\nz5KCkjduF7fMLZpYFKo=\n-----END PRIVATE KEY-----\n";

    // A rustls client verifier that unconditionally trusts server certs.
    // Only ever used against the loopback test server above, never real traffic.
    #[derive(Debug)]
    struct AcceptAnyCert;
    impl tokio_rustls::rustls::client::danger::ServerCertVerifier for AcceptAnyCert {
        fn verify_server_cert(
            &self,
            _end_entity: &CertificateDer<'_>,
            _intermediates: &[CertificateDer<'_>],
            _server_name: &ServerName<'_>,
            _ocsp_response: &[u8],
            _now: tokio_rustls::rustls::pki_types::UnixTime,
        ) -> Result<tokio_rustls::rustls::client::danger::ServerCertVerified, tokio_rustls::rustls::Error> {
            Ok(tokio_rustls::rustls::client::danger::ServerCertVerified::assertion())
        }
        fn verify_tls12_signature(&self, _m: &[u8], _c: &CertificateDer<'_>, _dss: &tokio_rustls::rustls::DigitallySignedStruct) -> Result<tokio_rustls::rustls::client::danger::HandshakeSignatureValid, tokio_rustls::rustls::Error> {
            Ok(tokio_rustls::rustls::client::danger::HandshakeSignatureValid::assertion())
        }
        fn verify_tls13_signature(&self, _m: &[u8], _c: &CertificateDer<'_>, _dss: &tokio_rustls::rustls::DigitallySignedStruct) -> Result<tokio_rustls::rustls::client::danger::HandshakeSignatureValid, tokio_rustls::rustls::Error> {
            Ok(tokio_rustls::rustls::client::danger::HandshakeSignatureValid::assertion())
        }
        fn supported_verify_schemes(&self) -> Vec<tokio_rustls::rustls::SignatureScheme> {
            vec![
                tokio_rustls::rustls::SignatureScheme::RSA_PKCS1_SHA256,
                tokio_rustls::rustls::SignatureScheme::RSA_PKCS1_SHA384,
                tokio_rustls::rustls::SignatureScheme::RSA_PKCS1_SHA512,
                tokio_rustls::rustls::SignatureScheme::RSA_PSS_SHA256,
                tokio_rustls::rustls::SignatureScheme::RSA_PSS_SHA384,
                tokio_rustls::rustls::SignatureScheme::RSA_PSS_SHA512,
                tokio_rustls::rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
                tokio_rustls::rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
                tokio_rustls::rustls::SignatureScheme::ED25519,
            ]
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let cert_path = dir.path().join("cert.pem");
    let key_path = dir.path().join("key.pem");
    std::fs::write(&cert_path, TEST_CERT).unwrap();
    std::fs::write(&key_path, TEST_KEY).unwrap();

    let tls_port = find_free_port();
    let mut config = ServerConfig::default();
    config.port = find_free_port();
    config.tls_port = tls_port;
    config.tls_cert_file = Some(cert_path.to_string_lossy().to_string());
    config.tls_key_file = Some(key_path.to_string_lossy().to_string());
    config.databases = 4;

    let store = Arc::new(velodb::store::Store::new(config.databases));
    let cmd_table = StdArc::new(velodb::cmd::CommandTable::new());
    let replid = "0".repeat(40);

    let tls_server_config = velodb::net::tls::load_tls_config(&cert_path, &key_path).unwrap();
    let tls_listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{}", tls_port)).await.unwrap();
    tokio::spawn(velodb::net::tls::accept_loop(
        tls_listener, tls_server_config, store, cmd_table, config, None, None, replid,
    ));
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut client_config = tokio_rustls::rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(StdArc::new(AcceptAnyCert))
        .with_no_client_auth();
    client_config.alpn_protocols.clear();
    let connector = TlsConnector::from(StdArc::new(client_config));

    let tcp = TcpStream::connect(format!("127.0.0.1:{}", tls_port)).await.unwrap();
    let server_name = ServerName::try_from("localhost").unwrap();
    let mut tls_stream = connector.connect(server_name, tcp).await.unwrap();

    tls_stream.write_all(&encode_command(&["SET", "tlskey", "tlsvalue"])).await.unwrap();
    let mut buf = vec![0u8; 4096];
    let n = tls_stream.read(&mut buf).await.unwrap();
    assert!(String::from_utf8_lossy(&buf[..n]).contains("OK"), "SET over TLS should succeed");

    tls_stream.write_all(&encode_command(&["GET", "tlskey"])).await.unwrap();
    let n = tls_stream.read(&mut buf).await.unwrap();
    assert!(String::from_utf8_lossy(&buf[..n]).contains("tlsvalue"), "GET over TLS should return the value written over TLS");
}

#[tokio::test]
async fn test_info_keyspace_reports_expires_count() {
    let port = find_free_port();
    let handle = spawn_server(port).await;
    tokio::spawn(async move { handle.accept_loop().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    send_cmd(&mut stream, &["SET", "k1", "v"]).await;
    send_cmd(&mut stream, &["SET", "k2", "v"]).await;
    send_cmd(&mut stream, &["EXPIRE", "k2", "100"]).await;

    let resp = send_cmd(&mut stream, &["INFO"]).await;
    let s = String::from_utf8_lossy(&resp);
    assert!(s.contains("db0:keys=2,expires=1"), "expected 2 keys with 1 having a TTL: {}", s);
}

#[tokio::test]
async fn test_info_uptime_is_small_shortly_after_startup() {
    let port = find_free_port();
    let handle = spawn_server(port).await;
    tokio::spawn(async move { handle.accept_loop().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    let resp = send_cmd(&mut stream, &["INFO"]).await;
    let s = String::from_utf8_lossy(&resp);
    let uptime: u64 = s.lines().find(|l| l.starts_with("uptime_in_seconds:"))
        .and_then(|l| l.split(':').nth(1))
        .and_then(|v| v.trim().parse().ok())
        .expect("uptime_in_seconds must be present and parseable");
    assert!(uptime < 60, "uptime shortly after server start must be a small elapsed count, not a raw Unix timestamp; got {}", uptime);
}

#[tokio::test]
async fn test_eval_redis_call_variadic_set_persists() {
    // Regression: redis.call must accept variadic string args, matching
    // the real Redis Lua API — redis.call("SET", "k", "v"), NOT a table.
    // The old bridge expected (name, table) and silently no-oped on
    // variadic calls, with the nil return masked as +OK.
    let port = find_free_port();
    let handle = spawn_server(port).await;
    tokio::spawn(async move { handle.accept_loop().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    send_cmd(&mut stream, &["EVAL", r#"redis.call("SET", KEYS[1], ARGV[1]) return 1"#, "1", "luakey", "luaval"]).await;

    let resp = send_cmd(&mut stream, &["GET", "luakey"]).await;
    assert!(String::from_utf8_lossy(&resp).contains("luaval"), "redis.call SET from Lua must persist: {}", String::from_utf8_lossy(&resp));
}

#[tokio::test]
async fn test_eval_redis_call_get_returns_value() {
    let port = find_free_port();
    let handle = spawn_server(port).await;
    tokio::spawn(async move { handle.accept_loop().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    send_cmd(&mut stream, &["SET", "existing", "fromoutside"]).await;
    let resp = send_cmd(&mut stream, &["EVAL", r#"return redis.call("GET", KEYS[1])"#, "1", "existing"]).await;
    assert!(String::from_utf8_lossy(&resp).contains("fromoutside"), "redis.call GET must return the stored value: {}", String::from_utf8_lossy(&resp));
}

#[tokio::test]
async fn test_set_nx_does_not_overwrite() {
    // Regression: SET ... NX must fail (nil) when the key exists;
    // previously the flag was silently ignored and the value clobbered.
    let port = find_free_port();
    let handle = spawn_server(port).await;
    tokio::spawn(async move { handle.accept_loop().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    let resp = send_cmd(&mut stream, &["SET", "nxk", "v1", "NX"]).await;
    assert!(String::from_utf8_lossy(&resp).contains("OK"), "first NX SET must succeed");

    let resp = send_cmd(&mut stream, &["SET", "nxk", "v2", "NX"]).await;
    assert!(String::from_utf8_lossy(&resp).contains("$-1"), "second NX SET must return nil: {}", String::from_utf8_lossy(&resp));

    let resp = send_cmd(&mut stream, &["GET", "nxk"]).await;
    assert!(String::from_utf8_lossy(&resp).contains("v1"), "value must not be overwritten by failed NX SET");
}

#[tokio::test]
async fn test_set_xx_requires_existing_key() {
    let port = find_free_port();
    let handle = spawn_server(port).await;
    tokio::spawn(async move { handle.accept_loop().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    let resp = send_cmd(&mut stream, &["SET", "xxk", "v1", "XX"]).await;
    assert!(String::from_utf8_lossy(&resp).contains("$-1"), "XX SET on missing key must return nil: {}", String::from_utf8_lossy(&resp));

    send_cmd(&mut stream, &["SET", "xxk", "v1"]).await;
    let resp = send_cmd(&mut stream, &["SET", "xxk", "v2", "XX"]).await;
    assert!(String::from_utf8_lossy(&resp).contains("OK"), "XX SET on existing key must succeed");
    let resp = send_cmd(&mut stream, &["GET", "xxk"]).await;
    assert!(String::from_utf8_lossy(&resp).contains("v2"));
}

#[tokio::test]
async fn test_set_nx_with_expiry_flags_combined() {
    let port = find_free_port();
    let handle = spawn_server(port).await;
    tokio::spawn(async move { handle.accept_loop().await.unwrap() });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    let resp = send_cmd(&mut stream, &["SET", "nxex", "v", "NX", "EX", "100"]).await;
    assert!(String::from_utf8_lossy(&resp).contains("OK"));
    let resp = send_cmd(&mut stream, &["TTL", "nxex"]).await;
    let s = String::from_utf8_lossy(&resp).to_string();
    let ttl: i64 = s.trim_start_matches(':').trim().parse().unwrap_or(-99);
    assert!(ttl > 0 && ttl <= 100, "NX+EX combined must set both value and TTL, got ttl={}", ttl);
}
