// Copyright (c) 2025-Present VeloDB Contributors
// SPDX-License-Identifier: MIT

use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
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

async fn spawn_server(port: u16) -> ServerHandle {
    let mut config = ServerConfig::default();
    config.port = port;
    config.databases = 4;
    let store = std::sync::Arc::new(velodb::store::Store::new(config.databases));
    ServerHandle::new(&config, store, None).await.unwrap()
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

// ========= Persistence tests =========
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

