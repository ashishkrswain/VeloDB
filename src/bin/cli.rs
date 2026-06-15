// Copyright (c) 2025-Present VeloDB Contributors
// SPDX-License-Identifier: MIT

use clap::Parser;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

#[derive(Parser)]
#[command(name = "velodb-cli", version = "0.1.0")]
struct Args {
    #[arg(short = 'h', long, default_value = "127.0.0.1")]
    host: String,
    #[arg(short, long, default_value = "6379")]
    port: u16,
    #[arg(short, long)]
    command: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let addr = format!("{}:{}", args.host, args.port);
    let mut stream = TcpStream::connect(&addr).await?;
    if let Some(cmd) = args.command {
        execute_command(&mut stream, &cmd).await?;
    } else {
        interactive_loop(&mut stream).await?;
    }
    Ok(())
}

async fn execute_command(stream: &mut TcpStream, cmd: &str) -> anyhow::Result<()> {
    let resp = encode_command(cmd);
    stream.write_all(&resp).await?;
    let mut buf = vec![0u8; 4096];
    let n = stream.try_read(&mut buf).unwrap_or(0);
    print_response(&buf[..n]);
    Ok(())
}

async fn interactive_loop(stream: &mut TcpStream) -> anyhow::Result<()> {
    let stdin = tokio::io::stdin();
    let mut reader = BufReader::new(stdin);
    let mut line = String::new();
    println!("velodb-cli v0.1.0 - Type exit to quit");
    loop {
        line.clear();
        print!("velodb> ");
        let _ = std::io::Write::flush(&mut std::io::stdout());
        let n = reader.read_line(&mut line).await?;
        if n == 0 { break; }
        let cmd = line.trim();
        if cmd.is_empty() { continue; }
        if cmd.eq_ignore_ascii_case("exit") || cmd.eq_ignore_ascii_case("quit") { break; }
        let resp = encode_command(cmd);
        if let Err(e) = stream.write_all(&resp).await { eprintln!("err: {}", e); break; }
        let mut buf = vec![0u8; 65536];
        match stream.try_read(&mut buf) {
            Ok(0) => { println!("closed."); break; }
            Ok(n) => print_response(&buf[..n]),
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
            Err(e) => { eprintln!("err: {}", e); break; }
        }
    }
    Ok(())
}

fn encode_command(cmd: &str) -> Vec<u8> {
    let args = split_words(cmd);
    let mut out = Vec::new();
    out.push(b'*');
    out.extend(args.len().to_string().as_bytes());
    out.extend(b"\r\n");
    for arg in &args {
        out.push(b'$');
        out.extend(arg.len().to_string().as_bytes());
        out.extend(b"\r\n");
        out.extend(arg.as_bytes());
        out.extend(b"\r\n");
    }
    out
}

fn split_words(input: &str) -> Vec<String> {
    let mut words = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    for ch in input.chars() {
        match ch {
            '"' => in_quotes = !in_quotes,
            ' ' if !in_quotes => {
                if !current.is_empty() { words.push(std::mem::take(&mut current)); }
            }
            '\t' if !in_quotes => {
                if !current.is_empty() { words.push(std::mem::take(&mut current)); }
            }
            _ => current.push(ch),
        }
    }
    if !current.is_empty() { words.push(current); }
    words
}

fn print_response(data: &[u8]) {
    let s = String::from_utf8_lossy(data);
    for line in s.lines() {
        let line = line.trim();
        if line.is_empty() { continue; }
        if line.starts_with('+') { println!("{}", &line[1..]); }
        else if line.starts_with('-') { println!("(error) {}", &line[1..]); }
        else if line.starts_with(':') { println!("(integer) {}", &line[1..]); }
        else if line == "$-1" { println!("(nil)"); }
        else if !line.starts_with('*') && !line.starts_with('$') {
            println!("\"{}\"", line);
        }
    }
}
