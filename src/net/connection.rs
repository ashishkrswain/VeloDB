use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use bytes::Buf;
use std::sync::Arc;
use crate::store::Store;
use crate::cmd::CommandTable;
use crate::resp;
use crate::config::ServerConfig;
use crate::error::VeloDBError;

const MAX_QUERY_BUFFER: usize = 1024 * 1024 * 1024;

pub struct ClientContext { pub db_index: usize }
impl ClientContext { pub fn new() -> Self { Self { db_index: 0 } } }

pub async fn handle(
    mut socket: TcpStream,
    store: Arc<Store>,
    cmd_table: Arc<CommandTable>,
    _config: ServerConfig,
) -> anyhow::Result<()> {
    let mut buf = bytes::BytesMut::with_capacity(4096);
    let mut ctx = ClientContext::new();

    loop {
        socket.readable().await?;

        match socket.try_read_buf(&mut buf) {
            Ok(0) => break,
            Ok(_) => {
                if buf.len() > MAX_QUERY_BUFFER {
                    return Err(VeloDBError::protocol_error("query buffer limit exceeded").into());
                }
                while !buf.is_empty() {
                    match resp::parse_command(&buf) {
                        Ok((remaining, args)) => {
                            if args.is_empty() { break; }
                            let cmd_name = String::from_utf8_lossy(&args[0]).to_uppercase();
                            tracing::trace!("Command: {} with {} args", cmd_name, args.len() - 1);
                            let response = cmd_table.dispatch(&cmd_name, &store, &mut ctx, &args[1..]);
                            let resp_bytes = resp::serialize_response(&response);
                            socket.write_all(&resp_bytes).await?;
                            let consumed = buf.len() - remaining.len();
                            buf.advance(consumed);
                        }
                        Err(nom::Err::Incomplete(_)) => break,
                        Err(_) => {
                            let err = resp::RespValue::error("ERR protocol error");
                            let bytes = resp::serialize_response(&err);
                            socket.write_all(&bytes).await?;
                            return Err(VeloDBError::protocol_error("parse error").into());
                        }
                    }
                }
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}
