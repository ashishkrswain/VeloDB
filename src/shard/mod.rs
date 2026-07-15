// Copyright (c) 2025-Present VeloDB Contributors
// SPDX-License-Identifier: MIT

pub mod router;

use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use crate::store::Store;
use crate::cmd::CommandTable;
use crate::config::ServerConfig;
use crate::persist::aof::AofWriter;
use crate::replication::backlog::ReplBacklog;

use crate::net::connection;

pub struct ShardHandle {
    pub index: usize,
    pub tx: mpsc::UnboundedSender<(tokio::net::TcpStream, std::net::SocketAddr)>,
}

pub struct ShardedServer {
    pub shards: Vec<ShardHandle>,
}

impl ShardedServer {
    pub fn new(
        num_shards: usize,
        store: Arc<Store>,
        cmd_table: Arc<CommandTable>,
        config: &ServerConfig,
        aof_writer: Option<Arc<AofWriter>>,
        repl_backlog: Arc<std::sync::Mutex<ReplBacklog>>,
        replid: &str,
    ) -> Self {
        let mut shards = Vec::with_capacity(num_shards);

        for i in 0..num_shards {
            let (tx, mut rx) = mpsc::unbounded_channel::<(tokio::net::TcpStream, std::net::SocketAddr)>();

            let s = store.clone();
            let ct = cmd_table.clone();
            let aw = aof_writer.clone();
            let bl = repl_backlog.clone();
            let rid = replid.to_string();
            let cfg = config.clone();

            std::thread::spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();

                rt.block_on(async move {
                    tracing::info!("Shard {} runtime started", i);
                    while let Some((socket, addr)) = rx.recv().await {
                        let st = s.clone();
                        let cmd = ct.clone();
                        let aof = aw.clone();
                        let backlog = bl.clone();
                        let rid_clone = rid.clone();
                        let conn_cfg = cfg.clone();
                        tokio::spawn(async move {
                            if let Err(e) = connection::handle(socket, st, cmd, conn_cfg, aof, Some(backlog), rid_clone).await {
                                tracing::warn!("Connection error from {}: {}", addr, e);
                            }
                        });
                    }
                    tracing::info!("Shard {} runtime stopped", i);
                });
            });

            shards.push(ShardHandle { index: i, tx });
        }

        ShardedServer { shards }
    }

    pub async fn accept_loop(self, listener: TcpListener) -> anyhow::Result<()> {
        loop {
            let (socket, addr) = listener.accept().await?;
            tracing::debug!("New connection from {}", addr);

            // Route to shard — round-robin for now (slot routing needs first command parse)
            let shard_idx = (shard_for_addr(&addr)) as usize % self.shards.len();
            let _ = self.shards[shard_idx].tx.send((socket, addr));
        }
    }
}

fn shard_for_addr(addr: &std::net::SocketAddr) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    addr.hash(&mut h);
    h.finish()
}
