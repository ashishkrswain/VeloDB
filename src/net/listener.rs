// Copyright (c) 2025-Present VeloDB Contributors
// SPDX-License-Identifier: MIT

use tokio::net::TcpListener;
use std::sync::Arc;
use crate::store::Store;
use crate::cmd::CommandTable;
use crate::net::connection;
use crate::config::ServerConfig;
use crate::persist::aof::AofWriter;
use crate::replication::backlog::ReplBacklog;

pub struct ServerHandle {
    pub listener: TcpListener,
    pub store: Arc<Store>,
    pub cmd_table: Arc<CommandTable>,
    pub config: ServerConfig,
    pub aof_writer: Option<Arc<AofWriter>>,
    pub repl_backlog: Option<Arc<std::sync::Mutex<ReplBacklog>>>,
    pub replid: String,
}

impl ServerHandle {
    pub async fn new(config: &ServerConfig, store: Arc<Store>, aof_writer: Option<Arc<AofWriter>>, replid: String, repl_backlog: Option<Arc<std::sync::Mutex<ReplBacklog>>>) -> anyhow::Result<Self> {
        let addr = format!("{}:{}", config.bind_address, config.port);
        let listener = TcpListener::bind(&addr).await?;
        Ok(Self { listener, store, cmd_table: Arc::new(CommandTable::new()), config: config.clone(), aof_writer, repl_backlog, replid })
    }

    pub async fn accept_loop(&self) -> anyhow::Result<()> {
        loop {
            let (socket, addr) = self.listener.accept().await?;
            tracing::debug!("New connection from {}", addr);
            let store = self.store.clone();
            let cmd_table = self.cmd_table.clone();
            let config = self.config.clone();
            let aof_writer = self.aof_writer.clone();
            let repl_backlog = self.repl_backlog.clone();
            let replid = self.replid.clone();
            tokio::spawn(async move {
                if let Err(e) = connection::handle(socket, store, cmd_table, config, aof_writer, repl_backlog, replid).await {
                    tracing::warn!("Connection error from {}: {}", addr, e);
                }
                tracing::debug!("Connection from {} closed", addr);
            });
        }
    }
}
