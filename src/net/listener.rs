// Copyright (c) 2025-Present VeloDB Contributors
// SPDX-License-Identifier: MIT

use tokio::net::TcpListener;
use std::sync::Arc;
use crate::store::Store;
use crate::cmd::CommandTable;
use crate::net::connection;
use crate::config::ServerConfig;

pub struct ServerHandle {
    pub listener: TcpListener,
    pub store: Arc<Store>,
    pub cmd_table: Arc<CommandTable>,
    pub config: ServerConfig,
}

impl ServerHandle {
    pub async fn new(config: &ServerConfig) -> anyhow::Result<Self> {
        let addr = format!("{}:{}", config.bind_address, config.port);
        let listener = TcpListener::bind(&addr).await?;
        Ok(Self { listener, store: Arc::new(Store::new(config.databases)), cmd_table: Arc::new(CommandTable::new()), config: config.clone() })
    }

    pub async fn accept_loop(&self) -> anyhow::Result<()> {
        loop {
            let (socket, addr) = self.listener.accept().await?;
            tracing::debug!("New connection from {}", addr);
            let store = self.store.clone();
            let cmd_table = self.cmd_table.clone();
            let config = self.config.clone();
            tokio::spawn(async move {
                if let Err(e) = connection::handle(socket, store, cmd_table, config).await {
                    tracing::warn!("Connection error from {}: {}", addr, e);
                }
                tracing::debug!("Connection from {} closed", addr);
            });
        }
    }
}
