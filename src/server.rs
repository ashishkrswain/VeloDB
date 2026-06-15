// Copyright (c) 2025-Present VeloDB Contributors
// SPDX-License-Identifier: MIT

use crate::config::ServerConfig;
use crate::net::listener::ServerHandle;

pub async fn run(config: ServerConfig) -> anyhow::Result<()> {
    let handle = ServerHandle::new(&config).await?;
    tracing::info!("VeloDB server listening on {}:{}", config.bind_address, config.port);
    tracing::info!("Ready to accept connections");

    tokio::select! {
        result = handle.accept_loop() => result?,
        _ = tokio::signal::ctrl_c() => tracing::info!("SIGINT received, shutting down"),
    }

    tracing::info!("VeloDB server stopped");
    Ok(())
}
