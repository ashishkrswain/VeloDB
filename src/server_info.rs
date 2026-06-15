// Copyright (c) 2025-Present VeloDB Contributors
// SPDX-License-Identifier: MIT

use std::time::{SystemTime, UNIX_EPOCH};

pub struct ServerInfo {
    pub start_time: u64,
    pub connected_clients: std::sync::atomic::AtomicU64,
    pub total_commands: std::sync::atomic::AtomicU64,
}

impl ServerInfo {
    pub fn new() -> Self {
        Self {
            start_time: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
            connected_clients: std::sync::atomic::AtomicU64::new(0),
            total_commands: std::sync::atomic::AtomicU64::new(0),
        }
    }
}
