// Copyright (c) 2025-Present VeloDB Contributors
// SPDX-License-Identifier: MIT

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use crate::shard::router::SLOT_COUNT;

#[derive(Clone, Debug)]
pub enum SlotState {
    Assigned { node_id: String, addr: String },
    Migrating { from: String, to: String },
    Importing { from: String },
    Unassigned,
}

#[derive(Clone)]
pub struct SlotMap {
    pub slots: Vec<SlotState>,
    pub version: u64,
}

impl SlotMap {
    pub fn new() -> Self {
        Self { slots: vec![SlotState::Unassigned; SLOT_COUNT as usize], version: 0 }
    }

    pub fn assign_range(&mut self, node_id: &str, addr: &str, start: u16, end: u16) {
        for slot in start..=end {
            self.slots[slot as usize] = SlotState::Assigned { node_id: node_id.to_string(), addr: addr.to_string() };
        }
        self.version += 1;
    }

    pub fn get_slot(&self, slot: u16) -> &SlotState {
        &self.slots[slot as usize]
    }

    pub fn move_slot(&self, slot: u16) -> Option<String> {
        match &self.slots[slot as usize] {
            SlotState::Assigned { addr, .. } => Some(addr.clone()),
            SlotState::Migrating { to, .. } => Some(to.clone()),
            _ => None,
        }
    }

    pub fn owned_slots(&self, node_id: &str) -> Vec<u16> {
        self.slots.iter().enumerate()
            .filter(|(_, s)| matches!(s, SlotState::Assigned { node_id: id, .. } if id == node_id))
            .map(|(i, _)| i as u16)
            .collect()
    }
}

#[derive(Clone)]
pub struct ClusterState {
    pub node_id: String,
    pub node_addr: String,
    pub cluster_port: u16,
    pub slot_map: Arc<RwLock<SlotMap>>,
    pub peers: HashMap<String, String>, // node_id -> addr
}

impl ClusterState {
    pub fn new(node_id: String, node_addr: String, cluster_port: u16) -> Self {
        let mut slot_map = SlotMap::new();
        let total_slots = SLOT_COUNT;
        slot_map.assign_range(&node_id, &node_addr, 0, total_slots - 1);
        Self {
            node_id,
            node_addr,
            cluster_port,
            slot_map: Arc::new(RwLock::new(slot_map)),
            peers: HashMap::new(),
        }
    }

    pub fn add_peer(&mut self, node_id: String, addr: String) {
        self.peers.insert(node_id, addr);
    }

    pub fn remove_peer(&mut self, node_id: &str) {
        self.peers.remove(node_id);
    }
}

pub fn start_cluster_service(state: Arc<RwLock<ClusterState>>) {
    let handle = tokio::spawn(async move {
        let addr = {
            let s = state.read().unwrap();
            format!("{}:{}", s.node_addr.split(':').next().unwrap_or("127.0.0.1"), s.cluster_port)
        };

        let listener = match tokio::net::TcpListener::bind(&addr).await {
            Ok(l) => l,
            Err(e) => { tracing::warn!("Cluster bus failed to bind: {}", e); return; }
        };
        tracing::info!("Cluster bus listening on {}", addr);

        loop {
            let (mut socket, _) = match listener.accept().await {
                Ok(c) => c,
                Err(e) => { tracing::warn!("Cluster accept error: {}", e); continue; }
            };
            let state_clone = state.clone();
            tokio::spawn(async move {
                let mut buf = vec![0u8; 1024];
                if let Ok(n) = tokio::io::AsyncReadExt::read(&mut socket, &mut buf).await {
                    let _cmd = String::from_utf8_lossy(&buf[..n]).to_string();
                    let _ = tokio::io::AsyncWriteExt::write_all(&mut socket, b"+OK\r\n").await;
                }
            });
        }
    });
}
