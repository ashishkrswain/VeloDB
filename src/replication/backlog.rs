// Copyright (c) 2025-Present VeloDB Contributors
// SPDX-License-Identifier: MIT

use std::sync::Mutex;
use rand::Rng;

pub struct ReplBacklog {
    buffer: Vec<u8>,
    capacity: usize,
    write_pos: usize,
    pub global_offset: u64,
}

impl ReplBacklog {
    pub fn new(capacity: usize) -> Self {
        Self { buffer: vec![0u8; capacity], capacity, write_pos: 0, global_offset: 0 }
    }

    pub fn push(&mut self, data: &[u8]) {
        for &byte in data {
            self.buffer[self.write_pos] = byte;
            self.write_pos = (self.write_pos + 1) % self.capacity;
        }
        self.global_offset += data.len() as u64;
    }

    pub fn read_from(&self, offset: u64) -> Vec<u8> {
        let start_offset = self.global_offset.saturating_sub(self.capacity as u64);
        if offset < start_offset { return vec![]; }
        let relative_start = ((offset - start_offset) % self.capacity as u64) as usize;
        let len = (self.global_offset - offset) as usize;
        let mut result = Vec::with_capacity(len);
        for i in 0..len {
            let idx = (relative_start + i) % self.capacity;
            result.push(self.buffer[idx]);
        }
        result
    }

    pub fn can_serve_partial(&self, requested_replid: &str, current_replid: &str, offset: u64) -> bool {
        if requested_replid != current_replid { return false; }
        let start = self.global_offset.saturating_sub(self.capacity as u64);
        offset >= start && offset <= self.global_offset
    }
}

pub struct ReplicationState {
    pub replid: String,
    pub replid2: Option<String>,
    pub role: ReplRole,
    pub connected_replicas: usize,
}

pub enum ReplRole { Master, Replica }

impl ReplicationState {
    pub fn new() -> Self {
        let replid = generate_replid();
        Self { replid, replid2: None, role: ReplRole::Master, connected_replicas: 0 }
    }

    pub fn promote_to_master(&mut self) {
        self.replid2 = Some(self.replid.clone());
        self.replid = generate_replid();
        self.role = ReplRole::Master;
    }
}

fn generate_replid() -> String {
    let mut rng = rand::thread_rng();
    (0..40).map(|_| format!("{:x}", rng.gen_range(0..16))).collect()
}
