// Copyright (c) 2025-Present VeloDB Contributors
// SPDX-License-Identifier: MIT

use rand::Rng;

pub struct ReplBacklog {
    buffer: Vec<u8>,
    capacity: usize,
    write_pos: usize,
    pub global_offset: u64,
    /// Live replicas currently caught up and streaming. Fan-out happens
    /// under the same lock as the ring-buffer write so a replica that
    /// registers while holding the lock can never miss a write.
    replicas: Vec<(u64, tokio::sync::mpsc::UnboundedSender<Vec<u8>>)>,
    next_replica_id: u64,
}

impl ReplBacklog {
    pub fn new(capacity: usize) -> Self {
        Self { buffer: vec![0u8; capacity], capacity, write_pos: 0, global_offset: 0, replicas: Vec::new(), next_replica_id: 0 }
    }

    pub fn push(&mut self, data: &[u8]) {
        for &byte in data {
            self.buffer[self.write_pos] = byte;
            self.write_pos = (self.write_pos + 1) % self.capacity;
        }
        self.global_offset += data.len() as u64;
        self.replicas.retain(|(_, tx)| tx.send(data.to_vec()).is_ok());
    }

    /// Registers a live replica sender. Must be called while holding the
    /// backlog's lock (i.e. right after reading catch-up data via
    /// `read_from`) so no write lands in the gap between catch-up and
    /// going live. Returns a handle for `unregister_replica`.
    pub fn register_replica(&mut self, tx: tokio::sync::mpsc::UnboundedSender<Vec<u8>>) -> u64 {
        let id = self.next_replica_id;
        self.next_replica_id += 1;
        self.replicas.push((id, tx));
        id
    }

    pub fn unregister_replica(&mut self, id: u64) {
        self.replicas.retain(|(rid, _)| *rid != id);
    }

    pub fn replica_count(&self) -> usize {
        self.replicas.len()
    }

    pub fn read_from(&self, offset: u64) -> Vec<u8> {
        let start_offset = self.global_offset.saturating_sub(self.capacity as u64);
        if offset < start_offset || offset > self.global_offset { return vec![]; }
        let len = (self.global_offset - offset) as usize;
        let mut result = Vec::with_capacity(len);
        for i in 0..len as u64 {
            // Byte at absolute stream offset `o` always lives at buffer
            // index `o % capacity` — independent of any window start.
            let idx = ((offset + i) % self.capacity as u64) as usize;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_from_zero_returns_everything_pushed() {
        let mut bl = ReplBacklog::new(1024);
        bl.push(b"hello");
        bl.push(b"world");
        assert_eq!(bl.read_from(0), b"helloworld");
    }

    #[test]
    fn test_read_from_mid_offset() {
        let mut bl = ReplBacklog::new(1024);
        bl.push(b"hello");
        bl.push(b"world");
        assert_eq!(bl.read_from(5), b"world");
    }

    #[test]
    fn test_read_from_current_offset_returns_empty() {
        let mut bl = ReplBacklog::new(1024);
        bl.push(b"hello");
        assert_eq!(bl.read_from(5), b"");
    }

    #[test]
    fn test_read_from_wraps_around_ring_buffer() {
        let mut bl = ReplBacklog::new(8);
        bl.push(b"abcd"); // offset 0..4
        bl.push(b"efgh"); // offset 4..8, buffer full, no wrap yet
        bl.push(b"ij");   // offset 8..10, wraps: overwrites 'a','b'
        // start_offset = global_offset(10) - capacity(8) = 2
        assert_eq!(bl.read_from(2), b"cdefghij");
    }

    #[test]
    fn test_read_from_before_window_returns_empty_stale() {
        let mut bl = ReplBacklog::new(8);
        for _ in 0..3 { bl.push(b"abcd"); } // global_offset = 12, window start = 4
        assert_eq!(bl.read_from(0), Vec::<u8>::new(), "offset before the ring's retained window is unrecoverable");
    }

    #[test]
    fn test_can_serve_partial_requires_matching_replid() {
        let mut bl = ReplBacklog::new(1024);
        bl.push(b"data");
        assert!(!bl.can_serve_partial("other-id", "my-id", 0));
        assert!(bl.can_serve_partial("my-id", "my-id", 0));
    }

    #[test]
    fn test_push_fans_out_to_registered_replicas() {
        let mut bl = ReplBacklog::new(1024);
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        bl.register_replica(tx);
        bl.push(b"hello");
        assert_eq!(rx.try_recv().unwrap(), b"hello");
    }

    #[test]
    fn test_push_drops_disconnected_replicas() {
        let mut bl = ReplBacklog::new(1024);
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        bl.register_replica(tx);
        drop(rx);
        bl.push(b"hello"); // must not panic, and must prune the dead sender
        assert_eq!(bl.replica_count(), 0);
    }

    #[test]
    fn test_unregister_replica_stops_fanout() {
        let mut bl = ReplBacklog::new(1024);
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let id = bl.register_replica(tx);
        bl.unregister_replica(id);
        bl.push(b"hello");
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn test_can_serve_partial_rejects_offset_outside_window() {
        let mut bl = ReplBacklog::new(8);
        for _ in 0..5 { bl.push(b"abcd"); } // global_offset = 20, window start = 12
        assert!(!bl.can_serve_partial("id", "id", 0), "offset before window must reject");
        assert!(bl.can_serve_partial("id", "id", 12), "offset at window start must accept");
        assert!(bl.can_serve_partial("id", "id", 20), "offset at current position must accept");
        assert!(!bl.can_serve_partial("id", "id", 21), "offset beyond current position must reject");
    }
}
