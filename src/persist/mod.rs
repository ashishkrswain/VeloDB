// Copyright (c) 2025-Present VeloDB Contributors
// SPDX-License-Identifier: MIT

pub mod aof;
pub(crate) mod aof_rewrite;
pub mod rdb;

/// Process-wide unique id for scratch filenames (temp RDB files, etc).
/// PID alone collides when multiple operations run concurrently within
/// the same process (e.g. the test binary running many async tasks).
pub fn unique_temp_id() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    COUNTER.fetch_add(1, Ordering::Relaxed)
}
