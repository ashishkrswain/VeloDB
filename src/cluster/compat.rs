// Copyright (c) 2025-Present VeloDB Contributors
// SPDX-License-Identifier: MIT

use std::sync::{Arc, RwLock};
use crate::shard::router::{slot_for_key, SLOT_COUNT};
use crate::cluster::slots::ClusterState;
use crate::resp::RespValue;

pub fn check_slot_redirect(state: &Arc<RwLock<ClusterState>>, cmd_name: &str, args: &[Vec<u8>]) -> Option<RespValue> {
    let key_arg_idx = first_key_index(cmd_name);
    if key_arg_idx >= args.len() { return None; }

    let key = &args[key_arg_idx];
    let slot = slot_for_key(key);

    let slot_state = state.read().unwrap();
    let slot_map = slot_state.slot_map.read().unwrap();
    match slot_map.get_slot(slot) {
        crate::cluster::slots::SlotState::Assigned { node_id, .. } if node_id == &slot_state.node_id => None,
        crate::cluster::slots::SlotState::Assigned { addr, .. } => {
            Some(RespValue::error(format!("MOVED {} {}", slot, addr)))
        }
        crate::cluster::slots::SlotState::Migrating { to, .. } => {
            Some(RespValue::error(format!("MOVED {} {}", slot, to)))
        }
        crate::cluster::slots::SlotState::Importing { .. } => None,
        crate::cluster::slots::SlotState::Unassigned => None,
    }
}

fn first_key_index(_cmd_name: &str) -> usize {
    // Return index 1 for most commands (first arg is key)
    0
}

pub fn cluster_slots_response(state: &ClusterState) -> RespValue {
    let map = state.slot_map.read().unwrap();
    let mut result: Vec<RespValue> = Vec::new();
    let owned = map.owned_slots(&state.node_id);
    if !owned.is_empty() {
        result.push(RespValue::Array(Some(vec![
            RespValue::integer(owned.first().copied().unwrap_or(0) as i64),
            RespValue::integer(owned.last().copied().unwrap_or(0) as i64),
            RespValue::Array(Some(vec![
                RespValue::bulk_string(state.node_addr.split(':').next().unwrap_or("127.0.0.1")),
                RespValue::integer(state.cluster_port as i64),
            ])),
        ])));
    }
    RespValue::Array(Some(result))
}

pub fn cluster_nodes_response(state: &ClusterState) -> RespValue {
    let mut info = format!("{} {}:{}@{} master - 0 0 0 connected {}\n",
        state.node_id,
        state.node_addr.split(':').next().unwrap_or("127.0.0.1"),
        6379,
        state.cluster_port,
        owned_slots_range(state),
    );
    for (id, addr) in &state.peers {
        info.push_str(&format!("{} {}:{}@{} slave - 0 0 0 connected\n", id, addr, 6379, state.cluster_port));
    }
    RespValue::bulk_string(info.as_bytes())
}

fn owned_slots_range(state: &ClusterState) -> String {
    let slots = state.slot_map.read().unwrap().owned_slots(&state.node_id);
    if slots.is_empty() { return "".into(); }
    let first = slots.first().unwrap();
    let last = slots.last().unwrap();
    format!("{}-{}", first, last)
}
