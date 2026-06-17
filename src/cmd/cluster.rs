// Copyright (c) 2025-Present VeloDB Contributors
// SPDX-License-Identifier: MIT

use super::CommandDef;
use crate::store::Store;
use crate::resp::RespValue;
use crate::net::connection::ClientContext;
use crate::shard::router::{slot_for_key, SLOT_COUNT};
use std::sync::{Arc, RwLock};
use crate::cluster::slots::ClusterState;
use crate::cluster::compat;

pub const COMMANDS: &[CommandDef] = &[
    CommandDef { name: "CLUSTER", arity: -2, handler: cluster_cmd },
];

fn cluster_cmd(_store: &Store, _ctx: &mut ClientContext, args: &[Vec<u8>]) -> crate::error::Result<RespValue> {
    if args.is_empty() {
        return Err(crate::error::VeloDBError::wrong_number_of_args("CLUSTER"));
    }
    let subcmd = String::from_utf8_lossy(&args[0]).to_uppercase();

    // Get cluster state from thread-local storage (stub — in production would be passed via ServerHandle)
    let node_id = "node-unknown".to_string();
    let addr = "127.0.0.1".to_string();
    let cluster_port = 16379u16;
    let state = ClusterState::new(node_id, addr, cluster_port);

    match subcmd.as_str() {
        "SLOTS" => Ok(compat::cluster_slots_response(&state)),
        "NODES" => Ok(compat::cluster_nodes_response(&state)),
        "INFO" => Ok(RespValue::bulk_string("cluster_state:ok\r\ncluster_slots_assigned:16384\r\ncluster_slots_ok:16384\r\ncluster_known_nodes:1\r\ncluster_size:1\r\ncluster_current_epoch:0\r\n")),
        "MYID" => Ok(RespValue::bulk_string(state.node_id.as_bytes())),
        "KEYSLOT" => {
            if args.len() < 2 {
                return Err(crate::error::VeloDBError::wrong_number_of_args("CLUSTER KEYSLOT"));
            }
            let slot = slot_for_key(&args[1]);
            Ok(RespValue::integer(slot as i64))
        }
        "MEET" => Ok(RespValue::ok()),
        "RESET" => Ok(RespValue::ok()),
        "FORGET" => Ok(RespValue::ok()),
        "REPLICATE" => Ok(RespValue::ok()),
        "SETSLOT" => Ok(RespValue::ok()),
        "GETKEYSINSLOT" => Ok(RespValue::Array(Some(vec![]))),
        "COUNTKEYSINSLOT" => Ok(RespValue::integer(0)),
        "SAVECONFIG" => Ok(RespValue::ok()),
        _ => Err(crate::error::VeloDBError::unknown_command(format!("CLUSTER {}", subcmd))),
    }
}
