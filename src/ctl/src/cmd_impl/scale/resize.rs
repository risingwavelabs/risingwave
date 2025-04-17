// Copyright 2025 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::{HashMap, HashSet};
use std::process::exit;

use itertools::Itertools;
use risingwave_pb::meta::GetClusterInfoResponse;
use risingwave_pb::meta::update_worker_node_schedulability_request::Schedulability;
use thiserror_ext::AsReport;

use crate::common::CtlContext;

macro_rules! fail {
    ($($arg:tt)*) => {{
        println!($($arg)*);
        exit(1);
    }};
}

pub async fn update_schedulability(
    context: &CtlContext,
    workers: Vec<String>,
    target: Schedulability,
) -> anyhow::Result<()> {
    let meta_client = context.meta_client().await?;

    let GetClusterInfoResponse { worker_nodes, .. } = match meta_client.get_cluster_info().await {
        Ok(resp) => resp,
        Err(e) => {
            fail!("Failed to get cluster info: {}", e.as_report());
        }
    };

    let worker_ids: HashSet<_> = worker_nodes.iter().map(|worker| worker.id).collect();

    let worker_index_by_host: HashMap<_, _> = worker_nodes
        .iter()
        .map(|worker| {
            let host = worker.get_host().expect("worker host must be set");
            (format!("{}:{}", host.host, host.port), worker.id)
        })
        .collect();

    let mut target_worker_ids = HashSet::new();

    for worker in workers {
        let worker_id = worker
            .parse::<u32>()
            .ok()
            .or_else(|| worker_index_by_host.get(&worker).cloned());

        if let Some(worker_id) = worker_id
            && worker_ids.contains(&worker_id)
        {
            if !target_worker_ids.insert(worker_id) {
                println!("Warn: {} and {} are the same worker", worker, worker_id);
            }
        } else {
            fail!("Invalid worker id: {}", worker);
        }
    }

    let target_worker_ids = target_worker_ids.into_iter().collect_vec();

    meta_client
        .update_schedulability(&target_worker_ids, target)
        .await?;

    Ok(())
}
