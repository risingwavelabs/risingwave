// Copyright 2023 RisingWave Labs
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

use std::collections::BTreeMap;

use risingwave_common::monitor::connection::ConnectionMetrics;
use risingwave_common::util::addr::HostAddr;
use risingwave_pb::common::WorkerType;
use risingwave_pb::monitor_service::StackTraceResponse;
use risingwave_rpc_client::{CompactorClient, ComputeClientPool};

use crate::CtlContext;

pub async fn trace(context: &CtlContext) -> anyhow::Result<()> {
    let meta_client = context.meta_client().await?;

    let workers = meta_client.get_cluster_info().await?.worker_nodes;
    let compute_nodes = workers
        .into_iter()
        .filter(|w| w.r#type() == WorkerType::ComputeNode);

    let clients = ComputeClientPool::new(1, ConnectionMetrics::unused());

    let mut all_actor_traces = BTreeMap::new();
    let mut all_rpc_traces = BTreeMap::new();

    // FIXME: the compute node may not be accessible directly from risectl, we may let the meta
    // service collect the reports from all compute nodes in the future.
    for cn in compute_nodes {
        let client = clients.get(&cn).await?;
        let StackTraceResponse {
            actor_traces,
            rpc_traces,
            ..
        } = client.stack_trace().await?;

        all_actor_traces.extend(actor_traces);
        all_rpc_traces.extend(rpc_traces.into_iter().map(|(k, v)| {
            (
                format!("{} ({})", HostAddr::from(cn.get_host().unwrap()), k),
                v,
            )
        }));
    }

    if all_actor_traces.is_empty() && all_rpc_traces.is_empty() {
        println!("No traces found. No actors are running, or `--async-stack-trace` not set?");
    } else {
        println!("--- Actor Traces ---");
        for (key, trace) in all_actor_traces {
            println!(">> Actor {key}\n{trace}");
        }
        println!("--- RPC Traces ---");
        for (key, trace) in all_rpc_traces {
            println!(">> RPC {key}\n{trace}");
        }
    }

    let compactor_nodes = meta_client.list_worker_nodes(WorkerType::Compactor).await?;
    let mut all_compaction_task_traces = BTreeMap::new();
    for compactor in compactor_nodes {
        let addr: HostAddr = compactor.get_host().unwrap().into();
        let client = CompactorClient::new(addr).await?;
        let StackTraceResponse {
            compaction_task_traces,
            ..
        } = client.stack_trace().await?;
        all_compaction_task_traces.extend(compaction_task_traces);
    }
    if !all_compaction_task_traces.is_empty() {
        println!("--- Compactor Traces ---");
        for (key, trace) in all_compaction_task_traces {
            println!(">> Compaction Task {key}\n{trace}");
        }
    }

    Ok(())
}
