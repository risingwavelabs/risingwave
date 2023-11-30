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

use risingwave_common::util::addr::HostAddr;
use risingwave_pb::common::WorkerType;
use risingwave_pb::monitor_service::StackTraceResponse;
use risingwave_rpc_client::{CompactorClient, ComputeClientPool};

use crate::CtlContext;

fn merge(a: &mut StackTraceResponse, b: StackTraceResponse) {
    a.actor_traces.extend(b.actor_traces);
    a.rpc_traces.extend(b.rpc_traces);
    a.compaction_task_traces.extend(b.compaction_task_traces);
}

pub async fn dump(context: &CtlContext) -> anyhow::Result<()> {
    let mut all = Default::default();

    let meta_client = context.meta_client().await?;

    let compute_nodes = meta_client
        .list_worker_nodes(Some(WorkerType::ComputeNode))
        .await?;
    let clients = ComputeClientPool::default();

    // FIXME: the compute node may not be accessible directly from risectl, we may let the meta
    // service collect the reports from all compute nodes in the future.
    for cn in compute_nodes {
        let client = clients.get(&cn).await?;
        let response = client.stack_trace().await?;
        merge(&mut all, response);
    }

    let compactor_nodes = meta_client
        .list_worker_nodes(Some(WorkerType::Compactor))
        .await?;

    for compactor in compactor_nodes {
        let addr: HostAddr = compactor.get_host().unwrap().into();
        let client = CompactorClient::new(addr).await?;
        let response = client.stack_trace().await?;
        merge(&mut all, response);
    }

    if all.actor_traces.is_empty()
        && all.rpc_traces.is_empty()
        && all.compaction_task_traces.is_empty()
    {
        println!("No traces found. No actors are running, or `--async-stack-trace` not set?");
    } else {
        if !all.actor_traces.is_empty() {
            println!("--- Actor Traces ---");
            for (key, trace) in all.actor_traces {
                println!(">> Actor {key}\n{trace}");
            }
        }
        if !all.rpc_traces.is_empty() {
            println!("\n\n--- RPC Traces ---");
            for (key, trace) in all.rpc_traces {
                println!(">> RPC {key}\n{trace}");
            }
        }
        if !all.compaction_task_traces.is_empty() {
            println!("\n\n--- Compactor Traces ---");
            for (key, trace) in all.compaction_task_traces {
                println!(">> Compaction Task {key}\n{trace}");
            }
        }
    }

    Ok(())
}
