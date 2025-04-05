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

use anyhow::Context;
use risingwave_common::util::StackTraceResponseExt;
use risingwave_common::util::addr::HostAddr;
use risingwave_pb::common::WorkerType;
use risingwave_pb::monitor_service::stack_trace_request::ActorTracesFormat;
use risingwave_pb::monitor_service::{StackTraceRequest, StackTraceResponse};
use risingwave_rpc_client::{CompactorClient, ComputeClientPool};
use rw_diagnose_tools::await_tree::TreeView;

use crate::CtlContext;

pub async fn dump(context: &CtlContext, actor_traces_format: Option<String>) -> anyhow::Result<()> {
    let mut all = StackTraceResponse::default();

    let meta_client = context.meta_client().await?;

    let compute_nodes = meta_client
        .list_worker_nodes(Some(WorkerType::ComputeNode))
        .await?;
    let clients = ComputeClientPool::adhoc();

    let req = StackTraceRequest {
        actor_traces_format: match actor_traces_format.as_deref() {
            Some("text") => ActorTracesFormat::Text as i32,
            Some("json") | None => ActorTracesFormat::Json as i32,
            _ => return Err(anyhow::anyhow!("Invalid actor traces format")),
        },
    };

    // FIXME: the compute node may not be accessible directly from risectl, we may let the meta
    // service collect the reports from all compute nodes in the future.
    for cn in compute_nodes {
        let client = clients.get(&cn).await?;
        let response = client.stack_trace(req).await?;
        all.merge_other(response);
    }

    let compactor_nodes = meta_client
        .list_worker_nodes(Some(WorkerType::Compactor))
        .await?;

    for compactor in compactor_nodes {
        let addr: HostAddr = compactor.get_host().unwrap().into();
        let client = CompactorClient::new(addr).await?;
        let response = client.stack_trace(req).await?;
        all.merge_other(response);
    }

    if all.actor_traces.is_empty()
        && all.rpc_traces.is_empty()
        && all.compaction_task_traces.is_empty()
        && all.inflight_barrier_traces.is_empty()
    {
        eprintln!("No traces found. No actors are running, or `--async-stack-trace` not set?");
    }
    println!("{}", all.output());

    Ok(())
}

pub async fn bottleneck_detect(context: &CtlContext, path: Option<String>) -> anyhow::Result<()> {
    if let Some(path) = path {
        rw_diagnose_tools::await_tree::bottleneck_detect_from_file(path)
    } else {
        bottleneck_detect_real_time(context).await
    }
}

async fn bottleneck_detect_real_time(context: &CtlContext) -> anyhow::Result<()> {
    let meta_client = context.meta_client().await?;

    let compute_nodes = meta_client
        .list_worker_nodes(Some(WorkerType::ComputeNode))
        .await?;
    let clients = ComputeClientPool::adhoc();

    // request for json actor traces
    let req = StackTraceRequest::default();

    let mut bottleneck_actors_found = false;
    for cn in compute_nodes {
        let client = clients.get(&cn).await?;
        let response = client.stack_trace(req).await?;
        for (actor_id, trace) in response.actor_traces {
            let tree: TreeView =
                serde_json::from_str(&trace).context("Failed to parse JSON actor trace")?;
            if tree.is_bottleneck() {
                bottleneck_actors_found = true;
                println!(">> Actor {}", actor_id);
                println!("{}", tree);
            }
        }
    }

    if !bottleneck_actors_found {
        println!("No bottleneck actors detected.");
    }

    Ok(())
}
