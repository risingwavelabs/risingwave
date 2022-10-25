// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::BTreeMap;

use risingwave_common::util::addr::HostAddr;
use risingwave_pb::common::WorkerType;
use risingwave_pb::monitor_service::StackTraceResponse;
use risingwave_rpc_client::ComputeClientPool;

use crate::common::MetaServiceOpts;

pub async fn trace() -> anyhow::Result<()> {
    let meta_opts = MetaServiceOpts::from_env()?;
    let meta_client = meta_opts.create_meta_client().await?;

    let workers = meta_client.get_cluster_info().await?.worker_nodes;
    let compute_nodes = workers
        .into_iter()
        .filter(|w| w.r#type() == WorkerType::ComputeNode);

    let clients = ComputeClientPool::default();

    let mut all_actor_traces = BTreeMap::new();
    let mut all_rpc_traces = BTreeMap::new();

    // FIXME: the compute node may not be accessible directly from risectl, we may let the meta
    // service collect the reports from all compute nodes in the future.
    for cn in compute_nodes {
        let client = clients.get(&cn).await?;
        let StackTraceResponse {
            actor_traces,
            rpc_traces,
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

    Ok(())
}
