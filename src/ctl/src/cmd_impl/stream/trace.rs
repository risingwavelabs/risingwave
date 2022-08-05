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

use anyhow::bail;
use risingwave_pb::common::WorkerType;
use risingwave_pb::stream_service::ActorTraceRequest;
use risingwave_rpc_client::StreamClientPool;

use crate::common::MetaServiceOpts;

pub async fn trace(actor_id: Option<u32>) -> anyhow::Result<()> {
    let meta_opts = MetaServiceOpts::from_env()?;
    let meta_client = meta_opts.create_meta_client().await?;

    let workers = meta_client.get_cluster_info().await?.worker_nodes;
    let compute_nodes = workers
        .into_iter()
        .filter(|w| w.r#type() == WorkerType::ComputeNode);

    let clients = StreamClientPool::new(u64::MAX);

    let mut all_traces = BTreeMap::new();

    // FIXME: the compute node may not be accessible from the our network with their listen
    // addresses.
    for cn in compute_nodes {
        let mut client = clients.get(&cn).await?;
        let traces = client
            .actor_trace(ActorTraceRequest::default())
            .await?
            .into_inner()
            .actor_traces;
        all_traces.extend(traces);
    }

    if let Some(actor_id) = actor_id {
        if let Some(trace) = all_traces.get(&actor_id) {
            println!("{trace}");
        } else {
            bail!("Actor {actor_id} not found. Not running, or `RW_ASYNC_STACK_TRACE` is not set?");
        }
    } else if all_traces.is_empty() {
        println!("No traces found. No actors are running, or `RW_ASYNC_STACK_TRACE` is not set?");
    } else {
        for (key, trace) in all_traces {
            println!(">> Actor {key}\n{trace}");
        }
    }

    Ok(())
}
