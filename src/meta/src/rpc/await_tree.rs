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

use risingwave_common::util::StackTraceResponseExt as _;
use risingwave_pb::common::{WorkerNode, WorkerType};
use risingwave_pb::monitor_service::stack_trace_request::ActorTracesFormat;
use risingwave_pb::monitor_service::{StackTraceRequest, StackTraceResponse};
use risingwave_rpc_client::ComputeClientPool;

use crate::MetaResult;
use crate::manager::MetadataManager;

/// Dump the await tree of all nodes in the cluster, including compute nodes, compactor nodes,
/// and the current meta node.
pub async fn dump_cluster_await_tree(
    metadata_manager: &MetadataManager,
    meta_node_registry: &await_tree::Registry,
    actor_traces_format: ActorTracesFormat,
) -> MetaResult<StackTraceResponse> {
    let mut all = StackTraceResponse::default();

    let compute_nodes = metadata_manager
        .list_worker_node(Some(WorkerType::ComputeNode), None)
        .await?;
    let compute_traces = dump_worker_node_await_tree(&compute_nodes, actor_traces_format).await?;
    all.merge_other(compute_traces);

    let compactor_nodes = metadata_manager
        .list_worker_node(Some(WorkerType::Compactor), None)
        .await?;
    let compactor_traces =
        dump_worker_node_await_tree(&compactor_nodes, actor_traces_format).await?;
    all.merge_other(compactor_traces);

    let meta_traces = dump_meta_node_await_tree(meta_node_registry)?;
    all.merge_other(meta_traces);

    Ok(all)
}

/// Dump the await tree of the current meta node.
pub fn dump_meta_node_await_tree(
    meta_node_registry: &await_tree::Registry,
) -> MetaResult<StackTraceResponse> {
    let meta_traces = meta_node_registry
        .collect_all()
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();

    Ok(StackTraceResponse {
        meta_traces,
        ..Default::default()
    })
}

/// Dump the await tree of the given worker nodes (compute nodes or compactor nodes).
pub async fn dump_worker_node_await_tree(
    worker_nodes: impl IntoIterator<Item = &WorkerNode>,
    actor_traces_format: ActorTracesFormat,
) -> MetaResult<StackTraceResponse> {
    let mut all = StackTraceResponse::default();

    let req = StackTraceRequest {
        actor_traces_format: actor_traces_format as i32,
    };
    // This is also applicable to compactor.
    let compute_clients = ComputeClientPool::adhoc();

    for worker_node in worker_nodes {
        let client = compute_clients.get(worker_node).await?;
        let result = client.stack_trace(req).await?;

        all.merge_other(result);
    }

    Ok(all)
}
