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
use std::time::Duration;

use pgwire::pg_response::StatementType;
use risingwave_common::operator::unique_operator_id;
use risingwave_common::types::Fields;
use risingwave_pb::common::WorkerNode;
use risingwave_pb::meta::list_table_fragments_response::FragmentInfo;
use risingwave_pb::monitor_service::{GetProfileStatsRequest, GetProfileStatsResponse};
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::{MergeNode, StreamNode as PbStreamNode};
use risingwave_sqlparser::ast::AnalyzeTarget;
use tokio::time::sleep;

use crate::Binder;
use crate::catalog::root_catalog::SchemaPath;
use crate::error::Result;
use crate::handler::{HandlerArgs, RwPgResponse, RwPgResponseBuilder, RwPgResponseBuilderExt};
use crate::session::FrontendEnv;

#[derive(Fields)]
struct ExplainAnalyzeStreamJobOutput {
    identity: String,
    actor_ids: String,
    output_rows_per_second: String,
    downstream_backpressure_ratio: String,
}

pub async fn handle_explain_analyze_stream_job(
    handler_args: HandlerArgs,
    target: AnalyzeTarget,
) -> Result<RwPgResponse> {
    let profiling_duration = Duration::from_secs(10);
    let job_id = match &target {
        AnalyzeTarget::Id(id) => *id,
        AnalyzeTarget::Index(name)
        | AnalyzeTarget::Table(name)
        | AnalyzeTarget::Sink(name)
        | AnalyzeTarget::MaterializedView(name) => {
            let session = &handler_args.session;
            let db_name = session.database();
            let (schema_name, name) =
                Binder::resolve_schema_qualified_name(&db_name, name.clone())?;
            let search_path = session.config().search_path();
            let user_name = &session.user_name();
            let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);

            let catalog_reader = handler_args.session.env().catalog_reader();
            let catalog = catalog_reader.read_guard();

            match target {
                AnalyzeTarget::Index(_) => {
                    let (catalog, _schema_name) =
                        catalog.get_index_by_name(&db_name, schema_path, &name)?;
                    catalog.id.index_id
                }
                AnalyzeTarget::Table(_) => {
                    let (catalog, _schema_name) =
                        catalog.get_any_table_by_name(&db_name, schema_path, &name)?;
                    catalog.id.table_id
                }
                AnalyzeTarget::Sink(_) => {
                    let (catalog, _schema_name) =
                        catalog.get_sink_by_name(&db_name, schema_path, &name)?;
                    catalog.id.sink_id
                }
                AnalyzeTarget::MaterializedView(_) => {
                    let (catalog, _schema_name) =
                        catalog.get_any_table_by_name(&db_name, schema_path, &name)?;
                    catalog.id.table_id
                }
                AnalyzeTarget::Id(_) => unreachable!(),
            }
        }
    };

    let meta_client = handler_args.session.env().meta_client();

    // TODO(kwannoel): Only fetch the names, actor_ids and graph of the fragments
    let fragments = {
        let mut fragment_map = meta_client.list_table_fragments(&[job_id]).await?;
        assert_eq!(fragment_map.len(), 1, "expected only one fragment");
        let (fragment_job_id, table_fragment_info) = fragment_map.drain().next().unwrap();
        assert_eq!(fragment_job_id, job_id);
        table_fragment_info.fragments
    };
    let (root_node, adjacency_list) = extract_stream_node_infos(fragments);
    let operator_ids = adjacency_list.keys().copied().collect::<Vec<_>>();

    // Get the worker nodes
    let worker_nodes = list_stream_worker_nodes(handler_args.session.env()).await?;

    let aggregated_stats = {
        let mut aggregated_stats = StreamNodeStats::new();
        for node in &worker_nodes {
            let mut compute_client = handler_args.session.env().client_pool().get(node).await?;
            let stats = compute_client
                .monitor_client
                .get_profile_stats(GetProfileStatsRequest {
                    operator_ids: operator_ids.clone(),
                })
                .await
                .expect("get profiling stats failed");
            aggregated_stats.start_record(adjacency_list.keys(), &stats.into_inner());
        }

        sleep(profiling_duration).await;

        for node in &worker_nodes {
            let mut compute_client = handler_args.session.env().client_pool().get(node).await?;
            let stats = compute_client
                .monitor_client
                .get_profile_stats(GetProfileStatsRequest {
                    operator_ids: operator_ids.clone(),
                })
                .await
                .expect("get profiling stats failed");
            aggregated_stats.finish_record(adjacency_list.keys(), &stats.into_inner());
        }

        aggregated_stats
    };

    // Render graph with metrics
    let rows = render_graph_with_metrics(
        &adjacency_list,
        root_node,
        &aggregated_stats,
        &profiling_duration,
    );
    let builder = RwPgResponseBuilder::empty(StatementType::EXPLAIN);
    let builder = builder.rows(rows);
    Ok(builder.into())
}

/// This is an internal struct used ONLY for explain analyze stream job.
#[derive(Debug)]
struct StreamNode {
    fragment_id: u32,
    identity: String,
    actor_ids: Vec<u32>,
    dependencies: Vec<u64>,
}

impl StreamNode {
    fn new_for_dispatcher(fragment_id: u32) -> Self {
        StreamNode {
            fragment_id,
            identity: "Dispatcher".to_owned(),
            actor_ids: vec![],
            dependencies: vec![],
        }
    }
}

async fn list_stream_worker_nodes(env: &FrontendEnv) -> Result<Vec<WorkerNode>> {
    let worker_nodes = env.meta_client().list_all_nodes().await?;
    let stream_worker_nodes = worker_nodes
        .into_iter()
        .filter(|node| {
            node.property
                .as_ref()
                .map(|p| p.is_streaming)
                .unwrap_or_else(|| false)
        })
        .collect::<Vec<_>>();
    Ok(stream_worker_nodes)
}

type OperatorId = u64;

#[derive(Default)]
struct StreamNodeMetrics {
    operator_id: OperatorId,
    epoch: u32,
    total_output_throughput: u64,
    total_output_pending_ms: u64,
}

struct StreamNodeStats {
    inner: HashMap<OperatorId, StreamNodeMetrics>,
}

impl StreamNodeStats {
    fn new() -> Self {
        StreamNodeStats {
            inner: HashMap::new(),
        }
    }

    /// Establish metrics baseline for profiling
    fn start_record<'a>(
        &mut self,
        operator_ids: impl Iterator<Item = &'a OperatorId>,
        metrics: &'a GetProfileStatsResponse,
    ) {
        for operator_id in operator_ids {
            let stats = self.inner.entry(*operator_id).or_default();
            stats.operator_id = *operator_id;
            stats.epoch = 0;
            stats.total_output_throughput += metrics
                .stream_node_output_row_count
                .get(operator_id)
                .cloned()
                .unwrap_or(0);
            stats.total_output_pending_ms += metrics
                .stream_node_output_blocking_duration_ms
                .get(operator_id)
                .cloned()
                .unwrap_or(0);
        }
    }

    /// Compute the deltas for reporting
    fn finish_record<'a>(
        &mut self,
        operator_ids: impl Iterator<Item = &'a OperatorId>,
        metrics: &'a GetProfileStatsResponse,
    ) {
        for operator_id in operator_ids {
            if let Some(stats) = self.inner.get_mut(operator_id) {
                stats.total_output_throughput = metrics
                    .stream_node_output_row_count
                    .get(operator_id)
                    .cloned()
                    .unwrap_or(0)
                    - stats.total_output_throughput;
                stats.total_output_pending_ms = metrics
                    .stream_node_output_blocking_duration_ms
                    .get(operator_id)
                    .cloned()
                    .unwrap_or(0)
                    - stats.total_output_pending_ms;
            } else {
                // TODO: warn missing metrics!
            }
        }
    }
}

/// Extracts the root node of the plan, as well as the adjacency list
fn extract_stream_node_infos(
    fragments: Vec<FragmentInfo>,
) -> (OperatorId, HashMap<OperatorId, StreamNode>) {
    // Recursively extracts stream node info, and builds an adjacency list between stream nodes
    // and their dependencies
    fn extract_stream_node_info(
        fragment_id: u32,
        fragment_id_to_merge_operator_id: &mut HashMap<u32, OperatorId>,
        operator_id_to_stream_node: &mut HashMap<OperatorId, StreamNode>,
        node: &PbStreamNode,
        actor_id: u32,
    ) {
        let identity = node
            .identity
            .split_ascii_whitespace()
            .next()
            .unwrap()
            .to_owned();
        let operator_id = unique_operator_id(fragment_id, node.operator_id);
        if let Some(merge_node) = node.node_body.as_ref()
            && let NodeBody::Merge(box MergeNode {
                upstream_fragment_id,
                ..
            }) = merge_node
        {
            fragment_id_to_merge_operator_id.insert(*upstream_fragment_id, operator_id);
        }
        let dependencies = &node.input;
        let entry = operator_id_to_stream_node
            .entry(operator_id)
            .or_insert_with(|| {
                let dependencies = dependencies
                    .iter()
                    .map(|input| unique_operator_id(fragment_id, input.operator_id))
                    .collect();
                StreamNode {
                    fragment_id,
                    identity,
                    actor_ids: vec![],
                    dependencies,
                }
            });
        entry.actor_ids.push(actor_id);
        for dependency in dependencies {
            extract_stream_node_info(
                fragment_id,
                fragment_id_to_merge_operator_id,
                operator_id_to_stream_node,
                dependency,
                actor_id,
            );
        }
    }

    // build adjacency list and hanging merge edges.
    // hanging merge edges will be filled in the following section.
    let mut operator_id_to_stream_node = HashMap::new();
    let mut fragment_id_to_merge_operator_id = HashMap::new();
    for fragment in fragments {
        let actors = fragment.actors;
        for actor in actors {
            let actor_id = actor.id;
            let node = actor.node.unwrap();
            extract_stream_node_info(
                fragment.id,
                &mut fragment_id_to_merge_operator_id,
                &mut operator_id_to_stream_node,
                &node,
                actor_id,
            );
        }
    }

    // find root node, and fill in dispatcher edges + nodes.
    let root_or_dispatch_nodes = find_root_nodes(&operator_id_to_stream_node);
    let mut root_node = None;
    for operator_id in root_or_dispatch_nodes {
        let node = operator_id_to_stream_node.get_mut(&operator_id).unwrap();
        let fragment_id = node.fragment_id;
        if let Some(merge_operator_id) = fragment_id_to_merge_operator_id.get(&fragment_id) {
            let mut dispatcher = StreamNode::new_for_dispatcher(fragment_id);
            dispatcher.dependencies.push(operator_id);
            assert!(
                operator_id_to_stream_node
                    .insert(fragment_id as _, dispatcher)
                    .is_none()
            );
            operator_id_to_stream_node
                .get_mut(merge_operator_id)
                .unwrap()
                .dependencies
                .push(fragment_id as _);
        } else {
            root_node = Some(operator_id);
        }
    }

    (root_node.unwrap(), operator_id_to_stream_node)
}

fn find_root_nodes(stream_nodes: &HashMap<u64, StreamNode>) -> HashSet<u64> {
    let mut all_nodes = stream_nodes.keys().copied().collect::<HashSet<_>>();
    for node in stream_nodes.values() {
        for dependency in &node.dependencies {
            all_nodes.remove(dependency);
        }
    }
    all_nodes
}

// Do a DFS based rendering. Each node will occupy its own row.
// Schema:
// | Operator ID | Identity | Actor IDs | Metrics ... |
// Each node will be indented based on its depth in the graph.
fn render_graph_with_metrics(
    adjacency_list: &HashMap<u64, StreamNode>,
    root_node: u64,
    stats: &StreamNodeStats,
    profiling_duration: &Duration,
) -> Vec<ExplainAnalyzeStreamJobOutput> {
    let profiling_duration_secs = profiling_duration.as_secs_f64();
    let mut rows = vec![];
    let mut stack = vec![(String::new(), true, root_node)];
    while let Some((prefix, last_child, node_id)) = stack.pop() {
        let Some(node) = adjacency_list.get(&node_id) else {
            continue;
        };
        let is_root = node_id == root_node;

        let identity_rendered = if is_root {
            node.identity.clone()
        } else {
            let connector = if last_child { "└─ " } else { "├─ " };
            format!("{}{}{}", prefix, connector, node.identity)
        };

        let child_prefix = if is_root {
            ""
        } else if last_child {
            "   "
        } else {
            "│  "
        };
        let child_prefix = format!("{}{}", prefix, child_prefix);

        let stats = stats.inner.get(&node_id);
        let (output_throughput, output_latency) = stats
            .map(|stats| (stats.total_output_throughput, stats.total_output_pending_ms))
            .unwrap_or((0, 0));
        let row = ExplainAnalyzeStreamJobOutput {
            identity: identity_rendered,
            actor_ids: node
                .actor_ids
                .iter()
                .map(|id| id.to_string())
                .collect::<Vec<_>>()
                .join(","),
            output_rows_per_second: (output_throughput as f64 / profiling_duration_secs)
                .to_string(),
            downstream_backpressure_ratio: (Duration::from_millis(output_latency).as_secs_f64()
                / node.actor_ids.len() as f64
                / profiling_duration_secs)
                .to_string(),
        };
        rows.push(row);
        for (position, dependency) in node.dependencies.iter().enumerate() {
            stack.push((child_prefix.clone(), position == 0, *dependency));
        }
    }
    rows
}
