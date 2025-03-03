use std::collections::{HashMap, HashSet};
use std::time::Duration;

use pgwire::pg_response::StatementType;
use risingwave_common::types::Fields;
use risingwave_pb::common::WorkerNode;
use risingwave_pb::meta::list_table_fragments_response::FragmentInfo;
use risingwave_pb::monitor_service::{GetProfileStatsRequest, GetProfileStatsResponse};
use risingwave_pb::stream_plan::StreamNode as PbStreamNode;
use tokio::time::sleep;

use crate::error::Result;
use crate::handler::{HandlerArgs, RwPgResponse, RwPgResponseBuilder, RwPgResponseBuilderExt};
use crate::session::FrontendEnv;

#[derive(Fields)]
struct ExplainAnalyzeStreamJobOutput {
    operator_id: String,
    identity: String,
    actor_ids: String,
    input_throughput: String,
    output_throughput: String,
    input_latency: String,
    output_latency: String,
}

pub async fn handle_explain_analyze_stream_job(
    handler_args: HandlerArgs,
    job_id: u32,
) -> Result<RwPgResponse> {
    // query meta for fragment graph (names only)
    let meta_client = handler_args.session.env().meta_client();
    // TODO(kwannoel): Only fetch the names, actor_ids and graph of the fragments
    // Otherwise memory utilization can be high
    let fragments = {
        let mut fragment_map = meta_client.list_table_fragments(&[job_id]).await?;
        assert_eq!(fragment_map.len(), 1, "expected only one fragment");
        let (fragment_job_id, table_fragment_info) = fragment_map.drain().next().unwrap();
        assert_eq!(fragment_job_id, job_id);
        table_fragment_info.fragments
    };
    let adjacency_list = extract_stream_node_infos(fragments);
    println!("adjacency list: {:?}", adjacency_list);
    let root_node = find_root_node(&adjacency_list);

    // Get the worker nodes
    let worker_nodes = list_stream_worker_nodes(handler_args.session.env()).await?;

    // Scrape baseline metrics from the compute node
    let mut aggregated_stats = StreamNodeStats::new();
    for node in &worker_nodes {
        let mut compute_client = handler_args.session.env().client_pool().get(node).await?;
        let stats = compute_client
            .monitor_client
            .get_profile_stats(GetProfileStatsRequest {})
            .await
            .expect("get profiling stats failed");
        aggregated_stats.start_record(adjacency_list.keys(), &stats.into_inner());
    }

    sleep(Duration::from_secs(10)).await;

    // Scrape metrics after `DURATION` from the meta node,
    // group by actors.
    for node in &worker_nodes {
        let mut compute_client = handler_args.session.env().client_pool().get(node).await?;
        let stats = compute_client
            .monitor_client
            .get_profile_stats(GetProfileStatsRequest {})
            .await
            .expect("get profiling stats failed");
        aggregated_stats.finish_record(adjacency_list.keys(), &stats.into_inner());
    }

    // Call get streaming stats via compute client.

    // Trigger barrier via meta node to stop profiling specific actors

    // Render graph with metrics
    let rows = render_graph_with_metrics(&adjacency_list, root_node, &aggregated_stats);
    let builder = RwPgResponseBuilder::empty(StatementType::EXPLAIN);
    let builder = builder.rows(rows);
    Ok(builder.into())
}

/// This is an internal struct used ONLY for explain analyze stream job.
#[derive(Debug)]
struct StreamNode {
    operator_id: u64,
    identity: String,
    actor_ids: Vec<u32>,
    dependencies: Vec<u64>,
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
    total_input_throughput: u32,
    total_output_throughput: u32,
    total_input_latency: u32,
    total_output_latency: u32,
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
            stats.total_input_throughput += metrics
                .stream_node_input_row_count
                .get(operator_id)
                .cloned()
                .unwrap_or(0);
            stats.total_output_throughput += metrics
                .stream_node_output_row_count
                .get(operator_id)
                .cloned()
                .unwrap_or(0);
            stats.total_input_latency += metrics
                .stream_node_input_blocking_duration_ns
                .get(operator_id)
                .cloned()
                .unwrap_or(0);
            stats.total_output_latency += metrics
                .stream_node_input_row_count
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
                stats.total_input_throughput = metrics
                    .stream_node_input_row_count
                    .get(operator_id)
                    .cloned()
                    .unwrap_or(0)
                    - stats.total_input_throughput;
                stats.total_output_throughput = metrics
                    .stream_node_output_row_count
                    .get(operator_id)
                    .cloned()
                    .unwrap_or(0)
                    - stats.total_output_throughput;
                stats.total_input_latency = metrics
                    .stream_node_input_blocking_duration_ns
                    .get(operator_id)
                    .cloned()
                    .unwrap_or(0)
                    - stats.total_input_latency;
                stats.total_output_latency = metrics
                    .stream_node_output_blocking_duration_ns
                    .get(operator_id)
                    .cloned()
                    .unwrap_or(0)
                    - stats.total_output_latency;
            } else {
                // TODO: warn missing metrics!
            }
        }
    }
}

fn extract_stream_node_infos(fragments: Vec<FragmentInfo>) -> HashMap<u64, StreamNode> {
    fn extract_stream_node_info(
        fragment_id: u32,
        operator_id_to_stream_node: &mut HashMap<u64, StreamNode>,
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
        let dependencies = &node.input;
        let entry = operator_id_to_stream_node
            .entry(operator_id)
            .or_insert_with(|| {
                let dependencies = dependencies
                    .iter()
                    .map(|input| unique_operator_id(fragment_id, input.operator_id))
                    .collect();
                StreamNode {
                    operator_id,
                    identity,
                    actor_ids: vec![],
                    dependencies,
                }
            });
        entry.actor_ids.push(actor_id);
        for dependency in dependencies {
            extract_stream_node_info(
                fragment_id,
                operator_id_to_stream_node,
                dependency,
                actor_id,
            );
        }
    }

    let mut operator_id_to_stream_node = HashMap::new();
    for fragment in fragments {
        let actors = fragment.actors;
        for actor in actors {
            let actor_id = actor.id;
            let node = actor.node.unwrap();
            extract_stream_node_info(
                fragment.id,
                &mut operator_id_to_stream_node,
                &node,
                actor_id,
            );
        }
    }
    operator_id_to_stream_node
}

fn find_root_node(stream_nodes: &HashMap<u64, StreamNode>) -> u64 {
    let mut all_nodes = stream_nodes.keys().copied().collect::<HashSet<_>>();
    for node in stream_nodes.values() {
        for dependency in &node.dependencies {
            all_nodes.remove(dependency);
        }
    }
    assert_eq!(
        all_nodes.len(),
        1,
        "expected only one root node: {:?}",
        all_nodes
    );
    let mut all_nodes = all_nodes.drain();
    all_nodes.next().unwrap()
}

// Do a DFS based rendering. Each node will occupy its own row.
// Schema:
// | Operator ID | Identity | Actor IDs | Metrics ... |
// Each node will be indented based on its depth in the graph.
fn render_graph_with_metrics(
    adjacency_list: &HashMap<u64, StreamNode>,
    root_node: u64,
    stats: &StreamNodeStats,
) -> Vec<ExplainAnalyzeStreamJobOutput> {
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
        let (input_throughput, output_throughput, input_latency, output_latency) = stats
            .map(|stats| {
                (
                    stats.total_input_throughput,
                    stats.total_output_throughput,
                    stats.total_input_latency,
                    stats.total_output_latency,
                )
            })
            .unwrap_or((0, 0, 0, 0));
        let row = ExplainAnalyzeStreamJobOutput {
            operator_id: node.operator_id.to_string(),
            identity: identity_rendered,
            actor_ids: node
                .actor_ids
                .iter()
                .map(|id| id.to_string())
                .collect::<Vec<_>>()
                .join(","),
            input_throughput: input_throughput.to_string(),
            output_throughput: output_throughput.to_string(),
            input_latency: input_latency.to_string(),
            output_latency: output_latency.to_string(),
        };
        rows.push(row);
        for (position, dependency) in node.dependencies.iter().enumerate() {
            stack.push((child_prefix.clone(), position == 0, *dependency));
        }
    }
    rows
}

/// Generate a globally unique operator id.
fn unique_operator_id(fragment_id: u32, operator_id: u64) -> u64 {
    assert!(operator_id <= u32::MAX as u64);
    ((fragment_id as u64) << 32) + operator_id
}
