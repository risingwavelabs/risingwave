use std::collections::{HashMap, HashSet};

use risingwave_pb::common::WorkerNode;
use risingwave_pb::meta::list_table_fragments_response::FragmentInfo;

use crate::error::Result;
use crate::handler::{HandlerArgs, RwPgResponse};
use crate::session::FrontendEnv;

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
        let (fragment_job_id, table_fragment_info) = fragment_map.drain().next().unwrap();
        assert_eq!(fragment_job_id, job_id);
        table_fragment_info.fragments
    };
    let adjacency_list = extract_stream_node_infos(fragments);
    let root_node = find_root_node(&adjacency_list);

    // Trigger barrier via meta node to profile specific actors

    // Scrape baseline metrics from the meta node, group by actors.

    // Scrape metrics after `DURATION` from the meta node,
    // group by actors.
    // Call get streaming stats via compute client.

    // Trigger barrier via meta node to stop profiling specific actors

    // Render graph with metrics
    let rows = render_graph_with_metrics(&adjacency_list, root_node);
    todo!()
}

/// This is an internal struct used ONLY for explain analyze stream job.
struct StreamNode {
    operator_id: u32,
    identity: String,
    actor_ids: Vec<u32>,
    dependencies: Vec<u32>,
}

async fn list_stream_worker_nodes(env: &FrontendEnv) -> Result<Vec<WorkerNode>> {
    let worker_nodes = env.meta_client().list_all_nodes().await?;
    let stream_worker_nodes = worker_nodes
        .into_iter()
        .filter(|node| node.property.as_ref().unwrap().is_streaming)
        .collect::<Vec<_>>();
    Ok(stream_worker_nodes)
}

type OperatorId = u32;

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

async fn get_stream_node_stats(
    env: &FrontendEnv,
    worker_nodes: &[WorkerNode],
) -> Result<StreamNodeStats> {
    for node in worker_nodes {
        let compute_client = env.client_pool().get(node).await?;
        // TODO
        // compute_client.monitor_client.get_profile_stats()
    }
    todo!()
}

fn extract_stream_node_infos(fragments: Vec<FragmentInfo>) -> HashMap<u32, StreamNode> {
    let mut operator_id_to_stream_node = HashMap::new();
    for fragment in fragments {
        let actors = fragment.actors;
        for actor in actors {
            let actor_id = actor.id;
            let node = actor.node.unwrap();
            let operator_id = node.operator_id as u32;
            let entry = operator_id_to_stream_node
                .entry(operator_id)
                .or_insert_with(|| {
                    let dependencies = node
                        .input
                        .into_iter()
                        .map(|input| input.operator_id as u32)
                        .collect();
                    StreamNode {
                        operator_id,
                        identity: node.identity,
                        actor_ids: vec![],
                        dependencies,
                    }
                });
            entry.actor_ids.push(actor_id);
        }
    }
    operator_id_to_stream_node
}

fn find_root_node(stream_nodes: &HashMap<u32, StreamNode>) -> u32 {
    let mut all_nodes = stream_nodes.keys().copied().collect::<HashSet<_>>();
    for node in stream_nodes.values() {
        for dependency in &node.dependencies {
            all_nodes.remove(dependency);
        }
    }
    assert_eq!(all_nodes.len(), 1);
    let mut all_nodes = all_nodes.drain();
    all_nodes.next().unwrap()
}

// Do a DFS based rendering. Each node will occupy its own row.
// Schema:
// | Operator ID | Identity | Actor IDs | Metrics ... |
// Each node will be indented based on its depth in the graph.
fn render_graph_with_metrics(
    adjacency_list: &HashMap<u32, StreamNode>,
    root_node: u32,
) -> Vec<Vec<String>> {
    let mut rows = vec![];
    let mut stack = vec![(String::new(), true, root_node)];
    while let Some((prefix, last_child, node_id)) = stack.pop() {
        let node = adjacency_list.get(&node_id).unwrap();
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

        let row = vec![
            node.operator_id.to_string(),
            identity_rendered,
            node.actor_ids
                .iter()
                .map(|id| id.to_string())
                .collect::<Vec<_>>()
                .join(", "),
        ];
        rows.push(row);
        for (position, dependency) in node.dependencies.iter().enumerate() {
            stack.push((child_prefix.clone(), position == 0, *dependency));
        }
    }
    rows
}

#[cfg(test)]
mod tests {
    use super::*;

    // Use this graph:
    // A -> B -> C -> D
    #[test]
    fn render_linear_graph() {
        let mut adjacency_list = HashMap::new();
        adjacency_list.insert(
            1,
            StreamNode {
                operator_id: 1,
                identity: "A".to_string(),
                actor_ids: vec![],
                dependencies: vec![2],
            },
        );
        adjacency_list.insert(
            2,
            StreamNode {
                operator_id: 2,
                identity: "B".to_string(),
                actor_ids: vec![],
                dependencies: vec![3],
            },
        );
        adjacency_list.insert(
            3,
            StreamNode {
                operator_id: 3,
                identity: "C".to_string(),
                actor_ids: vec![],
                dependencies: vec![4],
            },
        );
        adjacency_list.insert(
            4,
            StreamNode {
                operator_id: 4,
                identity: "D".to_string(),
                actor_ids: vec![],
                dependencies: vec![],
            },
        );
        let root_node = find_root_node(&adjacency_list);
        let rows = render_graph_with_metrics(&adjacency_list, root_node);
        assert_eq!(
            rows,
            vec![
                vec!["1".to_string(), "A".to_string(), "".to_string()],
                vec!["2".to_string(), "└─ B".to_string(), "".to_string()],
                vec!["3".to_string(), "   └─ C".to_string(), "".to_string()],
                vec!["4".to_string(), "      └─ D".to_string(), "".to_string()],
            ]
        );
    }

    // Use this graph:
    // A -> B -> C
    // |\
    // |  -> D -> E
    // |---> F
    #[test]
    fn render_tree_graph() {
        let mut adjacency_list = HashMap::new();
        adjacency_list.insert(
            1,
            StreamNode {
                operator_id: 1,
                identity: "A".to_string(),
                actor_ids: vec![],
                dependencies: vec![2, 4, 6],
            },
        );
        adjacency_list.insert(
            2,
            StreamNode {
                operator_id: 2,
                identity: "B".to_string(),
                actor_ids: vec![],
                dependencies: vec![3],
            },
        );
        adjacency_list.insert(
            3,
            StreamNode {
                operator_id: 3,
                identity: "C".to_string(),
                actor_ids: vec![],
                dependencies: vec![],
            },
        );
        adjacency_list.insert(
            4,
            StreamNode {
                operator_id: 4,
                identity: "D".to_string(),
                actor_ids: vec![],
                dependencies: vec![5],
            },
        );
        adjacency_list.insert(
            5,
            StreamNode {
                operator_id: 5,
                identity: "E".to_string(),
                actor_ids: vec![],
                dependencies: vec![],
            },
        );
        adjacency_list.insert(
            6,
            StreamNode {
                operator_id: 6,
                identity: "F".to_string(),
                actor_ids: vec![],
                dependencies: vec![],
            },
        );
        let root_node = find_root_node(&adjacency_list);
        let rows = render_graph_with_metrics(&adjacency_list, root_node);
        assert_eq!(
            rows,
            vec![
                vec!["1".to_string(), "A".to_string(), "".to_string()],
                vec!["6".to_string(), "├─ F".to_string(), "".to_string()],
                vec!["4".to_string(), "├─ D".to_string(), "".to_string()],
                vec!["5".to_string(), "│  └─ E".to_string(), "".to_string()],
                vec!["2".to_string(), "└─ B".to_string(), "".to_string()],
                vec!["3".to_string(), "   └─ C".to_string(), "".to_string()],
            ]
        );
    }
}
