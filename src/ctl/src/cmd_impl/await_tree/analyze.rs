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

use risingwave_pb::common::WorkerType;
use risingwave_pb::monitor_service::StackTraceRequest;
use risingwave_rpc_client::ComputeClientPool;

use crate::CtlContext;
use crate::cmd_impl::await_tree::tree::{SpanNodeView, TreeView};

impl TreeView {
    /// The target of this function is to analyze whether the current tree is the
    /// bottleneck.
    ///
    /// We assume there are three types of trees regarding the status in a stuck graph:
    /// 1. Input Blocking Tree, IB Tree
    /// 2. Output Blocking Tree, OB Tree
    /// 3. Bottleneck Tree, BN Tree
    /// Each Tree here maps to a specific actor and fragment of a streaming graph.
    /// The diagram below shows the logical structure of a stuck streaming graph:
    /// -------------------------------------------------------------------------------
    ///    OB(Source Executor)  ->  OB  ->  BN  ->  IB  ->  IB(Materialize Executor)
    /// -------------------------------------------------------------------------------
    ///   |   waiting for new epcoh   | bottleneck |      Waiting for input         |
    /// -------------------------------------------------------------------------------
    ///
    /// A typical look of OB Tree/IB Tree:
    /// Actor 123456: `XXXXX` [1595.653s]
    ///   Epoch 7509626714259456 [!!! 1582.513s]
    ///     CdcFilter 1DFBC0000271D [!!! 1582.423s]
    ///       Merge 1DFBC00000000 [!!! 1582.423s]
    ///         LocalInput (actor 122807) [!!! 1582.423s]
    /// Note:
    /// For an OB Tree, all the executors in the tree are waiting for source.
    /// However, the source executor is also blocked by barrier collection.
    /// For an IB Tree, all the executors in the tree are waiting for the input data from
    /// the bottleneck executor, which is the upstream of the IB Tree.
    ///
    /// A typical look of BN Tree:
    /// Actor 123456: `XXXXX` [1595.673s]
    ///   Epoch 7509625856917504 [!!! 1590.993s]
    ///     Materialize 9E2000000000D [!!! 1590.993s]
    ///       Project 9E2000000000C [!!! 1590.993s]
    ///         Project 9E2000000000B [!!! 1590.993s]
    ///           Project 9E2000000000A [!!! 1590.993s]
    ///             HashAgg 9E20000000009 [!!! 1590.993s] <== Bottleneck
    ///               Merge 9E20000000008 [980.020ms]
    ///                 LocalInput (actor 647685) [980.020ms]
    /// Note:
    /// There is usually a bottleneck executor throttling the whole graph. So we can
    /// detect the bottleneck by checking the elapsed time of the bottleneck executor
    /// and the average elapsed time of its children. If the elapsed time of the
    /// bottleneck executor is much larger than (eg, 5x) the average elapsed time of its
    /// children, we can say that the bottleneck executor is the bottleneck of the graph.
    ///
    /// A special look of IB Tree:
    /// Actor 123456: `XXXXX` [1595.653s]
    ///   Epoch 7509625856917504 [!!! 1591.003s]
    ///     Union 9E20200000007 [1.000s]
    ///       Merge 9E20200000003 [1.000s]
    ///         LocalInput (actor 647689) [1.000s]
    /// Note:
    /// In this case, although the bottleneck actor(BN Tree) throttles the whole graph,
    /// the bottleneck actor is still yielding output to downstream actors. A typical
    /// case is JOIN amplification. So the corresponding actors are actively processing
    /// the data but the EPOCH span is blocked.
    pub fn is_bottleneck(&self) -> bool {
        fn visit(node: &SpanNodeView, depth: usize) -> bool {
            let elapsed_secs = node.elapsed_ns as f64 / 1_000_000_000.0;
            let slow_span = !node.span.is_long_running && elapsed_secs >= 10.0;
            let is_epoch = node.span.name.starts_with("Epoch");

            if !is_epoch && !node.children.is_empty() {
                // IB Tree's `Epoch` span may have a long elapsed time, though it's not
                // a bottleneck. We exclude the `Epoch` span from the bottleneck detection
                let mut elapsed_sum = 0.;
                let mut elapsed_count = 0;
                for child in &node.children {
                    elapsed_count += 1;
                    elapsed_sum += child.elapsed_ns as f64 / 1_000_000_000.0;
                }
                let elapsed_avg = elapsed_sum / elapsed_count as f64;
                if slow_span && (elapsed_avg * 5.0 < elapsed_secs) {
                    return true;
                }
            }

            // visit children recursively
            for child in &node.children {
                if visit(child, depth + 1) {
                    return true;
                }
            }
            false
        }
        visit(&self.tree, 0)
    }
}

pub async fn bottleneck_detect(context: &CtlContext) -> anyhow::Result<()> {
    let meta_client = context.meta_client().await?;

    let compute_nodes = meta_client
        .list_worker_nodes(Some(WorkerType::ComputeNode))
        .await?;
    let clients = ComputeClientPool::adhoc();

    let req = StackTraceRequest {
        actor_traces_format: Some("json".to_owned()),
    };
    for cn in compute_nodes {
        let client = clients.get(&cn).await?;
        let response = client.stack_trace(req.clone()).await?;
        for (actor_id, trace) in response.actor_traces {
            let tree: TreeView = serde_json::from_str(&trace).unwrap();
            if tree.is_bottleneck() {
                println!(">> Actor {}", actor_id);
                println!("{}", tree);
            }
        }
    }

    Ok(())
}
