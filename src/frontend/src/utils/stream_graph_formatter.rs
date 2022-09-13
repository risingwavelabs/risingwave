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

use std::collections::HashMap;

use itertools::Itertools;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_pb::stream_plan::stream_fragment_graph::{StreamFragment, StreamFragmentEdge};
use risingwave_pb::stream_plan::{stream_node, StreamFragmentGraph, StreamNode};

pub fn explain_stream_graph(graph: &StreamFragmentGraph, is_verbose: bool) -> Result<String> {
    let mut output = String::new();
    StreamGraphFormatter::new(is_verbose)
        .explain_graph(graph, &mut output)
        .map_err(|e| ErrorCode::InternalError(format!("failed to explain stream graph: {}", e)))?;
    Ok(output)
}

/// A formatter to display the final stream plan graph, used for `explain (distsql) create
/// materialized view ...`
struct StreamGraphFormatter {
    /// exchange's operator_id -> edge
    edges: HashMap<u64, StreamFragmentEdge>,
}

impl StreamGraphFormatter {
    fn new(_is_verbose: bool) -> Self {
        StreamGraphFormatter {
            edges: HashMap::default(),
        }
    }

    fn explain_graph(
        &mut self,
        graph: &StreamFragmentGraph,
        f: &mut impl std::fmt::Write,
    ) -> std::fmt::Result {
        self.edges.clear();
        for edge in graph.edges.iter() {
            self.edges.insert(edge.link_id, edge.clone());
        }

        for (_, fragment) in graph.fragments.iter().sorted_by_key(|(id, _)| **id) {
            self.explain_fragment(fragment, f)?;
        }
        Ok(())
    }

    fn explain_fragment(
        &mut self,
        fragment: &StreamFragment,
        f: &mut impl std::fmt::Write,
    ) -> std::fmt::Result {
        writeln!(f, "Fragment {}", fragment.get_fragment_id())?;
        self.explain_node(1, fragment.node.as_ref().unwrap(), f)?;
        writeln!(f, "")
    }

    fn explain_node(
        &mut self,
        level: usize,
        node: &StreamNode,
        f: &mut impl std::fmt::Write,
    ) -> std::fmt::Result {
        let one_line_explain = match node.get_node_body().unwrap() {
            stream_node::NodeBody::Exchange(_) => {
                let edge = self.edges.get(&node.operator_id).unwrap();
                let upstream_fragment_id = edge.upstream_id;
                let dist = edge.dispatch_strategy.as_ref().unwrap();
                format!(
                    "StreamExchange {} from {}",
                    match dist.r#type() {
                        risingwave_pb::stream_plan::DispatcherType::Unspecified => unreachable!(),
                        risingwave_pb::stream_plan::DispatcherType::Hash =>
                            format!("Hash({:?})", dist.column_indices),
                        risingwave_pb::stream_plan::DispatcherType::Broadcast => unreachable!(),
                        risingwave_pb::stream_plan::DispatcherType::Simple => "Single".to_string(),
                        risingwave_pb::stream_plan::DispatcherType::NoShuffle =>
                            "No_shuffle".to_string(),
                    },
                    upstream_fragment_id
                )
            }
            _ => node.identity.clone(),
        };
        writeln!(f, "{}{}", " ".repeat(level * 2), one_line_explain)?;
        for input in node.input.iter() {
            self.explain_node(level + 1, input, f)?;
        }
        Ok(())
    }
}
