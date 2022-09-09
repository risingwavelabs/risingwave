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

use itertools::Itertools;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_pb::catalog::Table as ProstTable;
use risingwave_pb::stream_plan::stream_fragment_graph::StreamFragment;
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
    is_verbose: bool,
    tables: Vec<ProstTable>,
}

impl StreamGraphFormatter {
    fn new(is_verbose: bool) -> Self {
        StreamGraphFormatter {
            is_verbose,
            tables: vec![],
        }
    }

    fn explain_graph(
        &mut self,
        graph: &StreamFragmentGraph,
        f: &mut impl std::fmt::Write,
    ) -> std::fmt::Result {
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
            stream_node::NodeBody::Exchange(exchange) => {
                format!("exchange {:?}", exchange)
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
