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

use std::collections::{BTreeMap, HashMap};

use itertools::Itertools;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_pb::catalog::Table;
use risingwave_pb::stream_plan::stream_fragment_graph::{StreamFragment, StreamFragmentEdge};
use risingwave_pb::stream_plan::{stream_node, DispatcherType, StreamFragmentGraph, StreamNode};

use crate::TableCatalog;

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
    verbose: bool,
    tables: BTreeMap<u32, Table>,
}

impl StreamGraphFormatter {
    fn new(verbose: bool) -> Self {
        StreamGraphFormatter {
            edges: HashMap::default(),
            tables: BTreeMap::default(),
            verbose,
        }
    }

    /// collect the table catalog and return the table id
    fn add_table(&mut self, tb: &Table) -> u32 {
        self.tables.insert(tb.id, tb.clone());
        tb.id
    }

    fn explain_graph(
        &mut self,
        graph: &StreamFragmentGraph,
        f: &mut impl std::fmt::Write,
    ) -> std::fmt::Result {
        self.edges.clear();
        for edge in &graph.edges {
            self.edges.insert(edge.link_id, edge.clone());
        }

        for (_, fragment) in graph.fragments.iter().sorted_by_key(|(id, _)| **id) {
            self.explain_fragment(fragment, f)?;
        }
        let tbs = self.tables.clone();
        for tb in tbs.values() {
            self.explain_table(tb, f)?;
        }
        Ok(())
    }

    fn explain_table(&mut self, tb: &Table, f: &mut impl std::fmt::Write) -> std::fmt::Result {
        let tb = TableCatalog::from(tb.clone());
        writeln!(
            f,
            " Table {} {{ columns: [{}], primary key: {:?}, value indices: {:?}, distribution key: {:?}{} }}",
            tb.id,
            tb.columns
                .iter()
                .map(|c| {
                    if self.verbose {
                        format!("{}:{}", c.name(), c.data_type())
                    } else {
                        c.name().to_string()
                    }
                })
                .join(", "),
            tb.pk,
            tb.value_indices,
            tb.distribution_key,
            if let Some(vnode_col_idx) = tb.vnode_col_idx {
                format!(", vnode column idx: {}", vnode_col_idx)
            } else {
                "".to_string()
            }
        )
    }

    fn explain_fragment(
        &mut self,
        fragment: &StreamFragment,
        f: &mut impl std::fmt::Write,
    ) -> std::fmt::Result {
        writeln!(f, "Fragment {}", fragment.get_fragment_id())?;
        self.explain_node(1, fragment.node.as_ref().unwrap(), f)?;
        writeln!(f)
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
                        DispatcherType::Unspecified => unreachable!(),
                        DispatcherType::Hash => format!("Hash({:?})", dist.column_indices),
                        DispatcherType::Broadcast => "Broadcast".to_string(),
                        DispatcherType::Simple => "Single".to_string(),
                        DispatcherType::NoShuffle => "NoShuffle".to_string(),
                    },
                    upstream_fragment_id
                )
            }
            _ => node.identity.clone(),
        };
        writeln!(f, "{}{}", " ".repeat(level * 2), one_line_explain)?;
        let explain_table_oneline = match node.get_node_body().unwrap() {
            stream_node::NodeBody::Source(node) => Some(format!(
                "source state table: {}",
                self.add_table(node.get_state_table().unwrap())
            )),
            stream_node::NodeBody::Materialize(node) => Some(format!(
                "materialized table: {}",
                self.add_table(node.get_table().unwrap())
            )),
            stream_node::NodeBody::GlobalSimpleAgg(node) => Some(format!(
                "state tables: [{}]",
                node.internal_tables
                    .iter()
                    .map(|tb| { self.add_table(tb) })
                    .join(", ")
            )),
            stream_node::NodeBody::HashAgg(node) => Some(format!(
                "state tables: [{}]",
                node.internal_tables
                    .iter()
                    .map(|tb| { self.add_table(tb) })
                    .join(", ")
            )),
            stream_node::NodeBody::AppendOnlyTopN(node) => Some(format!(
                "state table: {}",
                self.add_table(node.get_table().unwrap())
            )),
            stream_node::NodeBody::HashJoin(node) => Some(format!(
                "left table: {}, right table {},{}{}",
                self.add_table(node.get_left_table().unwrap()),
                self.add_table(node.get_right_table().unwrap()),
                match &node.left_degree_table {
                    Some(tb) => format!(" left degree table: {},", self.add_table(tb)),
                    None => "".to_string(),
                },
                match &node.right_degree_table {
                    Some(tb) => format!(" right degree table: {},", self.add_table(tb)),
                    None => "".to_string(),
                },
            )),
            stream_node::NodeBody::TopN(node) => Some(format!(
                "state table: {}",
                self.add_table(node.get_table().unwrap())
            )),
            stream_node::NodeBody::Lookup(node) => Some(format!(
                "arrange table: {}",
                self.add_table(node.get_arrangement_table().unwrap())
            )),
            stream_node::NodeBody::Arrange(node) => Some(format!(
                "arrange table: {}",
                self.add_table(node.get_table().unwrap())
            )),
            stream_node::NodeBody::DynamicFilter(node) => Some(format!(
                "left table: {}, right table {}",
                self.add_table(node.get_left_table().unwrap()),
                self.add_table(node.get_right_table().unwrap()),
            )),
            stream_node::NodeBody::GroupTopN(node) => Some(format!(
                "state table: {}",
                self.add_table(node.get_table().unwrap())
            )),
            _ => None,
        };
        if let Some(explain_table_oneline) = explain_table_oneline {
            writeln!(f, "{}{}", " ".repeat(level * 2 + 4), explain_table_oneline)?;
        }

        if self.verbose {
            writeln!(
                f,
                "{}Output: [{}]",
                " ".repeat(level * 2 + 4),
                node.fields.iter().map(|f| f.get_name()).join(", ")
            )?;
            writeln!(
                f,
                "{}Stream key: [{}], {}",
                " ".repeat(level * 2 + 4),
                node.stream_key
                    .iter()
                    .map(|i| node.fields[*i as usize].get_name())
                    .join(", "),
                if node.append_only { "AppendOnly" } else { "" }
            )?;
        }
        for input in &node.input {
            self.explain_node(level + 1, input, f)?;
        }
        Ok(())
    }
}
