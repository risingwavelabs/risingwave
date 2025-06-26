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

use std::cmp::max;
use std::collections::{BTreeMap, HashMap};

use itertools::Itertools;
use petgraph::Graph;
use petgraph::dot::Dot;
use petgraph::graph::NodeIndex;
use pretty_xmlish::{Pretty, PrettyConfig};
use risingwave_common::util::stream_graph_visitor;
use risingwave_pb::catalog::Table;
use risingwave_pb::stream_plan::stream_fragment_graph::StreamFragmentEdge;
use risingwave_pb::stream_plan::{DispatcherType, StreamFragmentGraph, StreamNode, stream_node};

use super::PrettySerde;
use crate::TableCatalog;

/// ice: in the future, we may allow configurable width, boundaries, etc.
pub fn explain_stream_graph(graph: &StreamFragmentGraph, is_verbose: bool) -> String {
    let mut output = String::with_capacity(2048);
    let mut config = PrettyConfig {
        need_boundaries: false,
        width: 80,
        ..Default::default()
    };
    StreamGraphFormatter::new(is_verbose).explain_graph(graph, &mut config, &mut output);
    output
}

pub fn explain_stream_graph_as_dot(sg: &StreamFragmentGraph, is_verbose: bool) -> String {
    let graph = StreamGraphFormatter::new(is_verbose).explain_graph_as_dot(sg);
    let dot = Dot::new(&graph);
    dot.to_string()
}

/// A formatter to display the final stream plan graph, used for `explain (distsql) create
/// materialized view ...`
struct StreamGraphFormatter {
    /// exchange's `operator_id` -> edge
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
        config: &mut PrettyConfig,
        output: &mut String,
    ) {
        self.edges.clear();
        for edge in &graph.edges {
            self.edges.insert(edge.link_id, edge.clone());
        }
        let mut max_width = 80;
        for (_, fragment) in graph.fragments.iter().sorted_by_key(|(id, _)| **id) {
            output.push_str("Fragment ");
            output.push_str(&fragment.get_fragment_id().to_string());
            output.push('\n');
            let width = config.unicode(output, &self.explain_node(fragment.node.as_ref().unwrap()));
            max_width = max(width, max_width);
            config.width = max_width;
            output.push_str("\n\n");
        }
        for tb in self.tables.values() {
            let width = config.unicode(output, &self.explain_table(tb));
            max_width = max(width, max_width);
            config.width = max_width;
            output.push_str("\n\n");
        }
    }

    fn explain_graph_as_dot(&mut self, graph: &StreamFragmentGraph) -> Graph<String, String> {
        self.edges.clear();
        for edge in &graph.edges {
            self.edges.insert(edge.link_id, edge.clone());
        }

        let mut g = Graph::<String, String>::new();
        let mut nodes = HashMap::new();
        for (_, fragment) in graph.fragments.iter().sorted_by_key(|(id, _)| **id) {
            let mut label = String::new();
            label.push_str("Fragment ");
            label.push_str(&fragment.get_fragment_id().to_string());
            label.push('\n');
            nodes.insert(label.clone(), g.add_node(label.clone()));

            build_graph_from_pretty(
                &self.explain_node(fragment.node.as_ref().unwrap()),
                &mut g,
                &mut nodes,
                Some(&label),
            );
        }
        for tb in self.tables.values() {
            build_graph_from_pretty(&self.explain_table(tb), &mut g, &mut nodes, None);
        }
        g
    }

    fn explain_table<'a>(&self, tb: &Table) -> Pretty<'a> {
        let tb = TableCatalog::from(tb.clone());
        let columns = tb
            .columns
            .iter()
            .map(|c| {
                let s = if self.verbose {
                    format!("{}: {}", c.name(), c.data_type())
                } else {
                    c.name().to_owned()
                };
                Pretty::Text(s.into())
            })
            .collect();
        let columns = Pretty::Array(columns);
        let name = format!("Table {}", tb.id);
        let mut fields = Vec::with_capacity(5);
        fields.push(("columns", columns));
        fields.push((
            "primary key",
            Pretty::Array(tb.pk.iter().map(Pretty::debug).collect()),
        ));
        fields.push((
            "value indices",
            Pretty::Array(tb.value_indices.iter().map(Pretty::debug).collect()),
        ));
        fields.push((
            "distribution key",
            Pretty::Array(tb.distribution_key.iter().map(Pretty::debug).collect()),
        ));
        fields.push((
            "read pk prefix len hint",
            Pretty::debug(&tb.read_prefix_len_hint),
        ));
        if let Some(vnode_col_idx) = tb.vnode_col_index {
            fields.push(("vnode column idx", Pretty::debug(&vnode_col_idx)));
        }
        Pretty::childless_record(name, fields)
    }

    fn explain_node<'a>(&mut self, node: &StreamNode) -> Pretty<'a> {
        let one_line_explain = match node.get_node_body().unwrap() {
            stream_node::NodeBody::Exchange(_) => {
                let edge = self.edges.get(&node.operator_id).unwrap();
                let upstream_fragment_id = edge.upstream_id;
                let dist = edge.dispatch_strategy.as_ref().unwrap();
                format!(
                    "StreamExchange {} from {}",
                    match dist.r#type() {
                        DispatcherType::Unspecified => unreachable!(),
                        DispatcherType::Hash => format!("Hash({:?})", dist.dist_key_indices),
                        DispatcherType::Broadcast => "Broadcast".to_owned(),
                        DispatcherType::Simple => "Single".to_owned(),
                        DispatcherType::NoShuffle => "NoShuffle".to_owned(),
                    },
                    upstream_fragment_id
                )
            }
            _ => node.identity.clone(),
        };

        let mut tables: Vec<(String, u32)> = Vec::with_capacity(7);
        let mut node_copy = node.clone();

        stream_graph_visitor::visit_stream_node_tables_inner(
            &mut node_copy,
            true,
            false,
            |table, table_name| {
                tables.push((table_name.to_owned(), self.add_table(table)));
            },
        );

        let mut fields = Vec::with_capacity(3);
        if !tables.is_empty() {
            fields.push((
                "tables",
                Pretty::Array(
                    tables
                        .into_iter()
                        .map(|(name, id)| Pretty::Text(format!("{}: {}", name, id).into()))
                        .collect(),
                ),
            ));
        }
        if self.verbose {
            fields.push((
                "output",
                Pretty::Array(
                    node.fields
                        .iter()
                        .map(|f| Pretty::display(f.get_name()))
                        .collect(),
                ),
            ));
            fields.push((
                "stream key",
                Pretty::Array(
                    node.stream_key
                        .iter()
                        .map(|i| Pretty::display(node.fields[*i as usize].get_name()))
                        .collect(),
                ),
            ));
        }
        let children = node
            .input
            .iter()
            .map(|input| self.explain_node(input))
            .collect();
        Pretty::simple_record(one_line_explain, fields, children)
    }
}

pub fn build_graph_from_pretty(
    pretty: &Pretty<'_>,
    graph: &mut Graph<String, String>,
    nodes: &mut HashMap<String, NodeIndex>,
    parent_label: Option<&str>,
) {
    if let Pretty::Record(r) = pretty {
        let mut label = String::new();
        label.push_str(&r.name);
        for (k, v) in &r.fields {
            label.push('\n');
            label.push_str(k);
            label.push_str(": ");
            label.push_str(
                &serde_json::to_string(&PrettySerde(v.clone(), false))
                    .expect("failed to serialize plan to dot"),
            );
        }
        // output alignment.
        if !r.fields.is_empty() {
            label.push('\n');
        }

        let current_node = *nodes
            .entry(label.clone())
            .or_insert_with(|| graph.add_node(label.clone()));

        if let Some(parent_label) = parent_label {
            if let Some(&parent_node) = nodes.get(parent_label) {
                graph.add_edge(parent_node, current_node, "".to_owned());
            }
        }

        for child in &r.children {
            build_graph_from_pretty(child, graph, nodes, Some(&label));
        }
    }
}
