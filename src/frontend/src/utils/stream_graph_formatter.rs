// Copyright 2024 RisingWave Labs
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

use std::borrow::Cow;
use std::cmp::max;
use std::collections::{BTreeMap, HashMap};

use itertools::Itertools;
use pretty_xmlish::{Pretty, PrettyConfig, XmlNode};
use risingwave_common::util::stream_graph_visitor;
use risingwave_pb::catalog::Table;
use risingwave_pb::stream_plan::agg_call_state::MaterializedInputState;
use risingwave_pb::stream_plan::stream_fragment_graph::StreamFragmentEdge;
use risingwave_pb::stream_plan::{
    agg_call_state, stream_node, DispatcherType, StreamFragmentGraph, StreamNode,
};

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

    fn pretty_add_table<'a>(&mut self, tb: &Table) -> Pretty<'a> {
        Pretty::debug(&self.add_table(tb))
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

    fn explain_table<'a>(&self, tb: &Table) -> Pretty<'a> {
        let tb = TableCatalog::from(tb.clone());
        let columns = tb
            .columns
            .iter()
            .map(|c| {
                let s = if self.verbose {
                    format!("{}: {}", c.name(), c.data_type())
                } else {
                    c.name().to_string()
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
                        DispatcherType::Broadcast => "Broadcast".to_string(),
                        DispatcherType::Simple => "Single".to_string(),
                        DispatcherType::NoShuffle => "NoShuffle".to_string(),
                    },
                    upstream_fragment_id
                )
            }
            _ => node.identity.clone(),
        };

        let mut fields: Vec<(Cow<'_, str>, Pretty<'_>)> = Vec::with_capacity(7);
        let mut node_copy = node.clone();
        stream_graph_visitor::visit_stream_node_tables_inner(
            &mut node_copy,
            false,
            |table, table_name| {
                fields.push((table_name.to_string().into(), self.pretty_add_table(table)));
            },
        );

        if self.verbose {
            fields.push((
                "output".into(),
                Pretty::Array(
                    node.fields
                        .iter()
                        .map(|f| Pretty::display(f.get_name()))
                        .collect(),
                ),
            ));
            fields.push((
                "stream key".into(),
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
        Pretty::Record(XmlNode::new(one_line_explain.into(), fields, children))
    }

    fn call_states<'a>(
        &mut self,
        agg_call_states: &[risingwave_pb::stream_plan::AggCallState],
    ) -> Pretty<'a> {
        let vec = agg_call_states
            .iter()
            .filter_map(|state| match state.get_inner().unwrap() {
                agg_call_state::Inner::ValueState(_) => None,
                agg_call_state::Inner::MaterializedInputState(MaterializedInputState {
                    table,
                    ..
                }) => Some(self.pretty_add_table(table.as_ref().unwrap())),
            })
            .collect();
        Pretty::Array(vec)
    }

    fn distinct_tables<'a>(
        &mut self,
        node: &StreamNode,
        inner: &HashMap<u32, Table>,
    ) -> Pretty<'a> {
        let in_fields = &node.get_input()[0].fields;
        Pretty::Array(
            inner
                .iter()
                .sorted_by_key(|(i, _)| *i)
                .map(|(i, table)| {
                    let fmt = format!(
                        "(distinct key: {}, table id: {})",
                        in_fields[*i as usize].name,
                        self.add_table(table)
                    );
                    Pretty::Text(fmt.into())
                })
                .collect(),
        )
    }
}
