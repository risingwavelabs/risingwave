// Copyright 2023 RisingWave Labs
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

use std::collections::{BTreeMap, HashMap};

use itertools::Itertools;
use pretty_xmlish::{Pretty, PrettyConfig};
use risingwave_pb::catalog::Table;
use risingwave_pb::stream_plan::agg_call_state::{MaterializedInputState, TableState};
use risingwave_pb::stream_plan::stream_fragment_graph::{StreamFragment, StreamFragmentEdge};
use risingwave_pb::stream_plan::{
    agg_call_state, stream_node, DispatcherType, StreamFragmentGraph, StreamNode,
};

use crate::TableCatalog;

/// ice: in the future, we may allow configurable width, boundaries, etc.
pub fn explain_stream_graph(graph: &StreamFragmentGraph, is_verbose: bool) -> String {
    let mut output = String::with_capacity(2048);
    let pretty = StreamGraphFormatter::new(is_verbose).explain_graph(graph);
    let mut config = PrettyConfig::default();
    config.need_boundaries = false;
    config.width = 120;
    config.unicode(&mut output, &pretty);
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

    fn explain_graph<'a>(&mut self, graph: &StreamFragmentGraph) -> Pretty<'a> {
        self.edges.clear();
        for edge in &graph.edges {
            self.edges.insert(edge.link_id, edge.clone());
        }
        let mut pretties = Vec::with_capacity(100);
        for (_, fragment) in graph.fragments.iter().sorted_by_key(|(id, _)| **id) {
            pretties.push(self.explain_fragment(fragment));
        }
        for tb in self.tables.values() {
            pretties.push(self.explain_table(tb));
        }
        Pretty::Array(pretties)
    }

    fn explain_table<'a>(&mut self, tb: &Table) -> Pretty<'a> {
        let tb = TableCatalog::from(tb.clone());
        let columns = Pretty::Array(
            tb.columns
                .iter()
                .map(|c| {
                    let s = if self.verbose {
                        format!("{}: {}", c.name(), c.data_type())
                    } else {
                        c.name().to_string()
                    };
                    Pretty::Text(s.into())
                })
                .collect::<Vec<_>>(),
        );
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
        if let Some(vnode_col_idx) = tb.vnode_col_index {
            fields.push(("vnode column idx", Pretty::debug(&vnode_col_idx)));
        }
        Pretty::childless_record(name, fields)
    }

    fn explain_fragment<'a>(&mut self, fragment: &StreamFragment) -> Pretty<'a> {
        let id = Some(fragment.get_fragment_id());
        self.explain_node(id, fragment.node.as_ref().unwrap())
    }

    fn explain_node<'a>(&mut self, fragment_id: Option<u32>, node: &StreamNode) -> Pretty<'a> {
        let one_line_explain = match node.get_node_body().unwrap() {
            stream_node::NodeBody::Exchange(_) => {
                let edge = self.edges.get(&node.operator_id).unwrap();
                let upstream_fragment_id = edge.upstream_id;
                let dist = edge.dispatch_strategy.as_ref().unwrap();
                format!(
                    "{}StreamExchange {} from {}",
                    if let Some(id) = fragment_id {
                        format!("Fragment {} ", id)
                    } else {
                        "".to_string()
                    },
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

        let mut fields = Vec::with_capacity(7);
        match node.get_node_body().unwrap() {
            stream_node::NodeBody::Source(node) if let Some(source) = node.source_inner => {
                fields.push((
                    "source state table",
                    self.pretty_add_table(source.get_state_table().unwrap()),
                ));
            }
            stream_node::NodeBody::Source(node) => {}
            stream_node::NodeBody::Materialize(node) => fields.push((
                "materialized table",
                self.pretty_add_table(node.get_table().unwrap()),
            )),
            stream_node::NodeBody::GlobalSimpleAgg(node) => {
                fields.push((
                    "result table",
                    self.pretty_add_table(node.get_result_table().unwrap()),
                ));
                fields.push(("state tables", self.call_states(&node.agg_call_states)));
            }
            stream_node::NodeBody::HashAgg(node) => {
                fields.push((
                    "result table",
                    self.pretty_add_table(node.get_result_table().unwrap()),
                ));
                fields.push(("state tables", self.call_states(&node.agg_call_states)));
            },
            stream_node::NodeBody::HashJoin(node) => {
                fields.push((
                    "left table",
                    self.pretty_add_table(node.get_left_table().unwrap()),
                ));
                fields.push((
                    "right table",
                    self.pretty_add_table(node.get_right_table().unwrap()),
                ));
                if let Some(tb) = &node.left_degree_table {
                    fields.push((
                        "left degree table",
                        self.pretty_add_table(tb),
                    ));
                }
                if let Some(tb) = &node.right_degree_table {
                    fields.push((
                        "right degree table",
                        self.pretty_add_table(tb),
                    ));
                }
            }
            stream_node::NodeBody::TopN(node) =>{
                fields.push((
                    "state table",
                    self.pretty_add_table(node.get_table().unwrap()),
                ));
            }
            stream_node::NodeBody::AppendOnlyTopN(node) => {
                fields.push((
                    "state table",
                    self.pretty_add_table(node.get_table().unwrap()),
                ));
            }
            stream_node::NodeBody::Arrange(node) => {
                fields.push((
                    "arrange table",
                    self.pretty_add_table(node.get_table().unwrap()),
                ));
            }
            stream_node::NodeBody::DynamicFilter(node) => {
                fields.push((
                    "left table",
                    self.pretty_add_table(node.get_left_table().unwrap()),
                ));
                fields.push((
                    "right table",
                    self.pretty_add_table(node.get_right_table().unwrap()),
                ));
            }
            stream_node::NodeBody::GroupTopN(node) => {
                let table = self.pretty_add_table(node.get_table().unwrap());
                fields.push((
                    "state table",
                    table,
                ));
            }
            stream_node::NodeBody::AppendOnlyGroupTopN(node) => {
                fields.push((
                    "state table",
                    self.pretty_add_table(node.get_table().unwrap()),
                ));
            }
            stream_node::NodeBody::Now(node) => {
                fields.push((
                    "state table",
                    self.pretty_add_table(node.get_state_table().unwrap()),
                ));
            }
            _ => {},
        };

        if self.verbose {
            fields.push((
                "output",
                Pretty::Array(
                    node.fields
                        .iter()
                        .map(|f| Pretty::debug(f.get_name()))
                        .collect(),
                ),
            ));
            // writeln!(
            //     f,
            //     "{}Stream key: [{}], {}",
            //     " ".repeat(level * 2 + 4),
            //     node.stream_key
            //         .iter()
            //         .map(|i| node.fields[*i as usize].get_name())
            //         .join(", "),
            //     if node.append_only { "AppendOnly" } else { "" }
            // )?;
            fields.push((
                "stream key",
                Pretty::Array(
                    node.stream_key
                        .iter()
                        .map(|i| Pretty::debug(node.fields[*i as usize].get_name()))
                        .collect(),
                ),
            ));
        }
        let children = node
            .input
            .iter()
            .map(|input| self.explain_node(None, input))
            .collect();
        Pretty::simple_record(one_line_explain, fields, children)
    }

    fn call_states<'a>(
        &mut self,
        agg_call_states: &[risingwave_pb::stream_plan::AggCallState],
    ) -> Pretty<'a> {
        let vec = agg_call_states
            .iter()
            .filter_map(|state| match state.get_inner().unwrap() {
                agg_call_state::Inner::ResultValueState(_) => None,
                agg_call_state::Inner::TableState(TableState { table })
                | agg_call_state::Inner::MaterializedInputState(MaterializedInputState {
                    table,
                    ..
                }) => Some(self.pretty_add_table(table.as_ref().unwrap())),
            })
            .collect();
        Pretty::Array(vec)
    }
}
