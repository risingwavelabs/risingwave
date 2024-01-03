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

use risingwave_pb::catalog::Table;
use risingwave_pb::stream_plan::stream_fragment_graph::StreamFragment;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::{agg_call_state, StreamNode};

/// A utility for visiting and mutating the [`NodeBody`] of the [`StreamNode`]s recursively.
pub fn visit_stream_node<F>(stream_node: &mut StreamNode, mut f: F)
where
    F: FnMut(&mut NodeBody),
{
    fn visit_inner<F>(stream_node: &mut StreamNode, f: &mut F)
    where
        F: FnMut(&mut NodeBody),
    {
        f(stream_node.node_body.as_mut().unwrap());
        for input in &mut stream_node.input {
            visit_inner(input, f);
        }
    }

    visit_inner(stream_node, &mut f)
}

/// A utility for to accessing the [`StreamNode`]. The returned bool is used to determine whether the access needs to continue.
pub fn visit_stream_node_cont<F>(stream_node: &mut StreamNode, mut f: F)
where
    F: FnMut(&mut StreamNode) -> bool,
{
    fn visit_inner<F>(stream_node: &mut StreamNode, f: &mut F)
    where
        F: FnMut(&mut StreamNode) -> bool,
    {
        if !f(stream_node) {
            return;
        }
        for input in &mut stream_node.input {
            visit_inner(input, f);
        }
    }

    visit_inner(stream_node, &mut f)
}

/// A utility for visiting and mutating the [`NodeBody`] of the [`StreamNode`]s in a
/// [`StreamFragment`] recursively.
pub fn visit_fragment<F>(fragment: &mut StreamFragment, f: F)
where
    F: FnMut(&mut NodeBody),
{
    visit_stream_node(fragment.node.as_mut().unwrap(), f)
}

/// Visit the tables of a [`StreamNode`].
fn visit_stream_node_tables_inner<F>(
    stream_node: &mut StreamNode,
    internal_tables_only: bool,
    mut f: F,
) where
    F: FnMut(&mut Table, &str),
{
    macro_rules! always {
        ($table:expr, $name:expr) => {{
            let table = $table
                .as_mut()
                .unwrap_or_else(|| panic!("internal table {} should always exist", $name));
            f(table, $name);
        }};
    }

    macro_rules! optional {
        ($table:expr, $name:expr) => {
            if let Some(table) = &mut $table {
                f(table, $name);
            }
        };
    }

    macro_rules! repeated {
        ($tables:expr, $name:expr) => {
            for table in &mut $tables {
                f(table, $name);
            }
        };
    }

    visit_stream_node(stream_node, |body| {
        match body {
            // Join
            NodeBody::HashJoin(node) => {
                // TODO: make the degree table optional
                always!(node.left_table, "HashJoinLeft");
                always!(node.left_degree_table, "HashJoinDegreeLeft");
                always!(node.right_table, "HashJoinRight");
                always!(node.right_degree_table, "HashJoinDegreeRight");
            }
            NodeBody::DynamicFilter(node) => {
                if node.condition_always_relax {
                    always!(node.left_table, "DynamicFilterLeftNotSatisfy");
                } else {
                    always!(node.left_table, "DynamicFilterLeft");
                }

                always!(node.right_table, "DynamicFilterRight");
            }

            // Aggregation
            NodeBody::HashAgg(node) => {
                assert_eq!(node.agg_call_states.len(), node.agg_calls.len());
                always!(node.intermediate_state_table, "HashAggState");
                for (call_idx, state) in node.agg_call_states.iter_mut().enumerate() {
                    match state.inner.as_mut().unwrap() {
                        agg_call_state::Inner::ValueState(_) => {}
                        agg_call_state::Inner::MaterializedInputState(s) => {
                            always!(s.table, &format!("HashAggCall{}", call_idx));
                        }
                    }
                }
                for (distinct_col, dedup_table) in &mut node.distinct_dedup_tables {
                    f(dedup_table, &format!("HashAggDedupForCol{}", distinct_col));
                }
            }
            NodeBody::SimpleAgg(node) => {
                assert_eq!(node.agg_call_states.len(), node.agg_calls.len());
                always!(node.intermediate_state_table, "SimpleAggState");
                for (call_idx, state) in node.agg_call_states.iter_mut().enumerate() {
                    match state.inner.as_mut().unwrap() {
                        agg_call_state::Inner::ValueState(_) => {}
                        agg_call_state::Inner::MaterializedInputState(s) => {
                            always!(s.table, &format!("SimpleAggCall{}", call_idx));
                        }
                    }
                }
                for (distinct_col, dedup_table) in &mut node.distinct_dedup_tables {
                    f(
                        dedup_table,
                        &format!("SimpleAggDedupForCol{}", distinct_col),
                    );
                }
            }

            // Top-N
            NodeBody::AppendOnlyTopN(node) => {
                always!(node.table, "AppendOnlyTopN");
            }
            NodeBody::TopN(node) => {
                always!(node.table, "TopN");
            }
            NodeBody::AppendOnlyGroupTopN(node) => {
                always!(node.table, "AppendOnlyGroupTopN");
            }
            NodeBody::GroupTopN(node) => {
                always!(node.table, "GroupTopN");
            }

            // Source
            NodeBody::Source(node) => {
                if let Some(source) = &mut node.source_inner {
                    always!(source.state_table, "Source");
                }
            }
            NodeBody::StreamFsFetch(node) => {
                if let Some(source) = &mut node.node_inner {
                    always!(source.state_table, "FsFetch");
                }
            }

            // Sink
            NodeBody::Sink(node) => {
                // A sink with a kv log store should have a state table.
                optional!(node.table, "Sink")
            }

            // Now
            NodeBody::Now(node) => {
                always!(node.state_table, "Now");
            }

            // Watermark filter
            NodeBody::WatermarkFilter(node) => {
                assert!(!node.tables.is_empty());
                repeated!(node.tables, "WatermarkFilter");
            }

            // Shared arrangement
            NodeBody::Arrange(node) => {
                always!(node.table, "Arrange");
            }

            // Dedup
            NodeBody::AppendOnlyDedup(node) => {
                always!(node.state_table, "AppendOnlyDedup");
            }

            // EOWC over window
            NodeBody::EowcOverWindow(node) => {
                always!(node.state_table, "EowcOverWindow");
            }

            NodeBody::OverWindow(node) => {
                always!(node.state_table, "OverWindow");
            }

            // Sort
            NodeBody::Sort(node) => {
                always!(node.state_table, "Sort");
            }

            // Stream Scan
            NodeBody::StreamScan(node) => {
                optional!(node.state_table, "StreamScan")
            }

            // Stream Cdc Scan
            NodeBody::StreamCdcScan(node) => {
                always!(node.state_table, "StreamCdcScan")
            }

            // Note: add internal tables for new nodes here.
            NodeBody::Materialize(node) if !internal_tables_only => {
                always!(node.table, "Materialize")
            }
            _ => {}
        }
    })
}

pub fn visit_stream_node_internal_tables<F>(stream_node: &mut StreamNode, f: F)
where
    F: FnMut(&mut Table, &str),
{
    visit_stream_node_tables_inner(stream_node, true, f)
}

#[allow(dead_code)]
pub fn visit_stream_node_tables<F>(stream_node: &mut StreamNode, f: F)
where
    F: FnMut(&mut Table, &str),
{
    visit_stream_node_tables_inner(stream_node, false, f)
}

/// Visit the internal tables of a [`StreamFragment`].
pub fn visit_internal_tables<F>(fragment: &mut StreamFragment, f: F)
where
    F: FnMut(&mut Table, &str),
{
    visit_stream_node_internal_tables(fragment.node.as_mut().unwrap(), f)
}
