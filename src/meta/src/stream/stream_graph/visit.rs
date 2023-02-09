use risingwave_pb::catalog::Table;
use risingwave_pb::stream_plan::stream_fragment_graph::StreamFragment;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::{agg_call_state, StreamNode};

/// A utility for visiting and mutating the [`NodeBody`] of the [`StreamNode`]s in a
/// [`StreamFragment`] recursively.
pub fn visit_fragment<F>(fragment: &mut StreamFragment, mut f: F)
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

    visit_inner(fragment.node.as_mut().unwrap(), &mut f)
}

/// Visit the internal tables of a [`StreamFragment`].
pub(super) fn visit_internal_tables<F>(fragment: &mut StreamFragment, mut f: F)
where
    F: FnMut(&mut Table, &'static str),
{
    macro_rules! always {
        ($table:expr, $name:expr) => {{
            let table = $table
                .as_mut()
                .unwrap_or_else(|| panic!("internal table {} should always exist", $name));
            f(table, $name);
        }};
    }

    #[allow(unused_macros)]
    macro_rules! optional {
        ($table:expr, $name:expr) => {
            if let Some(table) = &mut $table {
                f(table, $name);
            }
        };
    }

    visit_fragment(fragment, |body| {
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
                always!(node.left_table, "DynamicFilterLeft");
                always!(node.right_table, "DynamicFilterRight");
            }

            // Aggregation
            NodeBody::HashAgg(node) => {
                assert_eq!(node.agg_call_states.len(), node.agg_calls.len());
                always!(node.result_table, "HashAggResult");
                for state in &mut node.agg_call_states {
                    if let agg_call_state::Inner::MaterializedInputState(s) =
                        state.inner.as_mut().unwrap()
                    {
                        always!(s.table, "HashAgg");
                    }
                }
            }
            NodeBody::GlobalSimpleAgg(node) => {
                assert_eq!(node.agg_call_states.len(), node.agg_calls.len());
                always!(node.result_table, "GlobalSimpleAggResult");
                for state in &mut node.agg_call_states {
                    if let agg_call_state::Inner::MaterializedInputState(s) =
                        state.inner.as_mut().unwrap()
                    {
                        always!(s.table, "GlobalSimpleAgg");
                    }
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
            NodeBody::Now(node) => {
                always!(node.state_table, "Now");
            }

            // Shared arrangement
            NodeBody::Arrange(node) => {
                always!(node.table, "Arrange");
            }

            // Note: add internal tables for new nodes here.
            _ => {}
        }
    })
}
