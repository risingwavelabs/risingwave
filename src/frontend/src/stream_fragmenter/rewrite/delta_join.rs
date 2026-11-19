// Copyright 2022 RisingWave Labs
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

use std::rc::Rc;

use itertools::Itertools;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_pb::plan_common::PbField;
use risingwave_pb::stream_plan::lookup_node::ArrangementTableId;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::{
    DispatchStrategy, DispatcherType, ExchangeNode, LookupNode, LookupUnionNode,
    MultiwayDeltaIndexJoinNode, PbDispatchOutputMapping, StreamNode,
};

use super::super::{BuildFragmentGraphState, StreamFragment, StreamFragmentEdge};
use crate::error::ErrorCode::NotSupported;
use crate::error::Result;
use crate::stream_fragmenter::build_and_add_fragment;

fn build_no_shuffle_exchange_for_delta_join(
    state: &mut BuildFragmentGraphState,
    upstream: &StreamNode,
) -> StreamNode {
    StreamNode {
        operator_id: state.gen_operator_id(),
        identity: "NO SHUFFLE Exchange (Lookup and Merge)".into(),
        fields: upstream.fields.clone(),
        stream_key: upstream.stream_key.clone(),
        node_body: Some(NodeBody::Exchange(Box::new(ExchangeNode {
            strategy: Some(dispatch_no_shuffle(
                (0..(upstream.fields.len() as u32)).collect(),
            )),
        }))),
        input: vec![],
        stream_kind: upstream.stream_kind,
    }
}

fn build_consistent_hash_shuffle_exchange_for_delta_join(
    state: &mut BuildFragmentGraphState,
    upstream: &StreamNode,
    dist_key_indices: Vec<u32>,
) -> StreamNode {
    StreamNode {
        operator_id: state.gen_operator_id(),
        identity: "HASH Exchange (Lookup and Merge)".into(),
        fields: upstream.fields.clone(),
        stream_key: upstream.stream_key.clone(),
        node_body: Some(NodeBody::Exchange(Box::new(ExchangeNode {
            strategy: Some(dispatch_consistent_hash_shuffle(
                dist_key_indices,
                (0..(upstream.fields.len() as u32)).collect(),
            )),
        }))),
        input: vec![],
        stream_kind: upstream.stream_kind,
    }
}

fn dispatch_no_shuffle(output_indices: Vec<u32>) -> DispatchStrategy {
    DispatchStrategy {
        r#type: DispatcherType::NoShuffle.into(),
        dist_key_indices: vec![],
        output_mapping: PbDispatchOutputMapping::simple(output_indices).into(),
    }
}

fn dispatch_consistent_hash_shuffle(
    dist_key_indices: Vec<u32>,
    output_indices: Vec<u32>,
) -> DispatchStrategy {
    // Actually Hash shuffle is consistent hash shuffle now.
    DispatchStrategy {
        r#type: DispatcherType::Hash.into(),
        dist_key_indices,
        output_mapping: PbDispatchOutputMapping::simple(output_indices).into(),
    }
}

fn build_lookup_for_delta_join(
    state: &mut BuildFragmentGraphState,
    (exchange_node_arrangement, exchange_node_stream): (&StreamNode, &StreamNode),
    (output_fields, output_stream_key): (Vec<PbField>, Vec<u32>),
    lookup_node: LookupNode,
) -> StreamNode {
    StreamNode {
        operator_id: state.gen_operator_id(),
        identity: "Lookup".into(),
        fields: output_fields,
        stream_key: output_stream_key,
        node_body: Some(NodeBody::Lookup(Box::new(lookup_node))),
        input: vec![
            exchange_node_arrangement.clone(),
            exchange_node_stream.clone(),
        ],
        stream_kind: exchange_node_stream.stream_kind,
    }
}

fn build_delta_join_inner(
    state: &mut BuildFragmentGraphState,
    current_fragment: &StreamFragment,
    arrange_0_frag: Rc<StreamFragment>,
    arrange_1_frag: Rc<StreamFragment>,
    node: &StreamNode,
    is_local_table_id: bool,
) -> Result<StreamNode> {
    let delta_join_node = match &node.node_body {
        Some(NodeBody::DeltaIndexJoin(node)) => node,
        _ => unreachable!(),
    };
    let output_indices = &delta_join_node.output_indices;

    let arrange_0 = arrange_0_frag.node.as_ref().unwrap();
    let arrange_1 = arrange_1_frag.node.as_ref().unwrap();
    let exchange_a0l0 = build_no_shuffle_exchange_for_delta_join(state, arrange_0);
    let exchange_a0l1 = build_consistent_hash_shuffle_exchange_for_delta_join(
        state,
        arrange_0,
        delta_join_node
            .left_key
            .iter()
            .map(|x| *x as u32)
            .collect_vec(),
    );
    let exchange_a1l0 = build_consistent_hash_shuffle_exchange_for_delta_join(
        state,
        arrange_1,
        delta_join_node
            .right_key
            .iter()
            .map(|x| *x as u32)
            .collect_vec(),
    );
    let exchange_a1l1 = build_no_shuffle_exchange_for_delta_join(state, arrange_1);

    let i0_length = arrange_0.fields.len();
    let i1_length = arrange_1.fields.len();

    let i0_output_indices = (0..i0_length as u32).collect_vec();
    let i1_output_indices = (0..i1_length as u32).collect_vec();

    let lookup_0_column_reordering = {
        let tmp: Vec<i32> = (i1_length..i1_length + i0_length)
            .chain(0..i1_length)
            .map(|x| x as _)
            .collect_vec();
        output_indices
            .iter()
            .map(|&x| tmp[x as usize])
            .collect_vec()
    };
    // lookup left table by right stream
    let lookup_0 = build_lookup_for_delta_join(
        state,
        (&exchange_a1l0, &exchange_a0l0),
        (node.fields.clone(), node.stream_key.clone()),
        LookupNode {
            stream_key: delta_join_node.right_key.clone(),
            arrange_key: delta_join_node.left_key.clone(),
            use_current_epoch: false,
            // will be updated later to a global id
            arrangement_table_id: if is_local_table_id {
                Some(ArrangementTableId::TableId(delta_join_node.left_table_id))
            } else {
                Some(ArrangementTableId::IndexId(delta_join_node.left_table_id))
            },
            column_mapping: lookup_0_column_reordering,
            arrangement_table_info: delta_join_node.left_info.clone(),
            null_safe: delta_join_node.null_safe.clone(),
            // The left arrangement is backfilled.
            visibility_table: delta_join_node.left_visibility_table.clone(),
        },
    );
    let lookup_1_column_reordering = {
        let tmp: Vec<i32> = (0..i0_length + i1_length)
            .chain(0..i1_length)
            .map(|x| x as _)
            .collect_vec();
        output_indices
            .iter()
            .map(|&x| tmp[x as usize])
            .collect_vec()
    };
    // lookup right table by left stream
    let lookup_1 = build_lookup_for_delta_join(
        state,
        (&exchange_a0l1, &exchange_a1l1),
        (node.fields.clone(), node.stream_key.clone()),
        LookupNode {
            stream_key: delta_join_node.left_key.clone(),
            arrange_key: delta_join_node.right_key.clone(),
            use_current_epoch: true,
            // will be updated later to a global id
            arrangement_table_id: if is_local_table_id {
                Some(ArrangementTableId::TableId(delta_join_node.right_table_id))
            } else {
                Some(ArrangementTableId::IndexId(delta_join_node.right_table_id))
            },
            column_mapping: lookup_1_column_reordering,
            arrangement_table_info: delta_join_node.right_info.clone(),
            null_safe: delta_join_node.null_safe.clone(),
            visibility_table: None,
        },
    );

    let lookup_0_frag = build_and_add_fragment(state, lookup_0)?;
    let lookup_1_frag = build_and_add_fragment(state, lookup_1)?;

    // Place index(arrange) together with corresponding lookup operator, so that we can lookup on
    // the same node.
    state.fragment_graph.add_edge(
        arrange_0_frag.fragment_id,
        lookup_0_frag.fragment_id,
        StreamFragmentEdge {
            dispatch_strategy: dispatch_no_shuffle(i0_output_indices.clone()),
            link_id: exchange_a0l0.operator_id.as_raw_id(),
        },
    );

    // Use consistent hash shuffle to distribute the index(arrange) to another lookup operator, so
    // that we can find the correct node to lookup.
    state.fragment_graph.add_edge(
        arrange_0_frag.fragment_id,
        lookup_1_frag.fragment_id,
        StreamFragmentEdge {
            dispatch_strategy: dispatch_consistent_hash_shuffle(
                delta_join_node
                    .left_key
                    .iter()
                    .map(|x| *x as u32)
                    .collect_vec(),
                i0_output_indices,
            ),
            link_id: exchange_a0l1.operator_id.as_raw_id(),
        },
    );

    // Use consistent hash shuffle to distribute the index(arrange) to another lookup operator, so
    // that we can find the correct node to lookup.
    state.fragment_graph.add_edge(
        arrange_1_frag.fragment_id,
        lookup_0_frag.fragment_id,
        StreamFragmentEdge {
            dispatch_strategy: dispatch_consistent_hash_shuffle(
                delta_join_node
                    .right_key
                    .iter()
                    .map(|x| *x as u32)
                    .collect_vec(),
                i1_output_indices.clone(),
            ),
            link_id: exchange_a1l0.operator_id.as_raw_id(),
        },
    );

    // Place index(arrange) together with corresponding lookup operator, so that we can lookup on
    // the same node.
    state.fragment_graph.add_edge(
        arrange_1_frag.fragment_id,
        lookup_1_frag.fragment_id,
        StreamFragmentEdge {
            dispatch_strategy: dispatch_no_shuffle(i1_output_indices),
            link_id: exchange_a1l1.operator_id.as_raw_id(),
        },
    );

    let exchange_l0m =
        build_consistent_hash_shuffle_exchange_for_delta_join(state, node, node.stream_key.clone());
    let exchange_l1m =
        build_consistent_hash_shuffle_exchange_for_delta_join(state, node, node.stream_key.clone());

    // LookupUnion's inputs might have different distribution and we need to unify them by using
    // hash shuffle.
    //
    // `lookup_0` joins the changes of the right input with the left arrangement of the previous
    // epoch, which turns the old join result into `left_old join right_new`. `lookup_1` then
    // joins the changes of the left input with the right arrangement of the current epoch, which
    // turns it into `left_new join right_new`. The outputs must be emitted in this order, otherwise
    // the output is not a consistent change stream when both inputs change the same join result in
    // an epoch, e.g. an update in a self join.
    let union = StreamNode {
        operator_id: state.gen_operator_id(),
        identity: "Union".into(),
        fields: node.fields.clone(),
        stream_key: node.stream_key.clone(),
        node_body: Some(NodeBody::LookupUnion(Box::new(LookupUnionNode {
            order: vec![0, 1],
        }))),
        input: vec![exchange_l0m.clone(), exchange_l1m.clone()],
        stream_kind: node.stream_kind,
    };

    state.fragment_graph.add_edge(
        lookup_0_frag.fragment_id,
        current_fragment.fragment_id,
        StreamFragmentEdge {
            dispatch_strategy: dispatch_consistent_hash_shuffle(
                node.stream_key.clone(),
                (0..node.fields.len() as u32).collect(),
            ),
            link_id: exchange_l0m.operator_id.as_raw_id(),
        },
    );

    state.fragment_graph.add_edge(
        lookup_1_frag.fragment_id,
        current_fragment.fragment_id,
        StreamFragmentEdge {
            dispatch_strategy: dispatch_consistent_hash_shuffle(
                node.stream_key.clone(),
                (0..node.fields.len() as u32).collect(),
            ),
            link_id: exchange_l1m.operator_id.as_raw_id(),
        },
    );

    Ok(union)
}

// TODO: when distribution key is added to catalog, chain and delta join won't have any
// exchange in-between. Then we can safely remove this function.
fn pass_through_exchange(mut node: StreamNode) -> StreamNode {
    if let Some(NodeBody::Exchange(exchange)) = node.node_body {
        if let DispatcherType::NoShuffle = exchange.strategy.as_ref().unwrap().get_type().unwrap() {
            return node.input.remove(0);
        }
        panic!("exchange other than no_shuffle not allowed between delta join and arrange");
    } else {
        // pass
        node
    }
}

pub(crate) fn build_delta_join_without_arrange(
    state: &mut BuildFragmentGraphState,
    current_fragment: &StreamFragment,
    mut node: StreamNode,
) -> Result<StreamNode> {
    match &node.node_body {
        Some(NodeBody::DeltaIndexJoin(node)) => node,
        _ => unreachable!(),
    };

    let [arrange_0, arrange_1]: [_; 2] = std::mem::take(&mut node.input).try_into().unwrap();

    let arrange_0 = pass_through_exchange(arrange_0);
    let arrange_1 = pass_through_exchange(arrange_1);

    let arrange_0_frag = build_and_add_fragment(state, arrange_0)?;
    let arrange_1_frag = build_and_add_fragment(state, arrange_1)?;

    let union = build_delta_join_inner(
        state,
        current_fragment,
        arrange_0_frag,
        arrange_1_frag,
        &node,
        false,
    )?;

    if state.has_snapshot_backfill {
        return Err(NotSupported(
            "Delta join with snapshot backfill is not supported".to_owned(),
            "Please use a different join strategy or disable snapshot backfill by `SET streaming_use_snapshot_backfill = false`.".to_owned(),
        ).into());
    }

    Ok(union)
}

/// Rewrites a [`MultiwayDeltaIndexJoinNode`] into lookup fragments.
///
/// Every arrangement becomes a fragment. Every path becomes a chain of lookup fragments: the first
/// lookup reads the changes of the path's stream arrangement, and every following lookup reads the
/// output of the previous one. The stream input of a lookup is hash shuffled by its join key, which
/// is the distribution key of the looked-up arrangement, and the arrangement input is a no-shuffle
/// exchange, so each lookup runs together with the arrangement it looks up. The output of every
/// path is hash shuffled by the stream key into a `LookupUnion`, which emits the paths in order.
pub(crate) fn build_multiway_delta_join(
    state: &mut BuildFragmentGraphState,
    current_fragment: &StreamFragment,
    mut node: StreamNode,
) -> Result<StreamNode> {
    if state.has_snapshot_backfill {
        return Err(NotSupported(
            "Delta join with snapshot backfill is not supported".to_owned(),
            "Please use a different join strategy or disable snapshot backfill by `SET streaming_use_snapshot_backfill = false`.".to_owned(),
        ).into());
    }

    let join = match &node.node_body {
        Some(NodeBody::MultiwayDeltaIndexJoin(join)) => (**join).clone(),
        _ => unreachable!(),
    };
    let MultiwayDeltaIndexJoinNode {
        arrangements,
        paths,
        output_indices,
        dist_key,
    } = join;
    // The outputs of the paths have different distributions, so shuffle them by the distribution
    // key of the output before the union.
    let union_shuffle_key = if dist_key.is_empty() {
        node.stream_key.clone()
    } else {
        dist_key
    };

    let arrangement_nodes = std::mem::take(&mut node.input)
        .into_iter()
        .map(pass_through_exchange)
        .collect_vec();
    assert_eq!(arrangement_nodes.len(), arrangements.len());
    let arrangement_frags = arrangement_nodes
        .iter()
        .map(|arrangement| build_and_add_fragment(state, arrangement.clone()))
        .collect::<Result<Vec<_>>>()?;

    // The number of columns of each join input, and its offset in the concatenation of all join
    // inputs.
    let input_num = arrangements.iter().map(|a| a.input).max().unwrap() as usize + 1;
    let mut input_widths = vec![0; input_num];
    for (arrangement, arrangement_node) in arrangements.iter().zip_eq_fast(&arrangement_nodes) {
        input_widths[arrangement.input as usize] = arrangement_node.fields.len();
    }
    let input_offsets = input_widths
        .iter()
        .scan(0, |offset, width| {
            let current = *offset;
            *offset += width;
            Some(current)
        })
        .collect_vec();

    let all_columns = |node: &StreamNode| (0..node.fields.len() as u32).collect_vec();

    let mut union_inputs = Vec::with_capacity(paths.len());
    for path in &paths {
        let stream_arrangement = path.stream_arrangement as usize;

        // The fragment producing the rows joined so far on this path, its output, and the offset
        // of each join input in its output.
        let mut prev_frag = arrangement_frags[stream_arrangement].clone();
        let mut prev_node = arrangement_nodes[stream_arrangement].clone();
        let mut path_offsets = vec![None; input_num];
        path_offsets[arrangements[stream_arrangement].input as usize] = Some(0);

        for (step_idx, step) in path.steps.iter().enumerate() {
            let arrangement = &arrangements[step.arrangement as usize];
            let arrangement_node = &arrangement_nodes[step.arrangement as usize];
            let arrangement_frag = arrangement_frags[step.arrangement as usize].clone();

            let exchange_stream = build_consistent_hash_shuffle_exchange_for_delta_join(
                state,
                &prev_node,
                step.stream_key.clone(),
            );
            let exchange_arrangement =
                build_no_shuffle_exchange_for_delta_join(state, arrangement_node);

            let stream_width = prev_node.fields.len();
            path_offsets[arrangement.input as usize] = Some(stream_width);
            let is_last = step_idx + 1 == path.steps.len();
            let (fields, stream_key, column_mapping) = if is_last {
                // Output the columns of the join, see `output_indices`.
                let column_mapping = output_indices
                    .iter()
                    .map(|&i| {
                        let input = input_offsets
                            .iter()
                            .rposition(|&offset| offset <= i as usize)
                            .unwrap();
                        (path_offsets[input].unwrap() + i as usize - input_offsets[input]) as i32
                    })
                    .collect_vec();
                (node.fields.clone(), node.stream_key.clone(), column_mapping)
            } else {
                // Output the stream side and then the arrangement side.
                let fields = prev_node
                    .fields
                    .iter()
                    .chain(arrangement_node.fields.iter())
                    .cloned()
                    .collect_vec();
                let stream_key = prev_node
                    .stream_key
                    .iter()
                    .copied()
                    .chain(
                        arrangement_node
                            .stream_key
                            .iter()
                            .map(|&k| k + stream_width as u32),
                    )
                    .collect_vec();
                let column_mapping = (0..fields.len() as i32).collect_vec();
                (fields, stream_key, column_mapping)
            };

            let lookup = StreamNode {
                operator_id: state.gen_operator_id(),
                identity: "Lookup".into(),
                fields,
                stream_key,
                node_body: Some(NodeBody::Lookup(Box::new(LookupNode {
                    stream_key: step.stream_key.iter().map(|&k| k as i32).collect(),
                    arrange_key: step.arrange_key.iter().map(|&k| k as i32).collect(),
                    use_current_epoch: step.use_current_epoch,
                    arrangement_table_id: Some(ArrangementTableId::IndexId(arrangement.table_id)),
                    column_mapping,
                    arrangement_table_info: arrangement.info.clone(),
                    // The multi-way delta join only supports null-unsafe equal conditions.
                    null_safe: vec![false; step.stream_key.len()],
                    visibility_table: step.visibility_table.clone(),
                }))),
                // The lookup executor takes the stream input first and then the arrangement input.
                input: vec![exchange_stream.clone(), exchange_arrangement.clone()],
                stream_kind: node.stream_kind,
            };
            let lookup_frag = build_and_add_fragment(state, lookup.clone())?;

            // Shuffle the rows joined so far by the join key, which is the distribution key of the
            // looked-up arrangement.
            state.fragment_graph.add_edge(
                prev_frag.fragment_id,
                lookup_frag.fragment_id,
                StreamFragmentEdge {
                    dispatch_strategy: dispatch_consistent_hash_shuffle(
                        step.stream_key.clone(),
                        all_columns(&prev_node),
                    ),
                    link_id: exchange_stream.operator_id.as_raw_id(),
                },
            );
            // Place the arrangement together with the lookup, so that the lookup can read it on the
            // same node.
            state.fragment_graph.add_edge(
                arrangement_frag.fragment_id,
                lookup_frag.fragment_id,
                StreamFragmentEdge {
                    dispatch_strategy: dispatch_no_shuffle(all_columns(arrangement_node)),
                    link_id: exchange_arrangement.operator_id.as_raw_id(),
                },
            );

            prev_frag = lookup_frag;
            prev_node = lookup;
        }

        let exchange_union = build_consistent_hash_shuffle_exchange_for_delta_join(
            state,
            &node,
            union_shuffle_key.clone(),
        );
        state.fragment_graph.add_edge(
            prev_frag.fragment_id,
            current_fragment.fragment_id,
            StreamFragmentEdge {
                dispatch_strategy: dispatch_consistent_hash_shuffle(
                    union_shuffle_key.clone(),
                    all_columns(&node),
                ),
                link_id: exchange_union.operator_id.as_raw_id(),
            },
        );
        union_inputs.push(exchange_union);
    }

    // The paths are already in the order their outputs must be emitted.
    Ok(StreamNode {
        operator_id: state.gen_operator_id(),
        identity: "Union".into(),
        fields: node.fields.clone(),
        stream_key: node.stream_key.clone(),
        node_body: Some(NodeBody::LookupUnion(Box::new(LookupUnionNode {
            order: (0..union_inputs.len() as u32).collect(),
        }))),
        input: union_inputs,
        stream_kind: node.stream_kind,
    })
}
