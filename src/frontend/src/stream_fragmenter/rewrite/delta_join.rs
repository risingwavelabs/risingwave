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

use std::rc::Rc;

use itertools::Itertools;
use risingwave_common::error::Result;
use risingwave_pb::plan_common::PbField;
use risingwave_pb::stream_plan::lookup_node::ArrangementTableId;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::{
    DispatchStrategy, DispatcherType, ExchangeNode, LookupNode, LookupUnionNode, StreamNode,
};

use super::super::{BuildFragmentGraphState, StreamFragment, StreamFragmentEdge};
use crate::stream_fragmenter::build_and_add_fragment;

fn build_no_shuffle_exchange_for_delta_join(
    state: &mut BuildFragmentGraphState,
    upstream: &StreamNode,
) -> StreamNode {
    StreamNode {
        operator_id: state.gen_operator_id() as u64,
        identity: "NO SHUFFLE Exchange (Lookup and Merge)".into(),
        fields: upstream.fields.clone(),
        stream_key: upstream.stream_key.clone(),
        node_body: Some(NodeBody::Exchange(ExchangeNode {
            strategy: Some(dispatch_no_shuffle(
                (0..(upstream.fields.len() as u32)).collect(),
            )),
        })),
        input: vec![],
        append_only: upstream.append_only,
    }
}

fn build_consistent_hash_shuffle_exchange_for_delta_join(
    state: &mut BuildFragmentGraphState,
    upstream: &StreamNode,
    dist_key_indices: Vec<u32>,
) -> StreamNode {
    StreamNode {
        operator_id: state.gen_operator_id() as u64,
        identity: "HASH Exchange (Lookup and Merge)".into(),
        fields: upstream.fields.clone(),
        stream_key: upstream.stream_key.clone(),
        node_body: Some(NodeBody::Exchange(ExchangeNode {
            strategy: Some(dispatch_consistent_hash_shuffle(
                dist_key_indices,
                (0..(upstream.fields.len() as u32)).collect(),
            )),
        })),
        input: vec![],
        append_only: upstream.append_only,
    }
}

fn dispatch_no_shuffle(output_indices: Vec<u32>) -> DispatchStrategy {
    DispatchStrategy {
        r#type: DispatcherType::NoShuffle.into(),
        dist_key_indices: vec![],
        output_indices,
        downstream_table_name: None,
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
        output_indices,
        downstream_table_name: None,
    }
}

fn build_lookup_for_delta_join(
    state: &mut BuildFragmentGraphState,
    (exchange_node_arrangement, exchange_node_stream): (&StreamNode, &StreamNode),
    (output_fields, output_stream_key): (Vec<PbField>, Vec<u32>),
    lookup_node: LookupNode,
) -> StreamNode {
    StreamNode {
        operator_id: state.gen_operator_id() as u64,
        identity: "Lookup".into(),
        fields: output_fields,
        stream_key: output_stream_key,
        node_body: Some(NodeBody::Lookup(lookup_node)),
        input: vec![
            exchange_node_arrangement.clone(),
            exchange_node_stream.clone(),
        ],
        append_only: exchange_node_stream.append_only,
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
            link_id: exchange_a0l0.operator_id,
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
            link_id: exchange_a0l1.operator_id,
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
            link_id: exchange_a1l0.operator_id,
        },
    );

    // Place index(arrange) together with corresponding lookup operator, so that we can lookup on
    // the same node.
    state.fragment_graph.add_edge(
        arrange_1_frag.fragment_id,
        lookup_1_frag.fragment_id,
        StreamFragmentEdge {
            dispatch_strategy: dispatch_no_shuffle(i1_output_indices),
            link_id: exchange_a1l1.operator_id,
        },
    );

    let exchange_l0m =
        build_consistent_hash_shuffle_exchange_for_delta_join(state, node, node.stream_key.clone());
    let exchange_l1m =
        build_consistent_hash_shuffle_exchange_for_delta_join(state, node, node.stream_key.clone());

    // LookupUnion's inputs might have different distribution and we need to unify them by using
    // hash shuffle.
    let union = StreamNode {
        operator_id: state.gen_operator_id() as u64,
        identity: "Union".into(),
        fields: node.fields.clone(),
        stream_key: node.stream_key.clone(),
        node_body: Some(NodeBody::LookupUnion(LookupUnionNode { order: vec![1, 0] })),
        input: vec![exchange_l0m.clone(), exchange_l1m.clone()],
        append_only: node.append_only,
    };

    state.fragment_graph.add_edge(
        lookup_0_frag.fragment_id,
        current_fragment.fragment_id,
        StreamFragmentEdge {
            dispatch_strategy: dispatch_consistent_hash_shuffle(
                node.stream_key.clone(),
                (0..node.fields.len() as u32).collect(),
            ),
            link_id: exchange_l0m.operator_id,
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
            link_id: exchange_l1m.operator_id,
        },
    );

    Ok(union)
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

    // TODO: when distribution key is added to catalog, chain and delta join won't have any
    // exchange in-between. Then we can safely remove this function.
    fn pass_through_exchange(mut node: StreamNode) -> StreamNode {
        if let Some(NodeBody::Exchange(exchange)) = node.node_body {
            if let DispatcherType::NoShuffle =
                exchange.strategy.as_ref().unwrap().get_type().unwrap()
            {
                return node.input.remove(0);
            }
            panic!("exchange other than no_shuffle not allowed between delta join and arrange");
        } else {
            // pass
            node
        }
    }

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

    Ok(union)
}
