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
use risingwave_common::catalog::{ColumnDesc, Field};
use risingwave_common::error::Result;
use risingwave_common::try_match_expand;
use risingwave_common::util::sort_util::OrderType;
use risingwave_pb::plan_common::{Field as ProstField, OrderType as ProstOrderType};
use risingwave_pb::stream_plan::lookup_node::ArrangementTableId;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::{
    ArrangementInfo, DispatchStrategy, DispatcherType, ExchangeNode, LookupNode, LookupUnionNode,
    StreamNode,
};

use super::super::{BuildFragmentGraphState, StreamFragment, StreamFragmentEdge};
use crate::catalog::TableCatalog;
use crate::optimizer::plan_node::utils::TableCatalogBuilder;
use crate::stream_fragmenter::build_and_add_fragment;

/// All exchanges inside delta join is one-to-one exchange.
fn build_exchange_for_delta_join(
    state: &mut BuildFragmentGraphState,
    upstream: &StreamNode,
) -> StreamNode {
    StreamNode {
        operator_id: state.gen_operator_id() as u64,
        identity: "Exchange (Lookup and Merge)".into(),
        fields: upstream.fields.clone(),
        stream_key: upstream.stream_key.clone(),
        node_body: Some(NodeBody::Exchange(ExchangeNode {
            strategy: Some(dispatch_no_shuffle()),
        })),
        input: vec![],
        append_only: upstream.append_only,
    }
}

fn dispatch_no_shuffle() -> DispatchStrategy {
    DispatchStrategy {
        r#type: DispatcherType::NoShuffle.into(),
        column_indices: vec![],
    }
}

fn build_lookup_for_delta_join(
    state: &mut BuildFragmentGraphState,
    (exchange_node_arrangement, exchange_node_stream): (&StreamNode, &StreamNode),
    (output_fields, output_stream_key): (Vec<ProstField>, Vec<u32>),
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
    current_fragment: &mut StreamFragment,
    arrange_0_frag: StreamFragment,
    arrange_1_frag: StreamFragment,
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
    let exchange_a0l0 = build_exchange_for_delta_join(state, arrange_0);
    let exchange_a0l1 = build_exchange_for_delta_join(state, arrange_0);
    let exchange_a1l0 = build_exchange_for_delta_join(state, arrange_1);
    let exchange_a1l1 = build_exchange_for_delta_join(state, arrange_1);

    let i0_length = arrange_0.fields.len();
    let i1_length = arrange_1.fields.len();

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
            arrangement_table: Some(
                infer_internal_table_catalog(
                    delta_join_node.left_info.as_ref(),
                    // Use Arrange node's dist key.
                    try_match_expand!(arrange_0.get_node_body().unwrap(), NodeBody::Arrange)?
                        .distribution_key
                        .clone()
                        .iter()
                        .map(|x| *x as usize)
                        .collect(),
                    exchange_a0l0.append_only,
                )
                .to_state_table_prost(),
            ),
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
            arrangement_table: Some(
                infer_internal_table_catalog(
                    delta_join_node.right_info.as_ref(),
                    // Use Arrange node's dist key.
                    try_match_expand!(arrange_1.get_node_body().unwrap(), NodeBody::Arrange)?
                        .distribution_key
                        .clone()
                        .iter()
                        .map(|x| *x as usize)
                        .collect(),
                    exchange_a1l1.append_only,
                )
                .to_state_table_prost(),
            ),
        },
    );

    let lookup_0_frag = build_and_add_fragment(state, lookup_0)?;
    let lookup_1_frag = build_and_add_fragment(state, lookup_1)?;

    state.fragment_graph.add_edge(
        arrange_0_frag.fragment_id,
        lookup_0_frag.fragment_id,
        StreamFragmentEdge {
            dispatch_strategy: dispatch_no_shuffle(),
            same_worker_node: true,
            link_id: exchange_a0l0.operator_id,
        },
    );

    state.fragment_graph.add_edge(
        arrange_0_frag.fragment_id,
        lookup_1_frag.fragment_id,
        StreamFragmentEdge {
            dispatch_strategy: dispatch_no_shuffle(),
            // stream input doesn't need to be on the same worker node as lookup
            same_worker_node: false,
            link_id: exchange_a0l1.operator_id,
        },
    );

    state.fragment_graph.add_edge(
        arrange_1_frag.fragment_id,
        lookup_0_frag.fragment_id,
        StreamFragmentEdge {
            dispatch_strategy: dispatch_no_shuffle(),
            // stream input doesn't need to be on the same worker node as lookup
            same_worker_node: false,
            link_id: exchange_a1l0.operator_id,
        },
    );

    state.fragment_graph.add_edge(
        arrange_1_frag.fragment_id,
        lookup_1_frag.fragment_id,
        StreamFragmentEdge {
            dispatch_strategy: dispatch_no_shuffle(),
            same_worker_node: true,
            link_id: exchange_a1l1.operator_id,
        },
    );

    let exchange_l0m = build_exchange_for_delta_join(state, node);
    let exchange_l1m = build_exchange_for_delta_join(state, node);

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
            dispatch_strategy: dispatch_no_shuffle(),
            same_worker_node: false,
            link_id: exchange_l0m.operator_id,
        },
    );

    state.fragment_graph.add_edge(
        lookup_1_frag.fragment_id,
        current_fragment.fragment_id,
        StreamFragmentEdge {
            dispatch_strategy: dispatch_no_shuffle(),
            same_worker_node: false,
            link_id: exchange_l1m.operator_id,
        },
    );

    Ok(union)
}

pub(in super::super) fn build_delta_join_without_arrange(
    state: &mut BuildFragmentGraphState,
    current_fragment: &mut StreamFragment,
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
            unimplemented!()
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

fn infer_internal_table_catalog(
    arrangement_info: Option<&ArrangementInfo>,
    distribution_key: Vec<usize>,
    append_only: bool,
) -> TableCatalog {
    let arrangement_info = arrangement_info.unwrap();
    let mut internal_table_catalog_builder = TableCatalogBuilder::new();
    for column_desc in &arrangement_info.column_descs {
        internal_table_catalog_builder.add_column(&Field::from(&ColumnDesc::from(column_desc)));
    }

    for order in &arrangement_info.arrange_key_orders {
        internal_table_catalog_builder.add_order_column(
            order.index as usize,
            OrderType::from_prost(&ProstOrderType::from_i32(order.order_type).unwrap()),
        );
    }

    internal_table_catalog_builder.build(distribution_key, append_only)
}
