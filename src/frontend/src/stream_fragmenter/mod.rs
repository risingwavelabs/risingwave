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

mod graph;
use graph::*;
use risingwave_common::util::recursive::{self, Recurse as _};
use risingwave_connector::WithPropertiesExt;
use risingwave_pb::stream_plan::stream_fragment_graph::Parallelism;
use risingwave_pb::stream_plan::stream_node::NodeBody;
mod rewrite;

use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use educe::Educe;
use risingwave_common::catalog::TableId;
use risingwave_pb::plan_common::JoinType;
use risingwave_pb::stream_plan::{
    DispatchStrategy, DispatcherType, ExchangeNode, FragmentTypeFlag, NoOpNode, StreamContext,
    StreamFragmentGraph as StreamFragmentGraphProto, StreamNode, StreamScanType,
};

use self::rewrite::build_delta_join_without_arrange;
use crate::error::Result;
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::reorganize_elements_id;
use crate::optimizer::PlanRef;
use crate::scheduler::SchedulerResult;

/// The mutable state when building fragment graph.
#[derive(Educe)]
#[educe(Default)]
pub struct BuildFragmentGraphState {
    /// fragment graph field, transformed from input streaming plan.
    fragment_graph: StreamFragmentGraph,
    /// local fragment id
    next_local_fragment_id: u32,

    /// Next local table id to be allocated. It equals to total table ids cnt when finish stream
    /// node traversing.
    next_table_id: u32,

    /// rewrite will produce new operators, and we need to track next operator id
    #[educe(Default(expression = u32::MAX - 1))]
    next_operator_id: u32,

    /// dependent streaming job ids.
    dependent_table_ids: HashSet<TableId>,

    /// operator id to `LocalFragmentId` mapping used by share operator.
    share_mapping: HashMap<u32, LocalFragmentId>,
    /// operator id to `StreamNode` mapping used by share operator.
    share_stream_node_mapping: HashMap<u32, StreamNode>,
}

impl BuildFragmentGraphState {
    /// Create a new stream fragment with given node with generating a fragment id.
    fn new_stream_fragment(&mut self) -> StreamFragment {
        let fragment = StreamFragment::new(self.next_local_fragment_id);
        self.next_local_fragment_id += 1;
        fragment
    }

    /// Generate an operator id
    fn gen_operator_id(&mut self) -> u32 {
        self.next_operator_id -= 1;
        self.next_operator_id
    }

    /// Generate an table id
    pub fn gen_table_id(&mut self) -> u32 {
        let ret = self.next_table_id;
        self.next_table_id += 1;
        ret
    }

    /// Generate an table id
    pub fn gen_table_id_wrapped(&mut self) -> TableId {
        TableId::new(self.gen_table_id())
    }

    pub fn add_share_stream_node(&mut self, operator_id: u32, stream_node: StreamNode) {
        self.share_stream_node_mapping
            .insert(operator_id, stream_node);
    }

    pub fn get_share_stream_node(&mut self, operator_id: u32) -> Option<&StreamNode> {
        self.share_stream_node_mapping.get(&operator_id)
    }

    /// Generate a new stream node with `NoOp` body and the given `input`. The properties of the
    /// stream node will also be copied from the `input` node.
    pub fn gen_no_op_stream_node(&mut self, input: StreamNode) -> StreamNode {
        StreamNode {
            operator_id: self.gen_operator_id() as u64,
            identity: "StreamNoOp".into(),
            node_body: Some(NodeBody::NoOp(NoOpNode {})),

            // Take input's properties.
            stream_key: input.stream_key.clone(),
            append_only: input.append_only,
            fields: input.fields.clone(),

            input: vec![input],
        }
    }
}

pub fn build_graph(plan_node: PlanRef) -> SchedulerResult<StreamFragmentGraphProto> {
    let ctx = plan_node.plan_base().ctx();
    let plan_node = reorganize_elements_id(plan_node);

    let mut state = BuildFragmentGraphState::default();
    let stream_node = plan_node.to_stream_prost(&mut state)?;
    generate_fragment_graph(&mut state, stream_node).unwrap();
    let mut fragment_graph = state.fragment_graph.to_protobuf();

    // Set table ids.
    fragment_graph.dependent_table_ids = state
        .dependent_table_ids
        .into_iter()
        .map(|id| id.table_id)
        .collect();
    fragment_graph.table_ids_cnt = state.next_table_id;

    // Set parallelism and vnode count.
    {
        let config = ctx.session_ctx().config();

        fragment_graph.parallelism =
            config
                .streaming_parallelism()
                .map(|parallelism| Parallelism {
                    parallelism: parallelism.get(),
                });
        fragment_graph.max_parallelism = config.streaming_max_parallelism() as _;
    }

    // Set timezone.
    fragment_graph.ctx = Some(StreamContext {
        timezone: ctx.get_session_timezone(),
    });

    Ok(fragment_graph)
}

#[cfg(any())]
fn is_stateful_executor(stream_node: &StreamNode) -> bool {
    matches!(
        stream_node.get_node_body().unwrap(),
        NodeBody::HashAgg(_)
            | NodeBody::HashJoin(_)
            | NodeBody::DeltaIndexJoin(_)
            | NodeBody::StreamScan(_)
            | NodeBody::StreamCdcScan(_)
            | NodeBody::DynamicFilter(_)
    )
}

/// Do some dirty rewrites before building the fragments.
/// Currently, it will split the fragment with multiple stateful operators (those have high I/O
/// throughput) into multiple fragments, which may help improve the I/O concurrency.
/// Known as "no-shuffle exchange" or "1v1 exchange".
#[cfg(any())]
fn rewrite_stream_node(
    state: &mut BuildFragmentGraphState,
    stream_node: StreamNode,
    insert_exchange_flag: bool,
) -> Result<StreamNode> {
    let f = |child| {
        // For stateful operators, set `exchange_flag = true`. If it's already true,
        // force add an exchange.
        if is_stateful_executor(&child) {
            if insert_exchange_flag {
                let child_node = rewrite_stream_node(state, child, true)?;

                let strategy = DispatchStrategy {
                    r#type: DispatcherType::NoShuffle.into(),
                    dist_key_indices: vec![], // TODO: use distribution key
                    output_indices: (0..(child_node.fields.len() as u32)).collect(),
                };
                Ok(StreamNode {
                    stream_key: child_node.stream_key.clone(),
                    fields: child_node.fields.clone(),
                    node_body: Some(NodeBody::Exchange(ExchangeNode {
                        strategy: Some(strategy),
                    })),
                    operator_id: state.gen_operator_id() as u64,
                    append_only: child_node.append_only,
                    input: vec![child_node],
                    identity: "Exchange (NoShuffle)".to_string(),
                })
            } else {
                rewrite_stream_node(state, child, true)
            }
        } else {
            match child.get_node_body()? {
                // For exchanges, reset the flag.
                NodeBody::Exchange(_) => rewrite_stream_node(state, child, false),
                // Otherwise, recursively visit the children.
                _ => rewrite_stream_node(state, child, insert_exchange_flag),
            }
        }
    };
    Ok(StreamNode {
        input: stream_node
            .input
            .into_iter()
            .map(f)
            .collect::<Result<_>>()?,
        ..stream_node
    })
}

/// Generate fragment DAG from input streaming plan by their dependency.
fn generate_fragment_graph(
    state: &mut BuildFragmentGraphState,
    stream_node: StreamNode,
) -> Result<()> {
    // TODO: the 1v1 exchange is disabled for now, as it breaks the assumption of independent
    // scaling of fragments. We may introduce further optimization transparently to the fragmenter.
    // #4614
    #[cfg(any())]
    let stream_node = rewrite_stream_node(state, stream_node, is_stateful_executor(&stream_node))?;

    build_and_add_fragment(state, stream_node)?;
    Ok(())
}

/// Use the given `stream_node` to create a fragment and add it to graph.
fn build_and_add_fragment(
    state: &mut BuildFragmentGraphState,
    stream_node: StreamNode,
) -> Result<Rc<StreamFragment>> {
    let operator_id = stream_node.operator_id as u32;
    match state.share_mapping.get(&operator_id) {
        None => {
            let mut fragment = state.new_stream_fragment();
            let node = build_fragment(state, &mut fragment, stream_node)?;

            // It's possible that the stream node is rewritten while building the fragment, for
            // example, empty fragment to no-op fragment. We get the operator id again instead of
            // using the original one.
            let operator_id = node.operator_id as u32;

            assert!(fragment.node.is_none());
            fragment.node = Some(Box::new(node));
            let fragment_ref = Rc::new(fragment);

            state.fragment_graph.add_fragment(fragment_ref.clone());
            state
                .share_mapping
                .insert(operator_id, fragment_ref.fragment_id);
            Ok(fragment_ref)
        }
        Some(fragment_id) => Ok(state
            .fragment_graph
            .get_fragment(fragment_id)
            .unwrap()
            .clone()),
    }
}

/// Build new fragment and link dependencies by visiting children recursively, update
/// `requires_singleton` and `fragment_type` properties for current fragment.
fn build_fragment(
    state: &mut BuildFragmentGraphState,
    current_fragment: &mut StreamFragment,
    mut stream_node: StreamNode,
) -> Result<StreamNode> {
    recursive::tracker!().recurse(|_t| {
        // Update current fragment based on the node we're visiting.
        match stream_node.get_node_body()? {
            NodeBody::BarrierRecv(_) => {
                current_fragment.fragment_type_mask |= FragmentTypeFlag::BarrierRecv as u32
            }

            NodeBody::Source(node) => {
                current_fragment.fragment_type_mask |= FragmentTypeFlag::Source as u32;

                if let Some(source) = node.source_inner.as_ref()
                    && let Some(source_info) = source.info.as_ref()
                    && ((source_info.is_shared() && !source_info.is_distributed)
                        || source.with_properties.is_new_fs_connector())
                {
                    current_fragment.requires_singleton = true;
                }
            }

            NodeBody::Dml(_) => {
                current_fragment.fragment_type_mask |= FragmentTypeFlag::Dml as u32;
            }

            NodeBody::Materialize(_) => {
                current_fragment.fragment_type_mask |= FragmentTypeFlag::Mview as u32;
            }

            NodeBody::Sink(_) => {
                current_fragment.fragment_type_mask |= FragmentTypeFlag::Sink as u32
            }

            NodeBody::TopN(_) => current_fragment.requires_singleton = true,

            NodeBody::StreamScan(node) => {
                current_fragment.fragment_type_mask |= FragmentTypeFlag::StreamScan as u32;
                match node.stream_scan_type() {
                    StreamScanType::SnapshotBackfill => {
                        current_fragment.fragment_type_mask |=
                            FragmentTypeFlag::SnapshotBackfillStreamScan as u32;
                    }
                    StreamScanType::CrossDbSnapshotBackfill => {
                        current_fragment.fragment_type_mask |=
                            FragmentTypeFlag::CrossDbSnapshotBackfillStreamScan as u32;
                    }
                    StreamScanType::Unspecified
                    | StreamScanType::Chain
                    | StreamScanType::Rearrange
                    | StreamScanType::Backfill
                    | StreamScanType::UpstreamOnly
                    | StreamScanType::ArrangementBackfill => {}
                }
                // memorize table id for later use
                // The table id could be a upstream CDC source
                state
                    .dependent_table_ids
                    .insert(TableId::new(node.table_id));
                current_fragment.upstream_table_ids.push(node.table_id);
            }

            NodeBody::StreamCdcScan(_) => {
                current_fragment.fragment_type_mask |= FragmentTypeFlag::StreamScan as u32;
                // the backfill algorithm is not parallel safe
                current_fragment.requires_singleton = true;
            }

            NodeBody::CdcFilter(node) => {
                current_fragment.fragment_type_mask |= FragmentTypeFlag::CdcFilter as u32;
                // memorize upstream source id for later use
                state
                    .dependent_table_ids
                    .insert(node.upstream_source_id.into());
                current_fragment
                    .upstream_table_ids
                    .push(node.upstream_source_id);
            }
            NodeBody::SourceBackfill(node) => {
                current_fragment.fragment_type_mask |= FragmentTypeFlag::SourceScan as u32;
                // memorize upstream source id for later use
                let source_id = node.upstream_source_id;
                state.dependent_table_ids.insert(source_id.into());
                current_fragment.upstream_table_ids.push(source_id);
            }

            NodeBody::Now(_) => {
                // TODO: Remove this and insert a `BarrierRecv` instead.
                current_fragment.fragment_type_mask |= FragmentTypeFlag::Now as u32;
                current_fragment.requires_singleton = true;
            }

            NodeBody::Values(_) => {
                current_fragment.fragment_type_mask |= FragmentTypeFlag::Values as u32;
                current_fragment.requires_singleton = true;
            }

            NodeBody::StreamFsFetch(_) => {
                current_fragment.fragment_type_mask |= FragmentTypeFlag::FsFetch as u32;
            }

            _ => {}
        };

        // handle join logic
        if let NodeBody::DeltaIndexJoin(delta_index_join) = stream_node.node_body.as_mut().unwrap()
        {
            if delta_index_join.get_join_type()? == JoinType::Inner
                && delta_index_join.condition.is_none()
            {
                return build_delta_join_without_arrange(state, current_fragment, stream_node);
            } else {
                panic!("only inner join without non-equal condition is supported for delta joins");
            }
        }

        // Usually we do not expect exchange node to be visited here, which should be handled by the
        // following logic of "visit children" instead. If it does happen (for example, `Share` will be
        // transformed to an `Exchange`), it means we have an empty fragment and we need to add a no-op
        // node to it, so that the meta service can handle it correctly.
        if let NodeBody::Exchange(_) = stream_node.node_body.as_ref().unwrap() {
            stream_node = state.gen_no_op_stream_node(stream_node);
        }

        // Visit plan children.
        stream_node.input = stream_node
            .input
            .into_iter()
            .map(|mut child_node| {
                match child_node.get_node_body()? {
                    // When exchange node is generated when doing rewrites, it could be having
                    // zero input. In this case, we won't recursively visit its children.
                    NodeBody::Exchange(_) if child_node.input.is_empty() => Ok(child_node),
                    // Exchange node indicates a new child fragment.
                    NodeBody::Exchange(exchange_node) => {
                        let exchange_node_strategy = exchange_node.get_strategy()?.clone();

                        // Exchange node should have only one input.
                        let [input]: [_; 1] =
                            std::mem::take(&mut child_node.input).try_into().unwrap();
                        let child_fragment = build_and_add_fragment(state, input)?;

                        let result = state.fragment_graph.try_add_edge(
                            child_fragment.fragment_id,
                            current_fragment.fragment_id,
                            StreamFragmentEdge {
                                dispatch_strategy: exchange_node_strategy.clone(),
                                // Always use the exchange operator id as the link id.
                                link_id: child_node.operator_id,
                            },
                        );

                        // It's possible that there're multiple edges between two fragments, while the
                        // meta service and the compute node does not expect this. In this case, we
                        // manually insert a fragment of `NoOp` between the two fragments.
                        if result.is_err() {
                            // Assign a new operator id for the `Exchange`, so we can distinguish it
                            // from duplicate edges and break the sharing.
                            child_node.operator_id = state.gen_operator_id() as u64;

                            // Take the upstream plan node as the reference for properties of `NoOp`.
                            let ref_fragment_node = child_fragment.node.as_ref().unwrap();
                            let no_shuffle_strategy = DispatchStrategy {
                                r#type: DispatcherType::NoShuffle as i32,
                                dist_key_indices: vec![],
                                output_indices: (0..ref_fragment_node.fields.len() as u32)
                                    .collect(),
                            };

                            let no_shuffle_exchange_operator_id = state.gen_operator_id() as u64;

                            let no_op_fragment = {
                                let node = state.gen_no_op_stream_node(StreamNode {
                                    operator_id: no_shuffle_exchange_operator_id,
                                    identity: "StreamNoShuffleExchange".into(),
                                    node_body: Some(NodeBody::Exchange(Box::new(ExchangeNode {
                                        strategy: Some(no_shuffle_strategy.clone()),
                                    }))),
                                    input: vec![],

                                    // Take reference's properties.
                                    stream_key: ref_fragment_node.stream_key.clone(),
                                    append_only: ref_fragment_node.append_only,
                                    fields: ref_fragment_node.fields.clone(),
                                });

                                let mut fragment = state.new_stream_fragment();
                                fragment.node = Some(node.into());
                                Rc::new(fragment)
                            };

                            state.fragment_graph.add_fragment(no_op_fragment.clone());

                            state.fragment_graph.add_edge(
                                child_fragment.fragment_id,
                                no_op_fragment.fragment_id,
                                StreamFragmentEdge {
                                    // Use `NoShuffle` exhcnage strategy for upstream edge.
                                    dispatch_strategy: no_shuffle_strategy,
                                    link_id: no_shuffle_exchange_operator_id,
                                },
                            );
                            state.fragment_graph.add_edge(
                                no_op_fragment.fragment_id,
                                current_fragment.fragment_id,
                                StreamFragmentEdge {
                                    // Use the original exchange strategy for downstream edge.
                                    dispatch_strategy: exchange_node_strategy,
                                    link_id: child_node.operator_id,
                                },
                            );
                        }

                        Ok(child_node)
                    }

                    // For other children, visit recursively.
                    _ => build_fragment(state, current_fragment, child_node),
                }
            })
            .collect::<Result<_>>()?;
        Ok(stream_node)
    })
}
