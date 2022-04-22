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
use risingwave_common::error::Result;
use risingwave_pb::plan::Field;
use risingwave_pb::stream_plan::stream_node::Node;
use risingwave_pb::stream_plan::{
    ArrangeNode, DispatchStrategy, DispatcherType, ExchangeNode, LookupNode, StreamNode, UnionNode,
};

use crate::storage::MetaStore;
use crate::stream::graph::{StreamFragment, StreamFragmentEdge};
use crate::stream::StreamFragmenter;

impl<S> StreamFragmenter<S>
where
    S: MetaStore,
{
    /// All exchanges inside delta join is one-to-one exchange.
    fn build_exchange_for_delta_join(&mut self, upstream: &StreamNode) -> StreamNode {
        StreamNode {
            operator_id: self.gen_operator_id() as u64,
            identity: "Exchange (Lookup and Merge)".into(),
            fields: upstream.fields.clone(),
            pk_indices: upstream.pk_indices.clone(),
            node: Some(Node::ExchangeNode(ExchangeNode {
                strategy: Some(Self::dispatch_no_shuffle()),
            })),
            input: vec![],
            append_only: upstream.append_only,
        }
    }

    /// The ultimate exchange between delta join fragments and the upstream fragments.
    ///
    /// We should shuffle upstream data to make sure it meets the distribution requirement of hash
    /// join.
    fn build_input_with_exchange(
        &mut self,
        mut upstream: StreamNode,
    ) -> Result<(StreamFragmentEdge, StreamFragment, StreamNode)> {
        match &upstream.node {
            // If the upstream contains a exchange, we should follow that distribution.
            Some(Node::ExchangeNode(exchange_node)) => {
                let exchange_node = exchange_node.clone();
                assert_eq!(upstream.input.len(), 1);
                let child_node = upstream.input.remove(0);
                let child_fragment = self.build_and_add_fragment(child_node)?;
                Ok((
                    StreamFragmentEdge {
                        dispatch_strategy: exchange_node.get_strategy()?.clone(),
                        same_worker_node: false,
                        link_id: upstream.operator_id,
                    },
                    child_fragment,
                    StreamNode {
                        input: vec![],
                        ..upstream.clone()
                    },
                ))
            }
            // Otherwise, use 1-to-1 exchange
            _ => {
                let strategy = Self::dispatch_no_shuffle();
                let operator_id = self.gen_operator_id() as u64;
                let node = StreamNode {
                    input: vec![],
                    identity: "Exchange (Arrange)".into(),
                    fields: upstream.fields.clone(),
                    pk_indices: upstream.pk_indices.clone(),
                    node: Some(Node::ExchangeNode(ExchangeNode {
                        strategy: Some(strategy.clone()),
                    })),
                    operator_id,
                    append_only: upstream.append_only,
                };

                let child_fragment = self.build_and_add_fragment(upstream)?;

                Ok((
                    StreamFragmentEdge {
                        dispatch_strategy: strategy,
                        same_worker_node: false,
                        link_id: operator_id,
                    },
                    child_fragment,
                    node,
                ))
            }
        }
    }

    fn build_arrange_for_delta_join(
        &mut self,
        exchange_node: &StreamNode,
        arrange_key_indexes: Vec<i32>,
    ) -> StreamNode {
        StreamNode {
            operator_id: self.gen_operator_id() as u64,
            identity: "Arrange".into(),
            fields: exchange_node.fields.clone(),
            pk_indices: exchange_node.pk_indices.clone(),
            node: Some(Node::ArrangeNode(ArrangeNode {
                arrange_key_indexes,
            })),
            input: vec![exchange_node.clone()],
            append_only: exchange_node.append_only,
        }
    }

    fn dispatch_no_shuffle() -> DispatchStrategy {
        DispatchStrategy {
            r#type: DispatcherType::NoShuffle.into(),
            column_indices: vec![],
        }
    }

    fn build_lookup_for_delta_join(
        &mut self,
        (exchange_node_arrangement, exchange_node_stream): (&StreamNode, &StreamNode),
        (output_fields, output_pk_indices): (Vec<Field>, Vec<u32>),
        lookup_node: LookupNode,
    ) -> StreamNode {
        StreamNode {
            operator_id: self.gen_operator_id() as u64,
            identity: "Lookup".into(),
            fields: output_fields,
            pk_indices: output_pk_indices,
            node: Some(Node::LookupNode(lookup_node)),
            input: vec![
                exchange_node_arrangement.clone(),
                exchange_node_stream.clone(),
            ],
            append_only: exchange_node_stream.append_only,
        }
    }

    pub fn build_delta_join(
        &mut self,
        current_fragment: &mut StreamFragment,
        mut node: StreamNode,
    ) -> Result<StreamNode> {
        let hash_join_node = match &node.node {
            Some(Node::HashJoinNode(node)) => node,
            _ => unreachable!(),
        };

        assert_eq!(node.input.len(), 2);

        // Previous plan:
        //
        // ```
        // input_1 --\
        //            -- HashJoin --- output
        // input_2 --/
        // ```
        //
        // Rewritten plan:
        //
        // ```
        // input_1 --- Arrange --- Lookup ---\
        //                      X            --- Merge --- output
        // input_2 --- Arrange --- Lookup ---/
        // ```
        //
        // Within the whole process, the distribution doesn't get changed, so we always use
        // NoShuffle as exchange type.
        //
        // TODO: support multi-way join.

        let exchange_a0l0 = self.build_exchange_for_delta_join(&node.input[0]);
        let exchange_a0l1 = self.build_exchange_for_delta_join(&node.input[0]);
        let exchange_a1l0 = self.build_exchange_for_delta_join(&node.input[1]);
        let exchange_a1l1 = self.build_exchange_for_delta_join(&node.input[1]);

        let i0_length = node.input[0].fields.len();
        let i1_length = node.input[1].fields.len();

        let (link_i1a1, input_1_frag, exchange_i1a1) =
            self.build_input_with_exchange(node.input.remove(1))?;

        let (link_i0a0, input_0_frag, exchange_i0a0) =
            self.build_input_with_exchange(node.input.remove(0))?;

        let arrange_0 =
            self.build_arrange_for_delta_join(&exchange_i0a0, hash_join_node.left_key.clone());
        let arrange_1 =
            self.build_arrange_for_delta_join(&exchange_i1a1, hash_join_node.right_key.clone());

        let arrange_0_frag = self.build_and_add_fragment(arrange_0)?;
        let arrange_1_frag = self.build_and_add_fragment(arrange_1)?;

        self.fragment_graph.add_edge(
            input_0_frag.fragment_id,
            arrange_0_frag.fragment_id,
            link_i0a0,
        );

        self.fragment_graph.add_edge(
            input_1_frag.fragment_id,
            arrange_1_frag.fragment_id,
            link_i1a1,
        );

        let lookup_0 = self.build_lookup_for_delta_join(
            (&exchange_a1l0, &exchange_a0l0),
            (node.fields.clone(), node.pk_indices.clone()),
            LookupNode {
                stream_key: hash_join_node.right_key.clone(),
                arrange_key: hash_join_node.left_key.clone(),
                use_current_epoch: false,
                // will be filled later in StreamFragment::seal
                arrange_fragment_id: u32::MAX,
                arrange_local_fragment_id: arrange_0_frag.fragment_id.as_local_id(),
                arrange_operator_id: arrange_0_frag.node.unwrap().operator_id,
                column_mapping: (i1_length..i1_length + i0_length)
                    .chain(0..i1_length)
                    .map(|x| x as _)
                    .collect_vec(),
            },
        );

        let lookup_1 = self.build_lookup_for_delta_join(
            (&exchange_a0l1, &exchange_a1l1),
            (node.fields.clone(), node.pk_indices.clone()),
            LookupNode {
                stream_key: hash_join_node.left_key.clone(),
                arrange_key: hash_join_node.right_key.clone(),
                use_current_epoch: true,
                // will be filled later in StreamFragment::seal
                arrange_fragment_id: u32::MAX,
                arrange_local_fragment_id: arrange_1_frag.fragment_id.as_local_id(),
                arrange_operator_id: arrange_1_frag.node.unwrap().operator_id,
                column_mapping: (0..i0_length + i1_length).map(|x| x as _).collect_vec(),
            },
        );

        let lookup_0_frag = self.build_and_add_fragment(lookup_0)?;
        let lookup_1_frag = self.build_and_add_fragment(lookup_1)?;

        self.fragment_graph.add_edge(
            arrange_0_frag.fragment_id,
            lookup_0_frag.fragment_id,
            StreamFragmentEdge {
                dispatch_strategy: Self::dispatch_no_shuffle(),
                same_worker_node: false,
                link_id: exchange_a0l0.operator_id,
            },
        );

        self.fragment_graph.add_edge(
            arrange_0_frag.fragment_id,
            lookup_1_frag.fragment_id,
            StreamFragmentEdge {
                dispatch_strategy: Self::dispatch_no_shuffle(),
                same_worker_node: false,
                link_id: exchange_a0l1.operator_id,
            },
        );

        self.fragment_graph.add_edge(
            arrange_1_frag.fragment_id,
            lookup_0_frag.fragment_id,
            StreamFragmentEdge {
                dispatch_strategy: Self::dispatch_no_shuffle(),
                same_worker_node: false,
                link_id: exchange_a1l0.operator_id,
            },
        );

        self.fragment_graph.add_edge(
            arrange_1_frag.fragment_id,
            lookup_1_frag.fragment_id,
            StreamFragmentEdge {
                dispatch_strategy: Self::dispatch_no_shuffle(),
                same_worker_node: false,
                link_id: exchange_a1l1.operator_id,
            },
        );

        let exchange_l0m = self.build_exchange_for_delta_join(&node);
        let exchange_l1m = self.build_exchange_for_delta_join(&node);

        let union = StreamNode {
            operator_id: self.gen_operator_id() as u64,
            identity: "Union".into(),
            fields: node.fields.clone(),
            pk_indices: node.pk_indices.clone(),
            node: Some(Node::UnionNode(UnionNode {})),
            input: vec![exchange_l0m.clone(), exchange_l1m.clone()],
            append_only: node.append_only,
        };

        self.fragment_graph.add_edge(
            lookup_0_frag.fragment_id,
            current_fragment.fragment_id,
            StreamFragmentEdge {
                dispatch_strategy: Self::dispatch_no_shuffle(),
                same_worker_node: false,
                link_id: exchange_l0m.operator_id,
            },
        );

        self.fragment_graph.add_edge(
            lookup_1_frag.fragment_id,
            current_fragment.fragment_id,
            StreamFragmentEdge {
                dispatch_strategy: Self::dispatch_no_shuffle(),
                same_worker_node: false,
                link_id: exchange_l1m.operator_id,
            },
        );

        Ok(union)
    }
}
