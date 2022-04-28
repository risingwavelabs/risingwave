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

use std::collections::hash_map::HashMap;
use std::collections::{BTreeMap, HashSet};
use std::ops::Deref;
use std::sync::Arc;

use assert_matches::assert_matches;
use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_common::error::Result;
use risingwave_pb::stream_plan::stream_node::Node;
use risingwave_pb::stream_plan::{Dispatcher, DispatcherType, MergeNode, StreamActor, StreamNode};
use risingwave_pb::ProstFieldNotFound;

use crate::cluster::WorkerId;
use crate::model::{ActorId, LocalActorId, LocalFragmentId};
use crate::stream::{BuildGraphInfo, CreateMaterializedViewContext};

/// A list of actors with order.
#[derive(Debug, Clone)]
struct OrderedActorLink(pub Vec<LocalActorId>);

impl OrderedActorLink {
    pub fn to_global_ids(&self, actor_id_offset: u32, actor_id_len: u32) -> Self {
        Self(
            self.0
                .iter()
                .map(|x| x.to_global_id(actor_id_offset, actor_id_len))
                .collect(),
        )
    }

    pub fn as_global_ids(&self) -> Vec<u32> {
        Self::slice_as_global_ids(self.0.as_slice())
    }

    pub fn slice_as_global_ids(data: &[LocalActorId]) -> Vec<u32> {
        data.iter().map(|x| x.as_global_id()).collect()
    }
}

struct StreamActorDownstream {
    /// Dispatcher
    /// TODO: refactor to `DispatchStrategy`.
    dispatcher: Dispatcher,

    /// Downstream actors.
    actors: OrderedActorLink,

    /// Whether to place the downstream actors on the same node
    same_worker_node: bool,
}

struct StreamActorUpstream {
    /// Upstream actors
    actors: OrderedActorLink,

    /// Whether to place the upstream actors on the same node
    same_worker_node: bool,
}

/// [`StreamActorBuilder`] builds a stream actor in a stream DAG.
struct StreamActorBuilder {
    /// actor id field
    actor_id: LocalActorId,

    /// associated fragment id
    fragment_id: LocalFragmentId,

    /// associated stream node
    nodes: Arc<StreamNode>,

    /// downstream dispatchers (dispatcher, downstream actor, hash mapping)
    downstreams: Vec<StreamActorDownstream>,

    /// upstreams, exchange node operator_id -> upstream actor ids
    upstreams: HashMap<u64, StreamActorUpstream>,

    /// whether this actor builder has been sealed
    sealed: bool,
}

impl StreamActorBuilder {
    pub fn new(
        actor_id: LocalActorId,
        fragment_id: LocalFragmentId,
        node: Arc<StreamNode>,
    ) -> Self {
        Self {
            actor_id,
            fragment_id,
            nodes: node,
            downstreams: vec![],
            upstreams: HashMap::new(),
            sealed: false,
        }
    }

    pub fn get_fragment_id(&self) -> LocalFragmentId {
        self.fragment_id
    }

    /// Add a dispatcher to this actor. Note that the `downstream_actor_id` field must be left
    /// empty, as we will fill this out when building actors.
    pub fn add_dispatcher(
        &mut self,
        dispatcher: Dispatcher,
        downstream_actors: OrderedActorLink,
        same_worker_node: bool,
    ) {
        assert!(!self.sealed);
        // TODO: we should have a non-proto Dispatcher type here
        assert!(
            dispatcher.downstream_actor_id.is_empty(),
            "should leave downstream_actor_id empty, will be filled later"
        );
        assert!(
            dispatcher.hash_mapping.is_none(),
            "should leave hash_mapping empty, will be filled later"
        );
        self.downstreams.push(StreamActorDownstream {
            dispatcher,
            actors: downstream_actors,
            same_worker_node,
        });
    }

    /// Build an actor from given information. At the same time, convert local actor id to global
    /// actor id.
    pub fn seal(&mut self, actor_id_offset: u32, actor_id_len: u32) {
        assert!(!self.sealed);

        self.actor_id = self.actor_id.to_global_id(actor_id_offset, actor_id_len);
        self.downstreams = std::mem::take(&mut self.downstreams)
            .into_iter()
            .map(
                |StreamActorDownstream {
                     dispatcher,
                     actors: downstreams,
                     same_worker_node,
                 }| {
                    let downstreams = downstreams.to_global_ids(actor_id_offset, actor_id_len);

                    if dispatcher.r#type == DispatcherType::NoShuffle as i32 {
                        assert_eq!(
                            downstreams.0.len(),
                            1,
                            "no shuffle should only have one actor downstream"
                        );
                        assert!(
                            dispatcher.column_indices.is_empty(),
                            "should leave `column_indices` empty"
                        );
                    }

                    StreamActorDownstream {
                        dispatcher,
                        actors: downstreams,
                        same_worker_node,
                    }
                },
            )
            .collect();

        self.upstreams = std::mem::take(&mut self.upstreams)
            .into_iter()
            .map(
                |(
                    exchange_id,
                    StreamActorUpstream {
                        actors,
                        same_worker_node,
                    },
                )| {
                    (
                        exchange_id,
                        StreamActorUpstream {
                            actors: actors.to_global_ids(actor_id_offset, actor_id_len),
                            same_worker_node,
                        },
                    )
                },
            )
            .collect();
        self.sealed = true;
    }

    /// Build an actor after seal.
    pub fn build(&self) -> StreamActor {
        assert!(self.sealed);

        let mut dispatcher = self
            .downstreams
            .iter()
            .map(
                |StreamActorDownstream {
                     dispatcher, actors, ..
                 }| Dispatcher {
                    downstream_actor_id: actors.as_global_ids(),
                    ..dispatcher.clone()
                },
            )
            .collect_vec();

        // If there's no dispatcher, add an empty broadcast. TODO: Can be removed later.
        if dispatcher.is_empty() {
            dispatcher = vec![Dispatcher {
                r#type: DispatcherType::Broadcast.into(),
                ..Default::default()
            }]
        }

        StreamActor {
            actor_id: self.actor_id.as_global_id(),
            fragment_id: self.fragment_id.as_global_id(),
            nodes: Some(self.nodes.deref().clone()),
            dispatcher,
            upstream_actor_id: self
                .upstreams
                .iter()
                .flat_map(|(_, StreamActorUpstream { actors, .. })| actors.0.iter().copied())
                .map(|x| x.as_global_id())
                .collect(), // TODO: store each upstream separately
            same_worker_node_as_upstream: self.upstreams.iter().any(
                |(
                    _,
                    StreamActorUpstream {
                        same_worker_node, ..
                    },
                )| *same_worker_node,
            ),
        }
    }
}

/// [`StreamGraphBuilder`] build a stream graph. It injects some information to achieve
/// dependencies. See `build_inner` for more details.
#[derive(Default)]
pub struct StreamGraphBuilder {
    actor_builders: BTreeMap<LocalActorId, StreamActorBuilder>,

    table_node_actors: HashMap<TableId, BTreeMap<WorkerId, Vec<ActorId>>>,

    table_sink_actor_ids: HashMap<TableId, Vec<ActorId>>,

    upstream_distribution_keys: HashMap<TableId, Vec<i32>>,
}

impl StreamGraphBuilder {
    /// Resolve infos at first to avoid blocking call inside.
    pub fn fill_info(&mut self, info: BuildGraphInfo) {
        self.table_node_actors = info.table_node_actors;
        self.table_sink_actor_ids = info.table_sink_actor_ids;
        self.upstream_distribution_keys = info.upstream_distribution_keys;
    }

    /// Insert new generated actor.
    pub fn add_actor(
        &mut self,
        actor_id: LocalActorId,
        fragment_id: LocalFragmentId,
        node: Arc<StreamNode>,
    ) {
        self.actor_builders.insert(
            actor_id,
            StreamActorBuilder::new(actor_id, fragment_id, node),
        );
    }

    /// Number of actors in the graph builder
    pub fn actor_len(&self) -> usize {
        self.actor_builders.len()
    }

    /// Add dependency between two connected node in the graph. Only provide `mapping` when it's
    /// hash mapping.
    pub fn add_link(
        &mut self,
        upstream_actor_ids: &[LocalActorId],
        downstream_actor_ids: &[LocalActorId],
        exchange_operator_id: u64,
        dispatcher: Dispatcher,
        same_worker_node: bool,
    ) {
        if dispatcher.get_type().unwrap() == DispatcherType::NoShuffle {
            assert_eq!(
                upstream_actor_ids.len(),
                downstream_actor_ids.len(),
                "mismatched length when procssing no-shuffle exchange: {:?} -> {:?} on exchange {}",
                upstream_actor_ids,
                downstream_actor_ids,
                exchange_operator_id
            );

            // update 1v1 relationship
            upstream_actor_ids
                .iter()
                .zip_eq(downstream_actor_ids.iter())
                .for_each(|(upstream_id, downstream_id)| {
                    self.actor_builders
                        .get_mut(upstream_id)
                        .unwrap()
                        .add_dispatcher(
                            dispatcher.clone(),
                            OrderedActorLink(vec![*downstream_id]),
                            same_worker_node,
                        );

                    let ret = self
                        .actor_builders
                        .get_mut(downstream_id)
                        .unwrap()
                        .upstreams
                        .insert(
                            exchange_operator_id,
                            StreamActorUpstream {
                                actors: OrderedActorLink(vec![*upstream_id]),
                                same_worker_node,
                            },
                        );

                    assert!(
                        ret.is_none(),
                        "duplicated exchange input {} for no-shuffle actors {:?} -> {:?}",
                        exchange_operator_id,
                        upstream_id,
                        downstream_id
                    );
                });

            return;
        }

        // otherwise, make m * n links between actors.

        assert!(
            !same_worker_node,
            "same_worker_node only applies to 1v1 dispatchers."
        );

        // update actors to have dispatchers, link upstream -> downstream.
        upstream_actor_ids.iter().for_each(|upstream_id| {
            self.actor_builders
                .get_mut(upstream_id)
                .unwrap()
                .add_dispatcher(
                    dispatcher.clone(),
                    OrderedActorLink(downstream_actor_ids.to_vec()),
                    same_worker_node,
                );
        });

        // update actors to have upstreams, link downstream <- upstream.
        downstream_actor_ids.iter().for_each(|downstream_id| {
            let ret = self
                .actor_builders
                .get_mut(downstream_id)
                .unwrap()
                .upstreams
                .insert(
                    exchange_operator_id,
                    StreamActorUpstream {
                        actors: OrderedActorLink(upstream_actor_ids.to_vec()),
                        same_worker_node,
                    },
                );
            assert!(
                ret.is_none(),
                "duplicated exchange input {} for actors {:?} -> {:?}",
                exchange_operator_id,
                upstream_actor_ids,
                downstream_actor_ids
            );
        });
    }

    /// Build final stream DAG with dependencies with current actor builders.
    pub fn build(
        mut self,
        ctx: &mut CreateMaterializedViewContext,
        actor_id_offset: u32,
        actor_id_len: u32,
    ) -> Result<HashMap<LocalFragmentId, Vec<StreamActor>>> {
        let mut graph = HashMap::new();

        for builder in self.actor_builders.values_mut() {
            builder.seal(actor_id_offset, actor_id_len);
        }

        for builder in self.actor_builders.values() {
            let actor_id = builder.actor_id;
            let mut actor = builder.build();
            let mut upstream_actors = builder
                .upstreams
                .iter()
                .map(|(id, StreamActorUpstream { actors, .. })| (*id, actors.clone()))
                .collect();

            actor.nodes =
                Some(self.build_inner(ctx, actor.get_nodes()?, actor_id, &mut upstream_actors)?);

            graph
                .entry(builder.get_fragment_id())
                .or_insert(vec![])
                .push(actor);
        }
        for actor_ids in ctx.upstream_node_actors.values_mut() {
            actor_ids.sort_unstable();
            actor_ids.dedup();
        }

        Ok(graph)
    }

    /// Build stream actor inside, two works will be done:
    /// 1. replace node's input with [`MergeNode`] if it is `ExchangeNode`, and swallow
    /// mergeNode's input.
    /// 2. ignore root node when it's `ExchangeNode`.
    /// 3. replace node's `ExchangeNode` input with [`MergeNode`] and resolve its upstream actor
    /// ids if it is a `ChainNode`.
    fn build_inner(
        &self,
        ctx: &mut CreateMaterializedViewContext,
        stream_node: &StreamNode,
        actor_id: LocalActorId,
        upstream_actor_id: &mut HashMap<u64, OrderedActorLink>,
    ) -> Result<StreamNode> {
        let table_id_offset = ctx.table_id_offset;
        match stream_node.get_node()? {
            Node::ExchangeNode(_) => {
                panic!("ExchangeNode should be eliminated from the top of the plan node when converting fragments to actors")
            }
            Node::ChainNode(_) => self.resolve_chain_node(ctx, stream_node, actor_id),
            _ => {
                let mut new_stream_node = stream_node.clone();
                if let Node::HashJoinNode(node) = new_stream_node
                    .node
                    .as_mut()
                    .ok_or(ProstFieldNotFound("prost stream node field not found"))?
                {
                    // The operator id must be assigned with table ids. Otherwise it is a logic
                    // error.
                    let left_table_id = node.left_table_id + table_id_offset;
                    let right_table_id = left_table_id + 1;
                    node.left_table_id = left_table_id;
                    node.right_table_id = right_table_id;
                }
                for (idx, input) in stream_node.input.iter().enumerate() {
                    match input.get_node()? {
                        Node::ExchangeNode(_) => {
                            assert!(!input.get_fields().is_empty());
                            new_stream_node.input[idx] = StreamNode {
                                input: vec![],
                                pk_indices: input.pk_indices.clone(),
                                node: Some(Node::MergeNode(MergeNode {
                                    upstream_actor_id: upstream_actor_id
                                        .remove(&input.get_operator_id())
                                        .expect("failed to find upstream actor id for given exchange node").as_global_ids(),
                                    fields: input.get_fields().clone(),
                                })),
                                fields: input.get_fields().clone(),
                                operator_id: input.operator_id,
                                identity: "MergeExecutor".to_string(),
                                append_only: input.append_only,
                            };
                        }
                        Node::ChainNode(_) => {
                            new_stream_node.input[idx] =
                                self.resolve_chain_node(ctx, input, actor_id)?;
                        }
                        _ => {
                            new_stream_node.input[idx] =
                                self.build_inner(ctx, input, actor_id, upstream_actor_id)?;
                        }
                    }
                }
                Ok(new_stream_node)
            }
        }
    }

    // TODO: we may totally move this into stream manager because we don't resolve upstream actor
    // info here in Rust frontend.
    fn resolve_chain_node(
        &self,
        ctx: &mut CreateMaterializedViewContext,
        stream_node: &StreamNode,
        actor_id: LocalActorId,
    ) -> Result<StreamNode> {
        let Node::ChainNode(chain_node) = stream_node.get_node().unwrap()  else {
            unreachable!()
        };
        let input = stream_node.get_input();
        assert_eq!(input.len(), 2);
        let table_id = TableId::from(&chain_node.table_ref_id);

        let upstream_actor_ids = HashSet::<ActorId>::from_iter(
            ctx.table_sink_map
                .entry(table_id)
                .or_insert_with(|| self.table_sink_actor_ids.get(&table_id).unwrap().clone())
                .clone()
                .into_iter(),
        );

        if ctx.is_legacy_frontend {
            for &up_id in &upstream_actor_ids {
                ctx.dispatches
                    .entry(up_id)
                    .or_default()
                    .push(actor_id.as_global_id());
            }
            let chain_upstream_table_node_actors = self.table_node_actors.get(&table_id).unwrap();
            let chain_upstream_node_actors = chain_upstream_table_node_actors
                .iter()
                .flat_map(|(node_id, actor_ids)| {
                    actor_ids.iter().map(|actor_id| (*node_id, *actor_id))
                })
                .filter(|(_, actor_id)| upstream_actor_ids.contains(actor_id))
                .into_group_map();
            for (node_id, actor_ids) in chain_upstream_node_actors {
                ctx.upstream_node_actors
                    .entry(node_id)
                    .or_default()
                    .extend(actor_ids.iter());
            }
        }

        let merge_node = &input[0];
        assert_matches!(merge_node.node, Some(Node::MergeNode(_)));

        let mut batch_plan_node = input[1].clone();
        // Get distribution key from fragment_manager
        let distribution_keys = self
            .upstream_distribution_keys
            .get(&table_id)
            .unwrap()
            .clone();
        if let Some(Node::BatchPlanNode(ref mut node)) = batch_plan_node.node {
            node.distribution_keys = distribution_keys;
        } else {
            unreachable!("input[1].node should be a BatchPlanNode");
        }

        let chain_input = vec![
            StreamNode {
                input: vec![],
                pk_indices: stream_node.pk_indices.clone(),
                node: Some(Node::MergeNode(MergeNode {
                    upstream_actor_id: if ctx.is_legacy_frontend {
                        Vec::from_iter(upstream_actor_ids.into_iter())
                    } else {
                        vec![]
                    },
                    fields: chain_node.upstream_fields.clone(),
                })),
                fields: chain_node.upstream_fields.clone(),
                operator_id: merge_node.operator_id,
                identity: "MergeExecutor".to_string(),
                append_only: stream_node.append_only,
            },
            batch_plan_node,
        ];

        Ok(StreamNode {
            input: chain_input,
            pk_indices: stream_node.pk_indices.clone(),
            node: Some(Node::ChainNode(chain_node.clone())),
            operator_id: stream_node.operator_id,
            identity: "ChainExecutor".to_string(),
            fields: chain_node.upstream_fields.clone(),
            append_only: stream_node.append_only,
        })
    }
}
