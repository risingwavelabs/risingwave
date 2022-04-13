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

use std::collections::hash_map::{Entry, HashMap};
use std::collections::{BTreeMap, HashSet};
use std::ops::Deref;
use std::sync::Arc;

use assert_matches::assert_matches;
use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_common::error::Result;
use risingwave_pb::stream_plan::stream_node::Node;
use risingwave_pb::stream_plan::{
    ActorMapping, Dispatcher, DispatcherType, MergeNode, StreamActor, StreamNode,
};

use crate::cluster::WorkerId;
use crate::model::{ActorId, LocalActorId, LocalFragmentId};
use crate::storage::MetaStore;
use crate::stream::{CreateMaterializedViewContext, FragmentManagerRef};

#[derive(Debug, Clone)]
pub struct ActorLink(pub Vec<LocalActorId>);

impl ActorLink {
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

/// [`StreamActorBuilder`] builds a stream actor in a stream DAG.
pub struct StreamActorBuilder {
    /// actor id field
    actor_id: LocalActorId,

    /// associated fragment id
    fragment_id: LocalFragmentId,

    /// associated stream node
    nodes: Arc<StreamNode>,

    /// downstream dispatchers (dispatcher, downstream actor, hash mapping)
    downstreams: Vec<(Dispatcher, ActorLink, Option<Vec<LocalActorId>>)>,

    /// upstreams, exchange node operator_id -> upstream actor ids
    upstreams: HashMap<u64, ActorLink>,

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

    pub fn get_id(&self) -> LocalActorId {
        self.actor_id
    }

    pub fn get_fragment_id(&self) -> LocalFragmentId {
        self.fragment_id
    }

    /// Add a dispatcher to this actor. Note that the `downstream_actor_id` field must be left
    /// empty, as we will fill this out when building actors.
    pub fn add_dispatcher(
        &mut self,
        dispatcher: Dispatcher,
        downstream_actors: ActorLink,
        mapping: Option<Vec<LocalActorId>>,
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
        self.downstreams
            .push((dispatcher, downstream_actors, mapping));
    }

    /// Build an actor from given information. At the same time, convert local actor id to global
    /// actor id.
    pub fn seal(&mut self, actor_id_offset: u32, actor_id_len: u32) {
        assert!(!self.sealed);

        self.actor_id = self.actor_id.to_global_id(actor_id_offset, actor_id_len);
        self.downstreams = std::mem::take(&mut self.downstreams)
            .into_iter()
            .map(|(dispatcher, downstreams, mapping)| {
                let global_mapping =
                    mapping.map(|x| ActorLink(x).to_global_ids(actor_id_offset, actor_id_len));

                (
                    Dispatcher {
                        hash_mapping: global_mapping.as_ref().map(|x| ActorMapping {
                            hash_mapping: x.as_global_ids(),
                        }),
                        ..dispatcher
                    },
                    downstreams.to_global_ids(actor_id_offset, actor_id_len),
                    global_mapping.map(|ActorLink(x)| x),
                )
            })
            .collect();
        self.upstreams = std::mem::take(&mut self.upstreams)
            .into_iter()
            .map(|(exchange_id, actor_link)| {
                (
                    exchange_id,
                    actor_link.to_global_ids(actor_id_offset, actor_id_len),
                )
            })
            .collect();
        self.sealed = true;
    }

    /// Build an actor after seal.
    pub fn build(&self) -> StreamActor {
        assert!(self.sealed);

        let mut dispatcher = self
            .downstreams
            .iter()
            .map(|(dispatcher, downstreams, _)| Dispatcher {
                downstream_actor_id: downstreams.as_global_ids(),
                ..dispatcher.clone()
            })
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
                .flat_map(|(_, upstreams)| upstreams.0.iter().copied())
                .map(|x| x.as_global_id())
                .collect(), // TODO: store each upstream separately
        }
    }
}

/// [`StreamGraphBuilder`] build a stream graph. It injects some information to achieve
/// dependencies. See `build_inner` for more details.
pub struct StreamGraphBuilder<S> {
    actor_builders: BTreeMap<LocalActorId, StreamActorBuilder>,
    /// (ctx) fragment manager.
    fragment_manager: FragmentManagerRef<S>,
}

impl<S> StreamGraphBuilder<S>
where
    S: MetaStore,
{
    pub fn new(fragment_manager: FragmentManagerRef<S>) -> Self {
        Self {
            actor_builders: BTreeMap::new(),
            fragment_manager,
        }
    }

    /// Insert new generated actor.
    pub fn add_actor(&mut self, actor: StreamActorBuilder) {
        self.actor_builders.insert(actor.get_id(), actor);
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
        mapping: Option<Vec<LocalActorId>>,
    ) {
        upstream_actor_ids.iter().for_each(|upstream_id| {
            self.actor_builders
                .get_mut(upstream_id)
                .unwrap()
                .add_dispatcher(
                    dispatcher.clone(),
                    ActorLink(downstream_actor_ids.to_vec()),
                    mapping.clone(),
                );
        });

        downstream_actor_ids.iter().for_each(|downstream_id| {
            let ret = self
                .actor_builders
                .get_mut(downstream_id)
                .unwrap()
                .upstreams
                .insert(exchange_operator_id, ActorLink(upstream_actor_ids.to_vec()));
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
        &mut self,
        ctx: &mut CreateMaterializedViewContext,
        actor_id_offset: u32,
        actor_id_len: u32,
    ) -> Result<HashMap<LocalFragmentId, Vec<StreamActor>>> {
        let mut graph = HashMap::new();
        let mut table_sink_map = HashMap::new();
        let mut dispatches: HashMap<ActorId, Vec<ActorId>> = HashMap::new();
        let mut upstream_node_actors = HashMap::new();

        for builder in self.actor_builders.values_mut() {
            builder.seal(actor_id_offset, actor_id_len);
        }

        for builder in self.actor_builders.values() {
            let actor_id = builder.actor_id;
            let mut actor = builder.build();
            let mut dispatch_upstreams = vec![];
            let mut upstream_actors = builder.upstreams.clone();

            actor.nodes = Some(self.build_inner(
                &mut table_sink_map,
                &mut dispatch_upstreams,
                &mut upstream_node_actors,
                actor.get_nodes()?,
                &mut upstream_actors,
            )?);

            graph
                .entry(builder.get_fragment_id())
                .or_insert(vec![])
                .push(actor);

            for up_id in dispatch_upstreams {
                match dispatches.entry(up_id) {
                    Entry::Occupied(mut o) => {
                        o.get_mut().push(actor_id.as_global_id());
                    }
                    Entry::Vacant(v) => {
                        v.insert(vec![actor_id.as_global_id()]);
                    }
                }
            }
        }
        for actor_ids in upstream_node_actors.values_mut() {
            actor_ids.sort_unstable();
            actor_ids.dedup();
        }

        ctx.dispatches = dispatches;
        ctx.upstream_node_actors = upstream_node_actors;
        ctx.table_sink_map = table_sink_map;
        Ok(graph)
    }

    /// Build stream actor inside, two works will be done:
    /// 1. replace node's input with [`MergeNode`] if it is `ExchangeNode`, and swallow
    /// mergeNode's input.
    /// 2. ignore root node when it's `ExchangeNode`.
    /// 3. replace node's `ExchangeNode` input with [`MergeNode`] and resolve its upstream actor
    /// ids if it is a `ChainNode`.
    pub fn build_inner(
        &self,
        table_sink_map: &mut HashMap<TableId, Vec<ActorId>>,
        dispatch_upstreams: &mut Vec<ActorId>,
        upstream_node_actors: &mut HashMap<WorkerId, Vec<ActorId>>,
        stream_node: &StreamNode,
        upstream_actor_id: &mut HashMap<u64, ActorLink>,
    ) -> Result<StreamNode> {
        match stream_node.get_node()? {
            Node::ExchangeNode(_) => self.build_inner(
                table_sink_map,
                dispatch_upstreams,
                upstream_node_actors,
                stream_node.input.get(0).unwrap(),
                upstream_actor_id,
            ),
            Node::ChainNode(_) => self.resolve_chain_node(
                table_sink_map,
                dispatch_upstreams,
                upstream_node_actors,
                stream_node,
            ),
            _ => {
                let mut new_stream_node = stream_node.clone();
                for (idx, input) in stream_node.input.iter().enumerate() {
                    match input.get_node()? {
                        Node::ExchangeNode(exchange_node) => {
                            new_stream_node.input[idx] = StreamNode {
                                input: vec![],
                                pk_indices: input.pk_indices.clone(),
                                node: Some(Node::MergeNode(MergeNode {
                                    upstream_actor_id: upstream_actor_id
                                        .remove(&input.get_operator_id())
                                        .expect("failed to find upstream actor id for given exchange node").as_global_ids(),
                                    fields: exchange_node.get_fields().clone(),
                                })),
                                operator_id: input.operator_id,
                                identity: "MergeExecutor".to_string(),
                            };
                        }
                        Node::ChainNode(_) => {
                            new_stream_node.input[idx] = self.resolve_chain_node(
                                table_sink_map,
                                dispatch_upstreams,
                                upstream_node_actors,
                                input,
                            )?;
                        }
                        _ => {
                            new_stream_node.input[idx] = self.build_inner(
                                table_sink_map,
                                dispatch_upstreams,
                                upstream_node_actors,
                                input,
                                upstream_actor_id,
                            )?;
                        }
                    }
                }
                Ok(new_stream_node)
            }
        }
    }

    fn resolve_chain_node(
        &self,
        table_sink_map: &mut HashMap<TableId, Vec<ActorId>>,
        dispatch_upstreams: &mut Vec<ActorId>,
        upstream_node_actors: &mut HashMap<WorkerId, Vec<ActorId>>,
        stream_node: &StreamNode,
    ) -> Result<StreamNode> {
        if let Node::ChainNode(chain_node) = stream_node.get_node().unwrap() {
            let input = stream_node.get_input();
            assert_eq!(input.len(), 2);
            let table_id = TableId::from(&chain_node.table_ref_id);
            let upstream_actor_ids = HashSet::<ActorId>::from_iter(
                match table_sink_map.entry(table_id) {
                    Entry::Vacant(v) => {
                        let actor_ids = self
                            .fragment_manager
                            .blocking_get_table_sink_actor_ids(&table_id)?;
                        v.insert(actor_ids).clone()
                    }
                    Entry::Occupied(o) => o.get().clone(),
                }
                .into_iter(),
            );

            dispatch_upstreams.extend(upstream_actor_ids.iter());
            let chain_upstream_table_node_actors = self
                .fragment_manager
                .blocking_table_node_actors(&table_id)?;
            let chain_upstream_node_actors = chain_upstream_table_node_actors
                .iter()
                .flat_map(|(node_id, actor_ids)| {
                    actor_ids.iter().map(|actor_id| (*node_id, *actor_id))
                })
                .filter(|(_, actor_id)| upstream_actor_ids.contains(actor_id))
                .into_group_map();
            for (node_id, actor_ids) in chain_upstream_node_actors {
                match upstream_node_actors.entry(node_id) {
                    Entry::Occupied(mut o) => {
                        o.get_mut().extend(actor_ids.iter());
                    }
                    Entry::Vacant(v) => {
                        v.insert(actor_ids);
                    }
                }
            }

            let merge_node = &input[0];
            let batch_plan_node = &input[1];

            assert_matches!(merge_node.node, Some(Node::MergeNode(_)));
            assert_matches!(batch_plan_node.node, Some(Node::BatchPlanNode(_)));

            let chain_input = vec![
                StreamNode {
                    input: vec![],
                    pk_indices: stream_node.pk_indices.clone(),
                    node: Some(Node::MergeNode(MergeNode {
                        upstream_actor_id: Vec::from_iter(upstream_actor_ids.into_iter()),
                        fields: chain_node.upstream_fields.clone(),
                    })),
                    operator_id: merge_node.operator_id,
                    identity: "MergeExecutor".to_string(),
                },
                batch_plan_node.clone(),
            ];

            Ok(StreamNode {
                input: chain_input,
                pk_indices: stream_node.pk_indices.clone(),
                node: Some(Node::ChainNode(chain_node.clone())),
                operator_id: stream_node.operator_id,
                identity: "ChainExecutor".to_string(),
            })
        } else {
            unreachable!()
        }
    }
}
