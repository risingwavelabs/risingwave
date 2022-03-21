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
//
use std::collections::hash_map::{Entry, HashMap};
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::ops::Deref;
use std::sync::Arc;

use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_common::error::Result;
use risingwave_pb::common::HashMapping;
use risingwave_pb::stream_plan::stream_node::Node;
use risingwave_pb::stream_plan::{Dispatcher, DispatcherType, MergeNode, StreamActor, StreamNode};

use crate::cluster::NodeId;
use crate::model::{ActorId, FragmentId};
use crate::storage::MetaStore;
use crate::stream::{CreateMaterializedViewContext, FragmentManagerRef};

/// [`StreamActorBuilder`] build a stream actor in a stream DAG.
pub struct StreamActorBuilder {
    /// actor id field.
    actor_id: ActorId,
    /// associated fragment id.
    fragment_id: FragmentId,
    /// associated stream node.
    nodes: Arc<StreamNode>,
    /// dispatcher category.
    dispatcher: Option<Dispatcher>,
    /// downstream actor set.
    downstream_actors: BTreeSet<ActorId>,
    /// upstream actor array.
    upstream_actors: Vec<Vec<ActorId>>,
}

impl StreamActorBuilder {
    pub fn new(actor_id: ActorId, fragment_id: FragmentId, node: Arc<StreamNode>) -> Self {
        Self {
            actor_id,
            fragment_id,
            nodes: node,
            dispatcher: None,
            downstream_actors: BTreeSet::new(),
            upstream_actors: vec![],
        }
    }

    pub fn get_id(&self) -> ActorId {
        self.actor_id
    }

    pub fn get_fragment_id(&self) -> FragmentId {
        self.fragment_id
    }

    #[allow(dead_code)]
    pub fn set_simple_dispatcher(&mut self) {
        self.dispatcher = Some(Dispatcher {
            r#type: DispatcherType::Simple as i32,
            ..Default::default()
        })
    }

    pub fn set_hash_dispatcher(&mut self, column_indices: Vec<u32>, hash_mapping: HashMapping) {
        self.dispatcher = Some(Dispatcher {
            r#type: DispatcherType::Hash as i32,
            column_indices: column_indices.into_iter().map(|i| i as u32).collect(),
            hash_mapping: Some(hash_mapping),
            ..Default::default()
        })
    }

    #[allow(dead_code)]
    pub fn set_broadcast_dispatcher(&mut self) {
        self.dispatcher = Some(Dispatcher {
            r#type: DispatcherType::Broadcast as i32,
            ..Default::default()
        })
    }

    pub fn set_dispatcher(&mut self, dispatcher: Dispatcher) {
        self.dispatcher = Some(dispatcher);
    }

    /// Used by stream graph to inject upstream fields.
    pub fn get_upstream_actors(&self) -> Vec<Vec<ActorId>> {
        self.upstream_actors.clone()
    }

    pub fn build(&self) -> StreamActor {
        let mut upstream_actor_id = vec![];
        self.upstream_actors.iter().for_each(|v| {
            upstream_actor_id.append(&mut v.clone());
        });
        StreamActor {
            actor_id: self.actor_id,
            fragment_id: self.fragment_id,
            nodes: Some(self.nodes.deref().clone()),
            dispatcher: match self.dispatcher.clone() {
                Some(d) => vec![Dispatcher {
                    downstream_actor_id: self.downstream_actors.iter().copied().collect(),
                    ..d
                }],
                None => vec![],
            },
            upstream_actor_id,
        }
    }
}

/// [`StreamGraphBuilder`] build a stream graph. It injects some information to achieve
/// dependencies. See `build_inner` for more details.
pub struct StreamGraphBuilder<S> {
    actor_builders: BTreeMap<ActorId, StreamActorBuilder>,
    /// (ctx) fragment manager.
    fragment_manager_ref: FragmentManagerRef<S>,
}

impl<S> StreamGraphBuilder<S>
where
    S: MetaStore,
{
    pub fn new(fragment_manager_ref: FragmentManagerRef<S>) -> Self {
        Self {
            actor_builders: BTreeMap::new(),
            fragment_manager_ref,
        }
    }

    /// Insert new generated actor.
    pub fn add_actor(&mut self, actor: StreamActorBuilder) {
        self.actor_builders.insert(actor.get_id(), actor);
    }

    /// Add dependency between two connected node in the graph.
    pub fn add_dependency(&mut self, upstreams: &[ActorId], downstreams: &[ActorId]) {
        downstreams.iter().for_each(|&downstream| {
            upstreams.iter().for_each(|upstream| {
                self.actor_builders
                    .get_mut(upstream)
                    .unwrap()
                    .downstream_actors
                    .insert(downstream);
            });
            self.actor_builders
                .get_mut(&downstream)
                .unwrap()
                .upstream_actors
                .push(upstreams.to_vec());
        });
    }

    /// Build final stream DAG with dependencies with current actor builders.
    pub fn build(
        &self,
        ctx: &mut CreateMaterializedViewContext,
    ) -> Result<HashMap<FragmentId, Vec<StreamActor>>> {
        let mut graph = HashMap::new();
        let mut table_sink_map = HashMap::new();
        let mut dispatches: HashMap<ActorId, Vec<ActorId>> = HashMap::new();
        let mut upstream_node_actors = HashMap::new();

        for builder in self.actor_builders.values() {
            let mut actor = builder.build();
            let actor_id = actor.actor_id;

            let upstream_actors = builder.get_upstream_actors();
            let mut dispatch_upstreams = vec![];

            actor.nodes = Some(self.build_inner(
                &mut table_sink_map,
                &mut dispatch_upstreams,
                &mut upstream_node_actors,
                actor.get_nodes()?,
                &upstream_actors,
                0,
            )?);

            graph
                .entry(builder.get_fragment_id())
                .or_insert(vec![])
                .push(actor);

            for up_id in dispatch_upstreams {
                match dispatches.entry(up_id) {
                    Entry::Occupied(mut o) => {
                        o.get_mut().push(actor_id);
                    }
                    Entry::Vacant(v) => {
                        v.insert(vec![actor_id]);
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
    /// 1. replace node's input with [`MergeNode`] if it is [`ExchangeNode`], and swallow
    /// mergeNode's input.
    /// 2. ignore root node when it's [`ExchangeNode`].
    /// 3. replace node's [`ExchangeNode`] input with [`MergeNode`] and resolve its upstream actor
    /// ids if it is a [`ChainNode`].
    pub fn build_inner(
        &self,
        table_sink_map: &mut HashMap<TableId, Vec<ActorId>>,
        dispatch_upstreams: &mut Vec<ActorId>,
        upstream_node_actors: &mut HashMap<NodeId, Vec<ActorId>>,
        stream_node: &StreamNode,
        upstream_actor_id: &[Vec<ActorId>],
        next_idx: usize,
    ) -> Result<StreamNode> {
        match stream_node.get_node()? {
            Node::ExchangeNode(_) => self.build_inner(
                table_sink_map,
                dispatch_upstreams,
                upstream_node_actors,
                stream_node.input.get(0).unwrap(),
                upstream_actor_id,
                next_idx,
            ),
            Node::ChainNode(_) => self.resolve_chain_node(
                table_sink_map,
                dispatch_upstreams,
                upstream_node_actors,
                stream_node,
            ),
            _ => {
                let mut new_stream_node = stream_node.clone();
                let mut next_idx_new = next_idx;
                for (idx, input) in stream_node.input.iter().enumerate() {
                    match input.get_node()? {
                        Node::ExchangeNode(exchange_node) => {
                            assert!(next_idx_new < upstream_actor_id.len());
                            new_stream_node.input[idx] = StreamNode {
                                input: vec![],
                                pk_indices: input.pk_indices.clone(),
                                node: Some(Node::MergeNode(MergeNode {
                                    upstream_actor_id: upstream_actor_id
                                        .get(next_idx_new)
                                        .cloned()
                                        .unwrap(),
                                    fields: exchange_node.get_fields().clone(),
                                })),
                                operator_id: input.operator_id,
                                identity: "MergeExecutor".to_string(),
                            };
                            next_idx_new += 1;
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
                                next_idx_new,
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
        upstream_node_actors: &mut HashMap<NodeId, Vec<ActorId>>,
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
                            .fragment_manager_ref
                            .get_table_sink_actor_ids(&table_id)?;
                        v.insert(actor_ids).clone()
                    }
                    Entry::Occupied(o) => o.get().clone(),
                }
                .into_iter(),
            );

            dispatch_upstreams.extend(upstream_actor_ids.iter());
            let chain_upstream_table_node_actors =
                self.fragment_manager_ref.table_node_actors(&table_id)?;
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

            let chain_input = vec![
                StreamNode {
                    input: vec![],
                    pk_indices: chain_node
                        .pk_indices
                        .iter()
                        .map(|x| *x as u32)
                        .collect_vec(),
                    node: Some(Node::MergeNode(MergeNode {
                        upstream_actor_id: Vec::from_iter(upstream_actor_ids.into_iter()),
                        fields: chain_node.upstream_fields.clone(),
                    })),
                    operator_id: input.get(0).as_ref().unwrap().operator_id,
                    identity: "MergeExecutor".to_string(),
                },
                input.get(1).unwrap().clone(),
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
