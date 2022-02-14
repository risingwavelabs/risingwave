use std::collections::hash_map::{Entry, HashMap};
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Deref;
use std::sync::Arc;

use async_recursion::async_recursion;
use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_common::error::Result;
use risingwave_common::try_match_expand;
use risingwave_pb::stream_plan::dispatcher::DispatcherType;
use risingwave_pb::stream_plan::stream_node::Node;
use risingwave_pb::stream_plan::{Dispatcher, MergeNode, StreamActor, StreamNode};

use crate::model::{ActorId, FragmentId, TableRawId};
use crate::stream::FragmentManagerRef;

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

    #[allow(dead_code)]
    pub fn set_hash_dispatcher(&mut self, column_indices: Vec<usize>) {
        self.dispatcher = Some(Dispatcher {
            r#type: DispatcherType::Hash as i32,
            column_indices: column_indices.into_iter().map(|i| i as u32).collect(),
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
            dispatcher: self.dispatcher.clone(),
            downstream_actor_id: self.downstream_actors.iter().copied().collect(),
            upstream_actor_id,
        }
    }
}

/// [`StreamGraphBuilder`] build a stream graph. It will do some injection here to achieve
/// dependencies. See `build_inner` for more details.
pub struct StreamGraphBuilder {
    actor_builders: BTreeMap<ActorId, StreamActorBuilder>,
    /// (ctx) fragment manager.
    fragment_manager_ref: FragmentManagerRef,
}

impl StreamGraphBuilder {
    pub fn new(fragment_manager_ref: FragmentManagerRef) -> Self {
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
    pub async fn build(&self) -> Result<HashMap<FragmentId, Vec<StreamActor>>> {
        let mut graph = HashMap::new();
        let mut table_sink_map = HashMap::new();
        for builder in self.actor_builders.values() {
            let mut actor = builder.build();

            let upstream_actors = builder.get_upstream_actors();
            actor.nodes = Some(
                self.build_inner(&mut table_sink_map, actor.get_nodes()?, &upstream_actors, 0)
                    .await?,
            );
            graph
                .entry(builder.get_fragment_id())
                .or_insert(vec![])
                .push(actor);
        }
        Ok(graph)
    }

    /// Build stream actor inside, two works will be done:
    /// 1. replace node's input with [`MergeNode`] if it is [`ExchangeNode`], and swallow
    /// mergeNode's input.
    /// 2. ignore root node when it's [`ExchangeNode`].
    /// 3. replace node's [`ExchangeNode`] input with [`MergeNode`] and resolve its upstream actor
    /// ids if it is a [`ChainNode`].
    #[async_recursion]
    pub async fn build_inner(
        &self,
        table_sink_map: &mut HashMap<TableRawId, Vec<ActorId>>,
        stream_node: &StreamNode,
        upstream_actor_id: &[Vec<ActorId>],
        next_idx: usize,
    ) -> Result<StreamNode> {
        match stream_node.get_node()? {
            Node::ExchangeNode(_) => {
                self.build_inner(
                    table_sink_map,
                    stream_node.input.get(0).unwrap(),
                    upstream_actor_id,
                    next_idx,
                )
                .await
            }
            Node::ChainNode(_) => self.resolve_chain_node(table_sink_map, stream_node).await,
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
                                    input_column_descs: exchange_node
                                        .get_input_column_descs()
                                        .clone(),
                                })),
                                operator_id: input.operator_id,
                            };
                            next_idx_new += 1;
                        }
                        Node::ChainNode(_) => {
                            new_stream_node.input[idx] =
                                self.resolve_chain_node(table_sink_map, input).await?;
                        }
                        _ => {
                            new_stream_node.input[idx] = self
                                .build_inner(table_sink_map, input, upstream_actor_id, next_idx_new)
                                .await?;
                        }
                    }
                }
                Ok(new_stream_node)
            }
        }
    }

    async fn resolve_chain_node(
        &self,
        table_sink_map: &mut HashMap<TableRawId, Vec<ActorId>>,
        stream_node: &StreamNode,
    ) -> Result<StreamNode> {
        if let Node::ChainNode(chain_node) = stream_node.get_node().unwrap() {
            let input = stream_node.get_input();
            assert_eq!(input.len(), 2);
            let table_id = TableId::from(&chain_node.table_ref_id);
            let table_raw_id = chain_node.table_ref_id.as_ref().unwrap().table_id;
            let upstream_actor_ids = match table_sink_map.entry(table_raw_id) {
                Entry::Vacant(v) => {
                    let actor_ids = try_match_expand!(
                        self.fragment_manager_ref
                            .get_table_sink_actor_ids(&table_id)
                            .await,
                        Ok
                    )?;
                    v.insert(actor_ids)
                }
                Entry::Occupied(o) => o.into_mut(),
            }
            .to_owned();

            let chain_input = vec![
                StreamNode {
                    input: vec![],
                    pk_indices: chain_node
                        .pk_indices
                        .iter()
                        .map(|x| *x as u32)
                        .collect_vec(),
                    node: Some(Node::MergeNode(MergeNode {
                        upstream_actor_id: upstream_actor_ids,
                        input_column_descs: chain_node.upstream_column_descs.clone(),
                    })),
                    operator_id: input.get(0).as_ref().unwrap().operator_id,
                },
                input.get(1).unwrap().clone(),
            ];

            Ok(StreamNode {
                input: chain_input,
                pk_indices: stream_node.pk_indices.clone(),
                node: Some(Node::ChainNode(chain_node.clone())),
                operator_id: stream_node.operator_id,
            })
        } else {
            unreachable!()
        }
    }
}
