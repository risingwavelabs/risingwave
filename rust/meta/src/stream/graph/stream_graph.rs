use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::Deref;
use std::sync::Arc;

use risingwave_common::error::Result;
use risingwave_pb::stream_plan::dispatcher::DispatcherType;
use risingwave_pb::stream_plan::stream_node::Node;
use risingwave_pb::stream_plan::{Dispatcher, MergeNode, StreamActor, StreamNode};

use crate::model::{ActorId, FragmentId};

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
            upstream_actor_id: upstream_actor_id,
        }
    }
}

/// [`StreamGraphBuilder`] build a stream graph. It will do some injection here to achieve
/// dependencies. See `build_inner` for more details.
pub struct StreamGraphBuilder {
    actor_builders: BTreeMap<ActorId, StreamActorBuilder>,
}

impl StreamGraphBuilder {
    pub fn new() -> Self {
        Self {
            actor_builders: BTreeMap::new(),
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
    pub fn build(&self) -> Result<HashMap<FragmentId, Vec<StreamActor>>> {
        let mut graph = HashMap::new();
        for builder in self.actor_builders.values() {
            let mut actor = builder.build();
            let upstream_actors = builder.get_upstream_actors();
            actor.nodes = Some(self.build_inner(actor.get_nodes()?, &upstream_actors, 0)?);
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
    pub fn build_inner(
        &self,
        stream_node: &StreamNode,
        upstream_actor_id: &[Vec<ActorId>],
        next_idx: usize,
    ) -> Result<StreamNode> {
        if let Node::ExchangeNode(_) = stream_node.get_node()? {
            self.build_inner(
                stream_node.input.get(0).unwrap(),
                upstream_actor_id,
                next_idx,
            )
        } else {
            let mut new_stream_node = stream_node.clone();
            let mut next_idx_new = next_idx;
            for (idx, input) in stream_node.input.iter().enumerate() {
                if let Node::ExchangeNode(exchange_node) = input.get_node()? {
                    assert!(next_idx_new < upstream_actor_id.len());
                    new_stream_node.input[idx] = StreamNode {
                        input: vec![],
                        pk_indices: input.pk_indices.clone(),
                        node: Some(Node::MergeNode(MergeNode {
                            upstream_actor_id: upstream_actor_id
                                .get(next_idx_new)
                                .cloned()
                                .unwrap(),
                            input_column_descs: exchange_node.get_input_column_descs().clone(),
                        })),
                        operator_id: input.operator_id,
                    };
                    next_idx_new += 1;
                } else {
                    new_stream_node.input[idx] =
                        self.build_inner(input, upstream_actor_id, next_idx_new)?;
                }
            }
            Ok(new_stream_node)
        }
    }
}
