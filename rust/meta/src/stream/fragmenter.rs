use std::cmp::max;
use std::ops::Deref;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use async_recursion::async_recursion;
use risingwave_common::array::RwError;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::Result;
use risingwave_pb::stream_plan::stream_node::Node;
use risingwave_pb::stream_plan::{StreamActor, StreamNode};

use crate::manager::{IdCategory, IdGeneratorManagerRef};
use crate::stream::graph::{
    StreamActorBuilder, StreamFragment, StreamFragmentGraph, StreamGraphBuilder,
};

const PARALLEL_DEGREE_LOW_BOUND: u32 = 4;

/// [`StreamFragmenter`] generates the proto for interconnected actors for a streaming pipeline.
pub struct StreamFragmenter {
    /// fragment id generator.
    // TODO: replace fragment id with IdGenerator, fragment id may need to store in MetaStore.
    next_fragment_id: AtomicU32,
    /// fragment graph field, transformed from input streaming plan.
    fragment_graph: StreamFragmentGraph,
    /// stream graph builder, to build streaming DAG.
    stream_graph: StreamGraphBuilder,

    /// id generator, used to generate actor id.
    id_gen_manager_ref: IdGeneratorManagerRef,
    /// worker count, used to init actor parallelization.
    worker_count: u32,
}

impl StreamFragmenter {
    pub fn new(id_gen_manager_ref: IdGeneratorManagerRef, worker_count: u32) -> Self {
        Self {
            next_fragment_id: AtomicU32::new(0),
            fragment_graph: StreamFragmentGraph::new(None),
            stream_graph: StreamGraphBuilder::new(),

            id_gen_manager_ref,
            worker_count,
        }
    }

    /// Build a stream graph in two steps:
    /// (1) Break the streaming plan into fragments with their dependency.
    /// (2) Duplicate each fragment as parallel actors.
    pub async fn generate_graph(&mut self, stream_node: &StreamNode) -> Result<Vec<StreamActor>> {
        self.generate_fragment_graph(stream_node);
        self.build_graph_from_fragment(self.fragment_graph.get_root_fragment(), vec![])
            .await?;
        Ok(self.stream_graph.build())
    }

    /// Generate fragment DAG from input streaming plan by their dependency.
    fn generate_fragment_graph(&mut self, stream_node: &StreamNode) {
        let root_fragment = self.new_stream_fragment(Arc::new(stream_node.clone()));
        self.fragment_graph.add_root_fragment(root_fragment.clone());
        self.build_fragment(&root_fragment, stream_node);
    }

    /// Build new fragment and link dependency with its parent fragment.
    fn build_fragment(&mut self, parent_fragment: &StreamFragment, stream_node: &StreamNode) {
        for node in stream_node.get_input() {
            match node.get_node() {
                Node::ExchangeNode(_) => {
                    let child_fragment = self.new_stream_fragment(Arc::new(node.clone()));
                    self.fragment_graph.add_fragment(child_fragment.clone());
                    self.fragment_graph.link_child(
                        parent_fragment.get_fragment_id(),
                        child_fragment.get_fragment_id(),
                    );
                    self.build_fragment(&child_fragment, node);
                }
                _ => {
                    self.build_fragment(parent_fragment, node);
                }
            }
        }
    }

    fn new_stream_fragment(&self, node: Arc<StreamNode>) -> StreamFragment {
        StreamFragment::new(self.next_fragment_id.fetch_add(1, Ordering::Relaxed), node)
    }

    /// Generate actor id from id generator.
    async fn gen_actor_id(&self, interval: i32) -> Result<u32> {
        Ok(self
            .id_gen_manager_ref
            .generate_interval(IdCategory::Actor, interval)
            .await? as u32)
    }

    /// Build stream graph from fragment graph recursively. Setup dispatcher in actor and generate
    /// actors by their parallelism.
    #[async_recursion]
    async fn build_graph_from_fragment(
        &mut self,
        current_fragment: StreamFragment,
        last_fragment_actors: Vec<u32>,
    ) -> Result<()> {
        let root_fragment = self.fragment_graph.get_root_fragment();
        let mut current_actor_ids = vec![];
        let current_fragment_id = current_fragment.get_fragment_id();
        if current_fragment_id == root_fragment.get_fragment_id() {
            // Fragment on the root, generate an actor without dispatcher.
            let actor_id = self.gen_actor_id(1).await?;
            let mut actor_builder = StreamActorBuilder::new(actor_id, current_fragment.get_node());
            // Set `Broadcast` dispatcher for root fragment (means no dispatcher).
            actor_builder.set_broadcast_dispatcher();

            // Add actor. No dependency needed.
            current_actor_ids.push(actor_id);
            self.stream_graph.add_actor(actor_builder);
            self.stream_graph.set_root_actor(actor_id);
        } else {
            let parallel_degree = if self.fragment_graph.has_downstream(current_fragment_id) {
                // Fragment in the middle.
                // Generate one or multiple actors and connect them to the next fragment.
                // Currently, we assume the parallel degree is at least 4, and grows linearly with
                // more worker nodes added.
                max(self.worker_count * 2, PARALLEL_DEGREE_LOW_BOUND)
            } else {
                // Fragment on the source.
                self.worker_count
            };

            let node = current_fragment.get_node();
            let actor_ids = self.gen_actor_id(parallel_degree as i32).await?;
            let dispatcher = match node.get_node() {
                Node::ExchangeNode(exchange_node) => exchange_node.get_dispatcher(),
                _ => {
                    return Err(RwError::from(InternalError(format!(
                        "{:?} should not found.",
                        node.get_node()
                    ))));
                }
            };
            for id in actor_ids..actor_ids + parallel_degree {
                let stream_node = node.deref().clone();
                let mut actor_builder = StreamActorBuilder::new(id, Arc::new(stream_node));
                actor_builder.set_dispatcher(dispatcher.clone());
                self.stream_graph.add_actor(actor_builder);
                current_actor_ids.push(id);
            }
        }

        self.stream_graph
            .add_dependency(&current_actor_ids, &last_fragment_actors);

        // Recursively generating actors on the downstream level.
        if self.fragment_graph.has_downstream(current_fragment_id) {
            for fragment_id in self
                .fragment_graph
                .get_downstream_fragments(current_fragment_id)
                .unwrap()
            {
                self.build_graph_from_fragment(
                    self.fragment_graph.get_fragment_by_id(fragment_id).unwrap(),
                    current_actor_ids.clone(),
                )
                .await?;
            }
        };

        Ok(())
    }
}
