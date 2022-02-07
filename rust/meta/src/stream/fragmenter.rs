use std::cmp::max;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use async_recursion::async_recursion;
use risingwave_common::array::RwError;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::Result;
use risingwave_pb::stream_plan::dispatcher::DispatcherType;
use risingwave_pb::stream_plan::stream_node::Node;
use risingwave_pb::stream_plan::{Dispatcher, StreamActor, StreamNode};

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

    /// the list of ids of all source actors.
    source_actor_ids: Vec<u32>,
}

impl StreamFragmenter {
    pub fn new(id_gen_manager_ref: IdGeneratorManagerRef, worker_count: u32) -> Self {
        Self {
            next_fragment_id: AtomicU32::new(0),
            fragment_graph: StreamFragmentGraph::new(None),
            stream_graph: StreamGraphBuilder::new(),
            id_gen_manager_ref,
            worker_count,
            source_actor_ids: Vec::new(),
        }
    }

    /// Build a stream graph in two steps:
    /// (1) Break the streaming plan into fragments with their dependency.
    /// (2) Duplicate each fragment as parallel actors.
    ///
    /// Return a pair of (1) all stream actors, and (2) the actor id of sources to be forcefully
    /// round-robin scheduled.
    pub async fn generate_graph(
        &mut self,
        stream_node: &StreamNode,
    ) -> Result<(Vec<StreamActor>, Vec<u32>)> {
        self.generate_fragment_graph(stream_node)?;
        self.build_graph_from_fragment(self.fragment_graph.get_root_fragment(), vec![])
            .await?;
        Ok((self.stream_graph.build()?, self.source_actor_ids.clone()))
    }

    /// Generate fragment DAG from input streaming plan by their dependency.
    fn generate_fragment_graph(&mut self, stream_node: &StreamNode) -> Result<()> {
        self.build_root_fragment(stream_node)?;
        Ok(())
    }

    /// Use the given `stream_node` to create and add a root fragment.
    fn build_root_fragment(&mut self, stream_node: &StreamNode) -> Result<StreamFragment> {
        let mut fragment = self.new_stream_fragment(Arc::new(stream_node.clone()));
        self.build_fragment(&mut fragment, stream_node)?;
        self.fragment_graph.add_root_fragment(fragment.clone());
        Ok(fragment)
    }

    /// Build new fragment and link dependency with its parent (current) fragment, update
    /// `is_singleton` and `is_table_source` properties for current fragment.
    // TODO: Should we store the concurrency in StreamFragment directly?
    fn build_fragment(
        &mut self,
        current_fragment: &mut StreamFragment,
        node: &StreamNode,
    ) -> Result<()> {
        for child_node in node.get_input() {
            match child_node.get_node()? {
                Node::ExchangeNode(exchange_node) => {
                    // Build another root fragment for this node.
                    let child_fragment = self.build_root_fragment(child_node)?;
                    self.fragment_graph.link_child(
                        current_fragment.get_fragment_id(),
                        child_fragment.get_fragment_id(),
                    );

                    let is_simple_dispatcher =
                        exchange_node.get_dispatcher()?.get_type()? == DispatcherType::Simple;
                    current_fragment.is_singleton |= is_simple_dispatcher;
                }

                Node::SourceNode(_) => {
                    self.build_fragment(current_fragment, child_node)?;
                    current_fragment.is_table_source_fragment = true;
                }
                Node::TopNNode(_) => {
                    self.build_fragment(current_fragment, child_node)?;
                    // TODO: Force singleton for TopN as a workaround.
                    // We should implement two phase TopN.
                    current_fragment.is_singleton = true;
                }
                _ => self.build_fragment(current_fragment, child_node)?,
            };
        }

        Ok(())
    }

    fn new_stream_fragment(&self, node: Arc<StreamNode>) -> StreamFragment {
        let id = self.next_fragment_id.fetch_add(1, Ordering::Relaxed);
        StreamFragment::new(id, node)
    }

    /// Generate actor id from id generator.
    async fn gen_actor_id(&self, interval: i32) -> Result<u32> {
        Ok(self
            .id_gen_manager_ref
            .generate_interval::<{ IdCategory::Actor }>(interval)
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
        let parallel_degree = if current_fragment.is_singleton {
            1
        } else if self.fragment_graph.has_downstream(current_fragment_id) {
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
        let blackhole_dispatcher = Dispatcher {
            r#type: DispatcherType::Broadcast as i32,
            ..Default::default()
        };
        let dispatcher = if current_fragment_id == root_fragment.get_fragment_id() {
            &blackhole_dispatcher
        } else {
            match node.get_node()? {
                Node::ExchangeNode(exchange_node) => exchange_node.get_dispatcher()?,
                _ => {
                    return Err(RwError::from(InternalError(format!(
                        "{:?} should not found.",
                        node.get_node()
                    ))));
                }
            }
        };

        for id in actor_ids..actor_ids + parallel_degree {
            let mut actor_builder = StreamActorBuilder::new(id, node.clone());
            actor_builder.set_dispatcher(dispatcher.clone());
            self.stream_graph.add_actor(actor_builder);
            current_actor_ids.push(id);

            // If the current fragment is table source, then add the id to the source id list.
            if current_fragment.is_table_source_fragment {
                self.source_actor_ids.push(id);
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
