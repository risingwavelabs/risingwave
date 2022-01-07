use std::cmp::max;
use std::ops::Deref;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use async_recursion::async_recursion;
use risingwave_common::array::RwError;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::Result;
use risingwave_pb::meta::get_id_request::IdCategory;
use risingwave_pb::stream_plan::stream_node::Node;
use risingwave_pb::stream_plan::{StreamFragment, StreamNode};

use crate::manager::IdGeneratorManagerRef;
use crate::stream::graph::{
    StreamFragmentBuilder, StreamGraphBuilder, StreamStage, StreamStageGraph,
};

const PARALLEL_DEGREE_LOW_BOUND: u32 = 4;

/// [`StreamFragmenter`] generates the proto for interconnected actors for a streaming pipeline.
pub struct StreamFragmenter {
    /// stage id generator.
    next_stage_id: AtomicU32,
    /// stage graph field, transformed from input streaming plan.
    stage_graph: StreamStageGraph,
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
            next_stage_id: AtomicU32::new(0),
            stage_graph: StreamStageGraph::new(None),
            stream_graph: StreamGraphBuilder::new(),

            id_gen_manager_ref,
            worker_count,
        }
    }

    /// Build a stream graph in two steps:
    /// (1) Break the streaming plan into stages with their dependency.
    /// (2) Duplicate each stage as parallel fragments.
    pub async fn generate_graph(
        &mut self,
        stream_node: &StreamNode,
    ) -> Result<Vec<StreamFragment>> {
        self.generate_stage_graph(stream_node);
        self.build_graph_from_stage(self.stage_graph.get_root_stage(), vec![])
            .await?;
        Ok(self.stream_graph.build())
    }

    /// Generate stage DAG from input streaming plan by their dependency.
    fn generate_stage_graph(&mut self, stream_node: &StreamNode) {
        let root_stage = self.new_stream_stage(Arc::new(stream_node.clone()));
        self.stage_graph.add_root_stage(root_stage.clone());
        self.build_stage(&root_stage, stream_node);
    }

    /// Build new stage and link dependency with its parent stage.
    fn build_stage(&mut self, parent_stage: &StreamStage, stream_node: &StreamNode) {
        for node in stream_node.get_input() {
            match node.get_node() {
                Node::ExchangeNode(_) => {
                    let child_stage = self.new_stream_stage(Arc::new(node.clone()));
                    self.stage_graph.add_stage(child_stage.clone());
                    self.stage_graph
                        .link_child(parent_stage.get_stage_id(), child_stage.get_stage_id());
                    self.build_stage(&child_stage, node);
                }
                _ => {
                    self.build_stage(parent_stage, node);
                }
            }
        }
    }

    fn new_stream_stage(&self, node: Arc<StreamNode>) -> StreamStage {
        StreamStage::new(self.next_stage_id.fetch_add(1, Ordering::Relaxed), node)
    }

    /// Generate actor id from id generator.
    async fn gen_actor_id(&self, interval: i32) -> Result<u32> {
        Ok(self
            .id_gen_manager_ref
            .generate_interval(IdCategory::Fragment, interval)
            .await? as u32)
    }

    /// Build stream graph from stage graph recursively. Setup dispatcher in fragment and generate
    /// actors by their parallelism.
    #[async_recursion]
    async fn build_graph_from_stage(
        &mut self,
        current_stage: StreamStage,
        last_stage_fragments: Vec<u32>,
    ) -> Result<()> {
        let root_stage = self.stage_graph.get_root_stage();
        let mut current_actor_ids = vec![];
        let current_stage_id = current_stage.get_stage_id();
        if current_stage_id == root_stage.get_stage_id() {
            // Stage on the root, generate an actor without dispatcher.
            let actor_id = self.gen_actor_id(1).await?;
            let mut fragment_builder =
                StreamFragmentBuilder::new(actor_id, current_stage.get_node());
            // Set `Broadcast` dispatcher for root stage (means no dispatcher).
            fragment_builder.set_broadcast_dispatcher();

            // Add fragment. No dependency needed.
            current_actor_ids.push(actor_id);
            self.stream_graph.add_fragment(fragment_builder);
            self.stream_graph.set_root_fragment(actor_id);
        } else {
            let parallel_degree = if self.stage_graph.has_downstream(current_stage_id) {
                // Stage in the middle.
                // Generate one or multiple actors and connect them to the next stage.
                // Currently, we assume the parallel degree is at least 4, and grows linearly with
                // more worker nodes added.
                max(self.worker_count * 2, PARALLEL_DEGREE_LOW_BOUND)
            } else {
                // Stage on the source.
                self.worker_count
            };

            let node = current_stage.get_node();
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
                let mut fragment_builder = StreamFragmentBuilder::new(id, Arc::new(stream_node));
                fragment_builder.set_dispatcher(dispatcher.clone());
                self.stream_graph.add_fragment(fragment_builder);
                current_actor_ids.push(id);
            }
        }

        self.stream_graph
            .add_dependency(&current_actor_ids, &last_stage_fragments);

        // Recursively generating fragments on the downstream level.
        if self.stage_graph.has_downstream(current_stage_id) {
            for stage_id in self
                .stage_graph
                .get_downstream_stages(current_stage_id)
                .unwrap()
            {
                self.build_graph_from_stage(
                    self.stage_graph.get_stage_by_id(stage_id).unwrap(),
                    current_actor_ids.clone(),
                )
                .await?;
            }
        };

        Ok(())
    }
}
