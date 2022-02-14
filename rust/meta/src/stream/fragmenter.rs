use std::cmp::max;
use std::collections::BTreeMap;
use std::sync::Arc;

use async_recursion::async_recursion;
use risingwave_common::array::RwError;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::Result;
use risingwave_pb::meta::table_fragments::fragment::FragmentType;
use risingwave_pb::meta::table_fragments::Fragment;
use risingwave_pb::stream_plan::dispatcher::DispatcherType;
use risingwave_pb::stream_plan::stream_node::Node;
use risingwave_pb::stream_plan::{Dispatcher, StreamNode};

use crate::manager::{IdCategory, IdGeneratorManagerRef};
use crate::model::{ActorId, FragmentId};
use crate::stream::graph::{
    StreamActorBuilder, StreamFragment, StreamFragmentGraph, StreamGraphBuilder,
};

const PARALLEL_DEGREE_LOW_BOUND: u32 = 4;

/// [`StreamFragmenter`] generates the proto for interconnected actors for a streaming pipeline.
pub struct StreamFragmenter {
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
            fragment_graph: StreamFragmentGraph::new(None),
            stream_graph: StreamGraphBuilder::new(),
            id_gen_manager_ref,
            worker_count,
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
    ) -> Result<BTreeMap<FragmentId, Fragment>> {
        self.generate_fragment_graph(stream_node).await?;
        self.build_graph_from_fragment(self.fragment_graph.get_root_fragment(), vec![])
            .await?;

        let stream_graph = self.stream_graph.build()?;
        stream_graph
            .iter()
            .map(|(&fragment_id, actors)| {
                Ok::<_, RwError>((
                    fragment_id,
                    Fragment {
                        fragment_id,
                        fragment_type: self.fragment_graph.get_fragment_type_by_id(fragment_id)?
                            as i32,
                        actors: actors.clone(),
                    },
                ))
            })
            .collect::<Result<BTreeMap<_, _>>>()
    }

    /// Generate fragment DAG from input streaming plan by their dependency.
    async fn generate_fragment_graph(&mut self, stream_node: &StreamNode) -> Result<()> {
        self.build_and_add_fragment(stream_node, true).await?;
        Ok(())
    }

    /// Use the given `stream_node` to create a fragment and add it to graph.
    async fn build_and_add_fragment(
        &mut self,
        stream_node: &StreamNode,
        is_root: bool,
    ) -> Result<StreamFragment> {
        let mut fragment = self.new_stream_fragment(stream_node.clone()).await?;
        self.build_fragment(&mut fragment, stream_node).await?;
        self.fragment_graph.add_fragment(fragment.clone(), is_root);
        Ok(fragment)
    }

    /// Build new fragment and link dependency with its parent (current) fragment, update
    /// `is_singleton` and `fragment_type` properties for current fragment.
    // TODO: Should we store the concurrency in StreamFragment directly?
    #[async_recursion]
    async fn build_fragment(
        &mut self,
        current_fragment: &mut StreamFragment,
        stream_node: &StreamNode,
    ) -> Result<()> {
        match stream_node.get_node()? {
            Node::SourceNode(_) => current_fragment.set_fragment_type(FragmentType::Source),
            Node::MviewNode(_) => current_fragment.set_fragment_type(FragmentType::Sink),

            // TODO: Force singleton for TopN as a workaround. We should implement two phase TopN.
            Node::TopNNode(_) => current_fragment.set_singleton(true),
            // TODO: Force Chain to be singleton as a workaround. Remove this if parallel Chain is
            // supported
            Node::ChainNode(_) => current_fragment.set_singleton(true),

            _ => {}
        };

        for child_node in stream_node.get_input() {
            match child_node.get_node()? {
                Node::ExchangeNode(exchange_node) => {
                    let child_fragment = self.build_and_add_fragment(child_node, false).await?;
                    self.fragment_graph.link_child(
                        current_fragment.get_fragment_id(),
                        child_fragment.get_fragment_id(),
                    );

                    let is_simple_dispatcher =
                        exchange_node.get_dispatcher()?.get_type()? == DispatcherType::Simple;
                    if is_simple_dispatcher {
                        current_fragment.set_singleton(true);
                    }
                }

                _ => self.build_fragment(current_fragment, child_node).await?,
            };
        }

        Ok(())
    }

    async fn new_stream_fragment(&self, node: StreamNode) -> Result<StreamFragment> {
        Ok(StreamFragment::new(
            self.gen_fragment_id().await?,
            Arc::new(node),
        ))
    }

    /// Generate fragment id for each fragment.
    async fn gen_fragment_id(&self) -> Result<FragmentId> {
        Ok(self
            .id_gen_manager_ref
            .generate::<{ IdCategory::Fragment }>()
            .await? as _)
    }

    /// Generate actor id from id generator.
    async fn gen_actor_id(&self, parallel_degree: u32) -> Result<ActorId> {
        Ok(self
            .id_gen_manager_ref
            .generate_interval::<{ IdCategory::Actor }>(parallel_degree as i32)
            .await? as _)
    }

    /// Build stream graph from fragment graph recursively. Setup dispatcher in actor and generate
    /// actors by their parallelism.
    #[async_recursion]
    async fn build_graph_from_fragment(
        &mut self,
        current_fragment: StreamFragment,
        last_fragment_actors: Vec<ActorId>,
    ) -> Result<()> {
        let root_fragment = self.fragment_graph.get_root_fragment();
        let mut current_actor_ids = vec![];
        let current_fragment_id = current_fragment.get_fragment_id();
        let parallel_degree = if current_fragment.is_singleton() {
            1
        } else {
            // Currently, we assume the parallel degree is at least 4, and grows linearly with
            // more worker nodes added.
            max(self.worker_count * 2, PARALLEL_DEGREE_LOW_BOUND)
        };

        let node = current_fragment.get_node();
        let actor_ids = self.gen_actor_id(parallel_degree).await?;
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
            let mut actor_builder = StreamActorBuilder::new(id, current_fragment_id, node.clone());
            actor_builder.set_dispatcher(dispatcher.clone());
            self.stream_graph.add_actor(actor_builder);
            current_actor_ids.push(id);
        }

        self.stream_graph
            .add_dependency(&current_actor_ids, &last_fragment_actors);

        // Recursively generating actors on the downstream level.
        if self.fragment_graph.has_upstream(current_fragment_id) {
            for fragment_id in self
                .fragment_graph
                .get_upstream_fragments(current_fragment_id)
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
