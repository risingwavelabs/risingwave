use std::cmp::max;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU32, Ordering};
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
    ///
    /// Return a pair of (1) all stream actors, and (2) the actor id of sources to be forcefully
    /// round-robin scheduled.
    pub async fn generate_graph(
        &mut self,
        stream_node: &StreamNode,
    ) -> Result<BTreeMap<FragmentId, Fragment>> {
        self.generate_fragment_graph(stream_node)?;
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
    fn generate_fragment_graph(&mut self, stream_node: &StreamNode) -> Result<()> {
        let mut root_fragment = self.new_stream_fragment(Arc::new(stream_node.clone()));
        let (is_singleton, is_source) = self.build_fragment(&mut root_fragment, stream_node)?;
        root_fragment.set_singleton(is_singleton);

        if is_source {
            root_fragment.set_fragment_type(FragmentType::Source);
        }
        self.fragment_graph.add_root_fragment(root_fragment.clone());

        Ok(())
    }

    /// Build new fragment and link dependency with its parent fragment.
    /// Return two flags.
    /// The first flag indicates that whether the parent should be singleton.
    /// The second flag indicates that whether the parent is a source.
    // TODO: Should we store the concurrency in StreamFragment directly?
    fn build_fragment(
        &mut self,
        parent_fragment: &mut StreamFragment,
        stream_node: &StreamNode,
    ) -> Result<(bool, bool)> {
        if let Some(Node::MviewNode(_)) = stream_node.node {
            parent_fragment.set_fragment_type(FragmentType::Sink);
        }

        let mut is_singleton = false;
        let mut is_source = false;

        for node in stream_node.get_input() {
            let (is_singleton1, is_source1) = match node.get_node()? {
                Node::ExchangeNode(exchange_node) => {
                    let mut child_fragment = self.new_stream_fragment(Arc::new(node.clone()));
                    let (child_is_singleton, child_is_source) =
                        self.build_fragment(&mut child_fragment, node)?;
                    child_fragment.set_singleton(child_is_singleton);
                    if child_is_source {
                        child_fragment.set_fragment_type(FragmentType::Source);
                    }
                    self.fragment_graph.add_fragment(child_fragment.clone());
                    self.fragment_graph.link_child(
                        parent_fragment.get_fragment_id(),
                        child_fragment.get_fragment_id(),
                    );
                    (
                        exchange_node.get_dispatcher()?.get_type()? == DispatcherType::Simple,
                        false,
                    )
                }
                Node::SourceNode(_) => {
                    let (parent_is_singleton, _) = self.build_fragment(parent_fragment, node)?;
                    (parent_is_singleton, true)
                }
                Node::TopNNode(_) => {
                    let (_, child_is_source) = self.build_fragment(parent_fragment, node)?;
                    // TODO: Force singleton for TopN as a workaround.
                    // We should implement two phase TopN.
                    (true, child_is_source)
                }
                Node::ChainNode(_) => {
                    let (_, child_is_source) = self.build_fragment(parent_fragment, node)?;
                    // TODO: Force Chain to be singleton as a workaround.
                    // Remove this if parallel Chain is supported
                    (true, child_is_source)
                }
                _ => self.build_fragment(parent_fragment, node)?,
            };
            is_singleton |= is_singleton1;
            is_source |= is_source1;
        }
        Ok((is_singleton, is_source))
    }

    fn new_stream_fragment(&self, node: Arc<StreamNode>) -> StreamFragment {
        StreamFragment::new(self.next_fragment_id.fetch_add(1, Ordering::Relaxed), node)
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
