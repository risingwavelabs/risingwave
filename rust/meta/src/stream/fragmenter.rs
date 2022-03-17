use std::collections::{BTreeMap, HashMap};
use std::ops::Range;
use std::sync::Arc;

use async_recursion::async_recursion;
use itertools::Itertools;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{Result, RwError};
use risingwave_pb::common::HashMapping;
use risingwave_pb::meta::table_fragments::fragment::FragmentType;
use risingwave_pb::meta::table_fragments::Fragment;
use risingwave_pb::stream_plan::dispatcher::DispatcherType;
use risingwave_pb::stream_plan::stream_node::Node;
use risingwave_pb::stream_plan::{Dispatcher, StreamNode};

use super::{CreateMaterializedViewContext, FragmentManagerRef};
use crate::cluster::ParallelUnitId;
use crate::manager::{IdCategory, IdGeneratorManagerRef};
use crate::model::{ActorId, FragmentId};
use crate::storage::MetaStore;
use crate::stream::graph::{
    StreamActorBuilder, StreamFragment, StreamFragmentGraph, StreamGraphBuilder,
};

/// [`StreamFragmenter`] generates the proto for interconnected actors for a streaming pipeline.
pub struct StreamFragmenter<S> {
    /// fragment graph field, transformed from input streaming plan.
    fragment_graph: StreamFragmentGraph,
    /// stream graph builder, to build streaming DAG.
    stream_graph: StreamGraphBuilder<S>,

    /// id generator, used to generate actor id.
    id_gen_manager_ref: IdGeneratorManagerRef<S>,
    /// hash mapping, used for hash dispatcher
    hash_mapping: Vec<ParallelUnitId>,
}

impl<S> StreamFragmenter<S>
where
    S: MetaStore,
{
    pub fn new(
        id_gen_manager_ref: IdGeneratorManagerRef<S>,
        fragment_manager_ref: FragmentManagerRef<S>,
        hash_mapping: Vec<ParallelUnitId>,
    ) -> Self {
        Self {
            fragment_graph: StreamFragmentGraph::new(None),
            stream_graph: StreamGraphBuilder::new(fragment_manager_ref),
            id_gen_manager_ref,
            hash_mapping,
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
        ctx: &mut CreateMaterializedViewContext,
    ) -> Result<BTreeMap<FragmentId, Fragment>> {
        self.generate_fragment_graph(stream_node).await?;
        self.build_graph_from_fragment(self.fragment_graph.get_root_fragment(), vec![])
            .await?;

        let stream_graph = self.stream_graph.build(ctx)?;

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

    /// Build new fragment and link dependencies by visiting children recursively, update
    /// `is_singleton` and `fragment_type` properties for current fragment.
    // TODO: Should we store the concurrency in StreamFragment directly?
    #[async_recursion]
    async fn build_fragment(
        &mut self,
        current_fragment: &mut StreamFragment,
        stream_node: &StreamNode,
    ) -> Result<()> {
        // Update current fragment based on the node we're visiting.
        match stream_node.get_node()? {
            Node::SourceNode(_) => current_fragment.set_fragment_type(FragmentType::Source),
            Node::MaterializeNode(_) => current_fragment.set_fragment_type(FragmentType::Sink),

            // TODO: Force singleton for TopN as a workaround. We should implement two phase TopN.
            Node::TopNNode(_) => current_fragment.set_singleton(true),
            // TODO: Force Chain to be singleton as a workaround. Remove this if parallel Chain is
            // supported
            Node::ChainNode(_) => current_fragment.set_singleton(true),

            _ => {}
        };

        // Visit children.
        for child_node in stream_node.get_input() {
            match child_node.get_node()? {
                // Exchange node indicates a new child fragment.
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

                // For other children, visit recursively.
                _ => self.build_fragment(current_fragment, child_node).await?,
            };
        }

        Ok(())
    }

    /// Create a new stream fragment with given node with generating a fragment id.
    async fn new_stream_fragment(&self, node: StreamNode) -> Result<StreamFragment> {
        let fragment_id = self
            .id_gen_manager_ref
            .generate::<{ IdCategory::Fragment }>()
            .await? as _;
        let fragment = StreamFragment::new(fragment_id, Arc::new(node));

        Ok(fragment)
    }

    /// Generate actor id from id generator.
    async fn gen_actor_ids(&self, parallel_degree: u32) -> Result<Range<ActorId>> {
        let start_actor_id = self
            .id_gen_manager_ref
            .generate_interval::<{ IdCategory::Actor }>(parallel_degree as i32)
            .await? as _;

        Ok(start_actor_id..start_actor_id + parallel_degree)
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
        let current_fragment_id = current_fragment.get_fragment_id();

        let parallel_degree = if current_fragment.is_singleton() {
            1
        } else {
            self.hash_mapping.iter().unique().count() as u32
        };
        let actor_ids = self.gen_actor_ids(parallel_degree).await?;

        let node = current_fragment.get_node();
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

        for id in actor_ids.clone() {
            let mut actor_builder = StreamActorBuilder::new(id, current_fragment_id, node.clone());

            // Construct a consistent hash mapping of actors based on that of parallel units. Set
            // the new mapping into hash dispatchers.
            if dispatcher.r#type == DispatcherType::Hash as i32 {
                // Theoretically, a hash dispatcher should have `self.hash_parallel_count` as the
                // number of its downstream actors. However, since the frontend optimizer is still
                // WIP, there exists some unoptimized situation where a hash dispatcher has ONLY
                // ONE downstream actor, which makes it behave like a simple dispatcher. As an
                // expedient, we specially compute the consistent hash mapping here. The `if`
                // branch could be removed after the optimizer has been fully implemented.
                let streaming_hash_mapping = if last_fragment_actors.len() == 1 {
                    vec![last_fragment_actors[0]; self.hash_mapping.len()]
                } else {
                    let hash_parallel_units = self.hash_mapping.iter().unique().collect_vec();
                    assert_eq!(last_fragment_actors.len(), hash_parallel_units.len());
                    let parallel_unit_actor_map: HashMap<_, _> = hash_parallel_units
                        .into_iter()
                        .zip_eq(last_fragment_actors.clone().into_iter())
                        .collect();
                    self.hash_mapping
                        .iter()
                        .map(|parallel_unit_id| parallel_unit_actor_map[parallel_unit_id])
                        .collect_vec()
                };
                actor_builder.set_hash_dispatcher(
                    dispatcher.column_indices.clone(),
                    HashMapping {
                        hash_mapping: streaming_hash_mapping,
                    },
                )
            } else {
                actor_builder.set_dispatcher(dispatcher.clone());
            }
            self.stream_graph.add_actor(actor_builder);
        }

        let current_actor_ids = actor_ids.collect_vec();
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
