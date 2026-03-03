// Copyright 2025 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::{HashMap, HashSet};

use risingwave_common::bitmap::Bitmap;
use risingwave_meta_model::WorkerId;
use risingwave_meta_model::fragment::DistributionType;
use risingwave_pb::common::{ActorInfo, HostAddress};
use risingwave_pb::id::{PartialGraphId, SubscriberId};
use risingwave_pb::stream_plan::update_mutation::MergeUpdate;
use risingwave_pb::stream_plan::{PbDispatcherType, StreamNode};
use tracing::warn;

use crate::barrier::rpc::ControlStreamManager;
use crate::controller::fragment::InflightFragmentInfo;
use crate::controller::utils::compose_dispatchers;
use crate::model::{
    ActorId, ActorNewNoShuffle, ActorUpstreams, DownstreamFragmentRelation, Fragment,
    FragmentActorDispatchers, FragmentDownstreamRelation, FragmentId, StreamActor,
    StreamJobActorsToCreate,
};

/// Fragment information needed by [`FragmentEdgeBuilder`] to compute dispatchers and merge nodes.
///
/// Contains actor bitmaps and resolved host addresses.
#[derive(Debug)]
pub(super) struct EdgeBuilderFragmentInfo {
    pub distribution_type: DistributionType,
    pub actors: HashMap<ActorId, Option<Bitmap>>,
    pub actor_location: HashMap<ActorId, HostAddress>,
    pub partial_graph_id: PartialGraphId,
}

impl EdgeBuilderFragmentInfo {
    /// Build from an already-inflight fragment (actors already materialized).
    pub fn from_inflight(
        info: &InflightFragmentInfo,
        partial_graph_id: PartialGraphId,
        control_stream_manager: &ControlStreamManager,
    ) -> Self {
        let (actors, actor_location) = info
            .actors
            .iter()
            .map(|(&actor_id, actor)| {
                (
                    (actor_id, actor.vnode_bitmap.clone()),
                    (actor_id, control_stream_manager.host_addr(actor.worker_id)),
                )
            })
            .unzip();
        Self {
            distribution_type: info.distribution_type,
            actors,
            actor_location,
            partial_graph_id,
        }
    }

    /// Build from a model `Fragment` with separately provided actors and locations.
    pub fn from_fragment(
        fragment: &Fragment,
        stream_actors: &HashMap<FragmentId, Vec<StreamActor>>,
        actor_worker: &HashMap<ActorId, WorkerId>,
        partial_graph_id: PartialGraphId,
        control_stream_manager: &ControlStreamManager,
    ) -> Self {
        let (actors, actor_location) = stream_actors
            .get(&fragment.fragment_id)
            .into_iter()
            .flatten()
            .map(|actor| {
                (
                    (actor.actor_id, actor.vnode_bitmap.clone()),
                    (
                        actor.actor_id,
                        control_stream_manager.host_addr(actor_worker[&actor.actor_id]),
                    ),
                )
            })
            .unzip();
        Self {
            distribution_type: fragment.distribution_type.into(),
            actors,
            actor_location,
            partial_graph_id,
        }
    }
}

#[derive(Debug)]
pub(super) struct FragmentEdgeBuildResult {
    pub(super) upstreams: HashMap<FragmentId, HashMap<ActorId, ActorUpstreams>>,
    pub(super) dispatchers: FragmentActorDispatchers,
    pub(super) merge_updates: HashMap<FragmentId, Vec<MergeUpdate>>,
}

impl FragmentEdgeBuildResult {
    pub(super) fn collect_actors_to_create(
        &mut self,
        actors: impl Iterator<
            Item = (
                FragmentId,
                &StreamNode,
                impl Iterator<Item = (&StreamActor, WorkerId)>,
                impl IntoIterator<Item = SubscriberId>,
            ),
        >,
    ) -> StreamJobActorsToCreate {
        let mut actors_to_create = StreamJobActorsToCreate::default();
        for (fragment_id, node, actors, subscriber_ids) in actors {
            let subscriber_ids: HashSet<_> = subscriber_ids.into_iter().collect();
            for (actor, worker_id) in actors {
                let upstreams = self
                    .upstreams
                    .get_mut(&fragment_id)
                    .and_then(|upstreams| upstreams.remove(&actor.actor_id))
                    .unwrap_or_default();
                let dispatchers = self
                    .dispatchers
                    .get_mut(&fragment_id)
                    .and_then(|upstreams| upstreams.remove(&actor.actor_id))
                    .unwrap_or_default();
                actors_to_create
                    .entry(worker_id)
                    .or_default()
                    .entry(fragment_id)
                    .or_insert_with(|| (node.clone(), vec![], subscriber_ids.clone()))
                    .1
                    .push((actor.clone(), upstreams, dispatchers))
            }
        }
        actors_to_create
    }

    pub(super) fn is_empty(&self) -> bool {
        self.merge_updates
            .values()
            .all(|updates| updates.is_empty())
            && self.dispatchers.values().all(|dispatchers| {
                dispatchers
                    .values()
                    .all(|dispatchers| dispatchers.is_empty())
            })
            && self
                .merge_updates
                .values()
                .all(|updates| updates.is_empty())
    }

    /// Extract the actor-level no-shuffle mapping from the dispatchers.
    ///
    /// Returns: `upstream_fragment_id -> downstream_fragment_id -> upstream_actor_id -> downstream_actor_id`
    pub(super) fn extract_no_shuffle(&self) -> ActorNewNoShuffle {
        let mut no_shuffle: ActorNewNoShuffle = HashMap::new();
        for (&upstream_fragment_id, actor_dispatchers) in &self.dispatchers {
            for (&actor_id, dispatchers) in actor_dispatchers {
                for dispatcher in dispatchers {
                    if dispatcher.r#type == PbDispatcherType::NoShuffle as i32 {
                        let downstream_fragment_id = dispatcher.dispatcher_id;
                        assert_eq!(
                            dispatcher.downstream_actor_id.len(),
                            1,
                            "NoShuffle dispatcher should have exactly one downstream actor"
                        );
                        let downstream_actor_id = dispatcher.downstream_actor_id[0];
                        no_shuffle
                            .entry(upstream_fragment_id)
                            .or_default()
                            .entry(downstream_fragment_id)
                            .or_default()
                            .insert(actor_id, downstream_actor_id);
                    }
                }
            }
        }
        no_shuffle
    }
}

pub(super) struct FragmentEdgeBuilder {
    fragments: HashMap<FragmentId, EdgeBuilderFragmentInfo>,
    result: FragmentEdgeBuildResult,
}

impl FragmentEdgeBuilder {
    /// Create a new edge builder from fragment edge information.
    pub(super) fn new(
        fragment_infos: impl IntoIterator<Item = (FragmentId, EdgeBuilderFragmentInfo)>,
    ) -> Self {
        let mut fragments = HashMap::new();
        for (fragment_id, info) in fragment_infos {
            fragments
                .try_insert(fragment_id, info)
                .expect("non-duplicate");
        }
        Self {
            fragments,
            result: FragmentEdgeBuildResult {
                upstreams: Default::default(),
                dispatchers: Default::default(),
                merge_updates: Default::default(),
            },
        }
    }

    pub(super) fn add_relations(&mut self, relations: &FragmentDownstreamRelation) {
        for (fragment_id, relations) in relations {
            for relation in relations {
                self.add_edge(*fragment_id, relation);
            }
        }
    }

    pub(super) fn add_edge(
        &mut self,
        fragment_id: FragmentId,
        downstream: &DownstreamFragmentRelation,
    ) {
        let Some(fragment) = &self.fragments.get(&fragment_id) else {
            if self
                .fragments
                .contains_key(&downstream.downstream_fragment_id)
            {
                panic!(
                    "cannot find fragment {} with downstream {:?}",
                    fragment_id, downstream
                )
            } else {
                // ignore fragment relation with both upstream and downstream not in the set of fragments
                return;
            }
        };
        let downstream_fragment = &self.fragments[&downstream.downstream_fragment_id];
        let dispatchers = compose_dispatchers(
            fragment.distribution_type,
            &fragment.actors,
            downstream.downstream_fragment_id,
            downstream_fragment.distribution_type,
            &downstream_fragment.actors,
            downstream.dispatcher_type,
            downstream.dist_key_indices.clone(),
            downstream.output_mapping.clone(),
        );
        let downstream_fragment_upstreams = self
            .result
            .upstreams
            .entry(downstream.downstream_fragment_id)
            .or_default();
        for (actor_id, dispatcher) in dispatchers {
            let actor_location = &fragment.actor_location[&actor_id];
            for downstream_actor in &dispatcher.downstream_actor_id {
                downstream_fragment_upstreams
                    .entry(*downstream_actor)
                    .or_default()
                    .entry(fragment_id)
                    .or_default()
                    .insert(
                        actor_id,
                        ActorInfo {
                            actor_id,
                            host: Some(actor_location.clone()),
                            partial_graph_id: fragment.partial_graph_id,
                        },
                    );
            }
            self.result
                .dispatchers
                .entry(fragment_id)
                .or_default()
                .entry(actor_id)
                .or_default()
                .push(dispatcher);
        }
    }

    pub(super) fn replace_upstream(
        &mut self,
        fragment_id: FragmentId,
        original_upstream_fragment_id: FragmentId,
        new_upstream_fragment_id: FragmentId,
    ) {
        let fragment_merge_updates = self.result.merge_updates.entry(fragment_id).or_default();
        if let Some(fragment_upstreams) = self.result.upstreams.get_mut(&fragment_id) {
            fragment_upstreams.retain(|&actor_id, actor_upstreams| {
                if let Some(new_upstreams) = actor_upstreams.remove(&new_upstream_fragment_id) {
                    fragment_merge_updates.push(MergeUpdate {
                        actor_id,
                        upstream_fragment_id: original_upstream_fragment_id,
                        new_upstream_fragment_id: Some(new_upstream_fragment_id),
                        added_upstream_actors: new_upstreams.into_values().collect(),
                        removed_upstream_actor_id: vec![],
                    })
                } else if cfg!(debug_assertions) {
                    panic!("cannot find new upstreams for actor {} in fragment {} to new_upstream {}. Current upstreams {:?}", actor_id, fragment_id, new_upstream_fragment_id, actor_upstreams);
                } else {
                    warn!(%actor_id, %fragment_id, %new_upstream_fragment_id, ?actor_upstreams, "cannot find new upstreams for actor");
                }
                !actor_upstreams.is_empty()
            })
        } else if cfg!(debug_assertions) {
            panic!(
                "cannot find new upstreams for fragment {} to new_upstream {} to replace {}. Current upstreams: {:?}",
                fragment_id,
                new_upstream_fragment_id,
                original_upstream_fragment_id,
                self.result.upstreams
            );
        } else {
            warn!(%fragment_id, %new_upstream_fragment_id, %original_upstream_fragment_id, upstreams = ?self.result.upstreams, "cannot find new upstreams to replace");
        }
    }

    /// Finalize the builder, returning the edge build result.
    pub(super) fn build(self) -> FragmentEdgeBuildResult {
        self.result
    }
}
