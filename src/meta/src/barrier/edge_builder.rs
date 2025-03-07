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

use std::collections::HashMap;

use risingwave_common::bitmap::Bitmap;
use risingwave_meta_model::WorkerId;
use risingwave_meta_model::fragment::DistributionType;
use risingwave_pb::stream_plan::StreamNode;
use risingwave_pb::stream_plan::update_mutation::MergeUpdate;
use tracing::warn;

use crate::controller::fragment::InflightFragmentInfo;
use crate::controller::utils::compose_dispatchers;
use crate::model::{
    ActorId, DownstreamFragmentRelation, FragmentActorDispatchers, FragmentActorUpstreams,
    FragmentDownstreamRelation, FragmentId, StreamActor, StreamJobActorsToCreate,
};

#[derive(Debug)]
struct FragmentInfo {
    distribution_type: DistributionType,
    actors: HashMap<ActorId, Option<Bitmap>>,
}

pub(super) struct FragmentEdgeBuildResult {
    pub(super) upstreams: HashMap<FragmentId, FragmentActorUpstreams>,
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
                impl Iterator<Item = (&StreamActor, WorkerId)> + '_,
            ),
        >,
    ) -> StreamJobActorsToCreate {
        let mut actors_to_create = StreamJobActorsToCreate::default();
        for (fragment_id, node, actors) in actors {
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
                    .or_insert_with(|| (node.clone(), vec![]))
                    .1
                    .push((actor.clone(), upstreams, dispatchers))
            }
        }
        actors_to_create
    }
}

pub(super) struct FragmentEdgeBuilder {
    fragments: HashMap<FragmentId, FragmentInfo>,
    result: FragmentEdgeBuildResult,
}

impl FragmentEdgeBuilder {
    pub(super) fn new(fragment_infos: impl Iterator<Item = &InflightFragmentInfo>) -> Self {
        let mut fragments = HashMap::new();
        for info in fragment_infos {
            fragments
                .try_insert(
                    info.fragment_id,
                    FragmentInfo {
                        distribution_type: info.distribution_type,
                        actors: info
                            .actors
                            .iter()
                            .map(|(actor_id, actor)| (*actor_id, actor.vnode_bitmap.clone()))
                            .collect(),
                    },
                )
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

    fn add_edge(&mut self, fragment_id: FragmentId, downstream: &DownstreamFragmentRelation) {
        let fragment = &self
            .fragments
            .get(&fragment_id)
            .unwrap_or_else(|| panic!("cannot find {}", fragment_id));
        let downstream_fragment = &self.fragments[&downstream.downstream_fragment_id];
        let dispatchers = compose_dispatchers(
            fragment.distribution_type,
            &fragment.actors,
            downstream.downstream_fragment_id,
            downstream_fragment.distribution_type,
            &downstream_fragment.actors,
            downstream.dispatcher_type,
            downstream.dist_key_indices.clone(),
            downstream.output_indices.clone(),
        );
        let downstream_fragment_upstreams = self
            .result
            .upstreams
            .entry(downstream.downstream_fragment_id)
            .or_default();
        for (actor_id, dispatcher) in dispatchers {
            for downstream_actor in &dispatcher.downstream_actor_id {
                downstream_fragment_upstreams
                    .entry(*downstream_actor)
                    .or_default()
                    .entry(fragment_id)
                    .or_default()
                    .insert(actor_id);
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
                        added_upstream_actor_id: new_upstreams.into_iter().collect(),
                        removed_upstream_actor_id: vec![],
                    })
                } else if cfg!(debug_assertions) {
                    panic!("cannot find new upstreams for actor {} in fragment {} to new_upstream {}. Current upstreams {:?}", actor_id, fragment_id, new_upstream_fragment_id, actor_upstreams);
                } else {
                    warn!(actor_id, fragment_id, new_upstream_fragment_id, ?actor_upstreams, "cannot find new upstreams for actor");
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
            warn!(fragment_id, new_upstream_fragment_id, original_upstream_fragment_id, upstreams = ?self.result.upstreams, "cannot find new upstreams to replace");
        }
    }

    pub(super) fn build(self) -> FragmentEdgeBuildResult {
        self.result
    }
}
