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
use std::marker::PhantomData;

use anyhow::anyhow;
use risingwave_common::bitmap::Bitmap;
use risingwave_meta_model::WorkerId;
use risingwave_meta_model::fragment::DistributionType;
use risingwave_pb::common::{ActorInfo, HostAddress};
use risingwave_pb::id::{PartialGraphId, SubscriberId};
use risingwave_pb::stream_plan::update_mutation::{DispatcherUpdate, MergeUpdate};
use risingwave_pb::stream_plan::{AddMutation, PbDispatcher, StreamNode, UpdateMutation};
use tracing::warn;

use crate::MetaResult;
use crate::barrier::rpc::ControlStreamManager;
use crate::controller::fragment::InflightFragmentInfo;
use crate::controller::utils::compose_dispatchers;
use crate::model::{
    ActorId, ActorNewNoShuffle, ActorUpstreams, DownstreamFragmentRelation, Fragment,
    FragmentActorDispatchers, FragmentDownstreamRelation, FragmentId, StreamActor,
    StreamJobActorsToCreate,
};

type ComposedEdge = (
    HashMap<ActorId, PbDispatcher>,
    HashMap<ActorId, ActorUpstreams>,
    Option<HashMap<ActorId, ActorId>>,
);

/// Fragment information needed by [`FragmentEdgeBuilder`] to compute dispatchers and merge nodes.
///
/// Contains actor bitmaps and resolved host addresses.
#[derive(Debug)]
struct EdgeBuilderFragmentInfo {
    distribution_type: DistributionType,
    actors: HashMap<ActorId, Option<Bitmap>>,
    actor_location: HashMap<ActorId, HostAddress>,
    partial_graph_id: PartialGraphId,
}

#[derive(Debug)]
enum FragmentStatus {
    Existing(EdgeBuilderFragmentInfo),
    New(EdgeBuilderFragmentInfo),
    Changed {
        before: EdgeBuilderFragmentInfo,
        after: EdgeBuilderFragmentInfo,
    },
}

impl EdgeBuilderFragmentInfo {
    /// Build from an already-inflight fragment (actors already materialized).
    fn from_inflight(
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
    fn from_fragment(
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
pub(crate) struct FragmentEdgeBuildResult {
    upstreams: HashMap<FragmentId, HashMap<ActorId, ActorUpstreams>>,
    dispatchers: FragmentActorDispatchers,
    merge_updates: HashMap<FragmentId, Vec<MergeUpdate>>,
    dispatcher_updates: Vec<DispatcherUpdate>,
    actor_new_no_shuffle: ActorNewNoShuffle,
}

impl FragmentEdgeBuildResult {
    pub(super) fn actor_new_no_shuffle(&self) -> &ActorNewNoShuffle {
        &self.actor_new_no_shuffle
    }

    fn validate_terminal_consumption(&self, applies_updates: bool) {
        // Upstreams to existing actors are intentionally unused by `Add` for sink-into-table:
        // that input is carried separately in `new_upstream_sinks`. `actor_new_no_shuffle` is
        // also excluded because split resolution only borrows it.
        let remaining_upstreams = if applies_updates {
            self.upstreams.values().map(HashMap::len).sum::<usize>()
        } else {
            0
        };
        let unapplied_dispatcher_updates = if applies_updates {
            0
        } else {
            self.dispatcher_updates.len()
        };
        let unapplied_merge_updates = if applies_updates {
            0
        } else {
            self.merge_updates.values().map(Vec::len).sum::<usize>()
        };
        let has_unconsumed_fields = remaining_upstreams != 0
            || unapplied_dispatcher_updates != 0
            || unapplied_merge_updates != 0;

        debug_assert!(
            !has_unconsumed_fields,
            "edge result has unconsumed fields: remaining_upstreams={remaining_upstreams}, unapplied_dispatcher_updates={unapplied_dispatcher_updates}, unapplied_merge_updates={unapplied_merge_updates}"
        );
        #[cfg(not(debug_assertions))]
        if has_unconsumed_fields {
            warn!(
                remaining_upstreams,
                unapplied_dispatcher_updates,
                unapplied_merge_updates,
                "edge result has unconsumed fields"
            );
        }
    }

    /// Apply the remaining dispatchers after newly created actors have been collected.
    pub(crate) fn apply_to_add_mutation(self, mutation: &mut AddMutation) {
        self.validate_terminal_consumption(false);
        for (actor_id, dispatchers) in self.dispatchers.into_values().flatten() {
            mutation
                .actor_dispatchers
                .entry(actor_id)
                .or_default()
                .dispatchers
                .extend(dispatchers);
        }
    }

    /// Apply the remaining edge updates after newly created actors have been collected.
    pub(crate) fn apply_to_update_mutation(self, mutation: &mut UpdateMutation) {
        self.validate_terminal_consumption(true);
        mutation.dispatcher_update.extend(self.dispatcher_updates);
        mutation
            .merge_update
            .extend(self.merge_updates.into_values().flatten());
        for (actor_id, dispatchers) in self.dispatchers.into_values().flatten() {
            mutation
                .actor_new_dispatchers
                .entry(actor_id)
                .or_default()
                .dispatchers
                .extend(dispatchers);
        }
    }

    pub(crate) fn collect_actors_to_create(
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
}

pub(crate) struct RegisteringFragments;
pub(crate) struct AddingRelations;

pub(crate) struct FragmentEdgeBuilder<State> {
    fragments: HashMap<FragmentId, FragmentStatus>,
    result: FragmentEdgeBuildResult,
    _state: PhantomData<State>,
}

impl FragmentEdgeBuilder<RegisteringFragments> {
    pub(crate) fn new() -> Self {
        Self {
            fragments: Default::default(),
            result: FragmentEdgeBuildResult {
                upstreams: Default::default(),
                dispatchers: Default::default(),
                merge_updates: Default::default(),
                dispatcher_updates: Default::default(),
                actor_new_no_shuffle: Default::default(),
            },
            _state: PhantomData,
        }
    }

    fn add_existing_fragment_infos(
        &mut self,
        fragment_infos: impl IntoIterator<Item = (FragmentId, EdgeBuilderFragmentInfo)>,
    ) {
        for (fragment_id, info) in fragment_infos {
            self.fragments
                .try_insert(fragment_id, FragmentStatus::Existing(info))
                .expect("non-duplicate");
        }
    }

    pub(crate) fn add_existing_fragments<'a>(
        mut self,
        fragments: impl IntoIterator<Item = &'a InflightFragmentInfo>,
        partial_graph_id: PartialGraphId,
        control_stream_manager: &ControlStreamManager,
    ) -> Self {
        self.add_existing_fragment_infos(fragments.into_iter().map(|fragment| {
            (
                fragment.fragment_id,
                EdgeBuilderFragmentInfo::from_inflight(
                    fragment,
                    partial_graph_id,
                    control_stream_manager,
                ),
            )
        }));
        self
    }

    fn add_new_fragment_infos(
        &mut self,
        fragment_infos: impl IntoIterator<Item = (FragmentId, EdgeBuilderFragmentInfo)>,
    ) {
        for (fragment_id, info) in fragment_infos {
            self.fragments
                .try_insert(fragment_id, FragmentStatus::New(info))
                .expect("new fragment must not already be registered");
        }
    }

    pub(crate) fn add_new_fragments<'a>(
        mut self,
        fragments: impl IntoIterator<Item = &'a InflightFragmentInfo>,
        partial_graph_id: PartialGraphId,
        control_stream_manager: &ControlStreamManager,
    ) -> Self {
        self.add_new_fragment_infos(fragments.into_iter().map(|fragment| {
            (
                fragment.fragment_id,
                EdgeBuilderFragmentInfo::from_inflight(
                    fragment,
                    partial_graph_id,
                    control_stream_manager,
                ),
            )
        }));
        self
    }

    pub(crate) fn add_new_logical_fragments<'a>(
        mut self,
        fragments: impl IntoIterator<Item = (PartialGraphId, &'a Fragment)>,
        stream_actors: &HashMap<FragmentId, Vec<StreamActor>>,
        actor_worker: &HashMap<ActorId, WorkerId>,
        control_stream_manager: &ControlStreamManager,
    ) -> Self {
        self.add_new_fragment_infos(fragments.into_iter().map(|(partial_graph_id, fragment)| {
            (
                fragment.fragment_id,
                EdgeBuilderFragmentInfo::from_fragment(
                    fragment,
                    stream_actors,
                    actor_worker,
                    partial_graph_id,
                    control_stream_manager,
                ),
            )
        }));
        self
    }

    fn replace_existing_fragment_actor_infos(
        &mut self,
        fragment_infos: impl IntoIterator<Item = (FragmentId, EdgeBuilderFragmentInfo)>,
    ) {
        for (fragment_id, after) in fragment_infos {
            let status = self
                .fragments
                .remove(&fragment_id)
                .expect("changed fragment must already be registered");
            let FragmentStatus::Existing(before) = status else {
                panic!("fragment {fragment_id} can only be changed once");
            };
            assert_eq!(
                before.distribution_type, after.distribution_type,
                "fragment distribution type cannot change"
            );
            for actor_id in before.actors.keys() {
                assert!(
                    !after.actors.contains_key(actor_id),
                    "changed fragment {fragment_id} must use entirely fresh actor ids: retained {actor_id}"
                );
            }
            self.fragments
                .insert(fragment_id, FragmentStatus::Changed { before, after });
        }
    }

    pub(crate) fn replace_existing_fragment_actors<'a>(
        mut self,
        fragments: impl IntoIterator<Item = &'a InflightFragmentInfo>,
        partial_graph_id: PartialGraphId,
        control_stream_manager: &ControlStreamManager,
    ) -> Self {
        self.replace_existing_fragment_actor_infos(fragments.into_iter().map(|fragment| {
            (
                fragment.fragment_id,
                EdgeBuilderFragmentInfo::from_inflight(
                    fragment,
                    partial_graph_id,
                    control_stream_manager,
                ),
            )
        }));
        self
    }

    pub(crate) fn finish_fragments(self) -> FragmentEdgeBuilder<AddingRelations> {
        FragmentEdgeBuilder {
            fragments: self.fragments,
            result: self.result,
            _state: PhantomData,
        }
    }
}

impl FragmentEdgeBuilder<AddingRelations> {
    pub(crate) fn add_relations(
        mut self,
        relations: &FragmentDownstreamRelation,
    ) -> MetaResult<Self> {
        for fragment_id in self.fragments.keys().copied().collect::<Vec<_>>() {
            let Some(relations) = relations.get(&fragment_id) else {
                continue;
            };
            for relation in relations {
                if self
                    .fragments
                    .contains_key(&relation.downstream_fragment_id)
                {
                    self.add_edge_inner(fragment_id, relation)?;
                }
            }
        }
        Ok(self)
    }

    /// Add a relation that is not yet installed in the runtime graph.
    pub(crate) fn add_edge(
        mut self,
        fragment_id: FragmentId,
        downstream: &DownstreamFragmentRelation,
    ) -> MetaResult<Self> {
        self.add_edge_inner(fragment_id, downstream)?;
        Ok(self)
    }

    fn add_edge_inner(
        &mut self,
        fragment_id: FragmentId,
        downstream: &DownstreamFragmentRelation,
    ) -> MetaResult<()> {
        let Some(fragment_status) = self.fragments.get(&fragment_id) else {
            if self
                .fragments
                .contains_key(&downstream.downstream_fragment_id)
            {
                return Err(anyhow!(
                    "cannot find fragment {} with downstream {:?}",
                    fragment_id,
                    downstream
                )
                .into());
            } else {
                // ignore fragment relation with both upstream and downstream not in the set of fragments
                return Ok(());
            }
        };
        let Some(downstream_status) = self.fragments.get(&downstream.downstream_fragment_id) else {
            // upstream is in the builder but downstream is not (e.g., edge to an independent job's fragment).
            // Skip this edge.
            return Ok(());
        };
        match (fragment_status, downstream_status) {
            // `FragmentStatus` describes the lifecycle of the fragment's actor set, not whether
            // the relation itself is new. Current mutation workflows only install relations where
            // at least one endpoint is new or changed, so an existing-to-existing relation is
            // already installed and needs no mutation.
            (FragmentStatus::Existing(_), FragmentStatus::Existing(_)) => {}
            (
                FragmentStatus::Existing(fragment),
                FragmentStatus::Changed {
                    before,
                    after: downstream_fragment,
                },
            ) => {
                let (dispatchers, upstreams, no_shuffle_map) =
                    Self::compose_edge(fragment_id, fragment, downstream, downstream_fragment);
                Self::add_no_shuffle_mapping(
                    &mut self.result,
                    fragment_id,
                    downstream.downstream_fragment_id,
                    no_shuffle_map,
                );
                Self::add_upstreams(
                    &mut self.result.upstreams,
                    downstream.downstream_fragment_id,
                    upstreams,
                );
                Self::add_dispatcher_updates(
                    &mut self.result.dispatcher_updates,
                    dispatchers,
                    before.actors.keys().copied(),
                );
            }
            (
                FragmentStatus::Changed {
                    before,
                    after: fragment,
                },
                FragmentStatus::Existing(downstream_fragment),
            ) => {
                let (dispatchers, upstreams, no_shuffle_map) =
                    Self::compose_edge(fragment_id, fragment, downstream, downstream_fragment);
                Self::add_no_shuffle_mapping(
                    &mut self.result,
                    fragment_id,
                    downstream.downstream_fragment_id,
                    no_shuffle_map,
                );
                Self::add_dispatchers(&mut self.result.dispatchers, fragment_id, dispatchers);
                Self::add_merge_updates(
                    &mut self.result.merge_updates,
                    fragment_id,
                    downstream.downstream_fragment_id,
                    downstream_fragment,
                    upstreams,
                    before.actors.keys().copied(),
                );
            }
            (FragmentStatus::New(_), FragmentStatus::Changed { .. })
            | (FragmentStatus::Changed { .. }, FragmentStatus::New(_)) => {
                return Err(anyhow!(
                    "an edge cannot connect new and changed fragments: {} -> {}",
                    fragment_id,
                    downstream.downstream_fragment_id,
                )
                .into());
            }
            (FragmentStatus::Existing(fragment), FragmentStatus::New(downstream_fragment))
            | (FragmentStatus::New(fragment), FragmentStatus::Existing(downstream_fragment))
            | (FragmentStatus::New(fragment), FragmentStatus::New(downstream_fragment))
            | (
                FragmentStatus::Changed {
                    after: fragment, ..
                },
                FragmentStatus::Changed {
                    after: downstream_fragment,
                    ..
                },
            ) => {
                let (dispatchers, upstreams, no_shuffle_map) =
                    Self::compose_edge(fragment_id, fragment, downstream, downstream_fragment);
                Self::add_no_shuffle_mapping(
                    &mut self.result,
                    fragment_id,
                    downstream.downstream_fragment_id,
                    no_shuffle_map,
                );
                Self::add_dispatchers(&mut self.result.dispatchers, fragment_id, dispatchers);
                Self::add_upstreams(
                    &mut self.result.upstreams,
                    downstream.downstream_fragment_id,
                    upstreams,
                );
            }
        }
        Ok(())
    }

    fn add_no_shuffle_mapping(
        result: &mut FragmentEdgeBuildResult,
        fragment_id: FragmentId,
        downstream_fragment_id: FragmentId,
        no_shuffle_map: Option<HashMap<ActorId, ActorId>>,
    ) {
        if let Some(no_shuffle_map) = no_shuffle_map {
            result
                .actor_new_no_shuffle
                .entry(fragment_id)
                .or_default()
                .insert(downstream_fragment_id, no_shuffle_map);
        }
    }

    fn add_dispatchers(
        fragment_dispatchers: &mut FragmentActorDispatchers,
        fragment_id: FragmentId,
        dispatchers: HashMap<ActorId, PbDispatcher>,
    ) {
        for (actor_id, dispatcher) in dispatchers {
            fragment_dispatchers
                .entry(fragment_id)
                .or_default()
                .entry(actor_id)
                .or_default()
                .push(dispatcher);
        }
    }

    fn add_upstreams(
        fragment_upstreams: &mut HashMap<FragmentId, HashMap<ActorId, ActorUpstreams>>,
        fragment_id: FragmentId,
        upstreams: HashMap<ActorId, ActorUpstreams>,
    ) {
        let target = fragment_upstreams.entry(fragment_id).or_default();
        for (actor_id, actor_upstreams) in upstreams {
            target.entry(actor_id).or_default().extend(actor_upstreams);
        }
    }

    fn add_dispatcher_updates(
        dispatcher_updates: &mut Vec<DispatcherUpdate>,
        dispatchers: HashMap<ActorId, PbDispatcher>,
        removed_downstream_actor_id: impl IntoIterator<Item = ActorId>,
    ) {
        let removed_downstream_actor_id: Vec<_> = removed_downstream_actor_id.into_iter().collect();
        for (actor_id, dispatcher) in dispatchers {
            dispatcher_updates.push(DispatcherUpdate {
                actor_id,
                dispatcher_id: dispatcher.dispatcher_id,
                hash_mapping: dispatcher.hash_mapping,
                added_downstream_actor_id: dispatcher.downstream_actor_id,
                removed_downstream_actor_id: removed_downstream_actor_id.clone(),
            });
        }
    }

    fn add_merge_updates(
        merge_updates: &mut HashMap<FragmentId, Vec<MergeUpdate>>,
        fragment_id: FragmentId,
        downstream_fragment_id: FragmentId,
        downstream_fragment: &EdgeBuilderFragmentInfo,
        upstreams: HashMap<ActorId, ActorUpstreams>,
        removed_upstream_actor_id: impl IntoIterator<Item = ActorId>,
    ) {
        let removed_upstream_actor_id: Vec<_> = removed_upstream_actor_id.into_iter().collect();
        let fragment_merge_updates = merge_updates.entry(downstream_fragment_id).or_default();
        for actor_id in downstream_fragment.actors.keys() {
            let added_upstream_actors = upstreams
                .get(actor_id)
                .and_then(|upstreams| upstreams.get(&fragment_id))
                .into_iter()
                .flat_map(|upstreams| upstreams.values().cloned())
                .collect();
            fragment_merge_updates.push(MergeUpdate {
                actor_id: *actor_id,
                upstream_fragment_id: fragment_id,
                new_upstream_fragment_id: None,
                added_upstream_actors,
                removed_upstream_actor_id: removed_upstream_actor_id.clone(),
            });
        }
    }

    fn compose_edge_dispatchers(
        fragment: &EdgeBuilderFragmentInfo,
        downstream: &DownstreamFragmentRelation,
        downstream_fragment: &EdgeBuilderFragmentInfo,
    ) -> (
        HashMap<ActorId, PbDispatcher>,
        Option<HashMap<ActorId, ActorId>>,
    ) {
        compose_dispatchers(
            fragment.distribution_type,
            &fragment.actors,
            downstream.downstream_fragment_id,
            downstream_fragment.distribution_type,
            &downstream_fragment.actors,
            downstream.dispatcher_type,
            downstream.dist_key_indices.clone(),
            downstream.output_mapping.clone(),
        )
    }

    fn compose_edge(
        fragment_id: FragmentId,
        fragment: &EdgeBuilderFragmentInfo,
        downstream: &DownstreamFragmentRelation,
        downstream_fragment: &EdgeBuilderFragmentInfo,
    ) -> ComposedEdge {
        let (dispatchers, no_shuffle_map) =
            Self::compose_edge_dispatchers(fragment, downstream, downstream_fragment);
        let mut upstreams: HashMap<ActorId, ActorUpstreams> = HashMap::new();
        for (&actor_id, dispatcher) in &dispatchers {
            let actor_location = &fragment.actor_location[&actor_id];
            for &downstream_actor in &dispatcher.downstream_actor_id {
                upstreams
                    .entry(downstream_actor)
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
        }
        (dispatchers, upstreams, no_shuffle_map)
    }

    pub(crate) fn replace_upstream(
        mut self,
        fragment_id: FragmentId,
        original_upstream_fragment_id: FragmentId,
        new_upstream_fragment_id: FragmentId,
    ) -> Self {
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
        self
    }

    /// Finalize the builder and return the generated edges and mutation-time deltas.
    pub(crate) fn build(self) -> FragmentEdgeBuildResult {
        self.result
    }
}

#[cfg(test)]
mod tests {
    use risingwave_meta_model::DispatcherType;
    use risingwave_pb::stream_plan::PbDispatchOutputMapping;

    use super::*;

    #[derive(Clone, Copy, Debug)]
    enum TestStatus {
        Existing,
        New,
        Changed,
    }

    fn actor(id: u32) -> ActorId {
        ActorId::new(id)
    }

    fn fragment(id: u32) -> FragmentId {
        FragmentId::new(id)
    }

    fn edge_info(
        distribution_type: DistributionType,
        actors: impl IntoIterator<Item = (u32, Option<Bitmap>)>,
    ) -> EdgeBuilderFragmentInfo {
        let actors: HashMap<_, _> = actors
            .into_iter()
            .map(|(id, bitmap)| (actor(id), bitmap))
            .collect();
        let actor_location = actors
            .keys()
            .map(|actor_id| {
                (
                    *actor_id,
                    HostAddress {
                        host: format!("actor-{actor_id}"),
                        port: 1234,
                    },
                )
            })
            .collect();
        EdgeBuilderFragmentInfo {
            distribution_type,
            actors,
            actor_location,
            partial_graph_id: PartialGraphId::new(1),
        }
    }

    fn single_info(actor_id: u32) -> EdgeBuilderFragmentInfo {
        edge_info(DistributionType::Single, [(actor_id, None)])
    }

    fn relation(target: FragmentId, dispatcher_type: DispatcherType) -> DownstreamFragmentRelation {
        DownstreamFragmentRelation {
            downstream_fragment_id: target,
            dispatcher_type,
            dist_key_indices: vec![],
            output_mapping: PbDispatchOutputMapping::default(),
        }
    }

    fn build_status_pair(
        source: TestStatus,
        target: TestStatus,
    ) -> MetaResult<FragmentEdgeBuildResult> {
        let source_fragment = fragment(1);
        let target_fragment = fragment(2);
        let existing = [(source_fragment, source, 1), (target_fragment, target, 11)]
            .into_iter()
            .filter(|(_, status, _)| !matches!(status, TestStatus::New))
            .map(|(fragment_id, _, actor_id)| (fragment_id, single_info(actor_id)));
        let mut builder = FragmentEdgeBuilder::new();
        builder.add_existing_fragment_infos(existing);
        for (fragment_id, status, actor_id) in
            [(source_fragment, source, 2), (target_fragment, target, 12)]
        {
            match status {
                TestStatus::Existing => {}
                TestStatus::New => {
                    builder.add_new_fragment_infos([(fragment_id, single_info(actor_id))]);
                }
                TestStatus::Changed => {
                    builder.replace_existing_fragment_actor_infos([(
                        fragment_id,
                        single_info(actor_id),
                    )]);
                }
            }
        }
        Ok(builder
            .finish_fragments()
            .add_relations(&HashMap::from([(
                source_fragment,
                vec![relation(target_fragment, DispatcherType::Broadcast)],
            )]))?
            .build())
    }

    #[test]
    fn test_endpoint_status_matrix() {
        for source in [TestStatus::Existing, TestStatus::New, TestStatus::Changed] {
            for target in [TestStatus::Existing, TestStatus::New, TestStatus::Changed] {
                let result = build_status_pair(source, target);
                if matches!(
                    (source, target),
                    (TestStatus::New, TestStatus::Changed) | (TestStatus::Changed, TestStatus::New)
                ) {
                    assert!(result.is_err(), "source={source:?}, target={target:?}");
                    continue;
                }
                let result = result.unwrap();
                assert_eq!(
                    result.dispatchers.contains_key(&fragment(1)),
                    matches!(source, TestStatus::New | TestStatus::Changed)
                        || matches!((source, target), (TestStatus::Existing, TestStatus::New)),
                    "source={source:?}, target={target:?}"
                );
                assert_eq!(
                    result.upstreams.contains_key(&fragment(2)),
                    matches!(target, TestStatus::New | TestStatus::Changed)
                        || matches!((source, target), (TestStatus::New, TestStatus::Existing)),
                    "source={source:?}, target={target:?}"
                );
                assert_eq!(
                    !result.dispatcher_updates.is_empty(),
                    matches!(
                        (source, target),
                        (TestStatus::Existing, TestStatus::Changed)
                    ),
                    "source={source:?}, target={target:?}"
                );
                assert_eq!(
                    result
                        .merge_updates
                        .values()
                        .any(|updates| !updates.is_empty()),
                    matches!(
                        (source, target),
                        (TestStatus::Changed, TestStatus::Existing)
                    ),
                    "source={source:?}, target={target:?}"
                );
            }
        }
    }

    #[test]
    fn test_hash_and_broadcast_dispatcher_updates() {
        for dispatcher_type in [DispatcherType::Hash, DispatcherType::Broadcast] {
            let source = fragment(1);
            let target = fragment(2);
            let bitmap = || Some(Bitmap::from_iter([true, true]));
            let mut builder = FragmentEdgeBuilder::new();
            builder.add_existing_fragment_infos([
                (source, edge_info(DistributionType::Single, [(1, None)])),
                (target, edge_info(DistributionType::Hash, [(11, bitmap())])),
            ]);
            builder.replace_existing_fragment_actor_infos([(
                target,
                edge_info(DistributionType::Hash, [(12, bitmap())]),
            )]);
            let result = builder
                .finish_fragments()
                .add_edge(source, &relation(target, dispatcher_type))
                .unwrap()
                .build();

            let update = &result.dispatcher_updates[0];
            assert_eq!(update.added_downstream_actor_id, vec![actor(12)]);
            assert_eq!(update.removed_downstream_actor_id, vec![actor(11)]);
            assert_eq!(
                update.hash_mapping.is_some(),
                dispatcher_type == DispatcherType::Hash
            );
        }
    }

    #[test]
    fn test_no_shuffle_changed_fragments() {
        let source = fragment(1);
        let target = fragment(2);
        let mut builder = FragmentEdgeBuilder::new();
        builder.add_existing_fragment_infos([(source, single_info(1)), (target, single_info(11))]);
        builder.replace_existing_fragment_actor_infos([
            (source, single_info(2)),
            (target, single_info(12)),
        ]);
        let result = builder
            .finish_fragments()
            .add_edge(source, &relation(target, DispatcherType::NoShuffle))
            .unwrap()
            .build();

        assert!(result.dispatcher_updates.is_empty());
        assert!(result.merge_updates.is_empty());
        assert_eq!(
            result.actor_new_no_shuffle[&source][&target][&actor(2)],
            actor(12)
        );
    }

    #[test]
    fn test_no_shuffle_updates_remove_before_actor_superset() {
        let source = fragment(1);
        let target = fragment(2);
        let actors = |left, right| {
            [
                (left, Some(Bitmap::from_iter([true, false]))),
                (right, Some(Bitmap::from_iter([false, true]))),
            ]
        };
        let relations =
            HashMap::from([(source, vec![relation(target, DispatcherType::NoShuffle)])]);

        let mut builder = FragmentEdgeBuilder::new();
        builder.add_existing_fragment_infos([
            (source, edge_info(DistributionType::Hash, actors(1, 2))),
            (target, edge_info(DistributionType::Hash, actors(11, 12))),
        ]);
        builder.replace_existing_fragment_actor_infos([(
            target,
            edge_info(DistributionType::Hash, actors(13, 14)),
        )]);
        let result = builder
            .finish_fragments()
            .add_relations(&relations)
            .unwrap()
            .build();

        assert_eq!(result.dispatcher_updates.len(), 2);
        for update in result.dispatcher_updates {
            assert_eq!(update.added_downstream_actor_id.len(), 1);
            let mut removed = update.removed_downstream_actor_id;
            removed.sort_unstable();
            assert_eq!(removed, vec![actor(11), actor(12)]);
        }

        let mut builder = FragmentEdgeBuilder::new();
        builder.add_existing_fragment_infos([
            (source, edge_info(DistributionType::Hash, actors(1, 2))),
            (target, edge_info(DistributionType::Hash, actors(11, 12))),
        ]);
        builder.replace_existing_fragment_actor_infos([(
            source,
            edge_info(DistributionType::Hash, actors(3, 4)),
        )]);
        let result = builder
            .finish_fragments()
            .add_relations(&relations)
            .unwrap()
            .build();

        let merge_updates = &result.merge_updates[&target];
        assert_eq!(merge_updates.len(), 2);
        for update in merge_updates {
            assert_eq!(update.added_upstream_actors.len(), 1);
            let mut removed = update.removed_upstream_actor_id.clone();
            removed.sort_unstable();
            assert_eq!(removed, vec![actor(1), actor(2)]);
        }
    }

    #[test]
    fn test_replace_upstream() {
        let old_source = fragment(1);
        let new_source = fragment(2);
        let target = fragment(3);
        let mut builder = FragmentEdgeBuilder::new();
        builder.add_existing_fragment_infos([(target, single_info(11))]);
        builder.add_new_fragment_infos([(new_source, single_info(2))]);
        let result = builder
            .finish_fragments()
            .add_edge(new_source, &relation(target, DispatcherType::Broadcast))
            .unwrap()
            .replace_upstream(target, old_source, new_source)
            .build();

        let update = &result.merge_updates[&target][0];
        assert_eq!(update.upstream_fragment_id, old_source);
        assert_eq!(update.new_upstream_fragment_id, Some(new_source));
        assert_eq!(update.added_upstream_actors[0].actor_id, actor(2));
    }

    #[test]
    fn test_new_to_existing_upstreams_are_allowed_in_add_mutation() {
        let source = fragment(1);
        let target = fragment(2);
        let mut builder = FragmentEdgeBuilder::new();
        builder.add_existing_fragment_infos([(target, single_info(11))]);
        builder.add_new_fragment_infos([(source, single_info(1))]);
        let mut result = builder
            .finish_fragments()
            .add_edge(source, &relation(target, DispatcherType::Broadcast))
            .unwrap()
            .build();
        let source_actor = StreamActor {
            actor_id: actor(1),
            fragment_id: source,
            vnode_bitmap: None,
            mview_definition: Default::default(),
            expr_context: None,
            config_override: Default::default(),
        };
        let node = StreamNode::default();
        let worker_id: WorkerId = 1.into();
        let actors_to_create = result.collect_actors_to_create(std::iter::once((
            source,
            &node,
            std::iter::once((&source_actor, worker_id)),
            [],
        )));

        assert_eq!(actors_to_create[&worker_id][&source].1[0].2.len(), 1);
        result.apply_to_add_mutation(&mut AddMutation::default());
    }

    #[test]
    fn test_attach_new_relation_dispatcher_to_add_mutation() {
        let source = fragment(1);
        let target = fragment(2);
        let mut builder = FragmentEdgeBuilder::new();
        builder.add_existing_fragment_infos([(source, single_info(1))]);
        builder.add_new_fragment_infos([(target, single_info(11))]);
        let mut result = builder
            .finish_fragments()
            .add_edge(source, &relation(target, DispatcherType::Broadcast))
            .unwrap()
            .build();
        let target_actor = StreamActor {
            actor_id: actor(11),
            fragment_id: target,
            vnode_bitmap: None,
            mview_definition: Default::default(),
            expr_context: None,
            config_override: Default::default(),
        };
        let node = StreamNode::default();
        result.collect_actors_to_create(std::iter::once((
            target,
            &node,
            std::iter::once((&target_actor, 1.into())),
            [],
        )));
        let mut mutation = AddMutation::default();

        result.apply_to_add_mutation(&mut mutation);

        assert_eq!(mutation.actor_dispatchers[&actor(1)].dispatchers.len(), 1);
        assert_eq!(
            mutation.actor_dispatchers[&actor(1)].dispatchers[0].downstream_actor_id,
            vec![actor(11)]
        );
    }

    #[test]
    fn test_add_relations_only_uses_registered_fragments() {
        let external_source = fragment(1);
        let source = fragment(2);
        let target = fragment(3);
        let mut builder = FragmentEdgeBuilder::new();
        builder.add_new_fragment_infos([(source, single_info(2)), (target, single_info(3))]);
        let result = builder
            .finish_fragments()
            .add_relations(&HashMap::from([
                (
                    external_source,
                    vec![relation(source, DispatcherType::Broadcast)],
                ),
                (source, vec![relation(target, DispatcherType::Broadcast)]),
            ]))
            .unwrap()
            .build();

        assert!(!result.dispatchers.contains_key(&external_source));
        assert!(!result.upstreams.contains_key(&source));
        assert!(result.dispatchers.contains_key(&source));
        assert!(result.upstreams.contains_key(&target));
    }

    #[test]
    fn test_add_edge_rejects_unregistered_upstream() {
        let source = fragment(1);
        let target = fragment(2);
        let mut builder = FragmentEdgeBuilder::new();
        builder.add_existing_fragment_infos([(target, single_info(2))]);

        let result = builder
            .finish_fragments()
            .add_edge(source, &relation(target, DispatcherType::Broadcast));

        assert!(result.is_err());
    }

    #[test]
    #[should_panic(expected = "must use entirely fresh actor ids")]
    fn test_changed_fragment_rejects_retained_actor_ids() {
        let fragment = fragment(1);
        let mut builder = FragmentEdgeBuilder::new();
        builder.add_existing_fragment_infos([(fragment, single_info(1))]);
        builder.replace_existing_fragment_actor_infos([(fragment, single_info(1))]);
    }
}
