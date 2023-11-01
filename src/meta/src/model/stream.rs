// Copyright 2023 RisingWave Labs
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

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::ops::AddAssign;

use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_common::hash::ParallelUnitId;
use risingwave_connector::source::SplitImpl;
use risingwave_pb::common::{ParallelUnit, ParallelUnitMapping};
use risingwave_pb::meta::table_fragments::actor_status::ActorState;
use risingwave_pb::meta::table_fragments::{ActorStatus, Fragment, State};
use risingwave_pb::meta::PbTableFragments;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::{
    FragmentTypeFlag, PbStreamEnvironment, StreamActor, StreamNode, StreamSource,
};

use super::{ActorId, FragmentId};
use crate::manager::{SourceId, WorkerId};
use crate::model::{MetadataModel, MetadataModelResult};
use crate::stream::{build_actor_connector_splits, build_actor_split_impls, SplitAssignment};

/// Column family name for table fragments.
const TABLE_FRAGMENTS_CF_NAME: &str = "cf/table_fragments";

/// Fragments of a streaming job.
///
/// We store whole fragments in a single column family as follow:
/// `table_id` => `TableFragments`.
#[derive(Debug, Clone)]
pub struct TableFragments {
    /// The table id.
    table_id: TableId,

    /// The state of the table fragments.
    state: State,

    /// The table fragments.
    pub fragments: BTreeMap<FragmentId, Fragment>,

    /// The status of actors
    pub actor_status: BTreeMap<ActorId, ActorStatus>,

    /// The splits of actors
    pub actor_splits: HashMap<ActorId, Vec<SplitImpl>>,

    /// The environment associated with this stream plan and its fragments
    pub env: StreamEnvironment,
}

#[derive(Debug, Clone, Default)]
pub struct StreamEnvironment {
    /// The timezone used to interpret timestamps and dates for conversion
    pub timezone: Option<String>,
}

impl StreamEnvironment {
    pub fn to_protobuf(&self) -> PbStreamEnvironment {
        PbStreamEnvironment {
            timezone: self.timezone.clone().unwrap_or("".into()),
        }
    }

    pub fn from_protobuf(prost: &PbStreamEnvironment) -> Self {
        Self {
            timezone: if prost.get_timezone().is_empty() {
                None
            } else {
                Some(prost.get_timezone().clone())
            },
        }
    }
}

impl MetadataModel for TableFragments {
    type KeyType = u32;
    type PbType = PbTableFragments;

    fn cf_name() -> String {
        TABLE_FRAGMENTS_CF_NAME.to_string()
    }

    fn to_protobuf(&self) -> Self::PbType {
        Self::PbType {
            table_id: self.table_id.table_id(),
            state: self.state as _,
            fragments: self.fragments.clone().into_iter().collect(),
            actor_status: self.actor_status.clone().into_iter().collect(),
            actor_splits: build_actor_connector_splits(&self.actor_splits),
            env: Some(self.env.to_protobuf()),
        }
    }

    fn from_protobuf(prost: Self::PbType) -> Self {
        let env = StreamEnvironment::from_protobuf(prost.get_env().unwrap());
        Self {
            table_id: TableId::new(prost.table_id),
            state: prost.state(),
            fragments: prost.fragments.into_iter().collect(),
            actor_status: prost.actor_status.into_iter().collect(),
            actor_splits: build_actor_split_impls(&prost.actor_splits),
            env,
        }
    }

    fn key(&self) -> MetadataModelResult<Self::KeyType> {
        Ok(self.table_id.table_id())
    }
}

impl TableFragments {
    /// Create a new `TableFragments` with state of `Initial`, with other fields empty.
    pub fn for_test(table_id: TableId, fragments: BTreeMap<FragmentId, Fragment>) -> Self {
        Self::new(
            table_id,
            fragments,
            &BTreeMap::new(),
            StreamEnvironment::default(),
        )
    }

    /// Create a new `TableFragments` with state of `Initial`, with the status of actors set to
    /// `Inactive` on the given parallel units.
    pub fn new(
        table_id: TableId,
        fragments: BTreeMap<FragmentId, Fragment>,
        actor_locations: &BTreeMap<ActorId, ParallelUnit>,
        env: StreamEnvironment,
    ) -> Self {
        let actor_status = actor_locations
            .iter()
            .map(|(&actor_id, parallel_unit)| {
                (
                    actor_id,
                    ActorStatus {
                        parallel_unit: Some(parallel_unit.clone()),
                        state: ActorState::Inactive as i32,
                    },
                )
            })
            .collect();

        Self {
            table_id,
            state: State::Initial,
            fragments,
            actor_status,
            actor_splits: HashMap::default(),
            env,
        }
    }

    pub fn fragment_ids(&self) -> impl Iterator<Item = FragmentId> + '_ {
        self.fragments.keys().cloned()
    }

    pub fn fragments(&self) -> Vec<&Fragment> {
        self.fragments.values().collect_vec()
    }

    /// Returns the table id.
    pub fn table_id(&self) -> TableId {
        self.table_id
    }

    /// Returns the state of the table fragments.
    pub fn state(&self) -> State {
        self.state
    }

    /// Returns the timezone of the table
    pub fn timezone(&self) -> Option<String> {
        self.env.timezone.clone()
    }

    /// Returns whether the table fragments is in `Created` state.
    pub fn is_created(&self) -> bool {
        self.state == State::Created
    }

    /// Set the table ID.
    // TODO: remove this workaround for replacing table.
    pub fn set_table_id(&mut self, table_id: TableId) {
        self.table_id = table_id;
    }

    /// Set the state of the table fragments.
    pub fn set_state(&mut self, state: State) {
        self.state = state;
    }

    /// Returns mview fragment vnode mapping.
    /// Note that: the sink fragment is also stored as `TableFragments`, it's possible that
    /// there's no fragment with `FragmentTypeFlag::Mview` exists.
    pub fn mview_vnode_mapping(&self) -> Option<(FragmentId, ParallelUnitMapping)> {
        self.fragments
            .values()
            .find(|fragment| {
                (fragment.get_fragment_type_mask() & FragmentTypeFlag::Mview as u32) != 0
            })
            .map(|fragment| {
                (
                    fragment.fragment_id,
                    // vnode mapping is always `Some`, even for singletons.
                    fragment.vnode_mapping.clone().unwrap(),
                )
            })
    }

    /// Update state of all actors
    pub fn update_actors_state(&mut self, state: ActorState) {
        for actor_status in self.actor_status.values_mut() {
            actor_status.set_state(state);
        }
    }

    pub fn set_actor_splits_by_split_assignment(&mut self, split_assignment: SplitAssignment) {
        self.actor_splits = split_assignment.into_values().flatten().collect();
    }

    /// Returns actor ids associated with this table.
    pub fn actor_ids(&self) -> Vec<ActorId> {
        self.fragments
            .values()
            .flat_map(|fragment| fragment.actors.iter().map(|actor| actor.actor_id))
            .collect()
    }

    pub fn actor_fragment_mapping(&self) -> HashMap<ActorId, FragmentId> {
        self.fragments
            .values()
            .flat_map(|fragment| {
                fragment
                    .actors
                    .iter()
                    .map(|actor| (actor.actor_id, fragment.fragment_id))
            })
            .collect()
    }

    /// Returns actors associated with this table.
    pub fn actors(&self) -> Vec<StreamActor> {
        self.fragments
            .values()
            .flat_map(|fragment| fragment.actors.clone())
            .collect()
    }

    /// Returns the actor ids with the given fragment type.
    fn filter_actor_ids(&self, check_type: impl Fn(u32) -> bool) -> Vec<ActorId> {
        self.fragments
            .values()
            .filter(|fragment| check_type(fragment.get_fragment_type_mask()))
            .flat_map(|fragment| fragment.actors.iter().map(|actor| actor.actor_id))
            .collect()
    }

    /// Returns barrier inject actor ids.
    pub fn barrier_inject_actor_ids(&self) -> Vec<ActorId> {
        Self::filter_actor_ids(self, |fragment_type_mask| {
            (fragment_type_mask
                & (FragmentTypeFlag::Source as u32
                    | FragmentTypeFlag::Now as u32
                    | FragmentTypeFlag::Values as u32
                    | FragmentTypeFlag::BarrierRecv as u32))
                != 0
        })
    }

    /// Returns mview actor ids.
    pub fn mview_actor_ids(&self) -> Vec<ActorId> {
        Self::filter_actor_ids(self, |fragment_type_mask| {
            (fragment_type_mask & FragmentTypeFlag::Mview as u32) != 0
        })
    }

    /// Returns values actor ids.
    pub fn values_actor_ids(&self) -> Vec<ActorId> {
        Self::filter_actor_ids(self, |fragment_type_mask| {
            (fragment_type_mask & FragmentTypeFlag::Values as u32) != 0
        })
    }

    /// Returns the fragment with the `Mview` type flag.
    pub fn mview_fragment(&self) -> Option<Fragment> {
        self.fragments
            .values()
            .find(|fragment| {
                (fragment.get_fragment_type_mask() & FragmentTypeFlag::Mview as u32) != 0
            })
            .cloned()
    }

    pub fn source_fragment(&self) -> Option<Fragment> {
        self.fragments
            .values()
            .find(|fragment| {
                (fragment.get_fragment_type_mask() & FragmentTypeFlag::Source as u32) != 0
            })
            .cloned()
    }

    /// Returns actors that contains Chain node.
    pub fn chain_actor_ids(&self) -> HashSet<ActorId> {
        Self::filter_actor_ids(self, |fragment_type_mask| {
            (fragment_type_mask & FragmentTypeFlag::ChainNode as u32) != 0
        })
        .into_iter()
        .collect()
    }

    /// Find the external stream source info inside the stream node, if any.
    pub fn find_stream_source(stream_node: &StreamNode) -> Option<&StreamSource> {
        if let Some(NodeBody::Source(source)) = stream_node.node_body.as_ref() {
            if let Some(inner) = &source.source_inner {
                return Some(inner);
            }
        }

        for child in &stream_node.input {
            if let Some(source) = Self::find_stream_source(child) {
                return Some(source);
            }
        }

        None
    }

    /// Extract the fragments that include source executors that contains an external stream source,
    /// grouping by source id.
    pub fn stream_source_fragments(&self) -> HashMap<SourceId, BTreeSet<FragmentId>> {
        let mut source_fragments = HashMap::new();

        for fragment in self.fragments() {
            for actor in &fragment.actors {
                if let Some(source_id) =
                    TableFragments::find_stream_source(actor.nodes.as_ref().unwrap())
                        .map(|s| s.source_id)
                {
                    source_fragments
                        .entry(source_id)
                        .or_insert(BTreeSet::new())
                        .insert(fragment.fragment_id as FragmentId);

                    break;
                }
            }
        }
        source_fragments
    }

    /// Resolve dependent table
    fn resolve_dependent_table(stream_node: &StreamNode, table_ids: &mut HashMap<TableId, usize>) {
        if let Some(NodeBody::Chain(chain)) = stream_node.node_body.as_ref() {
            table_ids
                .entry(TableId::new(chain.table_id))
                .or_default()
                .add_assign(1);
        }

        for child in &stream_node.input {
            Self::resolve_dependent_table(child, table_ids);
        }
    }

    /// Returns a mapping of dependent table ids of the `TableFragments`
    /// to their corresponding count.
    pub fn dependent_table_ids(&self) -> HashMap<TableId, usize> {
        let mut table_ids = HashMap::new();
        self.fragments.values().for_each(|fragment| {
            let actor = &fragment.actors[0];
            Self::resolve_dependent_table(actor.nodes.as_ref().unwrap(), &mut table_ids);
        });

        table_ids
    }

    /// Returns states of actors group by worker id.
    pub fn worker_actor_states(&self) -> BTreeMap<WorkerId, Vec<(ActorId, ActorState)>> {
        let mut map = BTreeMap::default();
        for (&actor_id, actor_status) in &self.actor_status {
            let node_id = actor_status.get_parallel_unit().unwrap().worker_node_id as WorkerId;
            map.entry(node_id)
                .or_insert_with(Vec::new)
                .push((actor_id, actor_status.state()));
        }
        map
    }

    /// Returns actor locations group by worker id.
    pub fn worker_actor_ids(&self) -> BTreeMap<WorkerId, Vec<ActorId>> {
        let mut map = BTreeMap::default();
        for (&actor_id, actor_status) in &self.actor_status {
            let node_id = actor_status.get_parallel_unit().unwrap().worker_node_id as WorkerId;
            map.entry(node_id).or_insert_with(Vec::new).push(actor_id);
        }
        map
    }

    pub fn worker_parallel_units(&self) -> HashMap<WorkerId, HashSet<ParallelUnitId>> {
        let mut map = HashMap::new();
        for actor_status in self.actor_status.values() {
            map.entry(actor_status.get_parallel_unit().unwrap().worker_node_id)
                .or_insert_with(HashSet::new)
                .insert(actor_status.get_parallel_unit().unwrap().id);
        }
        map
    }

    pub fn update_vnode_mapping(&mut self, migrate_map: &HashMap<ParallelUnitId, ParallelUnit>) {
        for fragment in self.fragments.values_mut() {
            if let Some(mapping) = &mut fragment.vnode_mapping {
                mapping.data.iter_mut().for_each(|id| {
                    if migrate_map.contains_key(id) {
                        *id = migrate_map.get(id).unwrap().id;
                    }
                });
            }
        }
    }

    /// Returns the status of actors group by worker id.
    pub fn worker_actors(&self, include_inactive: bool) -> BTreeMap<WorkerId, Vec<StreamActor>> {
        let mut actors = BTreeMap::default();
        for fragment in self.fragments.values() {
            for actor in &fragment.actors {
                let node_id = self.actor_status[&actor.actor_id]
                    .get_parallel_unit()
                    .unwrap()
                    .worker_node_id as WorkerId;
                if !include_inactive
                    && self.actor_status[&actor.actor_id].state == ActorState::Inactive as i32
                {
                    continue;
                }
                actors
                    .entry(node_id)
                    .or_insert_with(Vec::new)
                    .push(actor.clone());
            }
        }
        actors
    }

    pub fn worker_barrier_inject_actor_states(
        &self,
    ) -> BTreeMap<WorkerId, Vec<(ActorId, ActorState)>> {
        let mut map = BTreeMap::default();
        let barrier_inject_actor_ids = self.barrier_inject_actor_ids();
        for &actor_id in &barrier_inject_actor_ids {
            let actor_status = &self.actor_status[&actor_id];
            map.entry(actor_status.get_parallel_unit().unwrap().worker_node_id as WorkerId)
                .or_insert_with(Vec::new)
                .push((actor_id, actor_status.state()));
        }
        map
    }

    /// Returns actor map: `actor_id` => `StreamActor`.
    pub fn actor_map(&self) -> HashMap<ActorId, StreamActor> {
        let mut actor_map = HashMap::default();
        self.fragments.values().for_each(|fragment| {
            fragment.actors.iter().for_each(|actor| {
                actor_map.insert(actor.actor_id, actor.clone());
            });
        });
        actor_map
    }

    /// Returns the internal table ids without the mview table.
    pub fn internal_table_ids(&self) -> Vec<u32> {
        self.fragments
            .values()
            .flat_map(|f| f.state_table_ids.clone())
            .filter(|&t| t != self.table_id.table_id)
            .collect_vec()
    }

    /// Returns all internal table ids including the mview table.
    pub fn all_table_ids(&self) -> impl Iterator<Item = u32> + '_ {
        self.fragments
            .values()
            .flat_map(|f| f.state_table_ids.clone())
    }
}
