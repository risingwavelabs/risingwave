// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_common::types::ParallelUnitId;
use risingwave_pb::common::{Buffer, ParallelUnit, ParallelUnitMapping};
use risingwave_pb::meta::table_fragments::{ActorState, ActorStatus, Fragment};
use risingwave_pb::meta::TableFragments as ProstTableFragments;
use risingwave_pb::stream_plan::source_node::SourceType;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::{FragmentType, StreamActor, StreamNode};

use super::{ActorId, FragmentId};
use crate::manager::{SourceId, WorkerId};
use crate::model::{MetadataModel, MetadataModelResult};

/// Column family name for table fragments.
const TABLE_FRAGMENTS_CF_NAME: &str = "cf/table_fragments";

/// Fragments of a materialized view
///
/// We store whole fragments in a single column family as follow:
/// `table_id` => `TableFragments`.
#[derive(Debug, Clone)]
pub struct TableFragments {
    /// The table id.
    table_id: TableId,

    /// The table fragments.
    pub(crate) fragments: BTreeMap<FragmentId, Fragment>,

    /// The status of actors
    pub(crate) actor_status: BTreeMap<ActorId, ActorStatus>,

    /// Internal TableIds from all Fragments, included the table_id itself.
    table_to_fragment_map: HashMap<u32, FragmentId>,
}

impl MetadataModel for TableFragments {
    type KeyType = u32;
    type ProstType = ProstTableFragments;

    fn cf_name() -> String {
        TABLE_FRAGMENTS_CF_NAME.to_string()
    }

    fn to_protobuf(&self) -> Self::ProstType {
        Self::ProstType {
            table_id: self.table_id.table_id(),
            fragments: self.fragments.clone().into_iter().collect(),
            actor_status: self.actor_status.clone().into_iter().collect(),
        }
    }

    fn from_protobuf(prost: Self::ProstType) -> Self {
        let table_to_fragment_map: HashMap<u32, FragmentId> = prost
            .fragments
            .values()
            .flat_map(|f| f.state_table_ids.iter().map(|&t| (t, f.fragment_id)))
            .collect();

        Self {
            table_id: TableId::new(prost.table_id),
            fragments: prost.fragments.into_iter().collect(),
            actor_status: prost.actor_status.into_iter().collect(),
            table_to_fragment_map,
        }
    }

    fn key(&self) -> MetadataModelResult<Self::KeyType> {
        Ok(self.table_id.table_id())
    }
}

impl TableFragments {
    pub fn new(table_id: TableId, fragments: BTreeMap<FragmentId, Fragment>) -> Self {
        let table_to_fragment_map: HashMap<u32, FragmentId> = fragments
            .values()
            .flat_map(|f| f.state_table_ids.iter().map(|&t| (t, f.fragment_id)))
            .collect();

        Self {
            table_id,
            fragments,
            actor_status: BTreeMap::default(),
            table_to_fragment_map,
        }
    }

    pub fn fragment_ids(&self) -> impl Iterator<Item = FragmentId> + '_ {
        self.fragments.keys().cloned()
    }

    pub fn fragments(&self) -> Vec<&Fragment> {
        self.fragments.values().collect_vec()
    }

    /// Set the actor locations.
    pub fn set_actor_status(&mut self, actor_status: BTreeMap<ActorId, ActorStatus>) {
        self.actor_status = actor_status;
    }

    /// Returns the table id.
    pub fn table_id(&self) -> TableId {
        self.table_id
    }

    /// Returns sink fragment vnode mapping.
    /// Note that: the real sink fragment is also stored as `TableFragments`, it's possible that
    /// there's no fragment with `FragmentType::Sink` exists.
    pub fn sink_vnode_mapping(&self) -> Option<ParallelUnitMapping> {
        self.fragments
            .values()
            .find(|fragment| fragment.fragment_type == FragmentType::Sink as i32)
            .and_then(|fragment| fragment.vnode_mapping.clone())
    }

    /// Update state of all actors
    pub fn update_actors_state(&mut self, state: ActorState) {
        for actor_status in self.actor_status.values_mut() {
            actor_status.set_state(state);
        }
    }

    /// Returns actor ids associated with this table.
    pub fn actor_ids(&self) -> Vec<ActorId> {
        self.fragments
            .values()
            .flat_map(|fragment| fragment.actors.iter().map(|actor| actor.actor_id))
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
    fn filter_actor_ids(&self, fragment_type: FragmentType) -> Vec<ActorId> {
        self.fragments
            .values()
            .filter(|fragment| fragment.fragment_type == fragment_type as i32)
            .flat_map(|fragment| fragment.actors.iter().map(|actor| actor.actor_id))
            .collect()
    }

    /// Returns source actor ids.
    pub fn source_actor_ids(&self) -> Vec<ActorId> {
        Self::filter_actor_ids(self, FragmentType::Source)
    }

    /// Returns sink actor ids.
    pub fn sink_actor_ids(&self) -> Vec<ActorId> {
        Self::filter_actor_ids(self, FragmentType::Sink)
    }

    fn contains_chain(stream_node: &StreamNode) -> bool {
        if let Some(NodeBody::Chain(_)) = stream_node.node_body {
            return true;
        }

        for child in &stream_node.input {
            if Self::contains_chain(child) {
                return true;
            }
        }

        false
    }

    pub fn fetch_parallel_unit_by_actor(&self, actor_id: &ActorId) -> Option<ParallelUnit> {
        if let Some(status) = self.actor_status.get(actor_id) {
            status.parallel_unit.clone()
        } else {
            None
        }
    }

    pub fn fetch_stream_source_id(stream_node: &StreamNode) -> Option<SourceId> {
        if let Some(NodeBody::Source(s)) = stream_node.node_body.as_ref() {
            if s.source_type == SourceType::Source as i32 {
                return Some(s.source_id);
            }
        }

        for child in &stream_node.input {
            if let Some(source_id) = Self::fetch_stream_source_id(child) {
                return Some(source_id);
            }
        }

        None
    }

    /// Returns actors that contains Chain node.
    pub fn chain_actor_ids(&self) -> Vec<ActorId> {
        self.fragments
            .values()
            .flat_map(|fragment| {
                fragment
                    .actors
                    .iter()
                    .filter(|actor| Self::contains_chain(actor.nodes.as_ref().unwrap()))
                    .map(|actor| actor.actor_id)
            })
            .collect()
    }

    /// Resolve dependent table
    fn resolve_dependent_table(stream_node: &StreamNode, table_ids: &mut HashSet<TableId>) {
        if let Some(NodeBody::Chain(chain)) = stream_node.node_body.as_ref() {
            table_ids.insert(TableId::new(chain.table_id));
        }

        for child in &stream_node.input {
            Self::resolve_dependent_table(child, table_ids);
        }
    }

    /// Returns dependent table ids.
    pub fn dependent_table_ids(&self) -> HashSet<TableId> {
        let mut table_ids = HashSet::new();
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

    pub fn update_vnode_mapping(&mut self, migrate_map: &HashMap<ParallelUnitId, ParallelUnit>) {
        for fragment in self.fragments.values_mut() {
            if fragment.vnode_mapping.is_some() {
                if let Some(ref mut mapping) = fragment.vnode_mapping {
                    mapping.data.iter_mut().for_each(|id| {
                        if migrate_map.contains_key(id) {
                            *id = migrate_map.get(id).unwrap().id;
                        }
                    });
                }
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

    pub fn node_source_actor_states(&self) -> BTreeMap<WorkerId, Vec<(ActorId, ActorState)>> {
        let mut map = BTreeMap::default();
        let source_actor_ids = self.source_actor_ids();
        for &actor_id in &source_actor_ids {
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

    /// Returns fragment vnode mapping.
    pub fn fragment_vnode_mapping(&self, fragment_id: FragmentId) -> Option<ParallelUnitMapping> {
        if let Some(fragment) = self.fragments.get(&fragment_id) {
            fragment.vnode_mapping.clone()
        } else {
            None
        }
    }

    /// Returns sink actor vnode bitmap infos.
    pub fn sink_vnode_bitmap_info(&self) -> Vec<(ActorId, Option<Buffer>)> {
        self.fragments
            .values()
            .filter(|fragment| fragment.fragment_type == FragmentType::Sink as i32)
            .flat_map(|fragment| {
                fragment
                    .actors
                    .iter()
                    .map(|actor| (actor.actor_id, actor.vnode_bitmap.clone()))
            })
            .collect_vec()
    }

    pub fn sink_actor_parallel_units(&self) -> BTreeMap<ActorId, ParallelUnit> {
        let sink_actor_ids = self.sink_actor_ids();
        sink_actor_ids
            .iter()
            .map(|actor_id| {
                (
                    *actor_id,
                    self.actor_status[actor_id]
                        .get_parallel_unit()
                        .unwrap()
                        .clone(),
                )
            })
            .collect()
    }

    /// Generate topological order of fragments. If `index(a) < index(b)` in vec, then a is the
    /// downstream of b.
    pub fn generate_topological_order(&self) -> Vec<FragmentId> {
        let mut actionable_fragment_id = VecDeque::new();

        // If downstream_edges[x][y] exists, then there's an edge from x to y.
        let mut downstream_edges: HashMap<u32, HashSet<u32>> = HashMap::new();

        // Counts how many upstreams are there for a given fragment
        let mut upstream_cnts: HashMap<u32, usize> = HashMap::new();

        let mut result = vec![];

        let mut actor_to_fragment_mapping = HashMap::new();

        // Firstly, record actor -> fragment mapping
        for (fragment_id, fragment) in &self.fragments {
            for actor in &fragment.actors {
                let ret = actor_to_fragment_mapping.insert(actor.actor_id, *fragment_id);
                assert!(ret.is_none(), "duplicated actor id found");
            }
        }

        // Then, generate the DAG of fragments
        for (fragment_id, fragment) in &self.fragments {
            for upstream_actor in &fragment.actors {
                for dispatcher in &upstream_actor.dispatcher {
                    for downstream_actor in &dispatcher.downstream_actor_id {
                        let downstream_fragment_id =
                            actor_to_fragment_mapping.get(downstream_actor).unwrap();

                        let did_not_have = downstream_edges
                            .entry(*fragment_id)
                            .or_default()
                            .insert(*downstream_fragment_id);

                        if did_not_have {
                            *upstream_cnts.entry(*downstream_fragment_id).or_default() += 1;
                        }
                    }
                }
            }
        }

        // Find actionable fragments
        for fragment_id in self.fragments.keys() {
            if upstream_cnts.get(fragment_id).is_none() {
                actionable_fragment_id.push_back(*fragment_id);
            }
        }

        // After that, we can generate topological order
        while let Some(fragment_id) = actionable_fragment_id.pop_front() {
            result.push(fragment_id);

            // Find if we can process more fragments
            if let Some(downstreams) = downstream_edges.get(&fragment_id) {
                for downstream_id in downstreams.iter() {
                    let cnt = upstream_cnts
                        .get_mut(downstream_id)
                        .expect("the downstream should exist");

                    *cnt -= 1;
                    if *cnt == 0 {
                        upstream_cnts.remove(downstream_id);
                        actionable_fragment_id.push_back(*downstream_id);
                    }
                }
            }
        }

        if !upstream_cnts.is_empty() {
            // There are fragments that are not processed yet.
            panic!("not a DAG");
        }

        assert_eq!(result.len(), self.fragments.len());

        result
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

    /// Get the table mapping info from the fragment it belongs to.
    pub fn get_table_hash_mapping(&self, table_id: u32) -> Option<ParallelUnitMapping> {
        self.table_to_fragment_map.get(&table_id).map(|f| {
            let mut mapping = self.fragments[f].vnode_mapping.clone().unwrap();
            mapping.table_id = table_id;
            mapping
        })
    }
}
