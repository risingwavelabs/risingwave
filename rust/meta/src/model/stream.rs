use std::collections::{BTreeMap, HashMap};

use risingwave_common::catalog::TableId;
use risingwave_common::error::Result;
use risingwave_pb::meta::table_fragments::fragment::FragmentType;
use risingwave_pb::meta::table_fragments::{ActorState, ActorStatus, Fragment};
use risingwave_pb::meta::TableFragments as ProstTableFragments;
use risingwave_pb::plan::TableRefId;
use risingwave_pb::stream_plan::StreamActor;

use super::{ActorId, FragmentId};
use crate::cluster::NodeId;
use crate::model::MetadataModel;

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
    fragments: BTreeMap<FragmentId, Fragment>,
    /// The status of actors
    actor_status: BTreeMap<ActorId, ActorStatus>,
}

impl MetadataModel for TableFragments {
    type ProstType = ProstTableFragments;
    type KeyType = TableRefId;

    fn cf_name() -> String {
        TABLE_FRAGMENTS_CF_NAME.to_string()
    }

    fn to_protobuf(&self) -> Self::ProstType {
        Self::ProstType {
            table_ref_id: Some(TableRefId::from(&self.table_id)),
            fragments: self.fragments.clone().into_iter().collect(),
            actor_status: self.actor_status.clone().into_iter().collect(),
        }
    }

    fn from_protobuf(prost: Self::ProstType) -> Self {
        Self {
            table_id: TableId::from(&prost.table_ref_id),
            fragments: prost.fragments.into_iter().collect(),
            actor_status: prost.actor_status.into_iter().collect(),
        }
    }

    fn key(&self) -> Result<Self::KeyType> {
        Ok(Self::KeyType::from(&self.table_id))
    }
}

impl TableFragments {
    pub fn new(table_id: TableId, fragments: BTreeMap<FragmentId, Fragment>) -> Self {
        Self {
            table_id,
            fragments,
            actor_status: BTreeMap::default(),
        }
    }

    /// Set the actor locations.
    pub fn set_actor_status(&mut self, actor_status: BTreeMap<ActorId, ActorStatus>) {
        self.actor_status = actor_status;
    }

    /// Returns the table id.
    pub fn table_id(&self) -> TableId {
        self.table_id
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

    /// Returns status of actors group by node id.
    pub fn node_actors_status(&self) -> BTreeMap<NodeId, Vec<(ActorId, ActorState)>> {
        let mut map = BTreeMap::default();
        for (&actor_id, actor_status) in &self.actor_status {
            let node_id = actor_status.node_id as NodeId;
            map.entry(node_id)
                .or_insert_with(Vec::new)
                .push((actor_id, actor_status.state()));
        }
        map
    }

    /// Returns actor locations group by node id.
    pub fn node_actor_ids(&self) -> BTreeMap<NodeId, Vec<ActorId>> {
        let mut map = BTreeMap::default();
        for (&actor_id, actor_status) in &self.actor_status {
            let node_id = actor_status.node_id as NodeId;
            map.entry(node_id).or_insert_with(Vec::new).push(actor_id);
        }
        map
    }

    /// Returns the status of actors group by node id.
    pub fn node_actors(&self) -> BTreeMap<NodeId, Vec<StreamActor>> {
        let mut actors = BTreeMap::default();
        for fragment in self.fragments.values() {
            for actor in &fragment.actors {
                let node_id = self.actor_status[&actor.actor_id].node_id as NodeId;
                actors
                    .entry(node_id)
                    .or_insert_with(Vec::new)
                    .push(actor.clone());
            }
        }
        actors
    }

    pub fn node_source_actors_status(&self) -> BTreeMap<NodeId, Vec<(ActorId, ActorState)>> {
        let mut map = BTreeMap::default();
        let source_actor_ids = self.source_actor_ids();
        for &actor_id in &source_actor_ids {
            let actor_status = &self.actor_status[&actor_id];
            map.entry(actor_status.node_id as NodeId)
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
}
