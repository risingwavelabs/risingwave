use std::collections::{BTreeMap, HashMap};

use risingwave_common::catalog::TableId;
use risingwave_common::error::Result;
use risingwave_pb::meta::table_fragments::fragment::FragmentType;
use risingwave_pb::meta::table_fragments::{Fragment, State};
use risingwave_pb::meta::TableFragments as ProstTableFragments;
use risingwave_pb::plan::TableRefId;
use risingwave_pb::stream_plan::StreamActor;

use crate::model::MetadataModel;
use crate::storage::MetaStoreRef;

/// Column family name for table fragments.
const TABLE_FRAGMENTS_CF_NAME: &str = "cf/table_fragments";

/// We store whole fragments in a single column family as follow:
/// `table_id` => `TableFragments`.
pub struct TableFragments {
    /// The table id.
    table_id: TableId,
    /// The table state.
    state: State,
    /// The table fragments.
    fragments: BTreeMap<u32, Fragment>,
}

/// TODO: We also need to store `actor_id` -> `node_id` here.
pub struct ActorLocation(u32, u32);

impl MetadataModel for TableFragments {
    type ProstType = ProstTableFragments;
    type KeyType = TableRefId;

    fn cf_name() -> String {
        TABLE_FRAGMENTS_CF_NAME.to_string()
    }

    fn to_protobuf(&self) -> Self::ProstType {
        Self::ProstType {
            table_ref_id: Some(TableRefId::from(&self.table_id)),
            state: self.state as i32,
            fragments: self
                .fragments
                .clone()
                .into_iter()
                .collect::<HashMap<_, _>>(),
        }
    }

    fn from_protobuf(prost: Self::ProstType) -> Self {
        Self {
            table_id: TableId::from(&prost.table_ref_id),
            state: State::from_i32(prost.state).unwrap(),
            fragments: prost.fragments.into_iter().collect::<BTreeMap<_, _>>(),
        }
    }

    fn key(&self) -> Result<Self::KeyType> {
        Ok(Self::KeyType::from(&self.table_id))
    }
}

impl TableFragments {
    pub fn new(table_id: TableId, fragments: BTreeMap<u32, Fragment>) -> Self {
        Self {
            table_id,
            state: State::Creating,
            fragments,
        }
    }

    /// Returns the table id.
    pub fn table_id(&self) -> TableId {
        self.table_id.clone()
    }

    /// Returns whether the table finished creating.
    pub fn is_created(&self) -> bool {
        self.state == State::Created
    }

    /// Update table state.
    pub async fn update_state(&mut self, store: &MetaStoreRef, state: State) -> Result<()> {
        self.state = state;
        self.insert(store).await
    }

    /// Returns actor ids associated with this table.
    pub fn actor_ids(&self) -> Vec<u32> {
        let mut actors = vec![];
        self.fragments.values().for_each(|fragment| {
            actors.extend(
                fragment
                    .actors
                    .iter()
                    .map(|actor| actor.actor_id)
                    .collect::<Vec<u32>>(),
            )
        });
        actors
    }

    /// Returns actors associated with this table.
    pub fn actors(&self) -> Vec<StreamActor> {
        let mut actors = vec![];
        self.fragments
            .values()
            .for_each(|fragment| actors.extend(fragment.actors.clone()));
        actors
    }

    /// Returns the actor ids with the given fragment type.
    fn filter_actor_ids(&self, fragment_type: FragmentType) -> Vec<u32> {
        let mut actors = vec![];
        self.fragments.values().for_each(|fragment| {
            if fragment.fragment_type == fragment_type as i32 {
                actors.extend(
                    fragment
                        .actors
                        .iter()
                        .map(|actor| actor.actor_id)
                        .collect::<Vec<u32>>(),
                )
            }
        });
        actors
    }

    /// Returns source actor ids.
    pub fn source_actor_ids(&self) -> Vec<u32> {
        Self::filter_actor_ids(self, FragmentType::Source)
    }

    /// Returns sink actor ids.
    pub fn sink_actor_ids(&self) -> Vec<u32> {
        Self::filter_actor_ids(self, FragmentType::Sink)
    }
}
