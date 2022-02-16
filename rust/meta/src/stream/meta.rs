use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use risingwave_common::array::RwError;
use risingwave_common::catalog::TableId;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::Result;
use risingwave_common::try_match_expand;
use risingwave_pb::plan::TableRefId;
use risingwave_pb::stream_plan::StreamActor;

use crate::cluster::NodeId;
use crate::manager::MetaSrvEnv;
use crate::model::{ActorId, MetadataModel, TableFragments};
use crate::storage::MetaStoreRef;

pub struct FragmentManager {
    meta_store_ref: MetaStoreRef,
    table_fragments: DashMap<TableId, TableFragments>,
}

pub struct ActorInfos {
    /// node_id => actor_ids
    pub actor_maps: HashMap<NodeId, Vec<ActorId>>,

    /// all reachable source actors
    pub source_actor_maps: HashMap<NodeId, Vec<ActorId>>,
}

pub type FragmentManagerRef = Arc<FragmentManager>;

impl FragmentManager {
    pub async fn new(env: MetaSrvEnv) -> Result<Self> {
        let meta_store_ref = env.meta_store_ref();
        let table_fragments = try_match_expand!(
            TableFragments::list(&meta_store_ref).await,
            Ok,
            "TableFragments::list fail"
        )?;
        let fragment_map = DashMap::new();
        for table_fragment in table_fragments {
            fragment_map.insert(table_fragment.table_id(), table_fragment);
        }

        Ok(Self {
            meta_store_ref: env.meta_store_ref(),
            table_fragments: fragment_map,
        })
    }

    pub async fn add_table_fragments(&self, table_fragment: TableFragments) -> Result<()> {
        match self.table_fragments.entry(table_fragment.table_id()) {
            Entry::Occupied(_) => Err(RwError::from(InternalError(
                "table_fragment already exist!".to_string(),
            ))),
            Entry::Vacant(v) => {
                table_fragment.insert(&self.meta_store_ref).await?;
                v.insert(table_fragment);
                Ok(())
            }
        }
    }

    pub fn list_table_fragments(&self) -> Result<Vec<TableFragments>> {
        Ok(self
            .table_fragments
            .iter()
            .map(|f| f.value().clone())
            .collect())
    }

    pub async fn update_table_fragments(&self, table_fragment: TableFragments) -> Result<()> {
        match self.table_fragments.entry(table_fragment.table_id()) {
            Entry::Occupied(mut entry) => {
                table_fragment.insert(&self.meta_store_ref).await?;
                entry.insert(table_fragment);

                Ok(())
            }
            Entry::Vacant(_) => Err(RwError::from(InternalError(
                "table_fragment not exist!".to_string(),
            ))),
        }
    }

    pub async fn drop_table_fragments(&self, table_id: &TableId) -> Result<()> {
        match self.table_fragments.entry(table_id.clone()) {
            Entry::Occupied(entry) => {
                TableFragments::delete(&self.meta_store_ref, &TableRefId::from(table_id)).await?;
                entry.remove();

                Ok(())
            }
            Entry::Vacant(_) => Err(RwError::from(InternalError(
                "table_fragment not exist!".to_string(),
            ))),
        }
    }

    pub fn load_all_actors(&self) -> Result<ActorInfos> {
        let mut actor_maps = HashMap::new();
        let mut source_actor_ids = HashMap::new();
        self.table_fragments.iter().for_each(|entry| {
            // TODO: when swallow barrier available while blocking creating MV or MV on MV, refactor
            //  the filter logic.
            if entry.value().is_created() {
                let node_actors = entry.value().node_actor_ids();
                node_actors.iter().for_each(|(node_id, actor_ids)| {
                    let node_actor_ids = actor_maps.entry(*node_id).or_insert_with(Vec::new);
                    node_actor_ids.extend_from_slice(actor_ids);
                });

                let source_actors = entry.value().node_source_actors();
                source_actors.iter().for_each(|(node_id, actor_ids)| {
                    let node_actor_ids = source_actor_ids.entry(*node_id).or_insert_with(Vec::new);
                    node_actor_ids.extend_from_slice(actor_ids);
                });
            }
        });

        Ok(ActorInfos {
            actor_maps,
            source_actor_maps: source_actor_ids,
        })
    }

    pub fn load_all_node_actors(&self) -> Result<HashMap<NodeId, Vec<StreamActor>>> {
        let mut actor_maps = HashMap::new();
        self.table_fragments.iter().for_each(|entry| {
            entry
                .value()
                .node_actors()
                .iter()
                .for_each(|(node_id, actor_ids)| {
                    let node_actor_ids = actor_maps.entry(*node_id).or_insert_with(Vec::new);
                    node_actor_ids.extend_from_slice(actor_ids);
                });
        });

        Ok(actor_maps)
    }

    pub fn get_table_node_actors(
        &self,
        table_id: &TableId,
    ) -> Result<BTreeMap<NodeId, Vec<ActorId>>> {
        match self.table_fragments.get(table_id) {
            Some(table_fragment) => Ok(table_fragment.node_actor_ids()),
            None => Err(RwError::from(InternalError(
                "table_fragment not exist!".to_string(),
            ))),
        }
    }

    pub fn get_table_actor_ids(&self, table_id: &TableId) -> Result<Vec<ActorId>> {
        match self.table_fragments.get(table_id) {
            Some(table_fragment) => Ok(table_fragment.actor_ids()),
            None => Err(RwError::from(InternalError(
                "table_fragment not exist!".to_string(),
            ))),
        }
    }

    pub fn get_table_sink_actor_ids(&self, table_id: &TableId) -> Result<Vec<ActorId>> {
        match self.table_fragments.get(table_id) {
            Some(table_fragment) => Ok(table_fragment.sink_actor_ids()),
            None => Err(RwError::from(InternalError(
                "table_fragment not exist!".to_string(),
            ))),
        }
    }
}
