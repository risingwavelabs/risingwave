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
//
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use risingwave_common::catalog::TableId;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{Result, RwError};
use risingwave_common::try_match_expand;
use risingwave_pb::meta::table_fragments::ActorState;
use risingwave_pb::plan::TableRefId;
use risingwave_pb::stream_plan::StreamActor;

use crate::cluster::NodeId;
use crate::model::{ActorId, MetadataModel, TableFragments};
use crate::storage::MetaStore;

/// `FragmentManager` stores definition and status of fragment as well as the actors inside.
pub struct FragmentManager<S> {
    meta_store_ref: Arc<S>,

    table_fragments: DashMap<TableId, TableFragments>,
}

pub struct ActorInfos {
    /// node_id => actor_ids
    pub actor_maps: HashMap<NodeId, Vec<ActorId>>,

    /// all reachable source actors
    pub source_actor_maps: HashMap<NodeId, Vec<ActorId>>,
}

pub type FragmentManagerRef<S> = Arc<FragmentManager<S>>;

impl<S> FragmentManager<S>
where
    S: MetaStore,
{
    pub async fn new(meta_store_ref: Arc<S>) -> Result<Self> {
        let table_fragments = try_match_expand!(
            TableFragments::list(&*meta_store_ref).await,
            Ok,
            "TableFragments::list fail"
        )?;
        let fragment_map = DashMap::new();
        for table_fragment in table_fragments {
            fragment_map.insert(table_fragment.table_id(), table_fragment);
        }

        Ok(Self {
            meta_store_ref,
            table_fragments: fragment_map,
        })
    }

    pub async fn add_table_fragments(&self, table_fragment: TableFragments) -> Result<()> {
        match self.table_fragments.entry(table_fragment.table_id()) {
            Entry::Occupied(_) => Err(RwError::from(InternalError(
                "table_fragment already exist!".to_string(),
            ))),
            Entry::Vacant(v) => {
                table_fragment.insert(&*self.meta_store_ref).await?;
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
                table_fragment.insert(&*self.meta_store_ref).await?;
                entry.insert(table_fragment);

                Ok(())
            }
            Entry::Vacant(_) => Err(RwError::from(InternalError(
                "table_fragment not exist!".to_string(),
            ))),
        }
    }

    /// update table fragments with downstream actor ids in sink actors.
    pub async fn update_table_fragments_downstream(
        &self,
        table_id: &TableId,
        extra_downstream_actors: &HashMap<ActorId, Vec<ActorId>>,
    ) -> Result<()> {
        match self.table_fragments.entry(*table_id) {
            Entry::Occupied(mut entry) => {
                let table_fragment = entry.get_mut();
                for fragment in table_fragment.fragments.values_mut() {
                    for actor in &mut fragment.actors {
                        if let Some(downstream_actors) =
                            extra_downstream_actors.get(&actor.actor_id)
                        {
                            actor.dispatcher[0]
                                .downstream_actor_id
                                .extend(downstream_actors.iter().cloned());
                        }
                    }
                }
                table_fragment.insert(&*self.meta_store_ref).await?;

                Ok(())
            }
            Entry::Vacant(_) => Err(RwError::from(InternalError(
                "table_fragment not exist!".to_string(),
            ))),
        }
    }

    pub async fn drop_table_fragments(&self, table_id: &TableId) -> Result<()> {
        match self.table_fragments.entry(*table_id) {
            Entry::Occupied(entry) => {
                TableFragments::delete(&*self.meta_store_ref, &TableRefId::from(table_id)).await?;
                entry.remove();

                Ok(())
            }
            Entry::Vacant(_) => Err(RwError::from(InternalError(
                "table_fragment not exist!".to_string(),
            ))),
        }
    }

    /// Used in [`crate::barrier::BarrierManager`]
    pub fn load_all_actors(&self, creating_table_id: Option<TableId>) -> ActorInfos {
        let mut actor_maps = HashMap::new();
        let mut source_actor_ids = HashMap::new();
        for entry in &self.table_fragments {
            // TODO: when swallow barrier available while blocking creating MV or MV on MV, refactor
            //  the filter logic.
            let fragments = entry.value();
            let include_inactive = creating_table_id.contains(&fragments.table_id());
            let check_state = |s: ActorState| {
                s == ActorState::Running || include_inactive && s == ActorState::Inactive
            };

            for (node_id, actors_status) in fragments.node_actors_status() {
                for actor_status in actors_status {
                    if check_state(actor_status.1) {
                        actor_maps
                            .entry(node_id)
                            .or_insert_with(Vec::new)
                            .push(actor_status.0);
                    }
                }
            }

            let source_actors = fragments.node_source_actors_status();
            for (&node_id, actors_status) in &source_actors {
                for actor_status in actors_status {
                    if check_state(actor_status.1) {
                        source_actor_ids
                            .entry(node_id)
                            .or_insert_with(Vec::new)
                            .push(actor_status.0);
                    }
                }
            }
        }

        ActorInfos {
            actor_maps,
            source_actor_maps: source_actor_ids,
        }
    }

    pub fn all_node_actors(
        &self,
        include_inactive: bool,
    ) -> Result<HashMap<NodeId, Vec<StreamActor>>> {
        let mut actor_maps = HashMap::new();
        for entry in &self.table_fragments {
            for (node_id, actor_ids) in &entry.value().node_actors(include_inactive) {
                let node_actor_ids = actor_maps.entry(*node_id).or_insert_with(Vec::new);
                node_actor_ids.extend_from_slice(actor_ids);
            }
        }
        Ok(actor_maps)
    }

    pub fn table_node_actors(&self, table_id: &TableId) -> Result<BTreeMap<NodeId, Vec<ActorId>>> {
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
