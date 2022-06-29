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

use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use risingwave_common::catalog::TableId;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{Result, RwError};
use risingwave_common::try_match_expand;
use risingwave_common::types::{ParallelUnitId, VIRTUAL_NODE_COUNT};
use risingwave_common::util::compress::decompress_data;
use risingwave_pb::meta::table_fragments::ActorState;
use risingwave_pb::stream_plan::{FragmentType, StreamActor};
use tokio::sync::RwLock;

use crate::barrier::ChangedTableId;
use crate::cluster::WorkerId;
use crate::hummock::compaction_group::manager::CompactionGroupManagerRef;
use crate::manager::{HashMappingManagerRef, MetaSrvEnv};
use crate::model::{ActorId, MetadataModel, TableFragments, Transactional};
use crate::storage::{MetaStore, Transaction};
use crate::stream::record_table_vnode_mappings;

struct FragmentManagerCore {
    table_fragments: HashMap<TableId, TableFragments>,
}

/// `FragmentManager` stores definition and status of fragment as well as the actors inside.
pub struct FragmentManager<S: MetaStore> {
    meta_store: Arc<S>,

    core: RwLock<FragmentManagerCore>,

    compaction_group_manager: CompactionGroupManagerRef<S>,
}

pub struct ActorInfos {
    /// node_id => actor_ids
    pub actor_maps: HashMap<WorkerId, Vec<ActorId>>,

    /// all reachable source actors
    pub source_actor_maps: HashMap<WorkerId, Vec<ActorId>>,
}

#[derive(Default)]
pub struct BuildGraphInfo {
    pub table_node_actors: HashMap<TableId, BTreeMap<WorkerId, Vec<ActorId>>>,
    pub table_sink_actor_ids: HashMap<TableId, Vec<ActorId>>,
}

pub type FragmentManagerRef<S> = Arc<FragmentManager<S>>;

impl<S: MetaStore> FragmentManager<S>
where
    S: MetaStore,
{
    pub async fn new(
        env: MetaSrvEnv<S>,
        compaction_group_manager: CompactionGroupManagerRef<S>,
    ) -> Result<Self> {
        let meta_store = env.meta_store_ref();
        let table_fragments = try_match_expand!(
            TableFragments::list(&*meta_store).await,
            Ok,
            "TableFragments::list fail"
        )?;

        let table_fragments = table_fragments
            .into_iter()
            .map(|tf| (tf.table_id(), tf))
            .collect();

        Self::restore_vnode_mappings(env.hash_mapping_manager_ref(), &table_fragments)?;

        Ok(Self {
            meta_store,
            core: RwLock::new(FragmentManagerCore { table_fragments }),
            compaction_group_manager,
        })
    }

    pub async fn list_table_fragments(&self) -> Result<Vec<TableFragments>> {
        let map = &self.core.read().await.table_fragments;

        Ok(map.values().cloned().collect())
    }

    pub async fn update_table_fragments(&self, table_fragment: TableFragments) -> Result<()> {
        let map = &mut self.core.write().await.table_fragments;

        match map.entry(table_fragment.table_id()) {
            Entry::Occupied(mut entry) => {
                table_fragment.insert(&*self.meta_store).await?;
                entry.insert(table_fragment);

                Ok(())
            }
            Entry::Vacant(_) => Err(RwError::from(InternalError(format!(
                "table_fragment not exist: id={}",
                table_fragment.table_id()
            )))),
        }
    }

    pub async fn select_table_fragments_by_table_id(
        &self,
        table_id: &TableId,
    ) -> Result<TableFragments> {
        let map = &self.core.read().await.table_fragments;
        if let Some(table_fragments) = map.get(table_id) {
            Ok(table_fragments.clone())
        } else {
            Err(RwError::from(InternalError(format!(
                "table_fragment not exist: id={}",
                table_id
            ))))
        }
    }

    /// Start create a new `TableFragments` and insert it into meta store, currently the actors'
    /// state is `ActorState::Inactive`.
    pub async fn start_create_table_fragments(
        &self,
        table_fragment: TableFragments,
        table_properties: HashMap<String, String>,
    ) -> Result<()> {
        let map = &mut self.core.write().await.table_fragments;

        match map.entry(table_fragment.table_id()) {
            Entry::Occupied(_) => Err(RwError::from(InternalError(format!(
                "table_fragment already exist: id={}",
                table_fragment.table_id()
            )))),
            Entry::Vacant(v) => {
                // Register to compaction group beforehand.
                // If any following operation fails, the registration will be eventually reverted.
                self.compaction_group_manager
                    .register_table_fragments(&table_fragment, &table_properties)
                    .await?;

                table_fragment.insert(&*self.meta_store).await?;
                v.insert(table_fragment);
                Ok(())
            }
        }
    }

    /// Cancel creation of a new `TableFragments` and delete it from meta store.
    pub async fn cancel_create_table_fragments(&self, table_id: &TableId) -> Result<()> {
        let map = &mut self.core.write().await.table_fragments;

        match map.entry(*table_id) {
            Entry::Occupied(o) => {
                TableFragments::delete(&*self.meta_store, &table_id.table_id).await?;
                let table_fragments = o.remove();
                // Unregister from compaction group afterwards.
                if let Err(e) = self
                    .compaction_group_manager
                    .unregister_table_fragments(&table_fragments)
                    .await
                {
                    tracing::warn!(
                        "Failed to unregister table {}. It wll be unregistered eventually.\n{:#?}",
                        table_id,
                        e
                    );
                }
                Ok(())
            }
            Entry::Vacant(_) => Err(RwError::from(InternalError(format!(
                "table_fragment not exist: id={}",
                table_id
            )))),
        }
    }

    /// Finish create a new `TableFragments` and update the actors' state to `ActorState::Running`,
    /// besides also update all dependent tables' downstream actors info.
    pub async fn finish_create_table_fragments(
        &self,
        table_id: &TableId,
        dependent_table_actors: &[(TableId, HashMap<ActorId, Vec<ActorId>>)],
    ) -> Result<()> {
        let map = &mut self.core.write().await.table_fragments;

        if let Some(table_fragments) = map.get(table_id) {
            let mut transaction = Transaction::default();

            let mut table_fragments = table_fragments.clone();
            table_fragments.update_actors_state(ActorState::Running);
            table_fragments.upsert_in_transaction(&mut transaction)?;

            let mut dependent_tables = Vec::with_capacity(dependent_table_actors.len());
            for (dependent_table_id, extra_downstream_actors) in dependent_table_actors {
                let mut dependent_table = map
                    .get(dependent_table_id)
                    .ok_or_else(|| {
                        RwError::from(InternalError(format!(
                            "table_fragment not exist: id={}",
                            dependent_table_id
                        )))
                    })?
                    .clone();
                for fragment in dependent_table.fragments.values_mut() {
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
                dependent_table.upsert_in_transaction(&mut transaction)?;
                dependent_tables.push(dependent_table);
            }

            self.meta_store.txn(transaction).await?;
            map.insert(*table_id, table_fragments);
            for dependent_table in dependent_tables {
                map.insert(dependent_table.table_id(), dependent_table);
            }

            Ok(())
        } else {
            Err(RwError::from(InternalError(format!(
                "table_fragment not exist: id={}",
                table_id
            ))))
        }
    }

    /// Drop table fragments info and remove downstream actor infos in fragments from its dependent
    /// tables.
    pub async fn drop_table_fragments(&self, table_id: &TableId) -> Result<()> {
        let map = &mut self.core.write().await.table_fragments;

        if let Some(table_fragments) = map.get(table_id) {
            let mut transaction = Transaction::default();
            table_fragments.delete_in_transaction(&mut transaction)?;

            let dependent_table_ids = table_fragments.dependent_table_ids();
            let chain_actor_ids = table_fragments.chain_actor_ids();
            let mut dependent_tables = Vec::with_capacity(dependent_table_ids.len());
            for dependent_table_id in dependent_table_ids {
                let mut dependent_table = map
                    .get(&dependent_table_id)
                    .ok_or_else(|| {
                        RwError::from(InternalError(format!(
                            "table_fragment not exist: id={}",
                            dependent_table_id
                        )))
                    })?
                    .clone();
                for fragment in dependent_table.fragments.values_mut() {
                    if fragment.fragment_type == FragmentType::Sink as i32 {
                        for actor in &mut fragment.actors {
                            actor.dispatcher[0]
                                .downstream_actor_id
                                .retain(|x| !chain_actor_ids.contains(x));
                        }
                    }
                }
                dependent_table.upsert_in_transaction(&mut transaction)?;
                dependent_tables.push(dependent_table);
            }

            self.meta_store.txn(transaction).await?;
            let table_fragments = map.remove(table_id).unwrap();
            for dependent_table in dependent_tables {
                map.insert(dependent_table.table_id(), dependent_table);
            }
            // Unregister from compaction group afterwards.
            if let Err(e) = self
                .compaction_group_manager
                .unregister_table_fragments(&table_fragments)
                .await
            {
                tracing::warn!(
                    "Failed to unregister table {}. It wll be unregistered eventually.\n{:#?}",
                    table_id,
                    e
                );
            }
            Ok(())
        } else {
            Err(RwError::from(InternalError(format!(
                "table_fragment not exist: id={}",
                table_id
            ))))
        }
    }

    /// Used in [`crate::barrier::GlobalBarrierManager`]
    pub async fn load_all_actors(&self, with_creating_table: ChangedTableId) -> ActorInfos {
        let mut actor_maps = HashMap::new();
        let mut source_actor_ids = HashMap::new();

        let map = &self.core.read().await.table_fragments;
        for fragments in map.values() {
            let check_state = |s: ActorState| {
                with_creating_table.can_actor_send_or_collect(s, &fragments.table_id())
            };
            for (node_id, actor_states) in fragments.node_actor_states() {
                for actor_state in actor_states {
                    if check_state(actor_state.1) {
                        actor_maps
                            .entry(node_id)
                            .or_insert_with(Vec::new)
                            .push(actor_state.0);
                    }
                }
            }

            let source_actors = fragments.node_source_actor_states();
            for (&node_id, actor_states) in &source_actors {
                for actor_state in actor_states {
                    if check_state(actor_state.1) {
                        source_actor_ids
                            .entry(node_id)
                            .or_insert_with(Vec::new)
                            .push(actor_state.0);
                    }
                }
            }
        }

        ActorInfos {
            actor_maps,
            source_actor_maps: source_actor_ids,
        }
    }

    pub async fn all_node_actors(
        &self,
        include_inactive: bool,
    ) -> HashMap<WorkerId, Vec<StreamActor>> {
        let mut actor_maps = HashMap::new();

        let map = &self.core.read().await.table_fragments;
        for fragments in map.values() {
            for (node_id, actor_ids) in fragments.node_actors(include_inactive) {
                let node_actor_ids = actor_maps.entry(node_id).or_insert_with(Vec::new);
                node_actor_ids.extend(actor_ids);
            }
        }

        actor_maps
    }

    pub async fn all_chain_actor_ids(&self) -> HashSet<ActorId> {
        let map = &self.core.read().await.table_fragments;

        map.values()
            .flat_map(|table_fragment| table_fragment.chain_actor_ids())
            .collect::<HashSet<_>>()
    }

    pub async fn table_node_actors(
        &self,
        table_id: &TableId,
    ) -> Result<BTreeMap<WorkerId, Vec<ActorId>>> {
        let map = &self.core.read().await.table_fragments;
        match map.get(table_id) {
            Some(table_fragment) => Ok(table_fragment.node_actor_ids()),
            None => Err(RwError::from(InternalError(format!(
                "table_fragment not exist: id={}",
                table_id
            )))),
        }
    }

    pub async fn get_table_actor_ids(&self, table_id: &TableId) -> Result<Vec<ActorId>> {
        let map = &self.core.read().await.table_fragments;
        match map.get(table_id) {
            Some(table_fragment) => Ok(table_fragment.actor_ids()),
            None => Err(RwError::from(InternalError(format!(
                "table_fragment not exist: id={}",
                table_id
            )))),
        }
    }

    pub async fn get_table_sink_actor_ids(&self, table_id: &TableId) -> Result<Vec<ActorId>> {
        let map = &self.core.read().await.table_fragments;
        match map.get(table_id) {
            Some(table_fragment) => Ok(table_fragment.sink_actor_ids()),
            None => Err(RwError::from(InternalError(format!(
                "table_fragment not exist: id={}",
                table_id
            )))),
        }
    }

    // we will read three things at once, avoiding locking too much.
    pub async fn get_build_graph_info(
        &self,
        table_ids: &HashSet<TableId>,
    ) -> Result<BuildGraphInfo> {
        let map = &self.core.read().await.table_fragments;
        let mut info: BuildGraphInfo = Default::default();

        for table_id in table_ids {
            match map.get(table_id) {
                Some(table_fragment) => {
                    info.table_node_actors
                        .insert(*table_id, table_fragment.node_actor_ids());
                    info.table_sink_actor_ids
                        .insert(*table_id, table_fragment.sink_actor_ids());
                }
                None => {
                    return Err(RwError::from(InternalError(format!(
                        "table_fragment not exist: id={}",
                        table_id
                    ))));
                }
            }
        }
        Ok(info)
    }

    pub async fn get_sink_parallel_unit_ids(
        &self,
        table_ids: &HashSet<TableId>,
    ) -> Result<HashMap<TableId, BTreeMap<ParallelUnitId, ActorId>>> {
        let map = &self.core.read().await.table_fragments;
        let mut info: HashMap<TableId, BTreeMap<ParallelUnitId, ActorId>> = HashMap::new();

        for table_id in table_ids {
            match map.get(table_id) {
                Some(table_fragment) => {
                    info.insert(*table_id, table_fragment.parallel_unit_sink_actor_id());
                }
                None => {
                    return Err(RwError::from(InternalError(format!(
                        "table_fragment not exist: id={}",
                        table_id
                    ))));
                }
            }
        }

        Ok(info)
    }

    pub async fn get_tables_node_actors(
        &self,
        table_ids: &HashSet<TableId>,
    ) -> Result<HashMap<TableId, BTreeMap<WorkerId, Vec<ActorId>>>> {
        let map = &self.core.read().await.table_fragments;
        let mut info: HashMap<TableId, BTreeMap<WorkerId, Vec<ActorId>>> = HashMap::new();

        for table_id in table_ids {
            match map.get(table_id) {
                Some(table_fragment) => {
                    info.insert(*table_id, table_fragment.node_actor_ids());
                }
                None => {
                    return Err(RwError::from(InternalError(format!(
                        "table_fragment not exist: id={}",
                        table_id
                    ))));
                }
            }
        }

        Ok(info)
    }

    fn restore_vnode_mappings(
        hash_mapping_manager: HashMappingManagerRef,
        table_fragments: &HashMap<TableId, TableFragments>,
    ) -> Result<()> {
        for fragments in table_fragments.values() {
            for (fragment_id, fragment) in &fragments.fragments {
                let mapping = fragment.vnode_mapping.as_ref().unwrap();
                let vnode_mapping = decompress_data(&mapping.original_indices, &mapping.data);
                assert_eq!(vnode_mapping.len(), VIRTUAL_NODE_COUNT);
                hash_mapping_manager.set_fragment_hash_mapping(*fragment_id, vnode_mapping);

                // Looking at the first actor is enough, since all actors in one fragment have
                // identical state table id.
                let actor = fragment.actors.first().unwrap();
                let stream_node = actor.get_nodes()?;
                record_table_vnode_mappings(
                    &hash_mapping_manager,
                    stream_node,
                    fragment.fragment_id,
                )?;
            }
        }
        Ok(())
    }
}
