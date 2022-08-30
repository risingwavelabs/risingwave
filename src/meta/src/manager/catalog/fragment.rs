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

use anyhow::anyhow;
use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_common::{bail, try_match_expand};
use risingwave_pb::common::{Buffer, ParallelUnit, ParallelUnitMapping, WorkerNode};
use risingwave_pb::meta::table_fragments::ActorState;
use risingwave_pb::stream_plan::{Dispatcher, FragmentType, StreamActor};
use tokio::sync::{RwLock, RwLockReadGuard};

use crate::barrier::Reschedule;
use crate::manager::cluster::WorkerId;
use crate::manager::MetaSrvEnv;
use crate::model::{ActorId, FragmentId, MetadataModel, TableFragments, Transactional};
use crate::storage::{MetaStore, Transaction};
use crate::MetaResult;

pub struct FragmentManagerCore {
    table_fragments: HashMap<TableId, TableFragments>,
}

impl FragmentManagerCore {
    /// List all table vnode mapping info according to the fragment vnode mapping info.
    pub fn all_table_mappings(&self) -> impl Iterator<Item = ParallelUnitMapping> + '_ {
        self.table_fragments.values().flat_map(|table_fragments| {
            table_fragments
                .fragments
                .values()
                .flat_map(|fragment| {
                    let parallel_unit_mapping = fragment.vnode_mapping.as_ref().unwrap();
                    fragment
                        .state_table_ids
                        .iter()
                        .map(|internal_table_id| ParallelUnitMapping {
                            table_id: *internal_table_id,
                            original_indices: parallel_unit_mapping.original_indices.clone(),
                            data: parallel_unit_mapping.data.clone(),
                        })
                        .collect_vec()
                })
                .collect_vec()
        })
    }
}

/// `FragmentManager` stores definition and status of fragment as well as the actors inside.
pub struct FragmentManager<S: MetaStore> {
    meta_store: Arc<S>,

    core: RwLock<FragmentManagerCore>,
}

pub struct ActorInfos {
    /// node_id => actor_ids
    pub actor_maps: HashMap<WorkerId, Vec<ActorId>>,

    /// all reachable source actors
    pub source_actor_maps: HashMap<WorkerId, Vec<ActorId>>,
}

pub struct FragmentVNodeInfo {
    /// actor id => parallel unit
    pub actor_parallel_unit_maps: BTreeMap<ActorId, ParallelUnit>,

    /// fragment vnode mapping info
    pub vnode_mapping: Option<ParallelUnitMapping>,
}

#[derive(Default)]
pub struct BuildGraphInfo {
    pub table_sink_actor_ids: HashMap<TableId, Vec<ActorId>>,
}

pub type FragmentManagerRef<S> = Arc<FragmentManager<S>>;

impl<S: MetaStore> FragmentManager<S>
where
    S: MetaStore,
{
    pub async fn new(env: MetaSrvEnv<S>) -> MetaResult<Self> {
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

        Ok(Self {
            meta_store,
            core: RwLock::new(FragmentManagerCore { table_fragments }),
        })
    }

    pub async fn get_fragment_read_guard(&self) -> RwLockReadGuard<'_, FragmentManagerCore> {
        self.core.read().await
    }

    pub async fn list_table_fragments(&self) -> MetaResult<Vec<TableFragments>> {
        let map = &self.core.read().await.table_fragments;

        Ok(map.values().cloned().collect())
    }

    pub async fn batch_update_table_fragments(
        &self,
        table_fragments: &[TableFragments],
    ) -> MetaResult<()> {
        let map = &mut self.core.write().await.table_fragments;

        let mut transaction = Transaction::default();
        for table_fragment in table_fragments {
            if map.contains_key(&table_fragment.table_id()) {
                table_fragment.upsert_in_transaction(&mut transaction)?;
            } else {
                bail!("table_fragment not exist: id={}", table_fragment.table_id());
            }
        }

        self.meta_store.txn(transaction).await?;
        for table_fragment in table_fragments {
            map.insert(table_fragment.table_id(), table_fragment.clone());
        }

        Ok(())
    }

    pub async fn select_table_fragments_by_table_id(
        &self,
        table_id: &TableId,
    ) -> MetaResult<TableFragments> {
        let map = &self.core.read().await.table_fragments;
        if let Some(table_fragments) = map.get(table_id) {
            Ok(table_fragments.clone())
        } else {
            bail!("table_fragment not exist: id={}", table_id);
        }
    }

    /// Start create a new `TableFragments` and insert it into meta store, currently the actors'
    /// state is `ActorState::Inactive`.
    pub async fn start_create_table_fragments(
        &self,
        table_fragment: TableFragments,
    ) -> MetaResult<()> {
        let map = &mut self.core.write().await.table_fragments;

        match map.entry(table_fragment.table_id()) {
            Entry::Occupied(_) => bail!(
                "table_fragment already exist: id={}",
                table_fragment.table_id()
            ),
            Entry::Vacant(v) => {
                table_fragment.insert(&*self.meta_store).await?;
                v.insert(table_fragment);
                Ok(())
            }
        }
    }

    /// Cancel creation of a new `TableFragments` and delete it from meta store.
    pub async fn cancel_create_table_fragments(&self, table_id: &TableId) -> MetaResult<()> {
        let map = &mut self.core.write().await.table_fragments;

        match map.entry(*table_id) {
            Entry::Occupied(o) => {
                TableFragments::delete(&*self.meta_store, &table_id.table_id).await?;
                o.remove();
                Ok(())
            }
            Entry::Vacant(_) => bail!("table_fragment not exist: id={}", table_id),
        }
    }

    /// Finish create a new `TableFragments` and update the actors' state to `ActorState::Running`,
    /// besides also update all dependent tables' downstream actors info.
    pub async fn finish_create_table_fragments(
        &self,
        table_id: &TableId,
        dependent_table_actors: Vec<(TableId, HashMap<ActorId, Vec<Dispatcher>>)>,
    ) -> MetaResult<()> {
        let map = &mut self.core.write().await.table_fragments;

        if let Some(table_fragments) = map.get(table_id) {
            let mut transaction = Transaction::default();

            let mut table_fragments = table_fragments.clone();
            table_fragments.update_actors_state(ActorState::Running);
            table_fragments.upsert_in_transaction(&mut transaction)?;

            let mut dependent_tables = Vec::with_capacity(dependent_table_actors.len());
            for (dependent_table_id, mut new_dispatchers) in dependent_table_actors {
                let mut dependent_table = map
                    .get(&dependent_table_id)
                    .ok_or_else(|| anyhow!("table_fragment not exist: id={}", dependent_table_id))?
                    .clone();
                for fragment in dependent_table.fragments.values_mut() {
                    for actor in &mut fragment.actors {
                        // Extend new dispatchers to table fragments.
                        if let Some(new_dispatchers) = new_dispatchers.remove(&actor.actor_id) {
                            actor.dispatcher.extend(new_dispatchers);
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
            bail!("table_fragment not exist: id={}", table_id)
        }
    }

    /// Drop table fragments info and remove downstream actor infos in fragments from its dependent
    /// tables.
    pub async fn drop_table_fragments(&self, table_id: &TableId) -> MetaResult<()> {
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
                    .ok_or_else(|| anyhow!("table_fragment not exist: id={}", dependent_table_id))?
                    .clone();
                for fragment in dependent_table.fragments.values_mut() {
                    if fragment.fragment_type == FragmentType::Sink as i32 {
                        for actor in &mut fragment.actors {
                            // Remove these downstream actor ids from all dispatchers.
                            for dispatcher in &mut actor.dispatcher {
                                dispatcher
                                    .downstream_actor_id
                                    .retain(|x| !chain_actor_ids.contains(x));
                            }
                            // Remove empty dispatchers.
                            actor
                                .dispatcher
                                .retain(|d| !d.downstream_actor_id.is_empty());
                        }
                    }
                }
                dependent_table.upsert_in_transaction(&mut transaction)?;
                dependent_tables.push(dependent_table);
            }

            self.meta_store.txn(transaction).await?;
            map.remove(table_id).unwrap();
            for dependent_table in dependent_tables {
                map.insert(dependent_table.table_id(), dependent_table);
            }
            Ok(())
        } else {
            bail!("table_fragment not exist: id={}", table_id);
        }
    }

    /// Used in [`crate::barrier::GlobalBarrierManager`], load all actor that need to be sent or
    /// collected
    pub async fn load_all_actors(
        &self,
        check_state: impl Fn(ActorState, TableId, ActorId) -> bool,
    ) -> ActorInfos {
        let mut actor_maps = HashMap::new();
        let mut source_actor_maps = HashMap::new();

        let map = &self.core.read().await.table_fragments;
        for fragments in map.values() {
            for (worker_id, actor_states) in fragments.worker_actor_states() {
                for (actor_id, actor_state) in actor_states {
                    if check_state(actor_state, fragments.table_id(), actor_id) {
                        actor_maps
                            .entry(worker_id)
                            .or_insert_with(Vec::new)
                            .push(actor_id);
                    }
                }
            }

            let source_actors = fragments.node_source_actor_states();
            for (worker_id, actor_states) in source_actors {
                for (actor_id, actor_state) in actor_states {
                    if check_state(actor_state, fragments.table_id(), actor_id) {
                        source_actor_maps
                            .entry(worker_id)
                            .or_insert_with(Vec::new)
                            .push(actor_id);
                    }
                }
            }
        }

        ActorInfos {
            actor_maps,
            source_actor_maps,
        }
    }

    /// Used in [`crate::barrier::GlobalBarrierManager`]
    /// migrate actors and update fragments, generate migrate info
    pub async fn migrate_actors(
        &self,
        migrate_map: &HashMap<ActorId, WorkerId>,
        node_map: &HashMap<WorkerId, WorkerNode>,
    ) -> MetaResult<Vec<TableFragments>> {
        let mut parallel_unit_migrate_map = HashMap::new();
        let mut pu_map: HashMap<WorkerId, Vec<&ParallelUnit>> = HashMap::new();
        // split parallel units of node into types, map them with WorkerId
        for (node_id, node) in node_map {
            let pu = node.parallel_units.iter().collect_vec();
            pu_map.insert(*node_id, pu);
        }
        // update actor status and generate pu to pu migrate info
        let mut table_fragments = self.list_table_fragments().await?;
        let mut new_fragments = Vec::new();
        table_fragments.iter_mut().for_each(|fragment| {
            let mut flag = false;
            fragment
                .actor_status
                .iter_mut()
                .for_each(|(actor_id, status)| {
                    if let Some(new_node_id) = migrate_map.get(actor_id) {
                        if let Some(ref old_parallel_unit) = status.parallel_unit {
                            if let Entry::Vacant(e) =
                                parallel_unit_migrate_map.entry(old_parallel_unit.id)
                            {
                                let new_parallel_unit =
                                    pu_map.get_mut(new_node_id).unwrap().pop().unwrap();
                                e.insert(new_parallel_unit.clone());
                                status.parallel_unit = Some(new_parallel_unit.clone());
                                flag = true;
                            } else {
                                status.parallel_unit = Some(
                                    parallel_unit_migrate_map
                                        .get(&old_parallel_unit.id)
                                        .unwrap()
                                        .clone(),
                                );
                            }
                        }
                    };
                });
            if flag {
                // update vnode mapping of updated fragments
                fragment.update_vnode_mapping(&parallel_unit_migrate_map);
                new_fragments.push(fragment.clone());
            }
        });
        // update fragments
        self.batch_update_table_fragments(&new_fragments).await?;
        Ok(new_fragments)
    }

    pub async fn all_node_actors(
        &self,
        include_inactive: bool,
    ) -> HashMap<WorkerId, Vec<StreamActor>> {
        let mut actor_maps = HashMap::new();

        let map = &self.core.read().await.table_fragments;
        for fragments in map.values() {
            for (node_id, actor_ids) in fragments.worker_actors(include_inactive) {
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

    /// Get the actor ids of the fragment with `fragment_id` with `Running` status.
    pub async fn get_running_actors_of_fragment(
        &self,
        fragment_id: FragmentId,
    ) -> MetaResult<HashSet<ActorId>> {
        let map = &self.core.read().await.table_fragments;

        for table_fragment in map.values() {
            if let Some(fragment) = table_fragment.fragments.get(&fragment_id) {
                let running_actor_ids = fragment
                    .actors
                    .iter()
                    .map(|a| a.actor_id)
                    .filter(|a| table_fragment.actor_status[a].state == ActorState::Running as i32)
                    .collect();
                return Ok(running_actor_ids);
            }
        }

        bail!("fragment not found: {}", fragment_id)
    }

    /// Apply `Reschedule`s to fragments.
    pub async fn apply_reschedules(
        &self,
        mut reschedules: HashMap<FragmentId, Reschedule>,
    ) -> MetaResult<()> {
        let map = &mut self.core.write().await.table_fragments;
        let mut transaction = Transaction::default();

        for table_fragment in map.values_mut() {
            // Takes out the reschedules of the fragments in this table.
            let reschedules = reschedules
                .drain_filter(|fragment_id, _| table_fragment.fragments.contains_key(fragment_id))
                .collect_vec();
            let updated = !reschedules.is_empty();

            for (fragment_id, reschedule) in reschedules {
                let fragment = table_fragment.fragments.get_mut(&fragment_id).unwrap();

                // Add actors to this fragment: set the state to `Running`.
                // TODO: update vnode mapping for actors.
                for actor_id in reschedule.added_actors {
                    table_fragment
                        .actor_status
                        .get_mut(&actor_id)
                        .unwrap()
                        .set_state(ActorState::Running);
                }

                // Remove actors from this fragment.
                let removed_actors: HashSet<_> = reschedule.removed_actors.into_iter().collect();
                fragment
                    .actors
                    .retain(|a| !removed_actors.contains(&a.actor_id));
                for actor_id in removed_actors {
                    table_fragment.actor_status.remove(&actor_id);
                }

                // TODO: update merger at downstream and dispatcher at upstream in meta store.
            }

            if updated {
                table_fragment.upsert_in_transaction(&mut transaction)?;
            }
        }

        assert!(reschedules.is_empty(), "all reschedules must be applied");

        self.meta_store.txn(transaction).await?;
        Ok(())
    }

    pub async fn table_node_actors(
        &self,
        table_id: &TableId,
    ) -> MetaResult<BTreeMap<WorkerId, Vec<ActorId>>> {
        let map = &self.core.read().await.table_fragments;
        match map.get(table_id) {
            Some(table_fragment) => Ok(table_fragment.worker_actor_ids()),
            None => bail!("table_fragment not exist: id={}", table_id),
        }
    }

    pub async fn get_table_actor_ids(&self, table_id: &TableId) -> MetaResult<Vec<ActorId>> {
        let map = &self.core.read().await.table_fragments;
        match map.get(table_id) {
            Some(table_fragment) => Ok(table_fragment.actor_ids()),
            None => bail!("table_fragment not exist: id={}", table_id),
        }
    }

    pub async fn get_table_sink_actor_ids(&self, table_id: &TableId) -> MetaResult<Vec<ActorId>> {
        let map = &self.core.read().await.table_fragments;
        match map.get(table_id) {
            Some(table_fragment) => Ok(table_fragment.sink_actor_ids()),
            None => bail!("table_fragment not exist: id={}", table_id),
        }
    }

    // we will read three things at once, avoiding locking too much.
    pub async fn get_build_graph_info(
        &self,
        table_ids: &HashSet<TableId>,
    ) -> MetaResult<BuildGraphInfo> {
        let map = &self.core.read().await.table_fragments;
        let mut info: BuildGraphInfo = Default::default();

        for table_id in table_ids {
            match map.get(table_id) {
                Some(table_fragment) => {
                    info.table_sink_actor_ids
                        .insert(*table_id, table_fragment.sink_actor_ids());
                }
                None => {
                    bail!("table_fragment not exist: id={}", table_id);
                }
            }
        }
        Ok(info)
    }

    pub async fn get_sink_vnode_bitmap_info(
        &self,
        table_ids: &HashSet<TableId>,
    ) -> MetaResult<HashMap<TableId, Vec<(ActorId, Option<Buffer>)>>> {
        let map = &self.core.read().await.table_fragments;
        let mut info: HashMap<TableId, Vec<(ActorId, Option<Buffer>)>> = HashMap::new();

        for table_id in table_ids {
            match map.get(table_id) {
                Some(table_fragment) => {
                    info.insert(*table_id, table_fragment.sink_vnode_bitmap_info());
                }
                None => {
                    bail!("table_fragment not exist: id={}", table_id);
                }
            }
        }
        Ok(info)
    }

    pub async fn get_sink_fragment_vnode_info(
        &self,
        table_ids: &HashSet<TableId>,
    ) -> MetaResult<HashMap<TableId, FragmentVNodeInfo>> {
        let map = &self.core.read().await.table_fragments;
        let mut info: HashMap<TableId, FragmentVNodeInfo> = HashMap::new();

        for table_id in table_ids {
            match map.get(table_id) {
                Some(table_fragment) => {
                    info.insert(
                        *table_id,
                        FragmentVNodeInfo {
                            actor_parallel_unit_maps: table_fragment.sink_actor_parallel_units(),
                            vnode_mapping: table_fragment.sink_vnode_mapping(),
                        },
                    );
                }

                None => {
                    bail!("table_fragment not exist: id={}", table_id);
                }
            }
        }

        Ok(info)
    }

    pub async fn get_tables_worker_actors(
        &self,
        table_ids: &HashSet<TableId>,
    ) -> MetaResult<HashMap<TableId, BTreeMap<WorkerId, Vec<ActorId>>>> {
        let map = &self.core.read().await.table_fragments;
        let mut info: HashMap<TableId, BTreeMap<WorkerId, Vec<ActorId>>> = HashMap::new();

        for table_id in table_ids {
            match map.get(table_id) {
                Some(table_fragment) => {
                    info.insert(*table_id, table_fragment.worker_actor_ids());
                }
                None => {
                    bail!("table_fragment not exist: id={}", table_id);
                }
            }
        }

        Ok(info)
    }
}
