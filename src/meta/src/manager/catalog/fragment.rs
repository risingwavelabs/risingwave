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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use anyhow::{anyhow, Context};
use itertools::Itertools;
use risingwave_common::bail;
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::TableId;
use risingwave_common::hash::{ActorMapping, ParallelUnitId, ParallelUnitMapping};
use risingwave_common::util::stream_graph_visitor::visit_stream_node;
use risingwave_connector::source::SplitImpl;
use risingwave_pb::ddl_service::TableJobType;
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use risingwave_pb::meta::table_fragments::actor_status::ActorState;
use risingwave_pb::meta::table_fragments::{ActorStatus, Fragment, State};
use risingwave_pb::meta::FragmentParallelUnitMapping;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::update_mutation::MergeUpdate;
use risingwave_pb::stream_plan::{
    DispatchStrategy, Dispatcher, DispatcherType, FragmentTypeFlag, StreamActor, StreamNode,
};
use tokio::sync::{RwLock, RwLockReadGuard};

use crate::barrier::Reschedule;
use crate::manager::cluster::WorkerId;
use crate::manager::{commit_meta, commit_meta_with_trx, LocalNotification, MetaSrvEnv};
use crate::model::{
    ActorId, BTreeMapTransaction, FragmentId, MetadataModel, MigrationPlan, TableFragments,
    ValTransaction,
};
use crate::storage::Transaction;
use crate::stream::{SplitAssignment, TableRevision};
use crate::{MetaError, MetaResult};

pub struct FragmentManagerCore {
    table_fragments: BTreeMap<TableId, TableFragments>,
    table_revision: TableRevision,
}

impl FragmentManagerCore {
    /// List all fragment vnode mapping info that not in `State::Initial`.
    pub fn all_running_fragment_mappings(
        &self,
    ) -> impl Iterator<Item = FragmentParallelUnitMapping> + '_ {
        self.table_fragments
            .values()
            .filter(|tf| tf.state() != State::Initial)
            .flat_map(|table_fragments| {
                table_fragments.fragments.values().map(|fragment| {
                    let parallel_unit_mapping = fragment.vnode_mapping.clone().unwrap();
                    FragmentParallelUnitMapping {
                        fragment_id: fragment.fragment_id,
                        mapping: Some(parallel_unit_mapping),
                    }
                })
            })
    }

    fn running_fragment_parallelisms(
        &self,
        id_filter: Option<HashSet<FragmentId>>,
    ) -> HashMap<FragmentId, usize> {
        self.table_fragments
            .values()
            .filter(|tf| tf.state() != State::Initial)
            .flat_map(|table_fragments| {
                table_fragments.fragments.values().filter_map(|fragment| {
                    if let Some(id_filter) = id_filter.as_ref()
                        && !id_filter.contains(&fragment.fragment_id)
                    {
                        return None;
                    }
                    let parallelism = match fragment.vnode_mapping.as_ref() {
                        None => {
                            tracing::warn!(
                                "vnode mapping for fragment {} not found",
                                fragment.fragment_id
                            );
                            1
                        }
                        Some(m) => ParallelUnitMapping::from_protobuf(m).iter_unique().count(),
                    };
                    Some((fragment.fragment_id, parallelism))
                })
            })
            .collect()
    }

    pub fn table_fragments(&self) -> &BTreeMap<TableId, TableFragments> {
        &self.table_fragments
    }
}

/// `FragmentManager` stores definition and status of fragment as well as the actors inside.
pub struct FragmentManager {
    env: MetaSrvEnv,

    core: RwLock<FragmentManagerCore>,
}

pub struct ActorInfos {
    /// node_id => actor_ids
    pub actor_maps: HashMap<WorkerId, Vec<ActorId>>,

    /// all reachable barrier inject actors
    pub barrier_inject_actor_maps: HashMap<WorkerId, Vec<ActorId>>,
}

pub type FragmentManagerRef = Arc<FragmentManager>;

impl FragmentManager {
    pub async fn new(env: MetaSrvEnv) -> MetaResult<Self> {
        let table_fragments = TableFragments::list(env.meta_store()).await?;

        let table_fragments = table_fragments
            .into_iter()
            .map(|tf| (tf.table_id(), tf))
            .collect();

        let table_revision = TableRevision::get(env.meta_store()).await?;

        Ok(Self {
            env,
            core: RwLock::new(FragmentManagerCore {
                table_fragments,
                table_revision,
            }),
        })
    }

    pub async fn get_fragment_read_guard(&self) -> RwLockReadGuard<'_, FragmentManagerCore> {
        self.core.read().await
    }

    pub async fn list_dirty_table_fragments(
        &self,
        check_dirty: impl Fn(&TableFragments) -> bool,
    ) -> Vec<TableFragments> {
        self.core
            .read()
            .await
            .table_fragments
            .values()
            .filter(|tf| check_dirty(tf))
            .cloned()
            .collect_vec()
    }

    // FIXME: `list_table_fragments` would be too heavy for large cluster.
    pub async fn list_table_fragments(&self) -> Vec<TableFragments> {
        let map = &self.core.read().await.table_fragments;
        map.values().cloned().collect()
    }

    /// The `table_ids` here should correspond to stream jobs.
    /// We get their corresponding table fragment, and from there,
    /// we get the actors that are in the table fragment.
    pub async fn get_table_id_actor_mapping(
        &self,
        table_ids: &[TableId],
    ) -> HashMap<TableId, Vec<ActorId>> {
        let map = &self.core.read().await.table_fragments;
        let mut table_map = HashMap::new();
        for table_id in table_ids {
            if let Some(table_fragment) = map.get(table_id) {
                let mut actors = vec![];
                for fragment in table_fragment.fragments.values() {
                    for actor in &fragment.actors {
                        actors.push(actor.actor_id)
                    }
                }
                table_map.insert(*table_id, actors);
            }
        }
        table_map
    }

    /// Gets the counts for each upstream relation that each stream job
    /// indicated by `table_ids` depends on.
    /// For example in the following query:
    /// ```sql
    /// CREATE MATERIALIZED VIEW m1 AS
    ///   SELECT * FROM t1 JOIN t2 ON t1.a = t2.a JOIN t3 ON t2.b = t3.b
    /// ```
    ///
    /// We have t1 occurring once, and t2 occurring once.
    pub async fn get_upstream_relation_counts(
        &self,
        table_ids: &[TableId],
    ) -> HashMap<TableId, HashMap<TableId, usize>> {
        let map = &self.core.read().await.table_fragments;
        let mut upstream_relation_counts = HashMap::new();
        for table_id in table_ids {
            if let Some(table_fragments) = map.get(table_id) {
                let dependent_ids = table_fragments.dependent_table_ids();
                let r = upstream_relation_counts.insert(*table_id, dependent_ids);
                assert!(r.is_none(), "Each table_id should be unique!")
            } else {
                upstream_relation_counts.insert(*table_id, HashMap::new());
            }
        }
        upstream_relation_counts
    }

    pub fn get_mv_id_to_internal_table_ids_mapping(&self) -> Option<Vec<(u32, Vec<u32>)>> {
        match self.core.try_read() {
            Ok(core) => Some(
                core.table_fragments
                    .values()
                    .map(|tf| (tf.table_id().table_id(), tf.all_table_ids().collect_vec()))
                    .collect_vec(),
            ),
            Err(_) => None,
        }
    }

    pub async fn get_revision(&self) -> TableRevision {
        self.core.read().await.table_revision
    }

    pub async fn has_any_table_fragments(&self) -> bool {
        !self.core.read().await.table_fragments.is_empty()
    }

    async fn notify_fragment_mapping(&self, table_fragment: &TableFragments, operation: Operation) {
        // Notify all fragment mapping to frontend nodes
        for fragment in table_fragment.fragments.values() {
            let mapping = fragment
                .vnode_mapping
                .clone()
                .expect("no data distribution found");
            let fragment_mapping = FragmentParallelUnitMapping {
                fragment_id: fragment.fragment_id,
                mapping: Some(mapping),
            };

            self.env
                .notification_manager()
                .notify_frontend(operation, Info::ParallelUnitMapping(fragment_mapping))
                .await;
        }

        // Update serving vnode mappings.
        let fragment_ids = table_fragment.fragment_ids().collect();
        match operation {
            Operation::Add | Operation::Update => {
                self.env
                    .notification_manager()
                    .notify_local_subscribers(LocalNotification::FragmentMappingsUpsert(
                        fragment_ids,
                    ))
                    .await;
            }
            Operation::Delete => {
                self.env
                    .notification_manager()
                    .notify_local_subscribers(LocalNotification::FragmentMappingsDelete(
                        fragment_ids,
                    ))
                    .await;
            }
            _ => {
                tracing::warn!("unexpected fragment mapping op");
            }
        }
    }

    pub async fn select_table_fragments_by_table_id(
        &self,
        table_id: &TableId,
    ) -> MetaResult<TableFragments> {
        let map = &self.core.read().await.table_fragments;
        if let Some(table_fragment) = map.get(table_id) {
            Ok(table_fragment.clone())
        } else {
            Err(MetaError::fragment_not_found(table_id.table_id))
        }
    }

    pub async fn select_table_fragments_by_ids(
        &self,
        table_ids: &[TableId],
    ) -> MetaResult<Vec<TableFragments>> {
        let map = &self.core.read().await.table_fragments;
        let mut table_fragments = Vec::with_capacity(table_ids.len());
        for table_id in table_ids {
            table_fragments.push(if let Some(table_fragment) = map.get(table_id) {
                table_fragment.clone()
            } else {
                return Err(MetaError::fragment_not_found(table_id.table_id));
            });
        }
        Ok(table_fragments)
    }

    pub async fn get_table_id_table_fragment_map(
        &self,
        table_ids: &[TableId],
    ) -> MetaResult<HashMap<TableId, TableFragments>> {
        let map = &self.core.read().await.table_fragments;
        let mut id_to_fragment = HashMap::new();
        for table_id in table_ids {
            let table_fragment = if let Some(table_fragment) = map.get(table_id) {
                table_fragment.clone()
            } else {
                return Err(MetaError::fragment_not_found(table_id.table_id));
            };
            id_to_fragment.insert(*table_id, table_fragment);
        }
        Ok(id_to_fragment)
    }

    /// Start create a new `TableFragments` and insert it into meta store, currently the actors'
    /// state is `ActorState::Inactive` and the table fragments' state is `State::Initial`.
    pub async fn start_create_table_fragments(
        &self,
        table_fragment: TableFragments,
    ) -> MetaResult<()> {
        let mut guard = self.core.write().await;
        let current_revision = guard.table_revision;
        let map = &mut guard.table_fragments;
        let table_id = table_fragment.table_id();
        if map.contains_key(&table_id) {
            bail!("table_fragment already exist: id={}", table_id);
        }

        let mut table_fragments = BTreeMapTransaction::new(map);
        table_fragments.insert(table_id, table_fragment);
        let mut trx = Transaction::default();

        let next_revision = current_revision.next();
        next_revision.store(&mut trx);
        commit_meta_with_trx!(self, trx, table_fragments)?;
        guard.table_revision = next_revision;
        Ok(())
    }

    /// Called after the barrier collection of `CreateStreamingJob` command, which updates the
    /// streaming job's state from `State::Initial` to `State::Creating`, updates the
    /// actors' state to `ActorState::Running`, besides also updates all dependent tables'
    /// downstream actors info.
    ///
    /// Note that the table fragments' state will be kept `Creating`, which is only updated when the
    /// streaming job is completely created.
    pub async fn post_create_table_fragments(
        &self,
        table_id: &TableId,
        dependent_table_actors: Vec<(TableId, HashMap<ActorId, Vec<Dispatcher>>)>,
        split_assignment: SplitAssignment,
    ) -> MetaResult<()> {
        let map = &mut self.core.write().await.table_fragments;

        let mut table_fragments = BTreeMapTransaction::new(map);
        let mut table_fragment = table_fragments
            .get_mut(*table_id)
            .with_context(|| format!("table_fragment not exist: id={}", table_id))?;

        assert_eq!(table_fragment.state(), State::Initial);
        table_fragment.set_state(State::Creating);
        table_fragment.update_actors_state(ActorState::Running);
        table_fragment.set_actor_splits_by_split_assignment(split_assignment);
        let table_fragment = table_fragment.clone();

        for (dependent_table_id, mut new_dispatchers) in dependent_table_actors {
            let mut dependent_table =
                table_fragments
                    .get_mut(dependent_table_id)
                    .with_context(|| {
                        format!(
                            "dependent table_fragment not exist: id={}",
                            dependent_table_id
                        )
                    })?;
            for fragment in dependent_table.fragments.values_mut() {
                for actor in &mut fragment.actors {
                    // Extend new dispatchers to table fragments.
                    if let Some(new_dispatchers) = new_dispatchers.remove(&actor.actor_id) {
                        actor.dispatcher.extend(new_dispatchers);
                    }
                }
            }
        }
        commit_meta!(self, table_fragments)?;
        self.notify_fragment_mapping(&table_fragment, Operation::Add)
            .await;

        Ok(())
    }

    /// Called after the barrier collection of `ReplaceTable` command, which replaces the fragments
    /// of this table, and updates the downstream Merge to have the new upstream fragments.
    pub async fn post_replace_table(
        &self,
        old_table_fragments: &TableFragments,
        new_table_fragments: &TableFragments,
        merge_updates: &[MergeUpdate],
        dispatchers: &HashMap<ActorId, Vec<Dispatcher>>,
    ) -> MetaResult<()> {
        let table_id = old_table_fragments.table_id();
        let dummy_table_id = new_table_fragments.table_id();

        let mut guard = self.core.write().await;
        let current_revision = guard.table_revision;
        let map = &mut guard.table_fragments;

        let mut table_fragments = BTreeMapTransaction::new(map);

        // FIXME: we use a dummy table ID for new table fragments, so we can drop the old fragments
        // with the real table ID, then replace the dummy table ID with the real table ID. This is a
        // workaround for not having the version info in the fragment manager.
        #[allow(unused_variables)]
        let old_table_fragment = table_fragments
            .remove(table_id)
            .with_context(|| format!("table_fragment not exist: id={}", table_id))?;
        let mut table_fragment = table_fragments
            .remove(dummy_table_id)
            .with_context(|| format!("table_fragment not exist: id={}", dummy_table_id))?;

        assert_eq!(table_fragment.state(), State::Initial);
        table_fragment.set_table_id(table_id);

        // Directly set to `Created` and `Running` state.
        table_fragment.set_state(State::Created);
        table_fragment.update_actors_state(ActorState::Running);

        table_fragments.insert(table_id, table_fragment.clone());

        // Update downstream `Merge`s.
        let mut merge_updates: HashMap<_, _> = merge_updates
            .iter()
            .map(|update| (update.actor_id, update))
            .collect();

        let to_update_merge_table_ids = table_fragments
            .tree_ref()
            .iter()
            .filter(|(_, v)| {
                v.actor_ids()
                    .iter()
                    .any(|&actor_id| merge_updates.contains_key(&actor_id))
            })
            .map(|(k, _)| *k)
            .collect::<Vec<_>>();

        for table_id in to_update_merge_table_ids {
            let mut table_fragment = table_fragments
                .get_mut(table_id)
                .with_context(|| format!("table_fragment not exist: id={}", table_id))?;

            for actor in table_fragment
                .fragments
                .values_mut()
                .flat_map(|f| &mut f.actors)
            {
                if let Some(merge_update) = merge_updates.remove(&actor.actor_id) {
                    assert!(merge_update.removed_upstream_actor_id.is_empty());
                    assert!(merge_update.new_upstream_fragment_id.is_some());

                    let stream_node = actor.nodes.as_mut().unwrap();
                    visit_stream_node(stream_node, |body| {
                        if let NodeBody::Merge(m) = body
                            && m.upstream_fragment_id == merge_update.upstream_fragment_id
                        {
                            m.upstream_fragment_id = merge_update.new_upstream_fragment_id.unwrap();
                            m.upstream_actor_id = merge_update.added_upstream_actor_id.clone();
                        }
                    });
                }
            }
        }

        assert!(merge_updates.is_empty());

        let dropped_actor_ids = old_table_fragments
            .actor_ids()
            .into_iter()
            .collect::<HashSet<_>>();

        let mut dispatchers = dispatchers.to_owned();

        let to_update_dispatcher_table_ids = table_fragments
            .tree_ref()
            .iter()
            .filter(|(_, v)| {
                v.actor_ids()
                    .iter()
                    .any(|&actor_id| dispatchers.contains_key(&actor_id))
            })
            .map(|(k, _)| *k)
            .collect::<Vec<_>>();

        for table_id in to_update_dispatcher_table_ids {
            let mut table_fragment = table_fragments
                .get_mut(table_id)
                .with_context(|| format!("table_fragment not exist: id={}", table_id))?;

            for actor in table_fragment
                .fragments
                .values_mut()
                .flat_map(|f| &mut f.actors)
            {
                for dispatcher in &mut actor.dispatcher {
                    dispatcher
                        .downstream_actor_id
                        .retain(|actor_id| !dropped_actor_ids.contains(actor_id))
                }
                actor
                    .dispatcher
                    .retain(|d| !d.downstream_actor_id.is_empty());

                if let Some(new_dispatchers) = dispatchers.remove(&actor.actor_id) {
                    actor.dispatcher.extend(new_dispatchers);
                }
            }
        }

        assert!(dispatchers.is_empty());

        // Commit changes and notify about the changes.
        let mut trx = Transaction::default();

        // save next revision
        let next_revision = current_revision.next();
        next_revision.store(&mut trx);

        // commit
        commit_meta_with_trx!(self, trx, table_fragments)?;

        // update revision in memory
        guard.table_revision = next_revision;

        // FIXME: Do not notify frontend currently, because frontend nodes might refer to old table
        // catalog and need to access the old fragment. Let frontend nodes delete the old fragment
        // when they receive table catalog change. self.notify_fragment_mapping(&
        // old_table_fragment, Operation::Delete)     .await;
        self.notify_fragment_mapping(&table_fragment, Operation::Add)
            .await;

        Ok(())
    }

    /// Called after the finish of `CreateStreamingJob` command, i.e., streaming job is
    /// completely created, which updates the state from `Creating` to `Created`.
    pub async fn mark_table_fragments_created(&self, table_id: TableId) -> MetaResult<()> {
        let map = &mut self.core.write().await.table_fragments;

        let mut table_fragments = BTreeMapTransaction::new(map);
        let mut table_fragment = table_fragments
            .get_mut(table_id)
            .with_context(|| format!("table_fragment not exist: id={}", table_id))?;

        assert_eq!(table_fragment.state(), State::Creating);
        table_fragment.set_state(State::Created);
        commit_meta!(self, table_fragments)
    }

    /// Drop table fragments info and remove downstream actor infos in fragments from its dependent
    /// tables.
    /// If table fragments already deleted, this should just be noop,
    /// the delete function (`table_fragments.remove`) will not return an error.
    pub async fn drop_table_fragments_vec(&self, table_ids: &HashSet<TableId>) -> MetaResult<()> {
        let mut guard = self.core.write().await;
        let current_revision = guard.table_revision;

        let map = &mut guard.table_fragments;
        let to_delete_table_fragments = table_ids
            .iter()
            .filter_map(|table_id| map.get(table_id).cloned())
            .collect_vec();

        let mut table_fragments = BTreeMapTransaction::new(map);
        for table_fragment in &to_delete_table_fragments {
            table_fragments.remove(table_fragment.table_id());
            let chain_actor_ids = table_fragment.chain_actor_ids();
            let dependent_table_ids = table_fragment.dependent_table_ids();
            for (dependent_table_id, _) in dependent_table_ids {
                if table_ids.contains(&dependent_table_id) {
                    continue;
                }
                let mut dependent_table = table_fragments
                    .get_mut(dependent_table_id)
                    .with_context(|| {
                        format!(
                            "dependent table_fragment not exist: id={}",
                            dependent_table_id
                        )
                    })?;

                dependent_table
                    .fragments
                    .values_mut()
                    .filter(|f| (f.get_fragment_type_mask() & FragmentTypeFlag::Mview as u32) != 0)
                    .flat_map(|f| &mut f.actors)
                    .for_each(|a| {
                        a.dispatcher.retain_mut(|d| {
                            d.downstream_actor_id
                                .retain(|x| !chain_actor_ids.contains(x));
                            !d.downstream_actor_id.is_empty()
                        })
                    });
            }
        }

        if table_ids.is_empty() {
            commit_meta!(self, table_fragments)?;
        } else {
            let mut trx = Transaction::default();
            let next_revision = current_revision.next();
            next_revision.store(&mut trx);
            commit_meta_with_trx!(self, trx, table_fragments)?;
            guard.table_revision = next_revision;
        }

        for table_fragments in to_delete_table_fragments {
            if table_fragments.state() != State::Initial {
                self.notify_fragment_mapping(&table_fragments, Operation::Delete)
                    .await;
            }
        }

        Ok(())
    }

    /// Used in [`crate::barrier::GlobalBarrierManager`], load all actor that need to be sent or
    /// collected
    pub async fn load_all_actors(
        &self,
        check_state: impl Fn(ActorState, TableId, ActorId) -> bool,
    ) -> ActorInfos {
        let mut actor_maps = HashMap::new();
        let mut barrier_inject_actor_maps = HashMap::new();

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

            let barrier_inject_actors = fragments.worker_barrier_inject_actor_states();
            for (worker_id, actor_states) in barrier_inject_actors {
                for (actor_id, actor_state) in actor_states {
                    if check_state(actor_state, fragments.table_id(), actor_id) {
                        barrier_inject_actor_maps
                            .entry(worker_id)
                            .or_insert_with(Vec::new)
                            .push(actor_id);
                    }
                }
            }
        }

        ActorInfos {
            actor_maps,
            barrier_inject_actor_maps,
        }
    }

    async fn migrate_fragment_actors_inner(
        &self,
        migration_plan: &MigrationPlan,
        table_id: TableId,
    ) -> MetaResult<()> {
        let core = &mut *self.core.write().await;
        let current_revision = &mut core.table_revision;
        let map = &mut core.table_fragments;
        let mut table_trx = BTreeMapTransaction::new(map);
        let mut table_fragment = table_trx
            .get_mut(table_id)
            .with_context(|| format!("table_fragment not exist: id={}", table_id))?;

        for status in table_fragment.actor_status.values_mut() {
            if let Some(pu) = &status.parallel_unit
                && migration_plan.parallel_unit_plan.contains_key(&pu.id)
            {
                status.parallel_unit = Some(migration_plan.parallel_unit_plan[&pu.id].clone());
            }
        }
        table_fragment.update_vnode_mapping(&migration_plan.parallel_unit_plan);
        let table_fragment = table_fragment.clone();
        let next_revision = current_revision.next();

        let mut trx = Transaction::default();
        next_revision.store(&mut trx);
        commit_meta_with_trx!(self, trx, table_trx)?;
        *current_revision = next_revision;

        self.notify_fragment_mapping(&table_fragment, Operation::Update)
            .await;

        Ok(())
    }

    /// Used in [`crate::barrier::GlobalBarrierManager`]
    /// migrate actors and update fragments one by one according to the migration plan.
    pub async fn migrate_fragment_actors(&self, migration_plan: &MigrationPlan) -> MetaResult<()> {
        let to_migrate_table_fragments = self
            .core
            .read()
            .await
            .table_fragments
            .values()
            .filter(|tf| {
                for status in tf.actor_status.values() {
                    if let Some(pu) = &status.parallel_unit
                        && migration_plan.parallel_unit_plan.contains_key(&pu.id)
                    {
                        return true;
                    }
                }
                false
            })
            .map(|tf| tf.table_id())
            .collect_vec();

        for table_id in to_migrate_table_fragments {
            self.migrate_fragment_actors_inner(migration_plan, table_id)
                .await?;
        }

        Ok(())
    }

    pub async fn all_worker_parallel_units(&self) -> HashMap<WorkerId, HashSet<ParallelUnitId>> {
        let mut all_worker_parallel_units = HashMap::new();
        let map = &self.core.read().await.table_fragments;
        for table_fragment in map.values() {
            table_fragment.worker_parallel_units().into_iter().for_each(
                |(worker_id, parallel_units)| {
                    all_worker_parallel_units
                        .entry(worker_id)
                        .or_insert_with(HashSet::new)
                        .extend(parallel_units);
                },
            );
        }

        all_worker_parallel_units
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

    pub async fn update_actor_splits_by_split_assignment(
        &self,
        split_assignment: &SplitAssignment,
    ) -> MetaResult<()> {
        let map = &mut self.core.write().await.table_fragments;
        let to_update_table_fragments: HashMap<TableId, HashMap<ActorId, Vec<SplitImpl>>> = map
            .values()
            .filter(|t| t.fragment_ids().any(|f| split_assignment.contains_key(&f)))
            .map(|f| {
                let mut actor_splits = HashMap::new();
                f.fragment_ids().for_each(|fragment_id| {
                    if let Some(splits) = split_assignment.get(&fragment_id).cloned() {
                        actor_splits.extend(splits);
                    }
                });
                (f.table_id(), actor_splits)
            })
            .collect();

        let mut table_fragments = BTreeMapTransaction::new(map);
        for (table_id, actor_splits) in to_update_table_fragments {
            let mut table_fragment = table_fragments.get_mut(table_id).unwrap();
            table_fragment.actor_splits.extend(actor_splits);
        }
        commit_meta!(self, table_fragments)
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

    /// Add the newly added Actor to the `FragmentManager`
    pub async fn pre_apply_reschedules(
        &self,
        mut created_actors: HashMap<FragmentId, HashMap<ActorId, (StreamActor, ActorStatus)>>,
    ) -> HashMap<FragmentId, HashSet<ActorId>> {
        let map = &mut self.core.write().await.table_fragments;

        let mut applied_reschedules = HashMap::new();

        for table_fragments in map.values_mut() {
            let mut updated_actor_status = HashMap::new();

            for (fragment_id, fragment) in &mut table_fragments.fragments {
                if let Some(fragment_create_actors) = created_actors.remove(fragment_id) {
                    applied_reschedules
                        .entry(*fragment_id)
                        .or_insert_with(HashSet::new)
                        .extend(fragment_create_actors.keys());

                    for (actor_id, (actor, actor_status)) in fragment_create_actors {
                        fragment.actors.push(actor);
                        updated_actor_status.insert(actor_id, actor_status);
                    }
                }
            }

            table_fragments.actor_status.extend(updated_actor_status);
        }

        applied_reschedules
    }

    /// Undo the changes in `pre_apply_reschedules`
    pub async fn cancel_apply_reschedules(
        &self,
        applied_reschedules: HashMap<FragmentId, HashSet<ActorId>>,
    ) {
        let map = &mut self.core.write().await.table_fragments;
        for table_fragments in map.values_mut() {
            for (fragment_id, fragment) in &mut table_fragments.fragments {
                if let Some(fragment_create_actors) = applied_reschedules.get(fragment_id) {
                    table_fragments
                        .actor_status
                        .retain(|actor_id, _| !fragment_create_actors.contains(actor_id));
                    fragment
                        .actors
                        .retain(|actor| !fragment_create_actors.contains(&actor.actor_id));
                }
            }
        }
    }

    /// Apply `Reschedule`s to fragments.
    pub async fn post_apply_reschedules(
        &self,
        mut reschedules: HashMap<FragmentId, Reschedule>,
    ) -> MetaResult<()> {
        let mut guard = self.core.write().await;
        let current_version = guard.table_revision;

        let map = &mut guard.table_fragments;

        fn update_actors(
            actors: &mut Vec<ActorId>,
            to_remove: &HashSet<ActorId>,
            to_create: &[ActorId],
        ) {
            let actor_id_set: HashSet<_> = actors.iter().copied().collect();
            for actor_id in to_create {
                assert!(!actor_id_set.contains(actor_id));
            }
            for actor_id in to_remove {
                assert!(actor_id_set.contains(actor_id));
            }

            actors.retain(|actor_id| !to_remove.contains(actor_id));
            actors.extend_from_slice(to_create);
        }

        fn update_merge_node_upstream(
            stream_node: &mut StreamNode,
            upstream_fragment_id: &FragmentId,
            upstream_actors_to_remove: &HashSet<ActorId>,
            upstream_actors_to_create: &[ActorId],
        ) {
            visit_stream_node(stream_node, |body| {
                if let NodeBody::Merge(s) = body {
                    if s.upstream_fragment_id == *upstream_fragment_id {
                        update_actors(
                            s.upstream_actor_id.as_mut(),
                            upstream_actors_to_remove,
                            upstream_actors_to_create,
                        );
                    }
                }
            });
        }

        let new_created_actors: HashSet<_> = reschedules
            .values()
            .flat_map(|reschedule| reschedule.added_actors.clone())
            .collect();

        let to_update_table_fragments = map
            .values()
            .filter(|t| t.fragment_ids().any(|f| reschedules.contains_key(&f)))
            .map(|t| t.table_id())
            .collect_vec();
        let mut table_fragments = BTreeMapTransaction::new(map);
        let mut fragment_mapping_to_notify = vec![];

        for table_id in to_update_table_fragments {
            // Takes out the reschedules of the fragments in this table.
            let reschedules = reschedules
                .extract_if(|fragment_id, _| {
                    table_fragments
                        .get(&table_id)
                        .unwrap()
                        .fragments
                        .contains_key(fragment_id)
                })
                .collect_vec();

            for (fragment_id, reschedule) in reschedules {
                let Reschedule {
                    added_actors,
                    removed_actors,
                    vnode_bitmap_updates,
                    upstream_fragment_dispatcher_ids,
                    upstream_dispatcher_mapping,
                    downstream_fragment_ids,
                    actor_splits,
                } = reschedule;

                let mut table_fragment = table_fragments.get_mut(table_id).unwrap();

                // First step, update self fragment
                // Add actors to this fragment: set the state to `Running`.
                for actor_id in &added_actors {
                    table_fragment
                        .actor_status
                        .get_mut(actor_id)
                        .unwrap()
                        .set_state(ActorState::Running);
                }

                // Remove actors from this fragment.
                let removed_actor_ids: HashSet<_> = removed_actors.iter().cloned().collect();

                for actor_id in &removed_actor_ids {
                    table_fragment.actor_status.remove(actor_id);
                    table_fragment.actor_splits.remove(actor_id);
                }

                table_fragment.actor_splits.extend(actor_splits);

                let actor_status = table_fragment.actor_status.clone();
                let fragment = table_fragment.fragments.get_mut(&fragment_id).unwrap();

                fragment
                    .actors
                    .retain(|a| !removed_actor_ids.contains(&a.actor_id));

                // update vnode mapping for actors.
                for actor in &mut fragment.actors {
                    if let Some(bitmap) = vnode_bitmap_updates.get(&actor.actor_id) {
                        actor.vnode_bitmap = Some(bitmap.to_protobuf());
                    }
                }

                // update fragment's vnode mapping
                let mut actor_to_parallel_unit = HashMap::with_capacity(fragment.actors.len());
                let mut actor_to_vnode_bitmap = HashMap::with_capacity(fragment.actors.len());
                for actor in &fragment.actors {
                    let actor_status = &actor_status[&actor.actor_id];
                    let parallel_unit_id = actor_status.parallel_unit.as_ref().unwrap().id;
                    actor_to_parallel_unit.insert(actor.actor_id, parallel_unit_id);

                    if let Some(vnode_bitmap) = &actor.vnode_bitmap {
                        let bitmap = Bitmap::from(vnode_bitmap);
                        actor_to_vnode_bitmap.insert(actor.actor_id, bitmap);
                    }
                }

                let vnode_mapping = if actor_to_vnode_bitmap.is_empty() {
                    // If there's no `vnode_bitmap`, then the fragment must be a singleton fragment.
                    // We directly use the single parallel unit to construct the mapping.
                    // TODO: also fill `vnode_bitmap` for the actor of singleton fragment so that we
                    // don't need this branch.
                    let parallel_unit = *actor_to_parallel_unit.values().exactly_one().unwrap();
                    ParallelUnitMapping::new_single(parallel_unit)
                } else {
                    // Generate the parallel unit mapping from the fragment's actor bitmaps.
                    assert_eq!(actor_to_vnode_bitmap.len(), actor_to_parallel_unit.len());
                    ActorMapping::from_bitmaps(&actor_to_vnode_bitmap)
                        .to_parallel_unit(&actor_to_parallel_unit)
                }
                .to_protobuf();

                *fragment.vnode_mapping.as_mut().unwrap() = vnode_mapping.clone();

                // Notify fragment mapping to frontend nodes.
                let fragment_mapping = FragmentParallelUnitMapping {
                    fragment_id: fragment_id as FragmentId,
                    mapping: Some(vnode_mapping),
                };
                fragment_mapping_to_notify.push(fragment_mapping);

                // Second step, update upstream fragments
                // Update the dispatcher of the upstream fragments.
                for (upstream_fragment_id, dispatcher_id) in upstream_fragment_dispatcher_ids {
                    // here we assume the upstream fragment is in the same streaming job as this
                    // fragment. Cross-table references only occur in the case
                    // of Chain fragment, and the scale of Chain fragment does not introduce updates
                    // to the upstream Fragment (because of NoShuffle)
                    let upstream_fragment = table_fragment
                        .fragments
                        .get_mut(&upstream_fragment_id)
                        .unwrap();

                    for upstream_actor in &mut upstream_fragment.actors {
                        if new_created_actors.contains(&upstream_actor.actor_id) {
                            continue;
                        }

                        for dispatcher in &mut upstream_actor.dispatcher {
                            if dispatcher.dispatcher_id == dispatcher_id {
                                if let DispatcherType::Hash = dispatcher.r#type() {
                                    dispatcher.hash_mapping = upstream_dispatcher_mapping
                                        .as_ref()
                                        .map(|m| m.to_protobuf());
                                }

                                update_actors(
                                    dispatcher.downstream_actor_id.as_mut(),
                                    &removed_actor_ids,
                                    &added_actors,
                                );
                            }
                        }
                    }
                }

                // Update the merge executor of the downstream fragment.
                for &downstream_fragment_id in &downstream_fragment_ids {
                    let downstream_fragment = table_fragment
                        .fragments
                        .get_mut(&downstream_fragment_id)
                        .unwrap();
                    for downstream_actor in &mut downstream_fragment.actors {
                        if new_created_actors.contains(&downstream_actor.actor_id) {
                            continue;
                        }

                        update_actors(
                            downstream_actor.upstream_actor_id.as_mut(),
                            &removed_actor_ids,
                            &added_actors,
                        );

                        if let Some(node) = downstream_actor.nodes.as_mut() {
                            update_merge_node_upstream(
                                node,
                                &fragment_id,
                                &removed_actor_ids,
                                &added_actors,
                            );
                        }
                    }
                }
            }
        }

        assert!(reschedules.is_empty(), "all reschedules must be applied");

        // new empty transaction
        let mut trx = Transaction::default();

        // save next revision
        let next_revision = current_version.next();
        next_revision.store(&mut trx);

        // commit
        commit_meta_with_trx!(self, trx, table_fragments)?;

        // update revision in memory
        guard.table_revision = next_revision;

        for mapping in fragment_mapping_to_notify {
            self.env
                .notification_manager()
                .notify_frontend(Operation::Update, Info::ParallelUnitMapping(mapping))
                .await;
        }

        Ok(())
    }

    pub async fn table_node_actors(
        &self,
        table_ids: &HashSet<TableId>,
    ) -> MetaResult<BTreeMap<WorkerId, Vec<ActorId>>> {
        let map = &self.core.read().await.table_fragments;
        let table_fragments_vec = table_ids
            .iter()
            .map(|table_id| {
                map.get(table_id)
                    .ok_or_else(|| anyhow!("table_fragment not exist: id={}", table_id).into())
            })
            .collect::<MetaResult<Vec<_>>>()?;
        Ok(table_fragments_vec
            .iter()
            .map(|table_fragments| table_fragments.worker_actor_ids())
            .reduce(|mut btree_map, next_map| {
                next_map.into_iter().for_each(|(k, v)| {
                    btree_map.entry(k).or_default().extend(v);
                });
                btree_map
            })
            .unwrap())
    }

    pub async fn get_table_actor_ids(
        &self,
        table_ids: &HashSet<TableId>,
    ) -> MetaResult<Vec<ActorId>> {
        let map = &self.core.read().await.table_fragments;
        table_ids
            .iter()
            .map(|table_id| {
                map.get(table_id)
                    .map(|table_fragment| table_fragment.actor_ids())
                    .ok_or_else(|| anyhow!("table_fragment not exist: id={}", table_id).into())
            })
            .flatten_ok()
            .collect::<MetaResult<Vec<_>>>()
    }

    #[cfg(test)]
    pub async fn get_table_mview_actor_ids(&self, table_id: &TableId) -> MetaResult<Vec<ActorId>> {
        let map = &self.core.read().await.table_fragments;
        Ok(map
            .get(table_id)
            .with_context(|| format!("table_fragment not exist: id={}", table_id))?
            .mview_actor_ids())
    }

    /// Get and filter the upstream `Materialize` or `Source` fragments of the specified relations.
    pub async fn get_upstream_root_fragments(
        &self,
        upstream_table_ids: &HashSet<TableId>,
        table_job_type: Option<TableJobType>,
    ) -> MetaResult<HashMap<TableId, Fragment>> {
        let map = &self.core.read().await.table_fragments;
        let mut fragments = HashMap::new();

        for &table_id in upstream_table_ids {
            let table_fragments = map
                .get(&table_id)
                .with_context(|| format!("table_fragment not exist: id={}", table_id))?;
            match table_job_type.as_ref() {
                Some(TableJobType::SharedCdcSource) => {
                    if let Some(fragment) = table_fragments.source_fragment() {
                        fragments.insert(table_id, fragment);
                    }
                }
                // MV on MV, and other kinds of table job
                None | Some(TableJobType::General) | Some(TableJobType::Unspecified) => {
                    if let Some(fragment) = table_fragments.mview_fragment() {
                        fragments.insert(table_id, fragment);
                    }
                }
            }
        }

        Ok(fragments)
    }

    /// Get the downstream `Chain` fragments of the specified table.
    pub async fn get_downstream_chain_fragments(
        &self,
        table_id: TableId,
    ) -> MetaResult<Vec<(DispatchStrategy, Fragment)>> {
        let map = &self.core.read().await.table_fragments;

        let table_fragments = map
            .get(&table_id)
            .with_context(|| format!("table_fragment not exist: id={}", table_id))?;

        let mview_fragment = table_fragments.mview_fragment().unwrap();
        let downstream_dispatches: HashMap<_, _> = mview_fragment.actors[0]
            .dispatcher
            .iter()
            .map(|d| {
                let fragment_id = d.dispatcher_id as FragmentId;
                let strategy = DispatchStrategy {
                    r#type: d.r#type,
                    dist_key_indices: d.dist_key_indices.clone(),
                    output_indices: d.output_indices.clone(),
                    downstream_table_name: d.downstream_table_name.clone(),
                };
                (fragment_id, strategy)
            })
            .collect();

        // Find the fragments based on the fragment ids.
        let fragments = map
            .values()
            .flat_map(|table_fragments| {
                table_fragments
                    .fragments
                    .values()
                    .filter_map(|fragment| {
                        downstream_dispatches
                            .get(&fragment.fragment_id)
                            .map(|d| (d.clone(), fragment.clone()))
                    })
                    .inspect(|(_, f)| {
                        assert!((f.fragment_type_mask & FragmentTypeFlag::ChainNode as u32) != 0)
                    })
            })
            .collect_vec();

        assert_eq!(downstream_dispatches.len(), fragments.len());

        Ok(fragments)
    }

    /// Get the `Materialize` fragment of the specified table.
    pub async fn get_mview_fragment(&self, table_id: TableId) -> MetaResult<Fragment> {
        let map = &self.core.read().await.table_fragments;

        let table_fragments = map
            .get(&table_id)
            .with_context(|| format!("table_fragment not exist: id={}", table_id))?;
        let mview_fragment = table_fragments
            .mview_fragment()
            .with_context(|| format!("mview fragment not exist: id={}", table_id))?;

        Ok(mview_fragment)
    }

    pub async fn running_fragment_parallelisms(
        &self,
        id_filter: Option<HashSet<FragmentId>>,
    ) -> HashMap<FragmentId, usize> {
        self.core
            .read()
            .await
            .running_fragment_parallelisms(id_filter)
    }
}
