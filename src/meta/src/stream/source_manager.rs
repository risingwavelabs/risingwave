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

mod split_assignment;
mod worker;
use std::borrow::BorrowMut;
use std::cmp::Ordering;
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use risingwave_common::catalog::DatabaseId;
use risingwave_common::metrics::LabelGuardedIntGauge;
use risingwave_common::panic_if_debug;
use risingwave_connector::WithOptionsSecResolved;
use risingwave_connector::error::ConnectorResult;
use risingwave_connector::source::{
    ConnectorProperties, SourceEnumeratorContext, SourceEnumeratorInfo, SplitId, SplitImpl,
    SplitMetaData, fill_adaptive_split,
};
use risingwave_meta_model::SourceId;
use risingwave_pb::catalog::Source;
use risingwave_pb::source::{ConnectorSplit, ConnectorSplits};
use thiserror_ext::AsReport;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::{Mutex, MutexGuard, oneshot};
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;
use tokio::{select, time};
pub use worker::create_source_worker;
use worker::{ConnectorSourceWorkerHandle, create_source_worker_async};

use crate::MetaResult;
use crate::barrier::{BarrierScheduler, Command, ReplaceStreamJobPlan};
use crate::controller::catalog::SourceFragmentDiff;
use crate::manager::MetadataManager;
use crate::model::{ActorId, FragmentId};
use crate::rpc::metrics::MetaMetrics;

pub type SourceManagerRef = Arc<SourceManager>;
pub type SplitAssignment = HashMap<FragmentId, HashMap<ActorId, Vec<SplitImpl>>>;
pub type ThrottleConfig = HashMap<FragmentId, HashMap<ActorId, Option<u32>>>;
// ALTER CONNECTOR parameters, specifying the new parameters to be set for each job_id (source_id/sink_id)
pub type ConnectorPropsChange = HashMap<u32, HashMap<String, String>>;

const DEFAULT_SOURCE_TICK_TIMEOUT: Duration = Duration::from_secs(10);

/// `SourceManager` keeps fetching the latest split metadata from the external source services ([`worker::ConnectorSourceWorker::tick`]),
/// and sends a split assignment command if split changes detected ([`Self::tick`]).
pub struct SourceManager {
    pub paused: Mutex<()>,
    barrier_scheduler: BarrierScheduler,
    core: Mutex<SourceManagerCore>,
    pub metrics: Arc<MetaMetrics>,
}
pub struct SourceManagerCore {
    metadata_manager: MetadataManager,

    cached_fragment_ids: HashSet<i32>,

    /// Managed source loops
    managed_sources: HashMap<SourceId, ConnectorSourceWorkerHandle>,
    /// Fragments associated with each source
    source_fragments: HashMap<SourceId, BTreeSet<FragmentId>>,
    /// `source_id` -> `(fragment_id, upstream_fragment_id)`
    backfill_fragments: HashMap<SourceId, BTreeSet<(FragmentId, FragmentId)>>,
}

pub struct SourceManagerRunningInfo {
    pub source_fragments: HashMap<SourceId, BTreeSet<FragmentId>>,
    pub backfill_fragments: HashMap<SourceId, BTreeSet<(FragmentId, FragmentId)>>,
    pub actor_splits: HashMap<ActorId, Vec<SplitImpl>>,
}

impl SourceManagerCore {
    fn new(
        metadata_manager: MetadataManager,
        managed_sources: HashMap<SourceId, ConnectorSourceWorkerHandle>,
        source_fragments: HashMap<SourceId, BTreeSet<FragmentId>>,
        backfill_fragments: HashMap<SourceId, BTreeSet<(FragmentId, FragmentId)>>,
    ) -> Self {
        Self {
            metadata_manager,
            cached_fragment_ids: Default::default(),
            managed_sources,
            source_fragments,
            backfill_fragments,
        }
    }

    pub async fn sync_fragments(&mut self) -> MetaResult<()> {
        let SourceFragmentDiff {
            dropped_fragments,
            created_source_fragments,
            created_source_backfill_fragments,
            created_ids,
        } = self
            .metadata_manager
            .catalog_controller
            .diff_source_fragments(&self.cached_fragment_ids)
            .await?;

        self.drop_source_fragments(
            None,
            BTreeSet::from_iter(dropped_fragments.iter().cloned().map(|f| f as _)),
        );

        for fragment_id in dropped_fragments {
            if !self.cached_fragment_ids.remove(&fragment_id) {
                tracing::warn!("fragment {} is not in the cached fragment ids", fragment_id);
            }
        }

        for fragment_id in created_ids {
            if !self.cached_fragment_ids.insert(fragment_id) {
                tracing::warn!(
                    "fragment {} is already in the cached fragment ids",
                    fragment_id
                );
            }
        }

        for (fragment_id, source_id) in created_source_fragments {
            self.source_fragments
                .entry(source_id)
                .or_default()
                .insert(fragment_id as FragmentId);
        }

        for ((fragment_id, upstream_fragment_id), source_id) in created_source_backfill_fragments {
            self.backfill_fragments
                .entry(source_id)
                .or_default()
                .insert((
                    fragment_id as FragmentId,
                    upstream_fragment_id as FragmentId,
                ));
        }

        Ok(())
    }

    /// Updates states after all kinds of source change.
    pub fn apply_source_change(&mut self, source_change: SourceChange) {
        let mut finished_backfill_fragments = Default::default();
        let mut fragment_replacements = Default::default();
        let mut dropped_source_ids = Default::default();
        let mut recreate_source_id_map_new_props: Vec<(u32, HashMap<String, String>)> =
            Default::default();

        match source_change {
            SourceChange::CreateJobFinished {
                finished_backfill_fragments: finished_backfill_fragments_,
            } => {
                finished_backfill_fragments = finished_backfill_fragments_;
            }

            SourceChange::ReplaceJob {
                fragment_replacements: fragment_replacements_,
            } => {
                fragment_replacements = fragment_replacements_;
            }
            SourceChange::DropSource {
                dropped_source_ids: dropped_source_ids_,
            } => {
                dropped_source_ids = dropped_source_ids_;
            }
            SourceChange::UpdateSourceProps {
                source_id_map_new_props,
            } => {
                for (source_id, new_props) in source_id_map_new_props {
                    recreate_source_id_map_new_props.push((source_id, new_props));
                }
            }
        }

        for source_id in dropped_source_ids {
            let dropped_fragments = self.source_fragments.remove(&source_id);

            if let Some(handle) = self.managed_sources.remove(&source_id) {
                handle.terminate(dropped_fragments);
            }
            if let Some(_fragments) = self.backfill_fragments.remove(&source_id) {
                // TODO: enable this assertion after we implemented cleanup for backfill fragments
                // debug_assert!(
                //     fragments.is_empty(),
                //     "when dropping source, there should be no backfill fragments, got: {:?}",
                //     fragments
                // );
            }
        }

        for (source_id, fragments) in finished_backfill_fragments {
            let handle = self.managed_sources.get(&source_id).unwrap_or_else(|| {
                panic!(
                    "source {} not found when adding backfill fragments {:?}",
                    source_id, fragments
                );
            });
            handle.finish_backfill(fragments.iter().map(|(id, _up_id)| *id).collect());
        }

        for (old_fragment_id, new_fragment_id) in fragment_replacements {
            // TODO: add source_id to the fragment_replacements to avoid iterating all sources
            self.drop_source_fragments(None, BTreeSet::from([old_fragment_id]));

            for fragment_ids in self.backfill_fragments.values_mut() {
                let mut new_backfill_fragment_ids = fragment_ids.clone();
                for (fragment_id, upstream_fragment_id) in fragment_ids.iter() {
                    assert_ne!(
                        fragment_id, upstream_fragment_id,
                        "backfill fragment should not be replaced"
                    );
                    if *upstream_fragment_id == old_fragment_id {
                        new_backfill_fragment_ids.remove(&(*fragment_id, *upstream_fragment_id));
                        new_backfill_fragment_ids.insert((*fragment_id, new_fragment_id));
                    }
                }
                *fragment_ids = new_backfill_fragment_ids;
            }
        }

        for (source_id, new_props) in recreate_source_id_map_new_props {
            tracing::info!("recreate source {source_id} in source manager");
            if let Some(handle) = self.managed_sources.get_mut(&(source_id as _)) {
                // the update here should not involve fragments change and split change
                // Or we need to drop and recreate the source worker instead of updating inplace
                let props_wrapper =
                    WithOptionsSecResolved::without_secrets(new_props.into_iter().collect());
                let props = ConnectorProperties::extract(props_wrapper, false).unwrap(); // already checked when sending barrier
                handle.update_props(props);
            }
        }
    }

    fn drop_source_fragments(
        &mut self,
        source_id: Option<SourceId>,
        dropped_fragment_ids: BTreeSet<FragmentId>,
    ) {
        if let Some(source_id) = source_id {
            if let Entry::Occupied(mut entry) = self.source_fragments.entry(source_id) {
                let mut dropped_ids = vec![];
                let managed_fragment_ids = entry.get_mut();
                for fragment_id in &dropped_fragment_ids {
                    managed_fragment_ids.remove(fragment_id);
                    dropped_ids.push(*fragment_id);
                }
                if let Some(handle) = self.managed_sources.get(&source_id) {
                    handle.drop_fragments(dropped_ids);
                } else {
                    panic_if_debug!(
                        "source {source_id} not found when dropping fragment {dropped_ids:?}",
                    );
                }
                if managed_fragment_ids.is_empty() {
                    entry.remove();
                }
            }
        } else {
            self.source_fragments
                .retain(|source_id, fragment_ids| {
                    let mut dropped_ids_for_this_source = vec![];

                    fragment_ids.retain(|fragment_id| {
                        if dropped_fragment_ids.contains(fragment_id) {
                            dropped_ids_for_this_source.push(*fragment_id);
                            false
                        } else {
                            true
                        }
                    });

                    // If we dropped any fragments for this source, notify the managed source.
                    if !dropped_ids_for_this_source.is_empty() {
                        if let Some(handle) = self.managed_sources.get(source_id) {
                            handle.drop_fragments(dropped_ids_for_this_source);
                        } else {
                            panic_if_debug!(
                                "source {source_id} not found when dropping fragment {dropped_ids_for_this_source:?}",
                            );
                        }
                    }

                    // The key logic for the outer `retain`:
                    // If the `fragment_ids` vector is now empty, this closure returns `false`,
                    // which tells `self.source_fragments.retain` to REMOVE this source_id.
                    // Otherwise, it returns `true`, and the source_id is KEPT.
                    !fragment_ids.is_empty()
                });
        }
    }
}

impl SourceManager {
    const DEFAULT_SOURCE_TICK_INTERVAL: Duration = Duration::from_secs(10);

    pub async fn new(
        barrier_scheduler: BarrierScheduler,
        metadata_manager: MetadataManager,
        metrics: Arc<MetaMetrics>,
    ) -> MetaResult<Self> {
        let mut managed_sources = HashMap::new();
        {
            let sources = metadata_manager.list_sources().await?;
            for source in sources {
                create_source_worker_async(source, &mut managed_sources, metrics.clone())?
            }
        }

        let source_fragments = metadata_manager
            .catalog_controller
            .load_source_fragment_ids()
            .await?
            .into_iter()
            .map(|(source_id, fragment_ids)| {
                (
                    source_id as SourceId,
                    fragment_ids.into_iter().map(|id| id as _).collect(),
                )
            })
            .collect();
        let backfill_fragments = metadata_manager
            .catalog_controller
            .load_backfill_fragment_ids()
            .await?
            .into_iter()
            .map(|(source_id, fragment_ids)| {
                (
                    source_id as SourceId,
                    fragment_ids
                        .into_iter()
                        .map(|(id, up_id)| (id as _, up_id as _))
                        .collect(),
                )
            })
            .collect();

        let core = Mutex::new(SourceManagerCore::new(
            metadata_manager,
            managed_sources,
            source_fragments,
            backfill_fragments,
        ));

        Ok(Self {
            barrier_scheduler,
            core,
            paused: Mutex::new(()),
            metrics,
        })
    }

    pub async fn validate_source_once(
        &self,
        source_id: u32,
        new_source_props: WithOptionsSecResolved,
    ) -> MetaResult<()> {
        let props = ConnectorProperties::extract(new_source_props, false).unwrap();

        {
            let mut enumerator = props
                .create_split_enumerator(Arc::new(SourceEnumeratorContext {
                    metrics: self.metrics.source_enumerator_metrics.clone(),
                    info: SourceEnumeratorInfo { source_id },
                }))
                .await
                .context("failed to create SplitEnumerator")?;

            let _ = tokio::time::timeout(DEFAULT_SOURCE_TICK_TIMEOUT, enumerator.list_splits())
                .await
                .context("failed to list splits")??;
        }
        Ok(())
    }

    /// For replacing job (alter table/source, create sink into table).
    #[await_tree::instrument]
    pub async fn handle_replace_job(&self, replace_plan: &ReplaceStreamJobPlan) {
        self.apply_source_change(SourceChange::ReplaceJob {
            fragment_replacements: replace_plan.fragment_replacements(),
        })
        .await;
    }

    /// Updates states after all kinds of source change.
    /// e.g., split change (`post_collect` barrier) or scaling (`post_apply_reschedule`).
    #[await_tree::instrument("apply_source_change({source_change})")]
    pub async fn apply_source_change(&self, source_change: SourceChange) {
        let mut core = self.core.lock().await;
        core.apply_source_change(source_change);
    }

    /// create and register connector worker for source.
    #[await_tree::instrument("register_source({})", source.name)]
    pub async fn register_source(&self, source: &Source) -> MetaResult<()> {
        tracing::debug!("register_source: {}", source.get_id());
        let mut core = self.core.lock().await;
        if let Entry::Vacant(e) = core.managed_sources.entry(source.get_id() as _) {
            let handle = create_source_worker(source, self.metrics.clone())
                .await
                .context("failed to create source worker")?;
            e.insert(handle);
        } else {
            tracing::warn!("source {} already registered", source.get_id());
        }
        Ok(())
    }

    /// register connector worker for source.
    pub async fn register_source_with_handle(
        &self,
        source_id: SourceId,
        handle: ConnectorSourceWorkerHandle,
    ) {
        let mut core = self.core.lock().await;
        if let Entry::Vacant(e) = core.managed_sources.entry(source_id) {
            e.insert(handle);
        } else {
            tracing::warn!("source {} already registered", source_id);
        }
    }

    pub async fn list_assignments_from_db(&self) -> MetaResult<HashMap<ActorId, Vec<SplitImpl>>> {
        let core = self.core.lock().await;
        let splits = core
            .metadata_manager
            .catalog_controller
            .list_all_actor_splits()
            .await?;

        let splits = splits
            .into_iter()
            .map(|(actor_id, splits)| (actor_id as _, splits))
            .collect();

        Ok(splits)
    }

    pub async fn get_running_info_from_db(&self) -> MetaResult<SourceManagerRunningInfo> {
        let core = self.core.lock().await;
        Ok(SourceManagerRunningInfo {
            source_fragments: core.source_fragments.clone(),
            backfill_fragments: core.backfill_fragments.clone(),
            actor_splits: self.list_assignments_from_db().await?,
        })
    }

    /// Checks whether the external source metadata has changed, and sends a split assignment command
    /// if it has.
    ///
    /// This is also how a newly created `SourceExecutor` is initialized.
    /// (force `tick` in `Self::create_source_worker`)
    ///
    /// The command will first updates `SourceExecutor`'s splits, and finally calls `Self::apply_source_change`
    /// to update states in `SourceManager`.
    async fn tick(&self) -> MetaResult<()> {
        let split_assignment = {
            let mut core_guard = self.core.lock().await;
            core_guard.sync_fragments().await?;
            core_guard.reassign_splits().await?
        };

        for (database_id, split_assignment) in split_assignment {
            if !split_assignment.is_empty() {
                let command = Command::SourceChangeSplit(split_assignment);
                tracing::info!(command = ?command, "pushing down split assignment command");
                self.barrier_scheduler
                    .run_command(database_id, command)
                    .await?;
            }
        }

        Ok(())
    }

    pub async fn run(&self) -> MetaResult<()> {
        let mut ticker = time::interval(Self::DEFAULT_SOURCE_TICK_INTERVAL);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            let _pause_guard = self.paused.lock().await;
            if let Err(e) = self.tick().await {
                tracing::error!(
                    error = %e.as_report(),
                    "error happened while running source manager tick",
                );
            }
        }
    }

    /// Pause the tick loop in source manager until the returned guard is dropped.
    pub async fn pause_tick(&self) -> MutexGuard<'_, ()> {
        tracing::debug!("pausing tick lock in source manager");
        self.paused.lock().await
    }
}

#[derive(strum::Display)]
pub enum SourceChange {
    UpdateSourceProps {
        // the new properties to be set for each source_id
        // and the props should not affect split assignment and fragments
        source_id_map_new_props: HashMap<u32, HashMap<String, String>>,
    },
    /// `CREATE SOURCE` (shared), or `CREATE MV` is _finished_ (backfill is done).
    /// This is applied after `wait_streaming_job_finished`.
    /// XXX: Should we merge `CreateJob` into this?
    CreateJobFinished {
        /// (`source_id`, -> (`source_backfill_fragment_id`, `upstream_source_fragment_id`))
        finished_backfill_fragments: HashMap<SourceId, BTreeSet<(FragmentId, FragmentId)>>,
    },
    /// `DROP SOURCE` or `DROP MV`
    DropSource { dropped_source_ids: Vec<SourceId> },

    ReplaceJob {
        fragment_replacements: HashMap<FragmentId, FragmentId>,
    },
}

pub fn build_actor_connector_splits(
    splits: &HashMap<ActorId, Vec<SplitImpl>>,
) -> HashMap<u32, ConnectorSplits> {
    splits
        .iter()
        .map(|(&actor_id, splits)| {
            (
                actor_id,
                ConnectorSplits {
                    splits: splits.iter().map(ConnectorSplit::from).collect(),
                },
            )
        })
        .collect()
}

pub fn build_actor_split_impls(
    actor_splits: &HashMap<u32, ConnectorSplits>,
) -> HashMap<ActorId, Vec<SplitImpl>> {
    actor_splits
        .iter()
        .map(|(actor_id, ConnectorSplits { splits })| {
            (
                *actor_id,
                splits
                    .iter()
                    .map(|split| SplitImpl::try_from(split).unwrap())
                    .collect(),
            )
        })
        .collect()
}
