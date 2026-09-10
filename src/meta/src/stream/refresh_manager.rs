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

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::anyhow;
use chrono::{DateTime, Duration as ChronoDuration, NaiveDateTime, Utc};
use parking_lot::Mutex;
use risingwave_common::catalog::{DatabaseId, FragmentTypeFlag, TableId};
use risingwave_common::metrics::LabelGuardedIntCounter;
use risingwave_common::util::epoch::Epoch;
use risingwave_meta_model::ActorId;
use risingwave_meta_model::refresh_job::{self, RefreshState};
use risingwave_pb::catalog::table::OptionalAssociatedSourceId;
use risingwave_pb::id::SourceId;
use risingwave_pb::meta::{RefreshRequest, RefreshResponse};
use thiserror_ext::AsReport;
use tokio::sync::{Notify, oneshot};
use tokio::task::JoinHandle;

use super::ScaleControllerRef;
use crate::barrier::{BarrierScheduler, Command, SharedActorInfos};
use crate::manager::{MetaSrvEnv, MetadataManager};
use crate::rpc::metrics::GLOBAL_META_METRICS;
use crate::{MetaError, MetaResult};

pub type GlobalRefreshManagerRef = Arc<GlobalRefreshManager>;

/// Owns the lifecycle of `FULL_RELOAD` refresh cycles.
///
/// The persisted `refresh_job` row is the source of truth: a cycle is identified by its
/// `last_trigger_time` and every transition is a conditional update naming that cycle. In-memory
/// trackers only exist for cycles started by this meta process, so a job that is not idle and has
/// no tracker was abandoned by a recovery and only needs its staging table truncated.
pub struct GlobalRefreshManager {
    metadata_manager: MetadataManager,
    barrier_scheduler: BarrierScheduler,
    shared_actor_infos: SharedActorInfos,
    scale_controller: ScaleControllerRef,
    /// Serializes cycle transitions so that the job status and `cycles` are observed together.
    /// Never held while waiting for a barrier.
    cycle_lock: tokio::sync::Mutex<()>,
    cycles: Mutex<RefreshCycles>,
    refresh_job_metrics: Mutex<HashMap<TableId, RefreshJobMetrics>>,
    scheduler_notify: Notify,
    scheduler_interval: Duration,
}

#[derive(Default)]
struct RefreshCycles {
    trackers: HashMap<TableId, SingleTableRefreshProgressTracker>,
    table_ids_by_database: HashMap<DatabaseId, HashSet<TableId>>,
    /// Cycles whose `FinishRefresh` barrier is scheduled but not yet collected.
    pending_finish: HashMap<TableId, PendingFinish>,
    /// Tables whose cycle was abandoned by a recovery and is to be re-run once.
    retry_pending: HashSet<TableId>,
    /// Tables whose abandoned cycle has been re-run once. Cleared by a successful cycle, so a
    /// refresh that keeps crashing the cluster does not loop.
    retried_after_abort: HashSet<TableId>,
}

struct PendingFinish {
    database_id: DatabaseId,
    trigger_time: NaiveDateTime,
}

impl GlobalRefreshManager {
    pub async fn start(
        metadata_manager: MetadataManager,
        barrier_scheduler: BarrierScheduler,
        scale_controller: ScaleControllerRef,
        env: &MetaSrvEnv,
        scheduler_interval: Duration,
    ) -> MetaResult<(GlobalRefreshManagerRef, JoinHandle<()>, oneshot::Sender<()>)> {
        let shared_actor_infos = env.shared_actor_infos().clone();
        let manager = Arc::new(Self {
            metadata_manager: metadata_manager.clone(),
            barrier_scheduler,
            shared_actor_infos,
            scale_controller,
            cycle_lock: tokio::sync::Mutex::new(()),
            cycles: Mutex::new(RefreshCycles::default()),
            refresh_job_metrics: Mutex::new(HashMap::new()),
            scheduler_notify: Notify::new(),
            scheduler_interval,
        });

        manager.sync_refreshable_jobs().await?;

        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let join_handle = Self::spawn_scheduler(manager.clone(), shutdown_rx);

        Ok((manager, join_handle, shutdown_tx))
    }

    fn spawn_scheduler(
        manager: GlobalRefreshManagerRef,
        mut shutdown_rx: oneshot::Receiver<()>,
    ) -> JoinHandle<()> {
        let scheduler_interval = manager.scheduler_interval;
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(scheduler_interval);
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if let Err(err) = manager.handle_scheduler_tick().await {
                            tracing::warn!(error = %err.as_report(), "refresh scheduler tick failed");
                        }
                    }
                    _ = manager.scheduler_notify.notified() => {
                        if let Err(err) = manager.handle_scheduler_tick().await {
                            tracing::warn!(error = %err.as_report(), "refresh scheduler tick failed");
                        }
                    }
                    _ = &mut shutdown_rx => {
                        tracing::info!("refresh scheduler shutting down");
                        break;
                    }
                }
            }
        })
    }

    pub fn notify_scheduler(&self) {
        self.scheduler_notify.notify_one();
    }

    /// Starts a cycle and returns once its `RefreshStart` barrier is collected.
    pub async fn trigger_manual_refresh(
        &self,
        request: RefreshRequest,
    ) -> MetaResult<RefreshResponse> {
        let table_id = request.table_id;
        let associated_source_id = request.associated_source_id;
        tracing::info!(%table_id, %associated_source_id, "trigger manual refresh");

        self.begin_cycle(table_id, associated_source_id)
            .await?
            .await?;
        Ok(RefreshResponse { status: None })
    }

    /// Starts a refresh cycle and returns a future that resolves once its `RefreshStart` barrier is
    /// collected. The reschedule lock keeps the actor set fixed until the job is marked refreshing;
    /// after that, reschedules are rejected until the cycle ends.
    async fn begin_cycle(
        &self,
        table_id: TableId,
        associated_source_id: SourceId,
    ) -> MetaResult<impl Future<Output = MetaResult<()>> + use<>> {
        let _reschedule_guard = self.scale_controller.reschedule_lock.read().await;
        let _cycle_guard = self.cycle_lock.lock().await;

        let table = self
            .metadata_manager
            .catalog_controller
            .get_table_by_id(table_id)
            .await?;
        if !table.refreshable {
            return Err(MetaError::invalid_parameter(format!(
                "Table '{}' is not refreshable. Only tables created with REFRESHABLE flag support refresh.",
                table.name
            )));
        }
        if table.optional_associated_source_id != Some(associated_source_id.into()) {
            return Err(MetaError::invalid_parameter(format!(
                "Table '{}' is not associated with source '{}'. table.optional_associated_source_id: {:?}",
                table.name, associated_source_id, table.optional_associated_source_id
            )));
        }

        let job_id = table_id.as_job_id();
        let database_id = self
            .metadata_manager
            .catalog_controller
            .get_object_database_id(table_id)
            .await?;
        let staging_table_id = self
            .metadata_manager
            .catalog_controller
            .get_refresh_staging_table_id(job_id)
            .await?
            .ok_or_else(|| anyhow!("staging table not found for refreshable table {}", table_id))?;
        let job_fragments = self
            .metadata_manager
            .get_job_fragments_by_id(job_id)
            .await?;

        let trigger_time = now_millis();
        let mut tracker =
            SingleTableRefreshProgressTracker::new(database_id, staging_table_id, trigger_time);
        {
            let fragment_info_guard = self.shared_actor_infos.read_guard();
            for (fragment_id, fragment) in &job_fragments.fragments {
                let mask = fragment.fragment_type_mask;
                let stage = if mask.contains(FragmentTypeFlag::Mview) {
                    &mut tracker.mview
                } else if mask.contains(FragmentTypeFlag::Source)
                    && !mask.contains(FragmentTypeFlag::Dml)
                {
                    &mut tracker.list
                } else if mask.contains(FragmentTypeFlag::FsFetch) {
                    &mut tracker.fetch
                } else {
                    continue;
                };
                let fragment_info = fragment_info_guard
                    .get_fragment(*fragment_id)
                    .ok_or_else(|| MetaError::fragment_not_found(*fragment_id))?;
                stage.expected.extend(
                    fragment_info
                        .actors
                        .keys()
                        .map(|actor_id| *actor_id as ActorId),
                );
            }
        }

        if !self
            .metadata_manager
            .begin_refresh_job(table_id, trigger_time)
            .await?
        {
            let state = self
                .metadata_manager
                .catalog_controller
                .get_refresh_job_state(table_id)
                .await?;
            return Err(MetaError::invalid_parameter(format!(
                "Table '{}' is not in idle state. Current state: {}",
                table.name,
                state.map_or_else(|| "unknown".to_owned(), |state| state.to_string())
            )));
        }
        self.cycles.lock().register(table_id, tracker);

        let refresh_command = Command::Refresh {
            table_id,
            associated_source_id,
        };
        match self
            .barrier_scheduler
            .schedule_command(database_id, refresh_command)
        {
            Ok(collected) => {
                tracing::info!(%table_id, %trigger_time, "refresh cycle started");
                Ok(collected)
            }
            Err(err) => {
                // Nothing was queued, so the cycle can be reverted right away.
                self.metadata_manager
                    .finish_refresh_job(table_id, trigger_time, false)
                    .await?;
                self.remove_progress_tracker(table_id, "failure");
                Err(anyhow!(err)
                    .context(format!("Failed to refresh table {}", table_id))
                    .into())
            }
        }
    }

    pub fn mark_list_stage_finished(
        &self,
        table_id: TableId,
        actors: &HashSet<ActorId>,
    ) -> MetaResult<bool> {
        self.with_tracker(table_id, |tracker| tracker.list.report("list", actors))
    }

    pub fn mark_load_stage_finished(
        &self,
        table_id: TableId,
        actors: &HashSet<ActorId>,
    ) -> MetaResult<bool> {
        self.with_tracker(table_id, |tracker| tracker.fetch.report("fetch", actors))
    }

    /// Records the materialize actors that finished merging; the scheduler issues `FinishRefresh`
    /// once all of them have.
    pub fn mark_mview_stage_finished(
        &self,
        table_id: TableId,
        actors: &HashSet<ActorId>,
    ) -> MetaResult<()> {
        let complete = {
            let mut cycles = self.cycles.lock();
            let Some(tracker) = cycles.trackers.get_mut(&table_id) else {
                // The cycle was dropped or abandoned meanwhile.
                tracing::warn!(%table_id, ?actors, "ignore refresh finished report without tracker");
                return Ok(());
            };
            tracker.mview.report("mview", actors)?
        };
        if complete {
            self.notify_scheduler();
        }
        Ok(())
    }

    fn with_tracker(
        &self,
        table_id: TableId,
        f: impl FnOnce(&mut SingleTableRefreshProgressTracker) -> MetaResult<bool>,
    ) -> MetaResult<bool> {
        let mut cycles = self.cycles.lock();
        let tracker = cycles
            .trackers
            .get_mut(&table_id)
            .ok_or_else(|| anyhow!("Table tracker not found for table {}", table_id))?;
        f(tracker)
    }

    /// Post-collect of `FinishRefresh`: the staging table is truncated and the cycle ends.
    pub async fn complete_refresh(
        &self,
        table_id: TableId,
        trigger_time: NaiveDateTime,
        aborted: bool,
    ) -> MetaResult<()> {
        let _cycle_guard = self.cycle_lock.lock().await;
        let finished = self
            .metadata_manager
            .finish_refresh_job(table_id, trigger_time, !aborted)
            .await?;
        {
            let mut cycles = self.cycles.lock();
            if cycles
                .pending_finish
                .get(&table_id)
                .is_some_and(|pending| pending.trigger_time == trigger_time)
            {
                cycles.pending_finish.remove(&table_id);
            }
            if finished {
                if aborted {
                    if cycles.retried_after_abort.insert(table_id) {
                        cycles.retry_pending.insert(table_id);
                    } else {
                        tracing::warn!(%table_id, "abandoned refresh was already re-run once, waiting for the next trigger");
                    }
                } else {
                    cycles.retried_after_abort.remove(&table_id);
                    cycles.retry_pending.remove(&table_id);
                }
            }
        }
        if self
            .cycles
            .lock()
            .trackers
            .get(&table_id)
            .is_some_and(|tracker| tracker.trigger_time == trigger_time)
        {
            self.remove_progress_tracker(table_id, if aborted { "aborted" } else { "success" });
        }
        if finished {
            tracing::info!(%table_id, %trigger_time, aborted, "refresh cycle finished");
        } else {
            tracing::warn!(%table_id, %trigger_time, aborted, "FinishRefresh collected for a cycle that is no longer current");
        }
        self.notify_scheduler();
        Ok(())
    }

    /// Forgets the cycles of the given database (all databases if `None`) whose actors were
    /// restarted by a recovery. The scheduler finishes them as abandoned once the barrier queue is
    /// ready again.
    pub async fn clear_trackers(&self, database_id: Option<DatabaseId>) {
        let _cycle_guard = self.cycle_lock.lock().await;
        let mut cycles = self.cycles.lock();
        match database_id {
            None => {
                cycles.trackers.clear();
                cycles.table_ids_by_database.clear();
                cycles.pending_finish.clear();
            }
            Some(database_id) => {
                if let Some(table_ids) = cycles.table_ids_by_database.remove(&database_id) {
                    for table_id in table_ids {
                        cycles.trackers.remove(&table_id);
                    }
                }
                cycles
                    .pending_finish
                    .retain(|_, pending| pending.database_id != database_id);
            }
        }
    }

    async fn handle_scheduler_tick(&self) -> MetaResult<()> {
        let jobs = self.metadata_manager.list_refresh_jobs().await?;
        let active_table_ids = jobs.iter().map(|job| job.table_id).collect::<HashSet<_>>();
        self.refresh_job_metrics
            .lock()
            .retain(|table_id, _| active_table_ids.contains(table_id));
        {
            let mut cycles = self.cycles.lock();
            cycles
                .retried_after_abort
                .retain(|table_id| active_table_ids.contains(table_id));
            cycles
                .retry_pending
                .retain(|table_id| active_table_ids.contains(table_id));
        }

        self.finish_pending_cycles(&jobs).await;
        for job in &jobs {
            if let Err(err) = self.try_trigger_scheduled_refresh(job).await {
                tracing::warn!(
                    table_id = %job.table_id,
                    error = %err.as_report(),
                    "failed to trigger scheduled refresh"
                );
            }
        }
        Ok(())
    }

    /// Schedules `FinishRefresh` for cycles whose merge is complete and for cycles abandoned by a
    /// recovery. Failures are logged and retried on the next tick.
    async fn finish_pending_cycles(&self, jobs: &[refresh_job::Model]) {
        let _cycle_guard = self.cycle_lock.lock().await;
        for job in jobs {
            if job.current_status == RefreshState::Idle {
                continue;
            }
            let table_id = job.table_id;
            let Some(trigger_time) = job.last_trigger_time.map(millis_to_datetime) else {
                tracing::error!(%table_id, status = ?job.current_status, "refresh job is not idle but has no trigger time");
                continue;
            };
            let running = {
                let cycles = self.cycles.lock();
                if cycles
                    .pending_finish
                    .get(&table_id)
                    .is_some_and(|pending| pending.trigger_time == trigger_time)
                {
                    continue;
                }
                match cycles.trackers.get(&table_id) {
                    Some(tracker) if tracker.trigger_time == trigger_time => {
                        if !tracker.mview.is_complete() {
                            continue;
                        }
                        Some((tracker.database_id, tracker.staging_table_id))
                    }
                    _ => None,
                }
            };
            let (database_id, staging_table_id, aborted) = match running {
                Some((database_id, staging_table_id)) => (database_id, staging_table_id, false),
                None => match self.locate_abandoned_cycle(table_id).await {
                    Ok(Some((database_id, staging_table_id))) => {
                        (database_id, staging_table_id, true)
                    }
                    Ok(None) => continue,
                    Err(err) => {
                        tracing::warn!(%table_id, error = %err.as_report(), "failed to locate abandoned refresh, will retry");
                        continue;
                    }
                },
            };
            let command = Command::FinishRefresh {
                table_id,
                staging_table_id,
                trigger_time,
                aborted,
            };
            match self
                .barrier_scheduler
                .run_command_no_wait(database_id, command)
            {
                Ok(()) => {
                    self.cycles.lock().pending_finish.insert(
                        table_id,
                        PendingFinish {
                            database_id,
                            trigger_time,
                        },
                    );
                    tracing::info!(%table_id, %trigger_time, aborted, "FinishRefresh command scheduled");
                }
                Err(err) => {
                    tracing::warn!(%table_id, error = %err.as_report(), "failed to schedule FinishRefresh, will retry");
                }
            }
        }
    }

    /// The database and staging table of a job without a tracker, or `None` if the table is gone.
    async fn locate_abandoned_cycle(
        &self,
        table_id: TableId,
    ) -> MetaResult<Option<(DatabaseId, TableId)>> {
        let database_id = match self
            .metadata_manager
            .catalog_controller
            .get_object_database_id(table_id)
            .await
        {
            Ok(database_id) => database_id,
            Err(err) if err.is_catalog_id_not_found("object") => return Ok(None),
            Err(err) => return Err(err),
        };
        let staging_table_id = self
            .metadata_manager
            .catalog_controller
            .get_refresh_staging_table_id(table_id.as_job_id())
            .await?;
        Ok(staging_table_id.map(|staging_table_id| (database_id, staging_table_id)))
    }

    async fn sync_refreshable_jobs(&self) -> MetaResult<()> {
        let table_ids = self.metadata_manager.list_refreshable_table_ids().await?;
        for table_id in table_ids {
            self.metadata_manager.ensure_refresh_job(table_id).await?;
        }
        Ok(())
    }

    /// Starts a cycle for an idle job whose interval has elapsed, or whose last cycle was abandoned
    /// by a recovery.
    async fn try_trigger_scheduled_refresh(&self, job: &refresh_job::Model) -> MetaResult<()> {
        let table_id = job.table_id;
        let retry_abandoned = self.cycles.lock().retry_pending.contains(&table_id);

        let interval = job
            .trigger_interval_secs
            .filter(|secs| *secs > 0)
            .map(ChronoDuration::seconds);
        let cron_due = if let Some(interval) = interval {
            let last_run = match job.last_trigger_time {
                Some(last_run) => last_run,
                None => {
                    let Some(table) = self
                        .metadata_manager
                        .get_table_catalog_by_ids(&[table_id])
                        .await?
                        .pop()
                    else {
                        return Ok(());
                    };
                    Epoch(table.created_at_epoch())
                        .as_timestamptz()
                        .to_datetime_utc()
                        .timestamp_millis()
                }
            };
            Utc::now()
                .naive_utc()
                .signed_duration_since(millis_to_datetime(last_run))
                >= interval
        } else {
            false
        };
        if job.current_status != RefreshState::Idle {
            if cron_due {
                self.refresh_job_metrics
                    .lock()
                    .entry(table_id)
                    .or_insert_with(|| RefreshJobMetrics::new(table_id))
                    .cron_miss_count
                    .inc();
                tracing::warn!(%table_id, status = ?job.current_status, "skip scheduled refresh: the previous cycle is still running");
            }
            return Ok(());
        }
        if !retry_abandoned && !cron_due {
            return Ok(());
        }

        let table = self
            .metadata_manager
            .catalog_controller
            .get_table_by_id(table_id)
            .await?;
        if !table.refreshable {
            return Ok(());
        }
        let Some(OptionalAssociatedSourceId::AssociatedSourceId(associated_source_id)) =
            table.optional_associated_source_id
        else {
            tracing::warn!(%table_id, "skip scheduled refresh: missing associated source id");
            return Ok(());
        };

        if cron_due {
            self.refresh_job_metrics
                .lock()
                .entry(table_id)
                .or_insert_with(|| RefreshJobMetrics::new(table_id))
                .cron_trigger_count
                .inc();
            tracing::info!(%table_id, "trigger scheduled refresh at interval {:?}", interval);
        } else {
            tracing::info!(%table_id, "re-run the abandoned refresh once");
        }
        // The barrier is not awaited here; an interrupted cycle is finished by the abandoned path.
        let _collected = self.begin_cycle(table_id, associated_source_id).await?;
        self.cycles.lock().retry_pending.remove(&table_id);
        Ok(())
    }

    pub fn remove_progress_tracker(&self, table_id: TableId, status: &str) {
        let elapsed = {
            let mut cycles = self.cycles.lock();
            let elapsed = cycles
                .trackers
                .remove(&table_id)
                .map(|entry| entry.start_time.elapsed().as_secs());
            cycles.table_ids_by_database.values_mut().for_each(|set| {
                set.remove(&table_id);
            });
            elapsed
        };
        if let Some(elapsed) = elapsed {
            let table_id_label = table_id.to_string();
            GLOBAL_META_METRICS
                .refresh_job_duration
                .with_guarded_label_values(&[&table_id_label, status])
                .set(elapsed);
            let mut metrics = self.refresh_job_metrics.lock();
            let metrics = metrics
                .entry(table_id)
                .or_insert_with(|| RefreshJobMetrics::new(table_id))
                .finished
                .entry(status.to_owned())
                .or_insert_with(|| RefreshFinishedMetrics::new(table_id, status));
            metrics.count.inc();
        }
    }
}

impl RefreshCycles {
    fn register(&mut self, table_id: TableId, tracker: SingleTableRefreshProgressTracker) {
        self.table_ids_by_database
            .entry(tracker.database_id)
            .or_default()
            .insert(table_id);
        self.trackers.insert(table_id, tracker);
    }
}

/// The current time truncated to the millisecond precision of the persisted trigger time.
fn now_millis() -> NaiveDateTime {
    millis_to_datetime(Utc::now().timestamp_millis())
}

fn millis_to_datetime(millis: i64) -> NaiveDateTime {
    DateTime::from_timestamp_millis(millis)
        .expect("valid timestamp")
        .naive_utc()
}

struct RefreshJobMetrics {
    cron_trigger_count: LabelGuardedIntCounter,
    cron_miss_count: LabelGuardedIntCounter,
    finished: HashMap<String, RefreshFinishedMetrics>,
}

impl RefreshJobMetrics {
    fn new(table_id: TableId) -> Self {
        let table_id = table_id.to_string();
        Self {
            cron_trigger_count: GLOBAL_META_METRICS
                .refresh_cron_job_trigger_cnt
                .with_guarded_label_values(&[&table_id]),
            cron_miss_count: GLOBAL_META_METRICS
                .refresh_cron_job_miss_cnt
                .with_guarded_label_values(&[&table_id]),
            finished: HashMap::new(),
        }
    }
}

struct RefreshFinishedMetrics {
    count: LabelGuardedIntCounter,
}

impl RefreshFinishedMetrics {
    fn new(table_id: TableId, status: &str) -> Self {
        let table_id = table_id.to_string();
        Self {
            count: GLOBAL_META_METRICS
                .refresh_job_finish_cnt
                .with_guarded_label_values(&[&table_id, status]),
        }
    }
}

/// Progress of one refresh cycle, keyed by the actors expected to report at each stage.
#[derive(Debug)]
pub struct SingleTableRefreshProgressTracker {
    pub database_id: DatabaseId,
    pub staging_table_id: TableId,
    pub trigger_time: NaiveDateTime,
    pub list: StageProgress,
    pub fetch: StageProgress,
    pub mview: StageProgress,
    pub start_time: Instant,
}

impl SingleTableRefreshProgressTracker {
    pub fn new(
        database_id: DatabaseId,
        staging_table_id: TableId,
        trigger_time: NaiveDateTime,
    ) -> Self {
        Self {
            database_id,
            staging_table_id,
            trigger_time,
            list: StageProgress::default(),
            fetch: StageProgress::default(),
            mview: StageProgress::default(),
            start_time: Instant::now(),
        }
    }
}

#[derive(Debug, Default)]
pub struct StageProgress {
    pub expected: HashSet<ActorId>,
    pub finished: HashSet<ActorId>,
}

impl StageProgress {
    /// Returns true once every expected actor has reported; an unexpected reporter is an error.
    pub fn report(&mut self, stage: &str, actors: &HashSet<ActorId>) -> MetaResult<bool> {
        self.finished.extend(actors);
        if self.finished.len() < self.expected.len() {
            return Ok(false);
        }
        if self.finished == self.expected {
            Ok(true)
        } else {
            Err(anyhow!(
                "{stage} finished actors mismatch: expected: {:?}, actual: {:?}",
                self.expected,
                self.finished
            )
            .into())
        }
    }

    pub fn is_complete(&self) -> bool {
        !self.expected.is_empty() && self.finished == self.expected
    }
}
