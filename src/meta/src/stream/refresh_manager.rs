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
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::anyhow;
use chrono::{DateTime, Duration as ChronoDuration, NaiveDateTime, Utc};
use parking_lot::Mutex;
use risingwave_common::catalog::{DatabaseId, FragmentTypeFlag, TableId};
use risingwave_common::metrics::{LabelGuardedIntCounter, LabelGuardedUintGauge};
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
use crate::barrier::{BarrierScheduler, Command};
use crate::controller::fragment::InflightFragmentInfo;
use crate::manager::MetadataManager;
use crate::rpc::metrics::GLOBAL_META_METRICS;
use crate::{MetaError, MetaResult};

pub type GlobalRefreshManagerRef = Arc<GlobalRefreshManager>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TriggerReason {
    Manual,
    Cron,
    RetryAfterAbort,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RefreshStage {
    List,
    Fetch,
    Mview,
}

/// The actors of a refreshable table's job, grouped by the stage they report.
#[derive(Debug, Default)]
pub struct RefreshCycleActors {
    pub list: HashSet<ActorId>,
    pub fetch: HashSet<ActorId>,
    pub mview: HashSet<ActorId>,
}

impl RefreshCycleActors {
    pub fn from_fragments<'a>(fragments: impl Iterator<Item = &'a InflightFragmentInfo>) -> Self {
        let mut actors = Self::default();
        for fragment in fragments {
            let mask = fragment.fragment_type_mask;
            let stage = if mask.contains(FragmentTypeFlag::Mview) {
                &mut actors.mview
            } else if mask.contains(FragmentTypeFlag::Source)
                && !mask.contains(FragmentTypeFlag::Dml)
            {
                &mut actors.list
            } else if mask.contains(FragmentTypeFlag::FsFetch) {
                &mut actors.fetch
            } else {
                continue;
            };
            stage.extend(fragment.actors.keys().map(|actor_id| *actor_id as ActorId));
        }
        actors
    }
}

/// Owns the refresh cycles. The `refresh_job` row is the source of truth: a cycle is its
/// `last_trigger_time`, and both transitions are the post-collect of a barrier (`RefreshStart` moves
/// the job to `Refreshing`, `FinishRefresh` back to `Idle`). The in-memory tracker of a cycle is
/// created by the same barrier that started it, so a job that is not idle and has no tracker was
/// abandoned by a recovery and is finished as such by the scheduler.
pub struct GlobalRefreshManager {
    metadata_manager: MetadataManager,
    barrier_scheduler: BarrierScheduler,
    scale_controller: ScaleControllerRef,
    cycles: Mutex<Cycles>,
    scheduler_wakeup: Notify,
}

#[derive(Default)]
struct Cycles {
    trackers: HashMap<TableId, CycleTracker>,
    /// Cycles whose `FinishRefresh` barrier is scheduled but not yet collected, with the
    /// database the barrier belongs to.
    pending_finish: HashMap<TableId, (DatabaseId, NaiveDateTime)>,
    /// Tables whose `RefreshStart` barrier is being scheduled.
    starting: HashSet<TableId>,
    retry: HashMap<TableId, RetryState>,
    metrics: HashMap<TableId, RefreshJobMetrics>,
}

/// An abandoned cycle is re-run once; the state is cleared by a successful cycle.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RetryState {
    Owed,
    Used,
}

impl GlobalRefreshManager {
    pub async fn start(
        metadata_manager: MetadataManager,
        barrier_scheduler: BarrierScheduler,
        scale_controller: ScaleControllerRef,
        scheduler_interval: Duration,
    ) -> MetaResult<(GlobalRefreshManagerRef, JoinHandle<()>, oneshot::Sender<()>)> {
        for table_id in metadata_manager.list_refreshable_table_ids().await? {
            metadata_manager.ensure_refresh_job(table_id).await?;
        }
        let manager = Arc::new(Self {
            metadata_manager,
            barrier_scheduler,
            scale_controller,
            cycles: Mutex::new(Cycles::default()),
            scheduler_wakeup: Notify::new(),
        });
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let join_handle = tokio::spawn(
            manager
                .clone()
                .run_scheduler(shutdown_rx, scheduler_interval),
        );
        Ok((manager, join_handle, shutdown_tx))
    }

    /// Returns once the `RefreshStart` barrier of the cycle is collected.
    pub async fn trigger_manual_refresh(
        self: &Arc<Self>,
        request: RefreshRequest,
    ) -> MetaResult<RefreshResponse> {
        let table_id = request.table_id;
        let associated_source_id = request.associated_source_id;
        tracing::info!(%table_id, %associated_source_id, "trigger manual refresh");
        self.trigger_cycle(table_id, Some(associated_source_id), TriggerReason::Manual)
            .await?;
        Ok(RefreshResponse { status: None })
    }

    /// The cycle is started by a spawned task, so a caller that goes away does not release the
    /// reschedule guard before the `RefreshStart` barrier is collected.
    async fn trigger_cycle(
        self: &Arc<Self>,
        table_id: TableId,
        associated_source_id: Option<SourceId>,
        reason: TriggerReason,
    ) -> MetaResult<()> {
        {
            let mut cycles = self.cycles.lock();
            if cycles.trackers.contains_key(&table_id) || !cycles.starting.insert(table_id) {
                return Err(MetaError::invalid_parameter(format!(
                    "Table {} is not in idle state: a refresh cycle is in progress",
                    table_id
                )));
            }
        }
        let this = self.clone();
        let result = tokio::spawn(async move {
            let result = this
                .start_cycle(table_id, associated_source_id, reason)
                .await;
            this.cycles.lock().starting.remove(&table_id);
            result
        })
        .await;
        result.map_err(|err| anyhow!(err))?
    }

    async fn start_cycle(
        &self,
        table_id: TableId,
        associated_source_id: Option<SourceId>,
        reason: TriggerReason,
    ) -> MetaResult<()> {
        let catalog = &self.metadata_manager.catalog_controller;
        let table = catalog.get_table_by_id(table_id).await?;
        if !table.refreshable {
            return Err(MetaError::invalid_parameter(format!(
                "Table '{}' is not refreshable. Only tables created with REFRESHABLE flag support refresh.",
                table.name
            )));
        }
        let Some(OptionalAssociatedSourceId::AssociatedSourceId(table_source_id)) =
            table.optional_associated_source_id
        else {
            return Err(MetaError::invalid_parameter(format!(
                "Table '{}' has no associated source",
                table.name
            )));
        };
        let associated_source_id = associated_source_id.unwrap_or(table_source_id);
        if associated_source_id != table_source_id {
            return Err(MetaError::invalid_parameter(format!(
                "Table '{}' is not associated with source '{}'. table.optional_associated_source_id: {:?}",
                table.name, associated_source_id, table.optional_associated_source_id
            )));
        }
        if let Some(state) = catalog.get_refresh_job_state(table_id).await?
            && state != RefreshState::Idle
        {
            return Err(MetaError::invalid_parameter(format!(
                "Table '{}' is not in idle state. Current state: {}",
                table.name, state
            )));
        }
        let database_id = catalog.get_object_database_id(table_id).await?;
        let staging_table_id = catalog
            .get_refresh_staging_table_id(table_id.as_job_id())
            .await?
            .ok_or_else(|| anyhow!("staging table not found for refreshable table {}", table_id))?;

        // Like a creating job: no reschedule or replacement of the table between the check above
        // and the collection of the barrier, after which the job is `Refreshing`.
        let _reschedule_guard = self.scale_controller.reschedule_lock.read().await;
        let trigger_time = now_millis();
        tracing::info!(%table_id, %trigger_time, ?reason, "scheduling refresh cycle");
        self.barrier_scheduler
            .schedule_command(
                database_id,
                Command::Refresh {
                    table_id,
                    associated_source_id,
                    staging_table_id,
                    trigger_time,
                },
            )?
            .await?;

        let mut cycles = self.cycles.lock();
        if !cycles
            .trackers
            .get(&table_id)
            .is_some_and(|tracker| tracker.trigger_time == trigger_time)
        {
            return Err(anyhow!(
                "refresh of table {} did not start, the table was dropped",
                table_id
            )
            .into());
        }
        match reason {
            TriggerReason::Manual => {}
            TriggerReason::Cron => cycles.job_metrics(table_id).cron_trigger_count.inc(),
            TriggerReason::RetryAfterAbort => {
                cycles.retry.insert(table_id, RetryState::Used);
            }
        }
        Ok(())
    }

    /// Post-collect of the `RefreshStart` barrier.
    pub async fn cycle_started(
        &self,
        table_id: TableId,
        database_id: DatabaseId,
        associated_source_id: SourceId,
        trigger_time: NaiveDateTime,
        actors: RefreshCycleActors,
    ) -> MetaResult<()> {
        if !self
            .metadata_manager
            .begin_refresh_job(table_id, trigger_time)
            .await?
        {
            tracing::warn!(%table_id, %trigger_time, "RefreshStart collected for a table that is not idle, the cycle is not tracked");
            return Ok(());
        }
        self.cycles.lock().trackers.insert(
            table_id,
            CycleTracker::new(database_id, associated_source_id, trigger_time, actors),
        );
        tracing::info!(%table_id, %trigger_time, "refresh cycle started");
        Ok(())
    }

    /// Schedules the next stage once every actor of the stage reported; an unexpected reporter
    /// fails the barrier, which abandons the cycle through a recovery.
    pub fn report_stage(
        &self,
        table_id: TableId,
        stage: RefreshStage,
        actors: HashSet<ActorId>,
    ) -> MetaResult<()> {
        let mut cycles = self.cycles.lock();
        let Some(tracker) = cycles.trackers.get_mut(&table_id) else {
            // The cycle was abandoned or the table dropped meanwhile.
            tracing::warn!(%table_id, ?stage, ?actors, "ignore refresh stage report without tracker");
            return Ok(());
        };
        if !tracker.stage_mut(stage).report(stage, &actors)? {
            return Ok(());
        }
        let (database_id, associated_source_id, trigger_time) = (
            tracker.database_id,
            tracker.associated_source_id,
            tracker.trigger_time,
        );
        let command = match stage {
            RefreshStage::List => Command::ListFinish {
                table_id,
                associated_source_id,
            },
            RefreshStage::Fetch => Command::LoadFinish {
                table_id,
                associated_source_id,
            },
            RefreshStage::Mview => {
                cycles.schedule_finish(
                    &self.barrier_scheduler,
                    table_id,
                    database_id,
                    trigger_time,
                    false,
                );
                return Ok(());
            }
        };
        self.barrier_scheduler
            .run_command_no_wait(database_id, command)
            .map_err(|err| {
                anyhow!(err).context(format!(
                    "failed to schedule the next refresh stage of table {table_id}"
                ))
            })?;
        tracing::info!(%table_id, ?stage, "refresh stage finished");
        Ok(())
    }

    /// Post-collect of the `FinishRefresh` barrier.
    pub async fn complete_refresh(
        &self,
        table_id: TableId,
        trigger_time: NaiveDateTime,
        aborted: bool,
    ) -> MetaResult<()> {
        let finished = self
            .metadata_manager
            .finish_refresh_job(table_id, trigger_time, !aborted)
            .await?;
        let mut cycles = self.cycles.lock();
        if cycles
            .pending_finish
            .get(&table_id)
            .is_some_and(|(_, pending)| *pending == trigger_time)
        {
            cycles.pending_finish.remove(&table_id);
        }
        let tracker = cycles
            .trackers
            .get(&table_id)
            .is_some_and(|tracker| tracker.trigger_time == trigger_time)
            .then(|| cycles.trackers.remove(&table_id))
            .flatten();
        if !finished {
            tracing::warn!(%table_id, %trigger_time, aborted, "FinishRefresh collected for a cycle that is no longer current");
            return Ok(());
        }
        tracing::info!(%table_id, %trigger_time, aborted, "refresh cycle finished");
        let duration = tracker.map_or_else(
            || u64::try_from((Utc::now().naive_utc() - trigger_time).num_seconds()).unwrap_or(0),
            |tracker| tracker.start_time.elapsed().as_secs(),
        );
        cycles.record_finished(
            table_id,
            if aborted { "aborted" } else { "success" },
            duration,
        );
        if !aborted {
            cycles.retry.remove(&table_id);
        } else {
            match cycles.retry.get(&table_id) {
                None => {
                    cycles.retry.insert(table_id, RetryState::Owed);
                    self.scheduler_wakeup.notify_one();
                }
                Some(RetryState::Owed) => {}
                Some(RetryState::Used) => {
                    tracing::warn!(%table_id, "abandoned refresh was already re-run once, waiting for the next trigger");
                }
            }
        }
        Ok(())
    }

    /// Called by recovery; the cycles of the database are then finished as abandoned.
    pub fn clear_trackers(&self, database_id: Option<DatabaseId>) {
        let mut cycles = self.cycles.lock();
        match database_id {
            None => {
                cycles.trackers.clear();
                cycles.pending_finish.clear();
            }
            Some(database_id) => {
                cycles
                    .trackers
                    .retain(|_, tracker| tracker.database_id != database_id);
                cycles
                    .pending_finish
                    .retain(|_, (pending_db, _)| *pending_db != database_id);
            }
        }
    }

    /// Called when the table is dropped; `status` labels the metrics of an interrupted cycle.
    pub fn remove_progress_tracker(&self, table_id: TableId, status: &str) {
        let mut cycles = self.cycles.lock();
        if let Some(tracker) = cycles.trackers.remove(&table_id) {
            cycles.record_finished(table_id, status, tracker.start_time.elapsed().as_secs());
        }
        cycles.pending_finish.remove(&table_id);
        cycles.retry.remove(&table_id);
    }

    pub fn notify_scheduler(&self) {
        self.scheduler_wakeup.notify_one();
    }

    async fn run_scheduler(
        self: Arc<Self>,
        mut shutdown_rx: oneshot::Receiver<()>,
        scheduler_interval: Duration,
    ) {
        let mut ticker = tokio::time::interval(scheduler_interval);
        loop {
            tokio::select! {
                biased;
                _ = &mut shutdown_rx => {
                    tracing::info!("refresh scheduler shutting down");
                    break;
                }
                _ = self.scheduler_wakeup.notified() => {}
                _ = ticker.tick() => {}
            }
            self.tick().await;
        }
    }

    async fn tick(self: &Arc<Self>) {
        let jobs = match self.metadata_manager.list_refresh_jobs().await {
            Ok(jobs) => jobs,
            Err(err) => {
                tracing::warn!(error = %err.as_report(), "failed to list refresh jobs");
                return;
            }
        };
        let never_triggered: Vec<TableId> = jobs
            .iter()
            .filter(|job| job.last_trigger_time.is_none() && cron_interval(job).is_some())
            .map(|job| job.table_id)
            .collect();
        let created_at = match self.created_at_millis(&never_triggered).await {
            Ok(created_at) => created_at,
            Err(err) => {
                tracing::warn!(error = %err.as_report(), "failed to load the creation time of refreshable tables");
                HashMap::new()
            }
        };
        {
            let active: HashSet<_> = jobs.iter().map(|job| job.table_id).collect();
            let mut cycles = self.cycles.lock();
            cycles
                .metrics
                .retain(|table_id, _| active.contains(table_id));
            cycles.retry.retain(|table_id, _| active.contains(table_id));
            cycles
                .pending_finish
                .retain(|table_id, _| active.contains(table_id));
        }
        for job in &jobs {
            let table_id = job.table_id;
            let cron_due = cron_due(job, created_at.get(&table_id).copied());
            if job.current_status != RefreshState::Idle {
                self.finish_pending(job).await;
                if cron_due {
                    let mut cycles = self.cycles.lock();
                    cycles.job_metrics(table_id).cron_miss_count.inc();
                    tracing::warn!(%table_id, status = ?job.current_status, "skip scheduled refresh: the previous cycle is still running");
                }
                continue;
            }
            let reason = {
                let cycles = self.cycles.lock();
                if cycles.starting.contains(&table_id) {
                    continue;
                }
                if cycles.retry.get(&table_id) == Some(&RetryState::Owed) {
                    TriggerReason::RetryAfterAbort
                } else if cron_due {
                    TriggerReason::Cron
                } else {
                    continue;
                }
            };
            tracing::info!(%table_id, ?reason, "trigger refresh");
            let this = self.clone();
            tokio::spawn(async move {
                if let Err(err) = this.trigger_cycle(table_id, None, reason).await {
                    tracing::warn!(%table_id, ?reason, error = %err.as_report(), "failed to trigger refresh");
                }
            });
        }
    }

    /// Schedules `FinishRefresh` for a cycle whose merge is complete or which was abandoned;
    /// failures are retried on the next tick.
    async fn finish_pending(&self, job: &refresh_job::Model) {
        let table_id = job.table_id;
        let Some(trigger_time) = job.last_trigger_time.map(millis_to_datetime) else {
            tracing::error!(%table_id, status = ?job.current_status, "refresh job is not idle but has no trigger time");
            return;
        };
        let tracked = {
            let cycles = self.cycles.lock();
            if cycles
                .pending_finish
                .get(&table_id)
                .is_some_and(|(_, pending)| *pending == trigger_time)
            {
                return;
            }
            match cycles.trackers.get(&table_id) {
                Some(tracker) if tracker.trigger_time == trigger_time => {
                    if !tracker.mview.is_complete() {
                        return;
                    }
                    Some(tracker.database_id)
                }
                _ => None,
            }
        };
        let (database_id, aborted) = match tracked {
            Some(database_id) => (database_id, false),
            None => {
                match self
                    .metadata_manager
                    .catalog_controller
                    .get_object_database_id(table_id)
                    .await
                {
                    Ok(database_id) => (database_id, true),
                    Err(err) if err.is_catalog_id_not_found("object") => return,
                    Err(err) => {
                        tracing::warn!(%table_id, error = %err.as_report(), "failed to locate abandoned refresh, will retry");
                        return;
                    }
                }
            }
        };
        self.cycles.lock().schedule_finish(
            &self.barrier_scheduler,
            table_id,
            database_id,
            trigger_time,
            aborted,
        );
    }

    async fn created_at_millis(&self, table_ids: &[TableId]) -> MetaResult<HashMap<TableId, i64>> {
        if table_ids.is_empty() {
            return Ok(HashMap::new());
        }
        Ok(self
            .metadata_manager
            .get_table_catalog_by_ids(table_ids)
            .await?
            .into_iter()
            .map(|table| {
                (
                    table.id,
                    Epoch(table.created_at_epoch())
                        .as_timestamptz()
                        .to_datetime_utc()
                        .timestamp_millis(),
                )
            })
            .collect())
    }
}

impl Cycles {
    fn schedule_finish(
        &mut self,
        barrier_scheduler: &BarrierScheduler,
        table_id: TableId,
        database_id: DatabaseId,
        trigger_time: NaiveDateTime,
        aborted: bool,
    ) {
        if self
            .pending_finish
            .get(&table_id)
            .is_some_and(|(_, pending)| *pending == trigger_time)
        {
            return;
        }
        let command = Command::FinishRefresh {
            table_id,
            trigger_time,
            aborted,
        };
        match barrier_scheduler.run_command_no_wait(database_id, command) {
            Ok(()) => {
                self.pending_finish
                    .insert(table_id, (database_id, trigger_time));
                tracing::info!(%table_id, %trigger_time, aborted, "FinishRefresh command scheduled");
            }
            Err(err) => {
                tracing::warn!(%table_id, error = %err.as_report(), "failed to schedule FinishRefresh, will retry");
            }
        }
    }

    fn record_finished(&mut self, table_id: TableId, status: &str, duration_secs: u64) {
        let finished = self
            .job_metrics(table_id)
            .finished
            .entry(status.to_owned())
            .or_insert_with(|| RefreshFinishedMetrics::new(table_id, status));
        finished.count.inc();
        finished.duration.set(duration_secs);
    }

    fn job_metrics(&mut self, table_id: TableId) -> &mut RefreshJobMetrics {
        self.metrics
            .entry(table_id)
            .or_insert_with(|| RefreshJobMetrics::new(table_id))
    }
}

fn cron_interval(job: &refresh_job::Model) -> Option<ChronoDuration> {
    job.trigger_interval_secs
        .filter(|secs| *secs > 0)
        .map(ChronoDuration::seconds)
}

/// `created_at_millis` stands in for the last trigger time of a job that never ran.
fn cron_due(job: &refresh_job::Model, created_at_millis: Option<i64>) -> bool {
    let Some(interval) = cron_interval(job) else {
        return false;
    };
    let Some(last_run) = job.last_trigger_time.or(created_at_millis) else {
        return false;
    };
    Utc::now()
        .naive_utc()
        .signed_duration_since(millis_to_datetime(last_run))
        >= interval
}

/// Truncated to the millisecond precision of the persisted trigger time.
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
    duration: LabelGuardedUintGauge,
}

impl RefreshFinishedMetrics {
    fn new(table_id: TableId, status: &str) -> Self {
        let table_id = table_id.to_string();
        Self {
            count: GLOBAL_META_METRICS
                .refresh_job_finish_cnt
                .with_guarded_label_values(&[&table_id, status]),
            duration: GLOBAL_META_METRICS
                .refresh_job_duration
                .with_guarded_label_values(&[&table_id, status]),
        }
    }
}

#[derive(Debug)]
struct CycleTracker {
    database_id: DatabaseId,
    associated_source_id: SourceId,
    trigger_time: NaiveDateTime,
    list: StageProgress,
    fetch: StageProgress,
    mview: StageProgress,
    start_time: Instant,
}

impl CycleTracker {
    fn new(
        database_id: DatabaseId,
        associated_source_id: SourceId,
        trigger_time: NaiveDateTime,
        actors: RefreshCycleActors,
    ) -> Self {
        Self {
            database_id,
            associated_source_id,
            trigger_time,
            list: StageProgress::new(actors.list),
            fetch: StageProgress::new(actors.fetch),
            mview: StageProgress::new(actors.mview),
            start_time: Instant::now(),
        }
    }

    fn stage_mut(&mut self, stage: RefreshStage) -> &mut StageProgress {
        match stage {
            RefreshStage::List => &mut self.list,
            RefreshStage::Fetch => &mut self.fetch,
            RefreshStage::Mview => &mut self.mview,
        }
    }
}

#[derive(Debug)]
struct StageProgress {
    expected: HashSet<ActorId>,
    finished: HashSet<ActorId>,
}

impl StageProgress {
    fn new(expected: HashSet<ActorId>) -> Self {
        Self {
            expected,
            finished: HashSet::new(),
        }
    }

    /// True once every expected actor reported; an unexpected reporter is an error.
    fn report(&mut self, stage: RefreshStage, actors: &HashSet<ActorId>) -> MetaResult<bool> {
        self.finished.extend(actors);
        if self.finished.len() < self.expected.len() {
            return Ok(false);
        }
        if self.finished == self.expected {
            Ok(true)
        } else {
            Err(anyhow!(
                "{stage:?} finished actors mismatch: expected: {:?}, actual: {:?}",
                self.expected,
                self.finished
            )
            .into())
        }
    }

    fn is_complete(&self) -> bool {
        !self.expected.is_empty() && self.finished == self.expected
    }
}
