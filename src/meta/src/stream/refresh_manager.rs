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
use futures::FutureExt;
use futures::future::BoxFuture;
use risingwave_common::catalog::{DatabaseId, FragmentTypeFlag, TableId};
use risingwave_common::metrics::LabelGuardedIntCounter;
use risingwave_common::util::epoch::Epoch;
use risingwave_meta_model::ActorId;
use risingwave_meta_model::refresh_job::{self, RefreshState};
use risingwave_pb::catalog::table::OptionalAssociatedSourceId;
use risingwave_pb::id::SourceId;
use risingwave_pb::meta::{RefreshRequest, RefreshResponse};
use thiserror_ext::AsReport;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use super::ScaleControllerRef;
use crate::barrier::{BarrierScheduler, Command, SharedActorInfos};
use crate::manager::{MetaSrvEnv, MetadataManager};
use crate::rpc::metrics::GLOBAL_META_METRICS;
use crate::{MetaError, MetaResult};

pub type GlobalRefreshManagerRef = Arc<GlobalRefreshManager>;

/// Resolves once the `RefreshStart` barrier of a cycle is collected.
type Collected = BoxFuture<'static, MetaResult<()>>;

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

enum RefreshEvent {
    /// The sender holds the reschedule read lock until the reply arrives.
    Begin {
        table_id: TableId,
        /// Validated against the table's source when given.
        associated_source_id: Option<SourceId>,
        reason: TriggerReason,
        reply: oneshot::Sender<MetaResult<Collected>>,
    },
    /// Replies whether the stage is complete.
    StageFinished {
        table_id: TableId,
        stage: RefreshStage,
        actors: HashSet<ActorId>,
        reply: oneshot::Sender<MetaResult<bool>>,
    },
    Completed {
        table_id: TableId,
        trigger_time: NaiveDateTime,
        aborted: bool,
        reply: oneshot::Sender<MetaResult<()>>,
    },
    /// All databases if `None`.
    Recovered {
        database_id: Option<DatabaseId>,
        reply: oneshot::Sender<()>,
    },
    TableDropped {
        table_id: TableId,
        status: String,
    },
    Tick,
}

/// Handle of [`RefreshWorker`]; callers only send it events.
pub struct GlobalRefreshManager {
    event_tx: mpsc::UnboundedSender<RefreshEvent>,
    scale_controller: ScaleControllerRef,
}

impl GlobalRefreshManager {
    pub async fn start(
        metadata_manager: MetadataManager,
        barrier_scheduler: BarrierScheduler,
        scale_controller: ScaleControllerRef,
        env: &MetaSrvEnv,
        scheduler_interval: Duration,
    ) -> MetaResult<(GlobalRefreshManagerRef, JoinHandle<()>, oneshot::Sender<()>)> {
        for table_id in metadata_manager.list_refreshable_table_ids().await? {
            metadata_manager.ensure_refresh_job(table_id).await?;
        }

        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let manager = Arc::new(Self {
            event_tx,
            scale_controller,
        });
        let worker = RefreshWorker {
            handle: manager.clone(),
            metadata_manager,
            barrier_scheduler,
            shared_actor_infos: env.shared_actor_infos().clone(),
            trackers: HashMap::new(),
            table_ids_by_database: HashMap::new(),
            pending_finish: HashMap::new(),
            retry_pending: HashSet::new(),
            retried_after_abort: HashSet::new(),
            triggers_in_flight: HashSet::new(),
            metrics: HashMap::new(),
        };
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let join_handle = tokio::spawn(worker.run(event_rx, shutdown_rx, scheduler_interval));
        Ok((manager, join_handle, shutdown_tx))
    }

    fn send(&self, event: RefreshEvent) -> MetaResult<()> {
        self.event_tx
            .send(event)
            .map_err(|_| anyhow!("refresh worker is not running").into())
    }

    async fn request<T>(
        &self,
        event: impl FnOnce(oneshot::Sender<T>) -> RefreshEvent,
    ) -> MetaResult<T> {
        let (reply, rx) = oneshot::channel();
        self.send(event(reply))?;
        rx.await
            .map_err(|_| anyhow!("refresh worker is not running").into())
    }

    /// Starts a cycle and returns once its `RefreshStart` barrier is collected.
    pub async fn trigger_manual_refresh(
        &self,
        request: RefreshRequest,
    ) -> MetaResult<RefreshResponse> {
        let table_id = request.table_id;
        let associated_source_id = request.associated_source_id;
        tracing::info!(%table_id, %associated_source_id, "trigger manual refresh");
        self.trigger_cycle(table_id, Some(associated_source_id), TriggerReason::Manual)
            .await?
            .await?;
        Ok(RefreshResponse { status: None })
    }

    async fn trigger_cycle(
        &self,
        table_id: TableId,
        associated_source_id: Option<SourceId>,
        reason: TriggerReason,
    ) -> MetaResult<Collected> {
        let _reschedule_guard = self.scale_controller.reschedule_lock.read().await;
        self.request(|reply| RefreshEvent::Begin {
            table_id,
            associated_source_id,
            reason,
            reply,
        })
        .await?
    }

    pub async fn mark_list_stage_finished(
        &self,
        table_id: TableId,
        actors: &HashSet<ActorId>,
    ) -> MetaResult<bool> {
        self.stage_finished(table_id, RefreshStage::List, actors)
            .await
    }

    pub async fn mark_load_stage_finished(
        &self,
        table_id: TableId,
        actors: &HashSet<ActorId>,
    ) -> MetaResult<bool> {
        self.stage_finished(table_id, RefreshStage::Fetch, actors)
            .await
    }

    pub async fn mark_mview_stage_finished(
        &self,
        table_id: TableId,
        actors: &HashSet<ActorId>,
    ) -> MetaResult<()> {
        self.stage_finished(table_id, RefreshStage::Mview, actors)
            .await?;
        Ok(())
    }

    async fn stage_finished(
        &self,
        table_id: TableId,
        stage: RefreshStage,
        actors: &HashSet<ActorId>,
    ) -> MetaResult<bool> {
        self.request(|reply| RefreshEvent::StageFinished {
            table_id,
            stage,
            actors: actors.clone(),
            reply,
        })
        .await?
    }

    pub async fn complete_refresh(
        &self,
        table_id: TableId,
        trigger_time: NaiveDateTime,
        aborted: bool,
    ) -> MetaResult<()> {
        self.request(|reply| RefreshEvent::Completed {
            table_id,
            trigger_time,
            aborted,
            reply,
        })
        .await?
    }

    /// Called by recovery; the cycles of the database are then finished as abandoned.
    pub async fn clear_trackers(&self, database_id: Option<DatabaseId>) {
        let _ = self
            .request(|reply| RefreshEvent::Recovered { database_id, reply })
            .await;
    }

    pub fn remove_progress_tracker(&self, table_id: TableId, status: &str) {
        let _ = self.send(RefreshEvent::TableDropped {
            table_id,
            status: status.to_owned(),
        });
    }

    pub fn notify_scheduler(&self) {
        let _ = self.send(RefreshEvent::Tick);
    }
}

/// Owns the refresh cycles. The `refresh_job` row is the source of truth (a cycle is its
/// `last_trigger_time`, every transition is a conditional update); a job that is not idle and has no
/// tracker was abandoned by a recovery. The worker never waits for a barrier or a lock.
struct RefreshWorker {
    handle: GlobalRefreshManagerRef,
    metadata_manager: MetadataManager,
    barrier_scheduler: BarrierScheduler,
    shared_actor_infos: SharedActorInfos,
    trackers: HashMap<TableId, SingleTableRefreshProgressTracker>,
    table_ids_by_database: HashMap<DatabaseId, HashSet<TableId>>,
    /// Cycles whose `FinishRefresh` barrier is scheduled but not yet collected.
    pending_finish: HashMap<TableId, PendingFinish>,
    /// Tables whose cycle was abandoned by a recovery and is to be re-run once.
    retry_pending: HashSet<TableId>,
    /// Cleared by a successful cycle, so a refresh that keeps crashing the cluster does not loop.
    retried_after_abort: HashSet<TableId>,
    /// Tables with a spawned trigger whose `Begin` has not arrived yet.
    triggers_in_flight: HashSet<TableId>,
    metrics: HashMap<TableId, RefreshJobMetrics>,
}

struct PendingFinish {
    database_id: DatabaseId,
    trigger_time: NaiveDateTime,
}

impl RefreshWorker {
    async fn run(
        mut self,
        mut event_rx: mpsc::UnboundedReceiver<RefreshEvent>,
        mut shutdown_rx: oneshot::Receiver<()>,
        scheduler_interval: Duration,
    ) {
        let mut ticker = tokio::time::interval(scheduler_interval);
        loop {
            tokio::select! {
                biased;
                _ = &mut shutdown_rx => {
                    tracing::info!("refresh worker shutting down");
                    break;
                }
                Some(event) = event_rx.recv() => self.handle_event(event).await,
                _ = ticker.tick() => self.handle_tick().await,
            }
        }
    }

    async fn handle_event(&mut self, event: RefreshEvent) {
        match event {
            RefreshEvent::Begin {
                table_id,
                associated_source_id,
                reason,
                reply,
            } => {
                self.triggers_in_flight.remove(&table_id);
                let _ = reply.send(
                    self.begin_cycle(table_id, associated_source_id, reason)
                        .await,
                );
            }
            RefreshEvent::StageFinished {
                table_id,
                stage,
                actors,
                reply,
            } => {
                let _ = reply.send(self.stage_finished(table_id, stage, &actors));
            }
            RefreshEvent::Completed {
                table_id,
                trigger_time,
                aborted,
                reply,
            } => {
                let _ = reply.send(self.complete_cycle(table_id, trigger_time, aborted).await);
            }
            RefreshEvent::Recovered { database_id, reply } => {
                self.clear_trackers(database_id);
                let _ = reply.send(());
            }
            RefreshEvent::TableDropped { table_id, status } => {
                self.remove_tracker(table_id, &status);
            }
            RefreshEvent::Tick => self.handle_tick().await,
        }
    }

    async fn begin_cycle(
        &mut self,
        table_id: TableId,
        associated_source_id: Option<SourceId>,
        reason: TriggerReason,
    ) -> MetaResult<Collected> {
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
        self.table_ids_by_database
            .entry(database_id)
            .or_default()
            .insert(table_id);
        self.trackers.insert(table_id, tracker);

        let refresh_command = Command::Refresh {
            table_id,
            associated_source_id,
        };
        match self
            .barrier_scheduler
            .schedule_command(database_id, refresh_command)
        {
            Ok(collected) => {
                match reason {
                    TriggerReason::Manual => {}
                    TriggerReason::Cron => self.job_metrics(table_id).cron_trigger_count.inc(),
                    TriggerReason::RetryAfterAbort => {
                        self.retry_pending.remove(&table_id);
                    }
                }
                tracing::info!(%table_id, %trigger_time, ?reason, "refresh cycle started");
                Ok(collected.boxed())
            }
            Err(err) => {
                // Nothing was queued, so the cycle can be reverted right away.
                self.metadata_manager
                    .finish_refresh_job(table_id, trigger_time, false)
                    .await?;
                self.remove_tracker(table_id, "failure");
                Err(anyhow!(err)
                    .context(format!("Failed to refresh table {}", table_id))
                    .into())
            }
        }
    }

    fn stage_finished(
        &mut self,
        table_id: TableId,
        stage: RefreshStage,
        actors: &HashSet<ActorId>,
    ) -> MetaResult<bool> {
        let Some(tracker) = self.trackers.get_mut(&table_id) else {
            if stage == RefreshStage::Mview {
                // The cycle was dropped or abandoned meanwhile.
                tracing::warn!(%table_id, ?actors, "ignore refresh finished report without tracker");
                return Ok(false);
            }
            return Err(anyhow!("Table tracker not found for table {}", table_id).into());
        };
        let progress = match stage {
            RefreshStage::List => &mut tracker.list,
            RefreshStage::Fetch => &mut tracker.fetch,
            RefreshStage::Mview => &mut tracker.mview,
        };
        let complete = progress.report(stage, actors)?;
        if complete && stage == RefreshStage::Mview {
            let (database_id, staging_table_id, trigger_time) = (
                tracker.database_id,
                tracker.staging_table_id,
                tracker.trigger_time,
            );
            self.schedule_finish(table_id, database_id, staging_table_id, trigger_time, false);
        }
        Ok(complete)
    }

    async fn complete_cycle(
        &mut self,
        table_id: TableId,
        trigger_time: NaiveDateTime,
        aborted: bool,
    ) -> MetaResult<()> {
        let finished = self
            .metadata_manager
            .finish_refresh_job(table_id, trigger_time, !aborted)
            .await?;
        if self
            .pending_finish
            .get(&table_id)
            .is_some_and(|pending| pending.trigger_time == trigger_time)
        {
            self.pending_finish.remove(&table_id);
        }
        if self
            .trackers
            .get(&table_id)
            .is_some_and(|tracker| tracker.trigger_time == trigger_time)
        {
            self.remove_tracker(table_id, if aborted { "aborted" } else { "success" });
        }
        if !finished {
            tracing::warn!(%table_id, %trigger_time, aborted, "FinishRefresh collected for a cycle that is no longer current");
            return Ok(());
        }
        tracing::info!(%table_id, %trigger_time, aborted, "refresh cycle finished");
        if !aborted {
            self.retried_after_abort.remove(&table_id);
            self.retry_pending.remove(&table_id);
        } else if self.retried_after_abort.insert(table_id) {
            self.retry_pending.insert(table_id);
            self.spawn_trigger(table_id, TriggerReason::RetryAfterAbort);
        } else {
            tracing::warn!(%table_id, "abandoned refresh was already re-run once, waiting for the next trigger");
        }
        Ok(())
    }

    fn clear_trackers(&mut self, database_id: Option<DatabaseId>) {
        match database_id {
            None => {
                self.trackers.clear();
                self.table_ids_by_database.clear();
                self.pending_finish.clear();
            }
            Some(database_id) => {
                if let Some(table_ids) = self.table_ids_by_database.remove(&database_id) {
                    for table_id in table_ids {
                        self.trackers.remove(&table_id);
                    }
                }
                self.pending_finish
                    .retain(|_, pending| pending.database_id != database_id);
            }
        }
    }

    async fn handle_tick(&mut self) {
        let jobs = match self.metadata_manager.list_refresh_jobs().await {
            Ok(jobs) => jobs,
            Err(err) => {
                tracing::warn!(error = %err.as_report(), "failed to list refresh jobs");
                return;
            }
        };
        let active_table_ids = jobs.iter().map(|job| job.table_id).collect::<HashSet<_>>();
        self.metrics
            .retain(|table_id, _| active_table_ids.contains(table_id));
        self.retry_pending
            .retain(|table_id| active_table_ids.contains(table_id));
        self.retried_after_abort
            .retain(|table_id| active_table_ids.contains(table_id));
        self.pending_finish
            .retain(|table_id, _| active_table_ids.contains(table_id));

        for job in &jobs {
            let table_id = job.table_id;
            let cron_due = match self.cron_due(job).await {
                Ok(cron_due) => cron_due,
                Err(err) => {
                    tracing::warn!(%table_id, error = %err.as_report(), "failed to check the refresh schedule");
                    false
                }
            };
            if job.current_status != RefreshState::Idle {
                self.finish_pending(job).await;
                if cron_due {
                    self.job_metrics(table_id).cron_miss_count.inc();
                    tracing::warn!(%table_id, status = ?job.current_status, "skip scheduled refresh: the previous cycle is still running");
                }
                continue;
            }
            let reason = if self.retry_pending.contains(&table_id) {
                TriggerReason::RetryAfterAbort
            } else if cron_due {
                TriggerReason::Cron
            } else {
                continue;
            };
            self.spawn_trigger(table_id, reason);
        }
    }

    /// Failures are retried on the next tick.
    async fn finish_pending(&mut self, job: &refresh_job::Model) {
        let table_id = job.table_id;
        let Some(trigger_time) = job.last_trigger_time.map(millis_to_datetime) else {
            tracing::error!(%table_id, status = ?job.current_status, "refresh job is not idle but has no trigger time");
            return;
        };
        if self
            .pending_finish
            .get(&table_id)
            .is_some_and(|pending| pending.trigger_time == trigger_time)
        {
            return;
        }
        match self.trackers.get(&table_id) {
            Some(tracker) if tracker.trigger_time == trigger_time => {
                if tracker.mview.is_complete() {
                    let (database_id, staging_table_id) =
                        (tracker.database_id, tracker.staging_table_id);
                    self.schedule_finish(
                        table_id,
                        database_id,
                        staging_table_id,
                        trigger_time,
                        false,
                    );
                }
            }
            _ => match self.locate_abandoned_cycle(table_id).await {
                Ok(Some((database_id, staging_table_id))) => {
                    self.schedule_finish(
                        table_id,
                        database_id,
                        staging_table_id,
                        trigger_time,
                        true,
                    );
                }
                Ok(None) => {}
                Err(err) => {
                    tracing::warn!(%table_id, error = %err.as_report(), "failed to locate abandoned refresh, will retry");
                }
            },
        }
    }

    fn schedule_finish(
        &mut self,
        table_id: TableId,
        database_id: DatabaseId,
        staging_table_id: TableId,
        trigger_time: NaiveDateTime,
        aborted: bool,
    ) {
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
                self.pending_finish.insert(
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

    /// `None` if the table is gone.
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

    async fn cron_due(&self, job: &refresh_job::Model) -> MetaResult<bool> {
        let Some(interval) = job
            .trigger_interval_secs
            .filter(|secs| *secs > 0)
            .map(ChronoDuration::seconds)
        else {
            return Ok(false);
        };
        let last_run = match job.last_trigger_time {
            Some(last_run) => last_run,
            None => {
                let Some(table) = self
                    .metadata_manager
                    .get_table_catalog_by_ids(&[job.table_id])
                    .await?
                    .pop()
                else {
                    return Ok(false);
                };
                Epoch(table.created_at_epoch())
                    .as_timestamptz()
                    .to_datetime_utc()
                    .timestamp_millis()
            }
        };
        Ok(Utc::now()
            .naive_utc()
            .signed_duration_since(millis_to_datetime(last_run))
            >= interval)
    }

    /// The spawned task takes the reschedule lock, so the worker never waits for it.
    fn spawn_trigger(&mut self, table_id: TableId, reason: TriggerReason) {
        if !self.triggers_in_flight.insert(table_id) {
            return;
        }
        tracing::info!(%table_id, ?reason, "trigger refresh");
        let handle = self.handle.clone();
        tokio::spawn(async move {
            if let Err(err) = handle.trigger_cycle(table_id, None, reason).await {
                tracing::warn!(%table_id, ?reason, error = %err.as_report(), "failed to trigger refresh");
            }
        });
    }

    fn remove_tracker(&mut self, table_id: TableId, status: &str) {
        let Some(tracker) = self.trackers.remove(&table_id) else {
            return;
        };
        self.table_ids_by_database.values_mut().for_each(|set| {
            set.remove(&table_id);
        });
        let table_id_label = table_id.to_string();
        GLOBAL_META_METRICS
            .refresh_job_duration
            .with_guarded_label_values(&[&table_id_label, status])
            .set(tracker.start_time.elapsed().as_secs());
        self.job_metrics(table_id)
            .finished
            .entry(status.to_owned())
            .or_insert_with(|| RefreshFinishedMetrics::new(table_id, status))
            .count
            .inc();
    }

    fn job_metrics(&mut self, table_id: TableId) -> &mut RefreshJobMetrics {
        self.metrics
            .entry(table_id)
            .or_insert_with(|| RefreshJobMetrics::new(table_id))
    }
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
    /// True once every expected actor reported; an unexpected reporter is an error.
    pub fn report(&mut self, stage: RefreshStage, actors: &HashSet<ActorId>) -> MetaResult<bool> {
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

    pub fn is_complete(&self) -> bool {
        !self.expected.is_empty() && self.finished == self.expected
    }
}
