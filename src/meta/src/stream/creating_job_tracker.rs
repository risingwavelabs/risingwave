// Copyright 2026 RisingWave Labs
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

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use risingwave_common::catalog::{DatabaseId, FragmentTypeFlag};
use risingwave_common::id::JobId;
use risingwave_common::system_param::reader::SystemParamsRead;
use risingwave_common::util::stream_graph_visitor::visit_stream_node_body;
use risingwave_pb::common::ThrottleType;
use risingwave_pb::stream_plan::stream_node::PbNodeBody;
use risingwave_pb::stream_plan::throttle_mutation::ThrottleConfig;
use thiserror_ext::AsReport;
use tokio::sync::{Mutex, oneshot};

use crate::MetaResult;
use crate::barrier::{BarrierScheduler, Command};
use crate::error::MetaErrorInner;
use crate::manager::{LocalNotification, MetaSrvEnv, MetadataManager};
use crate::model::StreamJobFragments;

/// Snapshot of backfill limit presence and value for a job.
#[derive(Debug, Clone, Copy)]
pub(crate) struct BackfillRateLimitInfo {
    pub(crate) limit: Option<u32>,
    pub(crate) has_backfill_nodes: bool,
}

/// Extract backfill rate limit info from persisted fragments.
pub(crate) fn extract_backfill_rate_limit_from_fragments(
    fragments: &StreamJobFragments,
) -> BackfillRateLimitInfo {
    let mut seen_backfill_nodes = false;
    let mut rate_limit: Option<u32> = None;
    for fragment in fragments.fragments.values() {
        if !fragment
            .fragment_type_mask
            .contains_any(FragmentTypeFlag::backfill_rate_limit_fragments())
        {
            continue;
        }
        seen_backfill_nodes = true;
        visit_stream_node_body(&fragment.nodes, |node| {
            let node_rate_limit = match node {
                PbNodeBody::StreamCdcScan(node) => node.rate_limit,
                PbNodeBody::StreamScan(node) => node.rate_limit,
                PbNodeBody::SourceBackfill(node) => node.rate_limit,
                PbNodeBody::Sink(node) => node.rate_limit,
                _ => None,
            };
            let Some(node_rate_limit) = node_rate_limit else {
                return;
            };
            if let Some(existing) = rate_limit {
                if existing != node_rate_limit {
                    tracing::warn!(
                        existing,
                        node_rate_limit,
                        "inconsistent backfill rate limit detected in fragments"
                    );
                }
            } else {
                rate_limit = Some(node_rate_limit);
            }
        });
    }
    BackfillRateLimitInfo {
        limit: rate_limit,
        has_backfill_nodes: seen_backfill_nodes,
    }
}

// Tracks a creating job execution handle used for cancellation.
struct StreamingJobExecution {
    shutdown_tx: Option<oneshot::Sender<oneshot::Sender<bool>>>,
}

impl StreamingJobExecution {
    // Create a new execution entry.
    fn new(shutdown_tx: oneshot::Sender<oneshot::Sender<bool>>) -> Self {
        Self {
            shutdown_tx: Some(shutdown_tx),
        }
    }
}

// Tracks permit/queue state for a creating job.
enum PermitState {
    None,
    Running {
        permit: Option<tokio::sync::OwnedSemaphorePermit>,
    },
    Queued {
        database_id: DatabaseId,
        desired_rate_limit: Option<u32>,
    },
}

// Aggregates execution and permit state for a job.
struct CreatingJobEntry {
    execution: Option<StreamingJobExecution>,
    permit_state: PermitState,
}

impl CreatingJobEntry {
    // Create an empty entry.
    fn new() -> Self {
        Self {
            execution: None,
            permit_state: PermitState::None,
        }
    }

    // True when no execution or permit state is tracked.
    fn is_empty(&self) -> bool {
        self.execution.is_none() && matches!(self.permit_state, PermitState::None)
    }
}

/// Manages creating-job lifecycle, permits, and throttling queue.
pub struct CreatingJobTracker {
    metadata_manager: MetadataManager,
    barrier_scheduler: BarrierScheduler,
    semaphore: Arc<tokio::sync::Semaphore>,
    entries: Mutex<HashMap<JobId, CreatingJobEntry>>,
    queue: Mutex<VecDeque<JobId>>,
    notify: tokio::sync::Notify,
}

impl CreatingJobTracker {
    /// Build a tracker with current system param limits and recover background jobs.
    pub async fn new(
        env: MetaSrvEnv,
        metadata_manager: MetadataManager,
        barrier_scheduler: BarrierScheduler,
    ) -> MetaResult<Arc<Self>> {
        let mut permits = env
            .system_params_reader()
            .await
            .max_concurrent_creating_streaming_jobs() as usize;
        if permits == 0 {
            permits = tokio::sync::Semaphore::MAX_PERMITS;
        }

        let tracker = Arc::new(Self {
            metadata_manager,
            barrier_scheduler,
            semaphore: Arc::new(tokio::sync::Semaphore::new(permits)),
            entries: Mutex::new(HashMap::new()),
            queue: Mutex::new(VecDeque::new()),
            notify: tokio::sync::Notify::new(),
        });

        tracker.spawn_local_notification_listener(env, permits);
        tracker.spawn_drain_worker();

        tracker.recover_from_catalog().await?;
        Ok(tracker)
    }

    /// Rebuild permit/queue state based on persisted catalog state.
    pub async fn recover_from_catalog(self: &Arc<Self>) -> MetaResult<()> {
        let mut creating_jobs: Vec<_> = self
            .metadata_manager
            .list_background_creating_jobs()
            .await?
            .into_iter()
            .collect();
        creating_jobs.sort_unstable();

        for job_id in creating_jobs {
            if self.is_job_permit_tracked(job_id).await {
                continue;
            }
            let database_id = match self
                .metadata_manager
                .catalog_controller
                .get_object_database_id(job_id)
                .await
            {
                Ok(database_id) => database_id,
                Err(err) => {
                    tracing::warn!(
                        job_id = %job_id,
                        error = %err.as_report(),
                        "skip recovering job permit due to missing database"
                    );
                    continue;
                }
            };

            let (backfill_info, desired_rate_limit) = self
                .resolve_backfill_rate_limit_for_recovery(job_id)
                .await?;
            if !backfill_info.has_backfill_nodes {
                continue;
            }

            if desired_rate_limit == Some(0) {
                continue;
            }

            if backfill_info.limit == Some(0) {
                self.enqueue_job(job_id, database_id, desired_rate_limit)
                    .await;
                continue;
            }

            match self.semaphore.clone().try_acquire_owned() {
                Ok(permit) => {
                    self.add_running_job(job_id, database_id, Some(permit))
                        .await;
                }
                Err(_) => {
                    tracing::warn!(
                        job_id = %job_id,
                        "insufficient permits during recovery; tracking job as running without permit"
                    );
                    self.add_running_job(job_id, database_id, None).await;
                }
            }
        }

        Ok(())
    }

    /// Register a creating job execution handle.
    pub async fn add_execution(
        &self,
        job_id: JobId,
        shutdown_tx: oneshot::Sender<oneshot::Sender<bool>>,
    ) {
        let mut entries = self.entries.lock().await;
        let entry = entries.entry(job_id).or_insert_with(CreatingJobEntry::new);
        entry.execution = Some(StreamingJobExecution::new(shutdown_tx));
    }

    /// Remove the execution handle for a job.
    pub async fn remove_execution(&self, job_id: JobId) {
        let mut entries = self.entries.lock().await;
        if let Some(entry) = entries.get_mut(&job_id) {
            entry.execution = None;
            if entry.is_empty() {
                entries.remove(&job_id);
            }
        }
    }

    /// Cancel jobs, release permits, and return cancellation receivers.
    pub async fn cancel_jobs(
        self: &Arc<Self>,
        job_ids: Vec<JobId>,
    ) -> MetaResult<(HashMap<JobId, oneshot::Receiver<bool>>, Vec<JobId>)> {
        let mut receivers = HashMap::new();
        let mut background_job_ids = vec![];
        let mut queue_removals = Vec::new();
        let mut permits_to_drop = Vec::new();
        let mut entries = self.entries.lock().await;

        for job_id in job_ids {
            if let Some(entry) = entries.get_mut(&job_id) {
                match entry.permit_state {
                    PermitState::Queued { .. } => {
                        queue_removals.push(job_id);
                        entry.permit_state = PermitState::None;
                    }
                    PermitState::Running { ref mut permit, .. } => {
                        if let Some(permit) = permit.take() {
                            permits_to_drop.push(permit);
                        }
                        entry.permit_state = PermitState::None;
                    }
                    PermitState::None => {}
                }

                match entry.execution.as_mut() {
                    Some(execution) => {
                        if let Some(shutdown_tx) = execution.shutdown_tx.take() {
                            let (tx, rx) = oneshot::channel();
                            if shutdown_tx.send(tx).is_err() {
                                return Err(anyhow::anyhow!(
                                    "failed to send shutdown signal for streaming job {}: receiver dropped",
                                    job_id
                                )
                                .into());
                            }
                            receivers.insert(job_id, rx);
                        }
                    }
                    None => {
                        background_job_ids.push(job_id);
                    }
                }

                if entry.is_empty() {
                    entries.remove(&job_id);
                }
            } else {
                background_job_ids.push(job_id);
            }
        }
        drop(entries);

        if !queue_removals.is_empty() {
            let mut queue = self.queue.lock().await;
            queue.retain(|job_id| !queue_removals.contains(job_id));
        }

        drop(permits_to_drop);
        self.notify.notify_one();

        Ok((receivers, background_job_ids))
    }

    /// Register a job for permit tracking after barrier completes.
    pub async fn register_job(
        self: &Arc<Self>,
        job_id: JobId,
        database_id: DatabaseId,
        backfill_info: BackfillRateLimitInfo,
    ) -> MetaResult<()> {
        if !backfill_info.has_backfill_nodes {
            return Ok(());
        }
        if let Err(err) = self
            .metadata_manager
            .init_job_backfill_rate_limit_if_missing(job_id, backfill_info.limit)
            .await
        {
            tracing::warn!(
                job_id = %job_id,
                error = %err.as_report(),
                "failed to initialize backfill rate limit"
            );
        }
        if backfill_info.limit == Some(0) {
            return Ok(());
        }

        if self.is_job_permit_tracked(job_id).await {
            return Ok(());
        }

        match self.semaphore.clone().try_acquire_owned() {
            Ok(permit) => {
                self.add_running_job(job_id, database_id, Some(permit))
                    .await;
                Ok(())
            }
            Err(_) => {
                self.pause_job(job_id, database_id).await?;
                self.enqueue_job(job_id, database_id, backfill_info.limit)
                    .await;
                self.notify.notify_one();
                Ok(())
            }
        }
    }

    // Resolve desired backfill limit for recovery without mutating catalog.
    async fn resolve_backfill_rate_limit_for_recovery(
        self: &Arc<Self>,
        job_id: JobId,
    ) -> MetaResult<(BackfillRateLimitInfo, Option<u32>)> {
        let fragments = self
            .metadata_manager
            .get_job_fragments_by_id(job_id)
            .await?;
        let backfill_info = extract_backfill_rate_limit_from_fragments(&fragments);
        let desired_rate_limit = match self
            .metadata_manager
            .get_job_backfill_rate_limit(job_id)
            .await?
        {
            Some(limit) if limit >= 0 => Some(limit as u32),
            Some(_) => None,
            None => backfill_info.limit,
        };
        Ok((backfill_info, desired_rate_limit))
    }

    // Enqueue a job waiting for permit.
    async fn enqueue_job(
        self: &Arc<Self>,
        job_id: JobId,
        database_id: DatabaseId,
        desired_rate_limit: Option<u32>,
    ) {
        let mut entries = self.entries.lock().await;
        let entry = entries.entry(job_id).or_insert_with(CreatingJobEntry::new);
        if matches!(entry.permit_state, PermitState::Queued { .. }) {
            return;
        }
        entry.permit_state = PermitState::Queued {
            database_id,
            desired_rate_limit,
        };
        drop(entries);

        let mut queue = self.queue.lock().await;
        queue.push_back(job_id);
    }

    // Pause backfill by setting rate limit to zero.
    async fn pause_job(self: &Arc<Self>, job_id: JobId, database_id: DatabaseId) -> MetaResult<()> {
        self.apply_backfill_rate_limit(job_id, database_id, Some(0))
            .await
    }

    // Resume backfill with the desired rate limit.
    async fn resume_job(
        self: &Arc<Self>,
        job_id: JobId,
        database_id: DatabaseId,
        desired_rate_limit: Option<u32>,
    ) -> MetaResult<()> {
        self.apply_backfill_rate_limit(job_id, database_id, desired_rate_limit)
            .await
    }

    // Apply a backfill rate limit through fragments and barrier.
    async fn apply_backfill_rate_limit(
        self: &Arc<Self>,
        job_id: JobId,
        database_id: DatabaseId,
        rate_limit: Option<u32>,
    ) -> MetaResult<()> {
        let fragments = self
            .metadata_manager
            .apply_backfill_rate_limit_by_job_id_without_override(job_id, rate_limit)
            .await?;
        if fragments.is_empty() {
            return Ok(());
        }
        let jobs = HashSet::from_iter([job_id]);
        let throttle_config = ThrottleConfig {
            rate_limit,
            throttle_type: ThrottleType::Backfill as i32,
        };
        let config = fragments
            .into_iter()
            .map(|fragment_id| (fragment_id, throttle_config))
            .collect();
        self.barrier_scheduler
            .run_command(database_id, Command::Throttle { jobs, config })
            .await?;
        Ok(())
    }

    // Track a running job and spawn finish watcher if needed.
    async fn add_running_job(
        self: &Arc<Self>,
        job_id: JobId,
        database_id: DatabaseId,
        permit: Option<tokio::sync::OwnedSemaphorePermit>,
    ) {
        let mut entries = self.entries.lock().await;
        let entry = entries.entry(job_id).or_insert_with(CreatingJobEntry::new);
        let should_spawn = !matches!(entry.permit_state, PermitState::Running { .. });
        entry.permit_state = PermitState::Running { permit };
        if should_spawn {
            drop(entries);
            self.spawn_finish_watcher(job_id, database_id);
        }
    }

    // Watch for job finish and release permit.
    fn spawn_finish_watcher(self: &Arc<Self>, job_id: JobId, database_id: DatabaseId) {
        let tracker = Arc::clone(self);
        let metadata_manager = self.metadata_manager.clone();
        tokio::spawn(async move {
            let _ = metadata_manager
                .wait_streaming_job_finished(database_id, job_id)
                .await;
            tracker.on_job_finished(job_id).await;
        });
    }

    // Release permit state and wake drain worker.
    async fn on_job_finished(self: &Arc<Self>, job_id: JobId) {
        let mut entries = self.entries.lock().await;
        if let Some(entry) = entries.get_mut(&job_id) {
            if let PermitState::Running { ref mut permit, .. } = entry.permit_state {
                if let Some(permit) = permit.take() {
                    drop(permit);
                }
            }
            entry.permit_state = PermitState::None;
            if entry.is_empty() {
                entries.remove(&job_id);
            }
        }
        drop(entries);
        self.notify.notify_one();
    }

    // Check whether a job has permit or queue state tracked.
    async fn is_job_permit_tracked(self: &Arc<Self>, job_id: JobId) -> bool {
        self.entries
            .lock()
            .await
            .get(&job_id)
            .is_some_and(|entry| !matches!(entry.permit_state, PermitState::None))
    }

    // Listen for system param changes and adjust permits.
    fn spawn_local_notification_listener(self: &Arc<Self>, env: MetaSrvEnv, mut permits: usize) {
        let (local_notification_tx, mut local_notification_rx) =
            tokio::sync::mpsc::unbounded_channel();
        env.notification_manager()
            .insert_local_sender(local_notification_tx);
        let tracker = Arc::clone(self);
        let semaphore = Arc::clone(&self.semaphore);
        tokio::spawn(async move {
            while let Some(notification) = local_notification_rx.recv().await {
                let LocalNotification::SystemParamsChange(p) = &notification else {
                    continue;
                };
                let mut new_permits = p.max_concurrent_creating_streaming_jobs() as usize;
                if new_permits == 0 {
                    new_permits = tokio::sync::Semaphore::MAX_PERMITS;
                }
                match permits.cmp(&new_permits) {
                    std::cmp::Ordering::Less => {
                        semaphore.add_permits(new_permits - permits);
                        tracker.notify.notify_one();
                    }
                    std::cmp::Ordering::Equal => {}
                    std::cmp::Ordering::Greater => {
                        let to_release = permits - new_permits;
                        let reduced = semaphore.forget_permits(to_release);
                        if reduced != to_release {
                            tracing::warn!(
                                "no enough permits to release, expected {}, but reduced {}",
                                to_release,
                                reduced
                            );
                        }
                    }
                }
                tracing::info!(
                    "max_concurrent_creating_streaming_jobs changed from {} to {}",
                    permits,
                    new_permits
                );
                permits = new_permits;
            }
        });
    }

    // Background task to drain queued jobs when permits become available.
    fn spawn_drain_worker(self: &Arc<Self>) {
        let tracker = Arc::clone(self);
        tokio::spawn(async move {
            loop {
                tracker.notify.notified().await;
                tracker.drain_queue().await;
            }
        });
    }

    // Attempt to resume queued jobs in FIFO order.
    async fn drain_queue(self: &Arc<Self>) {
        loop {
            let next = {
                let mut queue = self.queue.lock().await;
                queue.pop_front()
            };
            let Some(job_id) = next else {
                break;
            };

            let (database_id, desired_rate_limit) = {
                let entries = self.entries.lock().await;
                let Some(entry) = entries.get(&job_id) else {
                    continue;
                };
                match &entry.permit_state {
                    PermitState::Queued {
                        database_id,
                        desired_rate_limit,
                    } => (*database_id, *desired_rate_limit),
                    _ => continue,
                }
            };

            let permit = match self.semaphore.clone().try_acquire_owned() {
                Ok(permit) => permit,
                Err(_) => {
                    let mut queue = self.queue.lock().await;
                    queue.push_front(job_id);
                    break;
                }
            };

            match self
                .resume_job(job_id, database_id, desired_rate_limit)
                .await
            {
                Ok(()) => {
                    self.add_running_job(job_id, database_id, Some(permit))
                        .await;
                }
                Err(err) => {
                    if matches!(err.inner(), MetaErrorInner::CatalogIdNotFound(..))
                        || err.is_cancelled()
                    {
                        drop(permit);
                        self.clear_permit_state(job_id).await;
                        continue;
                    }
                    tracing::warn!(
                        job_id = %job_id,
                        error = %err.as_report(),
                        "failed to resume queued job"
                    );
                    drop(permit);
                    self.enqueue_job(job_id, database_id, desired_rate_limit)
                        .await;
                    break;
                }
            }
        }
    }

    // Clear permit state for a job and drop empty entries.
    async fn clear_permit_state(self: &Arc<Self>, job_id: JobId) {
        let mut entries = self.entries.lock().await;
        if let Some(entry) = entries.get_mut(&job_id) {
            entry.permit_state = PermitState::None;
            if entry.is_empty() {
                entries.remove(&job_id);
            }
        }
    }
}
