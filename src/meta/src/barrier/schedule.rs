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

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, anyhow};
use assert_matches::assert_matches;
use await_tree::InstrumentAwait;
use itertools::Itertools;
use parking_lot::Mutex;
use prometheus::HistogramTimer;
use risingwave_common::catalog::{DatabaseId, TableId};
use risingwave_common::metrics::LabelGuardedHistogram;
use risingwave_hummock_sdk::HummockVersionId;
use tokio::select;
use tokio::sync::{oneshot, watch};
use tokio::time::Interval;
use tracing::{info, warn};

use super::notifier::Notifier;
use super::{Command, Scheduled};
use crate::barrier::context::GlobalBarrierWorkerContext;
use crate::hummock::HummockManagerRef;
use crate::rpc::metrics::MetaMetrics;
use crate::{MetaError, MetaResult};

pub(super) struct NewBarrier {
    pub command: Option<(DatabaseId, Command, Vec<Notifier>)>,
    pub span: tracing::Span,
    pub checkpoint: bool,
}

/// A queue for scheduling barriers.
///
/// We manually implement one here instead of using channels since we may need to update the front
/// of the queue to add some notifiers for instant flushes.
struct Inner {
    queue: Mutex<ScheduledQueue>,

    /// When `queue` is not empty anymore, all subscribers of this watcher will be notified.
    changed_tx: watch::Sender<()>,

    /// Used for recording send latency of each barrier.
    metrics: Arc<MetaMetrics>,
}

#[derive(Debug)]
enum QueueStatus {
    /// The queue is ready to accept new command.
    Ready,
    /// The queue is blocked to accept new command with the given reason.
    Blocked(String),
}

impl QueueStatus {
    fn is_blocked(&self) -> bool {
        matches!(self, Self::Blocked(_))
    }
}

struct ScheduledQueueItem {
    command: Command,
    notifiers: Vec<Notifier>,
    send_latency_timer: HistogramTimer,
    span: tracing::Span,
}

struct StatusQueue<T> {
    queue: T,
    status: QueueStatus,
}

struct DatabaseQueue {
    inner: VecDeque<ScheduledQueueItem>,
    send_latency: LabelGuardedHistogram,
}

type DatabaseScheduledQueue = StatusQueue<DatabaseQueue>;
type ScheduledQueue = StatusQueue<HashMap<DatabaseId, DatabaseScheduledQueue>>;

impl DatabaseScheduledQueue {
    fn new(database_id: DatabaseId, metrics: &MetaMetrics, status: QueueStatus) -> Self {
        Self {
            queue: DatabaseQueue {
                inner: Default::default(),
                send_latency: metrics
                    .barrier_send_latency
                    .with_guarded_label_values(&[database_id.database_id.to_string().as_str()]),
            },
            status,
        }
    }
}

impl<T> StatusQueue<T> {
    fn mark_blocked(&mut self, reason: String) {
        self.status = QueueStatus::Blocked(reason);
    }

    fn mark_ready(&mut self) -> bool {
        let prev_blocked = self.status.is_blocked();
        self.status = QueueStatus::Ready;
        prev_blocked
    }

    fn validate_item(&mut self, command: &Command) -> MetaResult<()> {
        // We don't allow any command to be scheduled when the queue is blocked, except for dropping streaming jobs.
        // Because we allow dropping streaming jobs when the cluster is under recovery, so we have to buffer the drop
        // command and execute it when the cluster is ready to clean up it.
        // TODO: this is just a workaround to allow dropping streaming jobs when the cluster is under recovery,
        // we need to refine it when catalog and streaming metadata can be handled in a transactional way.
        if let QueueStatus::Blocked(reason) = &self.status
            && !matches!(
                command,
                Command::DropStreamingJobs { .. } | Command::DropSubscription { .. }
            )
        {
            return Err(MetaError::unavailable(reason));
        }
        Ok(())
    }
}

fn tracing_span() -> tracing::Span {
    if tracing::Span::current().is_none() {
        tracing::Span::none()
    } else {
        tracing::info_span!(
            "barrier",
            checkpoint = tracing::field::Empty,
            epoch = tracing::field::Empty
        )
    }
}

/// The sender side of the barrier scheduling queue.
/// Can be cloned and held by other managers to schedule and run barriers.
#[derive(Clone)]
pub struct BarrierScheduler {
    inner: Arc<Inner>,

    /// Used for getting the latest snapshot after `FLUSH`.
    hummock_manager: HummockManagerRef,
}

impl BarrierScheduler {
    /// Create a pair of [`BarrierScheduler`] and [`ScheduledBarriers`], for scheduling barriers
    /// from different managers, and executing them in the barrier manager, respectively.
    pub fn new_pair(
        hummock_manager: HummockManagerRef,
        metrics: Arc<MetaMetrics>,
    ) -> (Self, ScheduledBarriers) {
        let inner = Arc::new(Inner {
            queue: Mutex::new(ScheduledQueue {
                queue: Default::default(),
                status: QueueStatus::Ready,
            }),
            changed_tx: watch::channel(()).0,
            metrics,
        });

        (
            Self {
                inner: inner.clone(),
                hummock_manager,
            },
            ScheduledBarriers { inner },
        )
    }

    /// Push a scheduled barrier into the queue.
    fn push(
        &self,
        database_id: DatabaseId,
        scheduleds: impl IntoIterator<Item = (Command, Notifier)>,
    ) -> MetaResult<()> {
        let mut queue = self.inner.queue.lock();
        let scheduleds = scheduleds.into_iter().collect_vec();
        scheduleds
            .iter()
            .try_for_each(|(command, _)| queue.validate_item(command))?;
        let queue = queue.queue.entry(database_id).or_insert_with(|| {
            DatabaseScheduledQueue::new(database_id, &self.inner.metrics, QueueStatus::Ready)
        });
        scheduleds
            .iter()
            .try_for_each(|(command, _)| queue.validate_item(command))?;
        for (command, notifier) in scheduleds {
            queue.queue.inner.push_back(ScheduledQueueItem {
                command,
                notifiers: vec![notifier],
                send_latency_timer: queue.queue.send_latency.start_timer(),
                span: tracing_span(),
            });
            if queue.queue.inner.len() == 1 {
                self.inner.changed_tx.send(()).ok();
            }
        }
        Ok(())
    }

    /// Try to cancel scheduled cmd for create streaming job, return true if the command exists previously and get cancelled.
    pub fn try_cancel_scheduled_create(&self, database_id: DatabaseId, table_id: TableId) -> bool {
        let queue = &mut self.inner.queue.lock();
        let Some(queue) = queue.queue.get_mut(&database_id) else {
            return false;
        };

        if let Some(idx) = queue.queue.inner.iter().position(|scheduled| {
            if let Command::CreateStreamingJob { info, .. } = &scheduled.command
                && info.stream_job_fragments.stream_job_id() == table_id
            {
                true
            } else {
                false
            }
        }) {
            queue.queue.inner.remove(idx).unwrap();
            true
        } else {
            false
        }
    }

    /// Run multiple commands and return when they're all completely finished (i.e., collected). It's ensured that
    /// multiple commands are executed continuously.
    ///
    /// Returns the barrier info of each command.
    ///
    /// TODO: atomicity of multiple commands is not guaranteed.
    #[await_tree::instrument("run_commands({})", commands.iter().join(", "))]
    async fn run_multiple_commands(
        &self,
        database_id: DatabaseId,
        commands: Vec<Command>,
    ) -> MetaResult<()> {
        let mut contexts = Vec::with_capacity(commands.len());
        let mut scheduleds = Vec::with_capacity(commands.len());

        for command in commands {
            let (started_tx, started_rx) = oneshot::channel();
            let (collect_tx, collect_rx) = oneshot::channel();

            contexts.push((started_rx, collect_rx));
            scheduleds.push((
                command,
                Notifier {
                    started: Some(started_tx),
                    collected: Some(collect_tx),
                },
            ));
        }

        self.push(database_id, scheduleds)?;

        for (injected_rx, collect_rx) in contexts {
            // Wait for this command to be injected, and record the result.
            tracing::trace!("waiting for injected_rx");
            injected_rx
                .instrument_await("wait_injected")
                .await
                .ok()
                .context("failed to inject barrier")??;

            tracing::trace!("waiting for collect_rx");
            // Throw the error if it occurs when collecting this barrier.
            collect_rx
                .instrument_await("wait_collected")
                .await
                .ok()
                .context("failed to collect barrier")??;
        }

        Ok(())
    }

    /// Run a command and return when it's completely finished (i.e., collected).
    ///
    /// Returns the barrier info of the actual command.
    pub async fn run_command(&self, database_id: DatabaseId, command: Command) -> MetaResult<()> {
        tracing::trace!("run_command: {:?}", command);
        let ret = self.run_multiple_commands(database_id, vec![command]).await;
        tracing::trace!("run_command finished");
        ret
    }

    /// Flush means waiting for the next barrier to collect.
    pub async fn flush(&self, database_id: DatabaseId) -> MetaResult<HummockVersionId> {
        let start = Instant::now();

        tracing::debug!("start barrier flush");
        self.run_multiple_commands(database_id, vec![Command::Flush])
            .await?;

        let elapsed = Instant::now().duration_since(start);
        tracing::debug!("barrier flushed in {:?}", elapsed);

        let version_id = self.hummock_manager.get_version_id().await;
        Ok(version_id)
    }
}

/// The receiver side of the barrier scheduling queue.
pub struct ScheduledBarriers {
    inner: Arc<Inner>,
}

/// Held by the [`crate::barrier::worker::GlobalBarrierWorker`] to execute these commands.
pub(super) struct PeriodicBarriers {
    min_interval: Interval,

    /// Force checkpoint in next barrier.
    force_checkpoint: bool,

    /// The numbers of barrier (checkpoint = false) since the last barrier (checkpoint = true)
    num_uncheckpointed_barrier: usize,
    checkpoint_frequency: usize,
}

impl PeriodicBarriers {
    pub(super) fn new(min_interval: Duration, checkpoint_frequency: usize) -> PeriodicBarriers {
        Self {
            min_interval: tokio::time::interval(min_interval),
            force_checkpoint: false,
            num_uncheckpointed_barrier: 0,
            checkpoint_frequency,
        }
    }

    pub(super) fn set_min_interval(&mut self, min_interval: Duration) {
        let set_new_interval = min_interval != self.min_interval.period();
        if set_new_interval {
            let mut min_interval = tokio::time::interval(min_interval);
            min_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            self.min_interval = min_interval;
        }
    }

    #[await_tree::instrument]
    pub(super) async fn next_barrier(
        &mut self,
        context: &impl GlobalBarrierWorkerContext,
    ) -> NewBarrier {
        let checkpoint = self.try_get_checkpoint();
        let scheduled = select! {
            biased;
            scheduled = context.next_scheduled() => {
                self.min_interval.reset();
                let checkpoint = scheduled.command.need_checkpoint() || checkpoint;
                NewBarrier {
                    command: Some((scheduled.database_id, scheduled.command, scheduled.notifiers)),
                    span: scheduled.span,
                    checkpoint,
                }
            },
            _ = self.min_interval.tick() => {
                NewBarrier {
                    command: None,
                    span: tracing_span(),
                    checkpoint,
                }
            }
        };
        self.update_num_uncheckpointed_barrier(scheduled.checkpoint);
        scheduled
    }
}

impl ScheduledBarriers {
    pub(super) async fn next_scheduled(&self) -> Scheduled {
        'outer: loop {
            let mut rx = self.inner.changed_tx.subscribe();
            {
                let mut queue = self.inner.queue.lock();
                if queue.status.is_blocked() {
                    continue;
                }
                for (database_id, queue) in &mut queue.queue {
                    if queue.status.is_blocked() {
                        continue;
                    }
                    if let Some(item) = queue.queue.inner.pop_front() {
                        item.send_latency_timer.observe_duration();
                        break 'outer Scheduled {
                            database_id: *database_id,
                            command: item.command,
                            notifiers: item.notifiers,
                            span: item.span,
                        };
                    }
                }
            }
            rx.changed().await.unwrap();
        }
    }
}

pub(super) enum MarkReadyOptions {
    Database(DatabaseId),
    Global {
        blocked_databases: HashSet<DatabaseId>,
    },
}

impl ScheduledBarriers {
    /// Pre buffered drop and cancel command, return true if any.
    pub(super) fn pre_apply_drop_cancel(&self, database_id: Option<DatabaseId>) -> bool {
        self.pre_apply_drop_cancel_scheduled(database_id)
    }

    /// Mark command scheduler as blocked and abort all queued scheduled command and notify with
    /// specific reason.
    pub(super) fn abort_and_mark_blocked(
        &self,
        database_id: Option<DatabaseId>,
        reason: impl Into<String>,
    ) {
        let mut queue = self.inner.queue.lock();
        fn database_blocked_reason(database_id: DatabaseId, reason: &String) -> String {
            format!("database {} unavailable {}", database_id, reason)
        }
        fn mark_blocked_and_notify_failed(
            database_id: DatabaseId,
            queue: &mut DatabaseScheduledQueue,
            reason: &String,
        ) {
            let reason = database_blocked_reason(database_id, reason);
            let err: MetaError = anyhow!("{}", reason).into();
            queue.mark_blocked(reason);
            while let Some(ScheduledQueueItem { notifiers, .. }) = queue.queue.inner.pop_front() {
                notifiers
                    .into_iter()
                    .for_each(|notify| notify.notify_collection_failed(err.clone()))
            }
        }
        if let Some(database_id) = database_id {
            let reason = reason.into();
            match queue.queue.entry(database_id) {
                Entry::Occupied(entry) => {
                    let queue = entry.into_mut();
                    if queue.status.is_blocked() {
                        if cfg!(debug_assertions) {
                            panic!("database {} marked as blocked twice", database_id);
                        } else {
                            warn!(?database_id, "database marked as blocked twice");
                        }
                    }
                    info!(?database_id, "database marked as blocked");
                    mark_blocked_and_notify_failed(database_id, queue, &reason);
                }
                Entry::Vacant(entry) => {
                    entry.insert(DatabaseScheduledQueue::new(
                        database_id,
                        &self.inner.metrics,
                        QueueStatus::Blocked(database_blocked_reason(database_id, &reason)),
                    ));
                }
            }
        } else {
            let reason = reason.into();
            if queue.status.is_blocked() {
                if cfg!(debug_assertions) {
                    panic!("cluster marked as blocked twice");
                } else {
                    warn!("cluster marked as blocked twice");
                }
            }
            info!("cluster marked as blocked");
            queue.mark_blocked(reason.clone());
            for (database_id, queue) in &mut queue.queue {
                mark_blocked_and_notify_failed(*database_id, queue, &reason);
            }
        }
    }

    /// Mark command scheduler as ready to accept new command.
    pub(super) fn mark_ready(&self, options: MarkReadyOptions) {
        let mut queue = self.inner.queue.lock();
        let queue = &mut *queue;
        match options {
            MarkReadyOptions::Database(database_id) => {
                info!(?database_id, "database marked as ready");
                let database_queue = queue.queue.entry(database_id).or_insert_with(|| {
                    DatabaseScheduledQueue::new(
                        database_id,
                        &self.inner.metrics,
                        QueueStatus::Ready,
                    )
                });
                if !database_queue.status.is_blocked() {
                    if cfg!(debug_assertions) {
                        panic!("database {} marked as ready twice", database_id);
                    } else {
                        warn!(?database_id, "database marked as ready twice");
                    }
                }
                if database_queue.mark_ready()
                    && !queue.status.is_blocked()
                    && !database_queue.queue.inner.is_empty()
                {
                    self.inner.changed_tx.send(()).ok();
                }
            }
            MarkReadyOptions::Global { blocked_databases } => {
                if !queue.status.is_blocked() {
                    if cfg!(debug_assertions) {
                        panic!("cluster marked as ready twice");
                    } else {
                        warn!("cluster marked as ready twice");
                    }
                }
                info!(?blocked_databases, "cluster marked as ready");
                let prev_blocked = queue.mark_ready();
                for database_id in &blocked_databases {
                    queue.queue.entry(*database_id).or_insert_with(|| {
                        DatabaseScheduledQueue::new(
                            *database_id,
                            &self.inner.metrics,
                            QueueStatus::Blocked(format!(
                                "database {} failed to recover in global recovery",
                                database_id
                            )),
                        )
                    });
                }
                for (database_id, queue) in &mut queue.queue {
                    if !blocked_databases.contains(database_id) {
                        queue.mark_ready();
                    }
                }
                if prev_blocked
                    && queue
                        .queue
                        .values()
                        .any(|database_queue| !database_queue.queue.inner.is_empty())
                {
                    self.inner.changed_tx.send(()).ok();
                }
            }
        }
    }

    /// Try to pre apply drop and cancel scheduled command and return them if any.
    /// It should only be called in recovery.
    pub(super) fn pre_apply_drop_cancel_scheduled(&self, database_id: Option<DatabaseId>) -> bool {
        let mut queue = self.inner.queue.lock();
        let mut applied = false;

        let mut pre_apply_drop_cancel = |queue: &mut DatabaseScheduledQueue| {
            while let Some(ScheduledQueueItem {
                notifiers, command, ..
            }) = queue.queue.inner.pop_front()
            {
                match command {
                    Command::DropStreamingJobs { .. } => {
                        applied = true;
                    }
                    Command::DropSubscription { .. } => {}
                    _ => {
                        unreachable!("only drop and cancel streaming jobs should be buffered");
                    }
                }
                notifiers.into_iter().for_each(|notify| {
                    notify.notify_collected();
                });
            }
        };

        if let Some(database_id) = database_id {
            assert_matches!(queue.status, QueueStatus::Ready);
            if let Some(queue) = queue.queue.get_mut(&database_id) {
                assert_matches!(queue.status, QueueStatus::Blocked(_));
                pre_apply_drop_cancel(queue);
            }
        } else {
            assert_matches!(queue.status, QueueStatus::Blocked(_));
            for queue in queue.queue.values_mut() {
                pre_apply_drop_cancel(queue);
            }
        }

        applied
    }
}

impl PeriodicBarriers {
    /// Whether the barrier(checkpoint = true) should be injected.
    fn try_get_checkpoint(&self) -> bool {
        self.num_uncheckpointed_barrier + 1 >= self.checkpoint_frequency || self.force_checkpoint
    }

    /// Make the `checkpoint` of the next barrier must be true
    pub fn force_checkpoint_in_next_barrier(&mut self) {
        self.force_checkpoint = true;
    }

    /// Update the `checkpoint_frequency`
    pub fn set_checkpoint_frequency(&mut self, frequency: usize) {
        self.checkpoint_frequency = frequency;
    }

    /// Update the `num_uncheckpointed_barrier`
    fn update_num_uncheckpointed_barrier(&mut self, checkpoint: bool) {
        if checkpoint {
            self.num_uncheckpointed_barrier = 0;
            self.force_checkpoint = false;
        } else {
            self.num_uncheckpointed_barrier += 1;
        }
    }
}
