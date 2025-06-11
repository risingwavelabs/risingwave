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
use risingwave_pb::catalog::Database;
use tokio::select;
use tokio::sync::{oneshot, watch};
use tokio_stream::wrappers::IntervalStream;
use tokio_stream::{StreamExt, StreamMap};
use tracing::{info, warn};

use super::notifier::Notifier;
use super::{Command, Scheduled};
use crate::barrier::context::GlobalBarrierWorkerContext;
use crate::hummock::HummockManagerRef;
use crate::rpc::metrics::MetaMetrics;
use crate::{MetaError, MetaResult};

pub(super) struct NewBarrier {
    pub database_id: DatabaseId,
    pub command: Option<(Command, Vec<Notifier>)>,
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

/// State specific to each database for barrier generation.
#[derive(Debug)]
pub struct DatabaseBarrierState {
    pub barrier_interval: Option<Duration>,
    pub checkpoint_frequency: Option<u64>,
    // Force checkpoint in next barrier.
    pub force_checkpoint: bool,
    // The numbers of barrier (checkpoint = false) since the last barrier (checkpoint = true)
    pub num_uncheckpointed_barrier: u64,
}

impl DatabaseBarrierState {
    fn new(barrier_interval_ms: Option<u32>, checkpoint_frequency: Option<u64>) -> Self {
        Self {
            barrier_interval: barrier_interval_ms.map(|ms| Duration::from_millis(ms as u64)),
            checkpoint_frequency,
            force_checkpoint: false,
            num_uncheckpointed_barrier: 0,
        }
    }
}

/// Held by the [`crate::barrier::worker::GlobalBarrierWorker`] to execute these commands.
#[derive(Default, Debug)]
pub struct PeriodicBarriers {
    /// Default system params for barrier interval and checkpoint frequency.
    sys_barrier_interval: Duration,
    sys_checkpoint_frequency: u64,
    /// Per-database state.
    databases: HashMap<DatabaseId, DatabaseBarrierState>,
    /// Holds `IntervalStream` for each database, keyed by `DatabaseId`.
    /// `StreamMap` will yield `(DatabaseId, Instant)` when a timer ticks.
    timer_streams: StreamMap<DatabaseId, IntervalStream>,
}

impl PeriodicBarriers {
    pub(super) fn new(
        sys_barrier_interval: Duration,
        sys_checkpoint_frequency: u64,
        database_infos: Vec<Database>,
    ) -> Self {
        let mut databases = HashMap::with_capacity(database_infos.len());
        let mut timer_streams = StreamMap::with_capacity(database_infos.len());
        database_infos.into_iter().for_each(|database| {
            let database_id: DatabaseId = database.id.into();
            let barrier_interval_ms = database.barrier_interval_ms;
            let checkpoint_frequency = database.checkpoint_frequency;
            databases.insert(
                database_id,
                DatabaseBarrierState::new(barrier_interval_ms, checkpoint_frequency),
            );
            let duration = if let Some(ms) = barrier_interval_ms {
                Duration::from_millis(ms as u64)
            } else {
                sys_barrier_interval
            };
            // Create an `IntervalStream` for the database with the specified interval.
            let mut interval = tokio::time::interval(duration);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            timer_streams.insert(database_id, IntervalStream::new(interval));
        });
        Self {
            sys_barrier_interval,
            sys_checkpoint_frequency,
            databases,
            timer_streams,
        }
    }

    /// Update the system barrier interval.
    pub(super) fn set_sys_barrier_interval(&mut self, duration: Duration) {
        if self.sys_barrier_interval == duration {
            return;
        }
        self.sys_barrier_interval = duration;
        // Reset the `IntervalStream` for all databases that use default param.
        for (db_id, db_state) in &mut self.databases {
            if db_state.barrier_interval.is_none() {
                let mut interval = tokio::time::interval(duration);
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                self.timer_streams
                    .insert(*db_id, IntervalStream::new(interval));
            }
        }
    }

    /// Update the system checkpoint frequency.
    pub fn set_sys_checkpoint_frequency(&mut self, frequency: u64) {
        if self.sys_checkpoint_frequency == frequency {
            return;
        }
        self.sys_checkpoint_frequency = frequency;
        // Reset the `num_uncheckpointed_barrier` for all databases that use default param.
        for db_state in self.databases.values_mut() {
            if db_state.checkpoint_frequency.is_none() {
                db_state.num_uncheckpointed_barrier = 0;
                db_state.force_checkpoint = false;
            }
        }
    }

    pub(super) fn update_database_barrier(
        &mut self,
        database_id: DatabaseId,
        barrier_interval_ms: Option<u32>,
        checkpoint_frequency: Option<u64>,
    ) {
        match self.databases.entry(database_id) {
            Entry::Occupied(mut entry) => {
                let db_state = entry.get_mut();
                db_state.barrier_interval =
                    barrier_interval_ms.map(|ms| Duration::from_millis(ms as u64));
                db_state.checkpoint_frequency = checkpoint_frequency;
                // Reset the `num_uncheckpointed_barrier` since the barrier interval or checkpoint frequency is changed.
                db_state.num_uncheckpointed_barrier = 0;
                db_state.force_checkpoint = false;
            }
            Entry::Vacant(entry) => {
                entry.insert(DatabaseBarrierState::new(
                    barrier_interval_ms,
                    checkpoint_frequency,
                ));
            }
        }

        // If the database already has a timer stream, reset it with the new interval.
        let duration = if let Some(ms) = barrier_interval_ms {
            Duration::from_millis(ms as u64)
        } else {
            self.sys_barrier_interval
        };
        self.timer_streams.insert(
            database_id,
            IntervalStream::new(tokio::time::interval(duration)),
        );
    }

    /// Make the `checkpoint` of the next barrier must be true.
    pub fn force_checkpoint_in_next_barrier(&mut self) {
        for db_state in self.databases.values_mut() {
            db_state.force_checkpoint = true;
        }
    }

    #[await_tree::instrument]
    pub(super) async fn next_barrier(
        &mut self,
        context: &impl GlobalBarrierWorkerContext,
    ) -> NewBarrier {
        let new_barrier = select! {
            biased;
            scheduled = context.next_scheduled() => {
                let database_id = scheduled.database_id;
                // Check if the database exists.
                assert!(self.databases.contains_key(&database_id), "database {} not found in periodic barriers", database_id);
                assert!(self.timer_streams.contains_key(&database_id), "timer stream for database {} not found in periodic barriers", database_id);
                // New command will trigger the barriers for all other databases, so reset all timers.
                for stream in self.timer_streams.values_mut() {
                    stream.as_mut().reset();
                }
                let checkpoint = scheduled.command.need_checkpoint() || self.try_get_checkpoint(database_id);
                NewBarrier {
                    database_id: scheduled.database_id,
                    command: Some((scheduled.command, scheduled.notifiers)),
                    span: scheduled.span,
                    checkpoint,
                }
            },
            // If there is no database, we won't wait for `Interval`, but only wait for command.
            // Normally it-branch will not be true because there is always at least one database.
            next_timer = self.timer_streams.next(), if !self.timer_streams.is_empty() => {
                let (database_id, _instant) = next_timer.unwrap();
                let checkpoint = self.try_get_checkpoint(database_id);
                NewBarrier {
                    database_id,
                    command: None,
                    span: tracing_span(),
                    checkpoint,
                }
            }
        };
        self.update_num_uncheckpointed_barrier(new_barrier.database_id, new_barrier.checkpoint);

        new_barrier
    }

    /// Whether the barrier(checkpoint = true) should be injected.
    fn try_get_checkpoint(&self, database_id: DatabaseId) -> bool {
        let db_state = self.databases.get(&database_id).unwrap();
        let checkpoint_frequency = db_state
            .checkpoint_frequency
            .unwrap_or(self.sys_checkpoint_frequency);
        db_state.num_uncheckpointed_barrier + 1 >= checkpoint_frequency || db_state.force_checkpoint
    }

    /// Update the `num_uncheckpointed_barrier`
    fn update_num_uncheckpointed_barrier(&mut self, database_id: DatabaseId, checkpoint: bool) {
        let db_state = self.databases.get_mut(&database_id).unwrap();
        if checkpoint {
            db_state.num_uncheckpointed_barrier = 0;
            db_state.force_checkpoint = false;
        } else {
            db_state.num_uncheckpointed_barrier += 1;
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_database(
        id: u32,
        barrier_interval_ms: Option<u32>,
        checkpoint_frequency: Option<u64>,
    ) -> Database {
        Database {
            id,
            name: format!("test_db_{}", id),
            barrier_interval_ms,
            checkpoint_frequency,
            ..Default::default()
        }
    }

    // Mock context for testing next_barrier
    struct MockGlobalBarrierWorkerContext {
        scheduled_rx: tokio::sync::Mutex<tokio::sync::mpsc::UnboundedReceiver<Scheduled>>,
    }

    impl MockGlobalBarrierWorkerContext {
        fn new() -> (Self, tokio::sync::mpsc::UnboundedSender<Scheduled>) {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            (
                Self {
                    scheduled_rx: tokio::sync::Mutex::new(rx),
                },
                tx,
            )
        }
    }

    impl GlobalBarrierWorkerContext for MockGlobalBarrierWorkerContext {
        async fn next_scheduled(&self) -> Scheduled {
            self.scheduled_rx.lock().await.recv().await.unwrap()
        }

        async fn commit_epoch(
            &self,
            _commit_info: crate::hummock::CommitEpochInfo,
        ) -> MetaResult<risingwave_pb::hummock::HummockVersionStats> {
            unimplemented!()
        }

        fn abort_and_mark_blocked(
            &self,
            _database_id: Option<DatabaseId>,
            _recovery_reason: crate::barrier::RecoveryReason,
        ) {
            unimplemented!()
        }

        fn mark_ready(&self, _options: MarkReadyOptions) {
            unimplemented!()
        }

        async fn post_collect_command<'a>(
            &'a self,
            _command: &'a crate::barrier::command::CommandContext,
        ) -> MetaResult<()> {
            unimplemented!()
        }

        async fn notify_creating_job_failed(&self, _database_id: Option<DatabaseId>, _err: String) {
            unimplemented!()
        }

        async fn finish_creating_job(
            &self,
            _job: crate::barrier::progress::TrackingJob,
        ) -> MetaResult<()> {
            unimplemented!()
        }

        async fn new_control_stream(
            &self,
            _node: &risingwave_pb::common::WorkerNode,
            _init_request: &risingwave_pb::stream_service::streaming_control_stream_request::PbInitRequest,
        ) -> MetaResult<risingwave_rpc_client::StreamingControlHandle> {
            unimplemented!()
        }

        async fn reload_runtime_info(
            &self,
        ) -> MetaResult<crate::barrier::BarrierWorkerRuntimeInfoSnapshot> {
            unimplemented!()
        }

        async fn reload_database_runtime_info(
            &self,
            _database_id: DatabaseId,
        ) -> MetaResult<Option<crate::barrier::DatabaseRuntimeInfoSnapshot>> {
            unimplemented!()
        }
    }

    #[tokio::test]
    async fn test_next_barrier_with_different_intervals() {
        // Create databases with different intervals
        let databases = vec![
            create_test_database(1, Some(50), Some(2)), // 50ms interval, checkpoint every 2
            create_test_database(2, Some(100), Some(3)), // 100ms interval, checkpoint every 3
            create_test_database(3, None, Some(5)), /* Use system default (200ms), checkpoint every 5 */
        ];

        let mut periodic = PeriodicBarriers::new(
            Duration::from_millis(200), // System default
            10,                         // System checkpoint frequency
            databases,
        );

        let (context, _tx) = MockGlobalBarrierWorkerContext::new();

        // Test that next_barrier returns reasonable results for periodic barriers
        let start_time = Instant::now();

        // Call next_barrier for each database once, because the first interval is returned immediately
        for _ in 0..3 {
            let barrier = periodic.next_barrier(&context).await;
            let elapsed = start_time.elapsed();
            assert!(barrier.command.is_none()); // Should be a periodic barrier, not a scheduled command
            assert!(!barrier.checkpoint); // First barrier shouldn't be a checkpoint
            assert!(elapsed <= Duration::from_millis(5)); // Should be very quick
        }

        // Since we have 3 databases with intervals 50ms, 100ms, and 200ms,
        // the first barrier should come from database 1 (50ms interval)
        let barrier = periodic.next_barrier(&context).await;
        let elapsed = start_time.elapsed();

        // Verify the barrier properties
        assert_eq!(barrier.database_id, DatabaseId::from(1));
        assert!(barrier.command.is_none()); // Should be a periodic barrier, not a scheduled command
        assert!(barrier.checkpoint); // Second barrier should be checkpoint for database 1
        assert!(elapsed >= Duration::from_millis(40) && elapsed <= Duration::from_millis(60)); // Should be around 50ms

        // Verify that the checkpoint frequency works
        let db1_id = DatabaseId::from(1);
        let db1_state = periodic.databases.get_mut(&db1_id).unwrap();
        assert_eq!(db1_state.num_uncheckpointed_barrier, 0); // Should reset after checkpoint
    }

    #[tokio::test]
    async fn test_next_barrier_with_scheduled_command() {
        let databases = vec![
            create_test_database(1, Some(1000), Some(2)), // Long interval to avoid interference
        ];

        let mut periodic = PeriodicBarriers::new(Duration::from_millis(1000), 10, databases);

        let (context, tx) = MockGlobalBarrierWorkerContext::new();

        // Skip first 3 barriers to allow time for the scheduled command
        for _ in 0..3 {
            periodic.next_barrier(&context).await;
        }

        // Schedule a command
        let scheduled_command = Scheduled {
            database_id: DatabaseId::from(1),
            command: Command::Flush,
            notifiers: vec![],
            span: tracing::Span::none(),
        };

        // Send scheduled command in background
        let tx_clone = tx.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            tx_clone.send(scheduled_command).unwrap();
        });

        let start_time = Instant::now();
        let barrier = periodic.next_barrier(&context).await;
        let elapsed = start_time.elapsed();

        // Should receive the scheduled command quickly
        assert!(elapsed <= Duration::from_millis(20));
        assert!(barrier.command.is_some());
        assert_eq!(barrier.database_id, DatabaseId::from(1));

        if let Some((command, _)) = barrier.command {
            assert!(matches!(command, Command::Flush));
        }
    }

    #[tokio::test]
    async fn test_next_barrier_multiple_databases_timing() {
        let databases = vec![
            create_test_database(1, Some(30), Some(10)), // Fast interval
            create_test_database(2, Some(100), Some(10)), // Slower interval
        ];

        let mut periodic = PeriodicBarriers::new(Duration::from_millis(500), 10, databases);

        let (context, _tx) = MockGlobalBarrierWorkerContext::new();

        let mut barrier_counts = HashMap::new();

        // Collect barriers for a short period
        let mut barriers = Vec::new();
        for _ in 0..5 {
            let barrier = periodic.next_barrier(&context).await;
            barriers.push(barrier);
        }

        // Count barriers per database
        for barrier in barriers {
            *barrier_counts.entry(barrier.database_id).or_insert(0) += 1;
        }

        // Database 1 (30ms interval) should have more barriers than database 2 (100ms interval)
        let db1_count = barrier_counts.get(&DatabaseId::from(1)).unwrap_or(&0);
        let db2_count = barrier_counts.get(&DatabaseId::from(2)).unwrap_or(&0);

        // Due to timing, db1 should generally have more barriers, but allow for some variance
        assert!(*db1_count >= *db2_count);
    }

    #[tokio::test]
    async fn test_next_barrier_force_checkpoint() {
        let databases = vec![create_test_database(1, Some(100), Some(10))];

        let mut periodic = PeriodicBarriers::new(Duration::from_millis(100), 10, databases);

        let (context, _tx) = MockGlobalBarrierWorkerContext::new();

        // Force checkpoint for next barrier
        periodic.force_checkpoint_in_next_barrier();

        let barrier = periodic.next_barrier(&context).await;

        // Should be a checkpoint barrier due to force_checkpoint
        assert!(barrier.checkpoint);
        assert_eq!(barrier.database_id, DatabaseId::from(1));
        assert!(barrier.command.is_none());
    }

    #[tokio::test]
    async fn test_next_barrier_checkpoint_frequency() {
        let databases = vec![create_test_database(1, Some(50), Some(2))]; // Checkpoint every 2 barriers

        let mut periodic = PeriodicBarriers::new(Duration::from_millis(50), 10, databases);

        let (context, _tx) = MockGlobalBarrierWorkerContext::new();

        // First barrier - should not be checkpoint
        let barrier1 = periodic.next_barrier(&context).await;
        assert!(!barrier1.checkpoint);

        // Second barrier - should be checkpoint (frequency = 2)
        let barrier2 = periodic.next_barrier(&context).await;
        assert!(barrier2.checkpoint);

        // Third barrier - should not be checkpoint (counter reset)
        let barrier3 = periodic.next_barrier(&context).await;
        assert!(!barrier3.checkpoint);
    }

    #[tokio::test]
    async fn test_update_database_barrier() {
        let databases = vec![create_test_database(1, Some(1000), Some(10))];

        let mut periodic = PeriodicBarriers::new(Duration::from_millis(500), 20, databases);

        // Update existing database
        periodic.update_database_barrier(DatabaseId::from(1), Some(2000), Some(15));

        let db_state = periodic.databases.get(&DatabaseId::from(1)).unwrap();
        assert_eq!(db_state.barrier_interval, Some(Duration::from_millis(2000)));
        assert_eq!(db_state.checkpoint_frequency, Some(15));
        assert_eq!(db_state.num_uncheckpointed_barrier, 0);
        assert!(!db_state.force_checkpoint);

        // Add new database
        periodic.update_database_barrier(DatabaseId::from(2), None, None);

        assert!(periodic.databases.contains_key(&DatabaseId::from(2)));
        let db2_state = periodic.databases.get(&DatabaseId::from(2)).unwrap();
        assert_eq!(db2_state.barrier_interval, None);
        assert_eq!(db2_state.checkpoint_frequency, None);
    }
}
