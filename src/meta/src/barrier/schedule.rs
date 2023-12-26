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

use std::collections::{HashSet, VecDeque};
use std::iter::once;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use anyhow::anyhow;
use assert_matches::assert_matches;
use risingwave_common::catalog::TableId;
use risingwave_pb::hummock::HummockSnapshot;
use risingwave_pb::meta::PausedReason;
use tokio::sync::{oneshot, watch, RwLock};

use super::notifier::{BarrierInfo, Notifier};
use super::{Command, Scheduled};
use crate::hummock::HummockManagerRef;
use crate::rpc::metrics::MetaMetrics;
use crate::{MetaError, MetaResult};

/// A queue for scheduling barriers.
///
/// We manually implement one here instead of using channels since we may need to update the front
/// of the queue to add some notifiers for instant flushes.
struct Inner {
    queue: RwLock<ScheduledQueue>,

    /// When `queue` is not empty anymore, all subscribers of this watcher will be notified.
    changed_tx: watch::Sender<()>,

    /// The numbers of barrier (checkpoint = false) since the last barrier (checkpoint = true)
    num_uncheckpointed_barrier: AtomicUsize,

    /// Force checkpoint in next barrier.
    force_checkpoint: AtomicBool,

    checkpoint_frequency: AtomicUsize,

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

struct ScheduledQueue {
    queue: VecDeque<Scheduled>,
    status: QueueStatus,
}

impl ScheduledQueue {
    fn new() -> Self {
        Self {
            queue: VecDeque::new(),
            status: QueueStatus::Ready,
        }
    }

    fn mark_blocked(&mut self, reason: String) {
        self.status = QueueStatus::Blocked(reason);
    }

    fn mark_ready(&mut self) {
        self.status = QueueStatus::Ready;
    }

    fn len(&self) -> usize {
        self.queue.len()
    }

    fn push_back(&mut self, scheduled: Scheduled) -> MetaResult<()> {
        // We don't allow any command to be scheduled when the queue is blocked, except for dropping streaming jobs.
        // Because we allow dropping streaming jobs when the cluster is under recovery, so we have to buffer the drop
        // command and execute it when the cluster is ready to clean up it.
        // TODO: this is just a workaround to allow dropping streaming jobs when the cluster is under recovery,
        // we need to refine it when catalog and streaming metadata can be handled in a transactional way.
        if let QueueStatus::Blocked(reason) = &self.status &&
            !matches!(scheduled.command, Command::DropStreamingJobs(_) | Command::CancelStreamingJob(_)) {
            return Err(MetaError::unavailable(reason));
        }
        self.queue.push_back(scheduled);
        Ok(())
    }
}

impl Inner {
    /// Create a new scheduled barrier with the given `checkpoint`, `command` and `notifiers`.
    fn new_scheduled(
        &self,
        checkpoint: bool,
        command: Command,
        notifiers: impl IntoIterator<Item = Notifier>,
    ) -> Scheduled {
        // Create a span only if we're being traced, instead of for every periodic barrier.
        let span = if tracing::Span::current().is_none() {
            tracing::Span::none()
        } else {
            tracing::info_span!("barrier", checkpoint, epoch = tracing::field::Empty)
        };

        Scheduled {
            command,
            notifiers: notifiers.into_iter().collect(),
            send_latency_timer: self.metrics.barrier_send_latency.start_timer(),
            span,
            checkpoint,
        }
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
        checkpoint_frequency: usize,
    ) -> (Self, ScheduledBarriers) {
        tracing::info!(
            "Starting barrier scheduler with: checkpoint_frequency={:?}",
            checkpoint_frequency,
        );
        let inner = Arc::new(Inner {
            queue: RwLock::new(ScheduledQueue::new()),
            changed_tx: watch::channel(()).0,
            num_uncheckpointed_barrier: AtomicUsize::new(0),
            checkpoint_frequency: AtomicUsize::new(checkpoint_frequency),
            force_checkpoint: AtomicBool::new(false),
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
    async fn push(&self, scheduleds: impl IntoIterator<Item = Scheduled>) -> MetaResult<()> {
        let mut queue = self.inner.queue.write().await;
        for scheduled in scheduleds {
            queue.push_back(scheduled)?;
            if queue.len() == 1 {
                self.inner.changed_tx.send(()).ok();
            }
        }
        Ok(())
    }

    /// Try to cancel scheduled cmd for create streaming job, return true if cancelled.
    pub async fn try_cancel_scheduled_create(&self, table_id: TableId) -> bool {
        let queue = &mut self.inner.queue.write().await;
        if let Some(idx) = queue.queue.iter().position(|scheduled| {
            if let Command::CreateStreamingJob {
                table_fragments, ..
            } = &scheduled.command
                && table_fragments.table_id() == table_id
            {
                true
            } else {
                false
            }
        }) {
            queue.queue.remove(idx).unwrap();
            true
        } else {
            false
        }
    }

    /// Attach `new_notifiers` to the very first scheduled barrier. If there's no one scheduled, a
    /// default barrier will be created. If `new_checkpoint` is true, the barrier will become a
    /// checkpoint.
    async fn attach_notifiers(
        &self,
        new_notifiers: Vec<Notifier>,
        new_checkpoint: bool,
    ) -> MetaResult<()> {
        let mut queue = self.inner.queue.write().await;
        match queue.queue.front_mut() {
            Some(Scheduled {
                notifiers,
                checkpoint,
                ..
            }) => {
                notifiers.extend(new_notifiers);
                *checkpoint = *checkpoint || new_checkpoint;
            }
            None => {
                // If no command scheduled, create a periodic barrier by default.
                queue.push_back(self.inner.new_scheduled(
                    new_checkpoint,
                    Command::barrier(),
                    new_notifiers,
                ))?;
                self.inner.changed_tx.send(()).ok();
            }
        }
        Ok(())
    }

    /// Wait for the next barrier to collect. Note that the barrier flowing in our stream graph is
    /// ignored, if exists.
    pub async fn wait_for_next_barrier_to_collect(&self, checkpoint: bool) -> MetaResult<()> {
        let (tx, rx) = oneshot::channel();
        let notifier = Notifier {
            collected: Some(tx),
            ..Default::default()
        };
        self.attach_notifiers(vec![notifier], checkpoint).await?;
        rx.await.unwrap()
    }

    /// Run multiple commands and return when they're all completely finished. It's ensured that
    /// multiple commands are executed continuously.
    ///
    /// Returns the barrier info of each command.
    ///
    /// TODO: atomicity of multiple commands is not guaranteed.
    async fn run_multiple_commands(&self, commands: Vec<Command>) -> MetaResult<Vec<BarrierInfo>> {
        let mut contexts = Vec::with_capacity(commands.len());
        let mut scheduleds = Vec::with_capacity(commands.len());

        for command in commands {
            let (injected_tx, injected_rx) = oneshot::channel();
            let (collect_tx, collect_rx) = oneshot::channel();
            let (finish_tx, finish_rx) = oneshot::channel();

            contexts.push((injected_rx, collect_rx, finish_rx));
            scheduleds.push(self.inner.new_scheduled(
                command.need_checkpoint(),
                command,
                once(Notifier {
                    injected: Some(injected_tx),
                    collected: Some(collect_tx),
                    finished: Some(finish_tx),
                }),
            ));
        }

        self.push(scheduleds).await?;

        let mut infos = Vec::with_capacity(contexts.len());

        for (injected_rx, collect_rx, finish_rx) in contexts {
            // Wait for this command to be injected, and record the result.
            let info = injected_rx
                .await
                .map_err(|e| anyhow!("failed to inject barrier: {}", e))?;
            infos.push(info);

            // Throw the error if it occurs when collecting this barrier.
            collect_rx
                .await
                .map_err(|e| anyhow!("failed to collect barrier: {}", e))??;

            // Wait for this command to be finished.
            finish_rx
                .await
                .map_err(|e| anyhow!("failed to finish command: {}", e))?;
        }

        Ok(infos)
    }

    /// Run a command with a `Pause` command before and `Resume` command after it. Used for
    /// configuration change.
    ///
    /// Returns the barrier info of the actual command.
    pub async fn run_config_change_command_with_pause(
        &self,
        command: Command,
    ) -> MetaResult<BarrierInfo> {
        self.run_multiple_commands(vec![
            Command::pause(PausedReason::ConfigChange),
            command,
            Command::resume(PausedReason::ConfigChange),
        ])
        .await
        .map(|i| i[1])
    }

    /// Run a command and return when it's completely finished.
    ///
    /// Returns the barrier info of the actual command.
    pub async fn run_command(&self, command: Command) -> MetaResult<BarrierInfo> {
        self.run_multiple_commands(vec![command])
            .await
            .map(|i| i[0])
    }

    /// Flush means waiting for the next barrier to collect.
    pub async fn flush(&self, checkpoint: bool) -> MetaResult<HummockSnapshot> {
        let start = Instant::now();

        tracing::debug!("start barrier flush");
        self.wait_for_next_barrier_to_collect(checkpoint).await?;

        let elapsed = Instant::now().duration_since(start);
        tracing::debug!("barrier flushed in {:?}", elapsed);

        let snapshot = self.hummock_manager.latest_snapshot();
        Ok(snapshot)
    }
}

/// The receiver side of the barrier scheduling queue.
/// Held by the [`super::GlobalBarrierManager`] to execute these commands.
pub struct ScheduledBarriers {
    inner: Arc<Inner>,
}

impl ScheduledBarriers {
    /// Pop a scheduled barrier from the queue, or a default checkpoint barrier if not exists.
    pub(super) async fn pop_or_default(&self) -> Scheduled {
        let mut queue = self.inner.queue.write().await;
        let checkpoint = self.try_get_checkpoint();
        let scheduled = match queue.queue.pop_front() {
            Some(mut scheduled) => {
                scheduled.checkpoint = scheduled.checkpoint || checkpoint;
                scheduled
            }
            None => {
                // If no command scheduled, create a periodic barrier by default.
                self.inner
                    .new_scheduled(checkpoint, Command::barrier(), std::iter::empty())
            }
        };
        self.update_num_uncheckpointed_barrier(scheduled.checkpoint);
        scheduled
    }

    /// Wait for at least one scheduled barrier in the queue.
    pub(super) async fn wait_one(&self) {
        let queue = self.inner.queue.read().await;
        if queue.len() > 0 {
            return;
        }
        let mut rx = self.inner.changed_tx.subscribe();
        drop(queue);

        rx.changed().await.unwrap();
    }

    /// Mark command scheduler as blocked and abort all queued scheduled command and notify with
    /// specific reason.
    pub(super) async fn abort_and_mark_blocked(&self, reason: impl Into<String> + Copy) {
        let mut queue = self.inner.queue.write().await;
        queue.mark_blocked(reason.into());
        while let Some(Scheduled { notifiers, .. }) = queue.queue.pop_front() {
            notifiers
                .into_iter()
                .for_each(|notify| notify.notify_collection_failed(anyhow!(reason.into()).into()))
        }
    }

    /// Mark command scheduler as ready to accept new command.
    pub(super) async fn mark_ready(&self) {
        let mut queue = self.inner.queue.write().await;
        queue.mark_ready();
    }

    /// Try to pre apply drop scheduled command and return the table ids of dropped streaming jobs.
    /// It should only be called in recovery.
    pub(super) async fn pre_apply_drop_scheduled(&self) -> HashSet<TableId> {
        let mut to_drop_tables = HashSet::new();
        let mut queue = self.inner.queue.write().await;
        assert_matches!(queue.status, QueueStatus::Blocked(_));

        while let Some(Scheduled {
            notifiers, command, ..
        }) = queue.queue.pop_front()
        {
            match command {
                Command::DropStreamingJobs(table_ids) => {
                    to_drop_tables.extend(table_ids);
                }
                Command::CancelStreamingJob(table_fragments) => {
                    let table_id = table_fragments.table_id();
                    to_drop_tables.insert(table_id);
                }
                _ => {
                    unreachable!("only drop streaming jobs should be buffered");
                }
            }
            notifiers.into_iter().for_each(|mut notify| {
                notify.notify_collected();
                notify.notify_finished();
            });
        }
        to_drop_tables
    }

    /// Whether the barrier(checkpoint = true) should be injected.
    fn try_get_checkpoint(&self) -> bool {
        self.inner
            .num_uncheckpointed_barrier
            .load(Ordering::Relaxed)
            >= self.inner.checkpoint_frequency.load(Ordering::Relaxed)
            || self.inner.force_checkpoint.load(Ordering::Relaxed)
    }

    /// Make the `checkpoint` of the next barrier must be true
    pub fn force_checkpoint_in_next_barrier(&self) {
        self.inner.force_checkpoint.store(true, Ordering::Relaxed)
    }

    /// Update the `checkpoint_frequency`
    pub fn set_checkpoint_frequency(&self, frequency: usize) {
        self.inner
            .checkpoint_frequency
            .store(frequency, Ordering::Relaxed);
    }

    /// Update the `num_uncheckpointed_barrier`
    fn update_num_uncheckpointed_barrier(&self, checkpoint: bool) {
        if checkpoint {
            self.inner
                .num_uncheckpointed_barrier
                .store(0, Ordering::Relaxed);
            self.inner.force_checkpoint.store(false, Ordering::Relaxed);
        } else {
            self.inner
                .num_uncheckpointed_barrier
                .fetch_add(1, Ordering::Relaxed);
        }
    }
}
