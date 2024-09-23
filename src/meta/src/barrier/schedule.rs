// Copyright 2024 RisingWave Labs
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
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context};
use assert_matches::assert_matches;
use parking_lot::Mutex;
use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::HummockVersionId;
use risingwave_pb::meta::PausedReason;
use tokio::select;
use tokio::sync::{oneshot, watch};
use tokio::time::Interval;

use super::notifier::Notifier;
use super::{Command, Scheduled};
use crate::hummock::HummockManagerRef;
use crate::model::ActorId;
use crate::rpc::metrics::MetaMetrics;
use crate::{MetaError, MetaResult};

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

pub(super) struct ScheduledQueue {
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
        if let QueueStatus::Blocked(reason) = &self.status
            && !matches!(
                scheduled.command,
                Command::DropStreamingJobs { .. } | Command::CancelStreamingJob(_)
            )
        {
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
            queue: Mutex::new(ScheduledQueue::new()),
            changed_tx: watch::channel(()).0,
            metrics,
        });

        (
            Self {
                inner: inner.clone(),
                hummock_manager,
            },
            ScheduledBarriers {
                num_uncheckpointed_barrier: 0,
                force_checkpoint: false,
                checkpoint_frequency,
                inner,
                min_interval: None,
            },
        )
    }

    /// Push a scheduled barrier into the queue.
    fn push(&self, scheduleds: impl IntoIterator<Item = Scheduled>) -> MetaResult<()> {
        let mut queue = self.inner.queue.lock();
        for scheduled in scheduleds {
            queue.push_back(scheduled)?;
            if queue.len() == 1 {
                self.inner.changed_tx.send(()).ok();
            }
        }
        Ok(())
    }

    /// Try to cancel scheduled cmd for create streaming job, return true if cancelled.
    pub fn try_cancel_scheduled_create(&self, table_id: TableId) -> bool {
        let queue = &mut self.inner.queue.lock();

        if let Some(idx) = queue.queue.iter().position(|scheduled| {
            if let Command::CreateStreamingJob { info, .. } = &scheduled.command
                && info.table_fragments.table_id() == table_id
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
    fn attach_notifiers(
        &self,
        new_notifiers: Vec<Notifier>,
        new_checkpoint: bool,
    ) -> MetaResult<()> {
        let mut queue = self.inner.queue.lock();
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
        self.attach_notifiers(vec![notifier], checkpoint)?;
        rx.await
            .ok()
            .context("failed to wait for barrier collect")?
    }

    /// Run multiple commands and return when they're all completely finished. It's ensured that
    /// multiple commands are executed continuously.
    ///
    /// Returns the barrier info of each command.
    ///
    /// TODO: atomicity of multiple commands is not guaranteed.
    async fn run_multiple_commands(&self, commands: Vec<Command>) -> MetaResult<()> {
        let mut contexts = Vec::with_capacity(commands.len());
        let mut scheduleds = Vec::with_capacity(commands.len());

        for command in commands {
            let (started_tx, started_rx) = oneshot::channel();
            let (collect_tx, collect_rx) = oneshot::channel();

            contexts.push((started_rx, collect_rx));
            scheduleds.push(self.inner.new_scheduled(
                command.need_checkpoint(),
                command,
                once(Notifier {
                    started: Some(started_tx),
                    collected: Some(collect_tx),
                }),
            ));
        }

        self.push(scheduleds)?;

        for (injected_rx, collect_rx) in contexts {
            // Wait for this command to be injected, and record the result.
            tracing::trace!("waiting for injected_rx");
            injected_rx
                .await
                .ok()
                .context("failed to inject barrier")??;

            tracing::trace!("waiting for collect_rx");
            // Throw the error if it occurs when collecting this barrier.
            collect_rx
                .await
                .ok()
                .context("failed to collect barrier")??;
        }

        Ok(())
    }

    /// Run a command with a `Pause` command before and `Resume` command after it. Used for
    /// configuration change.
    ///
    /// Returns the barrier info of the actual command.
    pub async fn run_config_change_command_with_pause(&self, command: Command) -> MetaResult<()> {
        self.run_multiple_commands(vec![
            Command::pause(PausedReason::ConfigChange),
            command,
            Command::resume(PausedReason::ConfigChange),
        ])
        .await
    }

    /// Run a command and return when it's completely finished.
    ///
    /// Returns the barrier info of the actual command.
    pub async fn run_command(&self, command: Command) -> MetaResult<()> {
        tracing::trace!("run_command: {:?}", command);
        let ret = self.run_multiple_commands(vec![command]).await;
        tracing::trace!("run_command finished");
        ret
    }

    /// Flush means waiting for the next barrier to collect.
    pub async fn flush(&self, checkpoint: bool) -> MetaResult<HummockVersionId> {
        let start = Instant::now();

        tracing::debug!("start barrier flush");
        self.wait_for_next_barrier_to_collect(checkpoint).await?;

        let elapsed = Instant::now().duration_since(start);
        tracing::debug!("barrier flushed in {:?}", elapsed);

        let version_id = self.hummock_manager.get_version_id().await;
        Ok(version_id)
    }
}

/// The receiver side of the barrier scheduling queue.
/// Held by the [`super::GlobalBarrierManager`] to execute these commands.
pub struct ScheduledBarriers {
    min_interval: Option<Interval>,

    /// Force checkpoint in next barrier.
    force_checkpoint: bool,

    /// The numbers of barrier (checkpoint = false) since the last barrier (checkpoint = true)
    num_uncheckpointed_barrier: usize,
    checkpoint_frequency: usize,
    inner: Arc<Inner>,
}

impl ScheduledBarriers {
    pub(super) fn set_min_interval(&mut self, min_interval: Duration) {
        let set_new_interval = match &self.min_interval {
            None => true,
            Some(prev_min_interval) => min_interval != prev_min_interval.period(),
        };
        if set_new_interval {
            let mut min_interval = tokio::time::interval(min_interval);
            min_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            self.min_interval = Some(min_interval);
        }
    }

    pub(super) async fn next_barrier(&mut self) -> Scheduled {
        let checkpoint = self.try_get_checkpoint();
        let scheduled = select! {
            biased;
            mut scheduled = self.inner.next_scheduled() => {
                if let Some(min_interval) = &mut self.min_interval {
                    min_interval.reset();
                }
                scheduled.checkpoint = scheduled.checkpoint || checkpoint;
                scheduled
            },
            _ = self.min_interval.as_mut().expect("should have set min interval").tick() => {
                self.inner
                    .new_scheduled(checkpoint, Command::barrier(), std::iter::empty())
            }
        };
        self.update_num_uncheckpointed_barrier(scheduled.checkpoint);
        scheduled
    }
}

impl Inner {
    async fn next_scheduled(&self) -> Scheduled {
        loop {
            let mut rx = self.changed_tx.subscribe();
            {
                let mut queue = self.queue.lock();
                if let Some(scheduled) = queue.queue.pop_front() {
                    break scheduled;
                }
            }
            rx.changed().await.unwrap();
        }
    }
}

impl ScheduledBarriers {
    /// Mark command scheduler as blocked and abort all queued scheduled command and notify with
    /// specific reason.
    pub(super) fn abort_and_mark_blocked(&self, reason: impl Into<String> + Copy) {
        let mut queue = self.inner.queue.lock();
        queue.mark_blocked(reason.into());
        while let Some(Scheduled { notifiers, .. }) = queue.queue.pop_front() {
            notifiers
                .into_iter()
                .for_each(|notify| notify.notify_collection_failed(anyhow!(reason.into()).into()))
        }
    }

    /// Mark command scheduler as ready to accept new command.
    pub(super) fn mark_ready(&self) {
        let mut queue = self.inner.queue.lock();
        queue.mark_ready();
    }

    /// Try to pre apply drop and cancel scheduled command and return them if any.
    /// It should only be called in recovery.
    pub(super) fn pre_apply_drop_cancel_scheduled(&self) -> (Vec<ActorId>, HashSet<TableId>) {
        let mut queue = self.inner.queue.lock();
        assert_matches!(queue.status, QueueStatus::Blocked(_));
        let (mut dropped_actors, mut cancel_table_ids) = (vec![], HashSet::new());

        while let Some(Scheduled {
            notifiers, command, ..
        }) = queue.queue.pop_front()
        {
            match command {
                Command::DropStreamingJobs { actors, .. } => {
                    dropped_actors.extend(actors);
                }
                Command::CancelStreamingJob(table_fragments) => {
                    let table_id = table_fragments.table_id();
                    cancel_table_ids.insert(table_id);
                }
                _ => {
                    unreachable!("only drop and cancel streaming jobs should be buffered");
                }
            }
            notifiers.into_iter().for_each(|notify| {
                notify.notify_collected();
            });
        }
        (dropped_actors, cancel_table_ids)
    }

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
