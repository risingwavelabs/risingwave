// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
use std::collections::VecDeque;
use std::iter::once;
use std::sync::Arc;
use std::time::Duration;

use futures::future::try_join_all;
use itertools::Itertools;
use risingwave_common::error::{Result, RwError, ToRwResult};
use risingwave_pb::common::worker_node::State::Running;
use risingwave_pb::common::WorkerType;
use risingwave_pb::data::Barrier;
use risingwave_pb::stream_service::InjectBarrierRequest;
use smallvec::SmallVec;
use tokio::sync::{oneshot, watch, RwLock};
use uuid::Uuid;

pub use self::command::Command;
use self::command::CommandContext;
use self::info::BarrierActorInfo;
use crate::cluster::{StoredClusterManager, StoredClusterManagerRef};
use crate::hummock::HummockManager;
use crate::manager::{EpochGeneratorRef, MetaSrvEnv, StreamClientsRef, INVALID_EPOCH};
use crate::rpc::metrics::MetaMetrics;
use crate::storage::MetaStore;
use crate::stream::FragmentManagerRef;

mod command;
mod info;

#[derive(Debug, Default)]
struct Notifier {
    /// Get notified when scheduled barrier is about to send or failed.
    to_send: Option<oneshot::Sender<Result<()>>>,

    /// Get notified when scheduled barrier is collected(finished) or failed.
    collected: Option<oneshot::Sender<Result<()>>>,
}

impl Notifier {
    /// Notify when we are about to send a barrier.
    fn notify_to_send(&mut self) {
        if let Some(tx) = self.to_send.take() {
            tx.send(Ok(())).ok();
        }
    }

    /// Notify when we have collected a barrier from all actors.
    fn notify_collected(&mut self) {
        if let Some(tx) = self.collected.take() {
            tx.send(Ok(())).ok();
        }
    }

    /// Notify when we failed to send a barrier or collected a barrier.
    fn notify_failed(&mut self, err: RwError) {
        if let Some(tx) = self.to_send.take() {
            tx.send(Err(err.clone())).ok();
        }
        if let Some(tx) = self.collected.take() {
            tx.send(Err(err)).ok();
        }
    }
}

type Scheduled = (Command, SmallVec<[Notifier; 1]>);

/// A buffer or queue for scheduling barriers.
struct ScheduledBarriers {
    buffer: RwLock<VecDeque<Scheduled>>,

    /// When `buffer` is not empty anymore, all subscribers of this watcher will be notified.
    changed_tx: watch::Sender<()>,
}

impl ScheduledBarriers {
    fn new() -> Self {
        Self {
            buffer: RwLock::new(VecDeque::new()),
            changed_tx: watch::channel(()).0,
        }
    }

    /// Pop a scheduled barrier from the buffer, or a default checkpoint barrier if not exists.
    async fn pop_or_default(&self) -> Scheduled {
        let mut buffer = self.buffer.write().await;

        // If no command scheduled, create periodic checkpoint barrier by default.
        buffer
            .pop_front()
            .unwrap_or_else(|| (Command::checkpoint(), Default::default()))
    }

    /// Wait for at least one scheduled barrier in the buffer.
    async fn wait_one(&self) {
        let buffer = self.buffer.read().await;
        if buffer.len() > 0 {
            return;
        }
        let mut rx = self.changed_tx.subscribe();
        drop(buffer);

        rx.changed().await.unwrap();
    }

    /// Push a scheduled barrier into the buffer.
    async fn push(&self, scheduled: Scheduled) {
        let mut buffer = self.buffer.write().await;
        buffer.push_back(scheduled);
        if buffer.len() == 1 {
            self.changed_tx.send(()).ok();
        }
    }

    /// Attach `new_notifiers` to the very first scheduled barrier. If there's no one scheduled, a
    /// default checkpoint barrier will be created.
    async fn attach_notifiers(&self, new_notifiers: impl IntoIterator<Item = Notifier>) {
        let mut buffer = self.buffer.write().await;
        match buffer.front_mut() {
            Some((_, notifiers)) => notifiers.extend(new_notifiers),
            None => {
                // If no command scheduled, create periodic checkpoint barrier by default.
                buffer.push_back((Command::checkpoint(), new_notifiers.into_iter().collect()));
                if buffer.len() == 1 {
                    self.changed_tx.send(()).ok();
                }
            }
        }
    }
}

/// [`BarrierManager`] sends barriers to all registered compute nodes and collect them, with
/// monotonic increasing epoch numbers. On compute nodes, [`LocalBarrierManager`] will serve these
/// requests and dispatch them to source actors.
///
/// Configuration change in our system is achieved by the mutation in the barrier. Thus,
/// [`BarrierManager`] provides a set of interfaces like a state machine, accepting [`Command`] that
/// carries info to build [`Mutation`]. To keep the consistency between barrier manager and meta
/// store, some actions like "drop materialized view" or "create mv on mv" must be done in barrier
/// manager transactionally using [`Command`].
pub struct BarrierManager<S> {
    cluster_manager: StoredClusterManagerRef<S>,

    fragment_manager: FragmentManagerRef<S>,

    epoch_generator: EpochGeneratorRef,

    hummock_manager: Arc<HummockManager<S>>,

    clients: StreamClientsRef,

    scheduled_barriers: ScheduledBarriers,

    metrics: Arc<MetaMetrics>,
}

// TODO: Persist barrier manager states in meta store including
// previous epoch number, current epoch number and barrier collection progress
impl<S> BarrierManager<S>
where
    S: MetaStore,
{
    const INTERVAL: Duration =
        Duration::from_millis(if cfg!(debug_assertions) { 5000 } else { 100 });

    /// Create a new [`BarrierManager`].
    pub fn new(
        env: MetaSrvEnv<S>,
        cluster_manager: Arc<StoredClusterManager<S>>,
        fragment_manager: FragmentManagerRef<S>,
        epoch_generator: EpochGeneratorRef,
        hummock_manager: Arc<HummockManager<S>>,
        metrics: Arc<MetaMetrics>,
    ) -> Self {
        Self {
            cluster_manager,
            fragment_manager,
            epoch_generator,
            clients: env.stream_clients_ref(),
            scheduled_barriers: ScheduledBarriers::new(),
            hummock_manager,
            metrics,
        }
    }

    /// Start an infinite loop to take scheduled barriers and send them.
    pub async fn run(&self) -> Result<()> {
        let mut min_interval = tokio::time::interval(Self::INTERVAL);
        min_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        let mut prev_epoch = INVALID_EPOCH;
        loop {
            tokio::select! {
                // Wait for the minimal interval,
                _ = min_interval.tick() => {},
                // ... or there's barrier scheduled.
                _ = self.scheduled_barriers.wait_one() => {}
            }
            // Get a barrier to send.
            let (command, notifiers) = self.scheduled_barriers.pop_or_default().await;

            let info = {
                let all_nodes = self
                    .cluster_manager
                    .list_worker_node(WorkerType::ComputeNode, Some(Running))
                    .await;
                let all_actor_infos = self
                    .fragment_manager
                    .load_all_actors(command.creating_table_id())
                    .await;
                BarrierActorInfo::resolve(all_nodes, all_actor_infos)
            };

            let new_epoch = self.epoch_generator.generate().into_inner();
            let command_context = CommandContext::new(
                self.fragment_manager.clone(),
                self.clients.clone(),
                &info,
                prev_epoch,
                new_epoch,
                command,
            );

            let mut notifiers = notifiers;
            notifiers.iter_mut().for_each(Notifier::notify_to_send);
            match self.run_inner(&command_context).await {
                Ok(_) => {
                    prev_epoch = new_epoch;
                    notifiers.iter_mut().for_each(Notifier::notify_collected);
                }
                Err(e) => {
                    notifiers
                        .iter_mut()
                        .for_each(|notify| notify.notify_failed(e.clone()));
                }
            }
        }
    }

    /// running a scheduled command.
    async fn run_inner<'a>(&self, command_context: &CommandContext<'a, S>) -> Result<()> {
        let mutation = command_context.to_mutation().await?;
        let info = command_context.info;

        let collect_futures = info.node_map.iter().filter_map(|(node_id, node)| {
            let actor_ids_to_send = info.actor_ids_to_send(node_id).collect_vec();
            let actor_ids_to_collect = info.actor_ids_to_collect(node_id).collect_vec();

            if actor_ids_to_collect.is_empty() {
                // No need to send or collect barrier for this node.
                assert!(actor_ids_to_send.is_empty());
                None
            } else {
                let mutation = mutation.clone();
                let request_id = Uuid::new_v4().to_string();
                let barrier = Barrier {
                    epoch: Some(risingwave_pb::data::Epoch {
                        curr: command_context.curr_epoch,
                        prev: command_context.prev_epoch,
                    }),
                    mutation: Some(mutation),
                    // TODO(chi): add distributed tracing
                    span: vec![],
                };

                async move {
                    let mut client = self.clients.get(node).await?;

                    let request = InjectBarrierRequest {
                        request_id,
                        barrier: Some(barrier),
                        actor_ids_to_send,
                        actor_ids_to_collect,
                    };
                    tracing::trace!(
                        target: "events::meta::barrier::inject_barrier",
                        "inject barrier request: {:#?}", request
                    );

                    // This RPC returns only if this worker node has collected this barrier.
                    client.inject_barrier(request).await.to_rw_result()?;

                    Ok::<_, RwError>(())
                }
                .into()
            }
        });

        // Wait for all barriers collected
        let timer = self.metrics.barrier_latency.start_timer();

        let collect_result = try_join_all(collect_futures).await;
        // Commit this epoch to Hummock
        if command_context.prev_epoch != INVALID_EPOCH {
            match collect_result {
                Ok(_) => {
                    self.hummock_manager
                        .commit_epoch(command_context.prev_epoch)
                        .await?
                }
                Err(_) => {
                    self.hummock_manager
                        .abort_epoch(command_context.prev_epoch)
                        .await?
                }
            };
        }
        collect_result?;

        timer.observe_duration();
        command_context.post_collect().await // do some post stuffs
    }
}

impl<S> BarrierManager<S>
where
    S: MetaStore,
{
    async fn do_schedule(&self, command: Command, notifier: Notifier) -> Result<()> {
        self.scheduled_barriers
            .push((command, once(notifier).collect()))
            .await;
        Ok(())
    }

    /// Schedule a command and return immediately.
    #[allow(dead_code)]
    pub async fn schedule_command(&self, command: Command) -> Result<()> {
        self.do_schedule(command, Default::default()).await
    }

    /// Schedule a command and return when its coresponding barrier is about to sent.
    #[allow(dead_code)]
    pub async fn issue_command(&self, command: Command) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.do_schedule(
            command,
            Notifier {
                to_send: Some(tx),
                ..Default::default()
            },
        )
        .await?;
        rx.await.unwrap()
    }

    /// Run a command and return when it's completely finished.
    pub async fn run_command(&self, command: Command) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.do_schedule(
            command,
            Notifier {
                collected: Some(tx),
                ..Default::default()
            },
        )
        .await?;
        rx.await.unwrap()
    }

    /// Wait for the next barrier to finish. Note that the barrier flowing in our stream graph is
    /// ignored, if exists.
    pub async fn wait_for_next_barrier(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        let notifier = Notifier {
            collected: Some(tx),
            ..Default::default()
        };
        self.scheduled_barriers
            .attach_notifiers(once(notifier))
            .await;
        rx.await.unwrap()
    }
}

pub type BarrierManagerRef<S> = Arc<BarrierManager<S>>;
