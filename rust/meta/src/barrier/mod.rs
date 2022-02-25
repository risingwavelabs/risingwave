use std::collections::VecDeque;
use std::iter::once;
use std::sync::Arc;
use std::time::Duration;

use futures::future::try_join_all;
use itertools::Itertools;
use risingwave_common::array::RwError;
use risingwave_common::error::{Result, ToRwResult};
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
use crate::manager::{EpochGeneratorRef, MetaSrvEnv, StreamClientsRef};
use crate::rpc::metrics::{MetaMetrics, DEFAULT_META_STATS};
use crate::storage::MetaStore;
use crate::stream::FragmentManagerRef;

mod command;
mod info;

#[derive(Debug, Default)]
struct Notifier {
    /// Get notified when scheduled barrier is about to send.
    to_send: Option<oneshot::Sender<()>>,

    /// Get notified when scheduled barrier is collected / finished.
    collected: Option<oneshot::Sender<()>>,
}

impl Notifier {
    /// Notify when we are about to send a barrier.
    fn notify_to_send(&mut self) {
        if let Some(tx) = self.to_send.take() {
            tx.send(()).ok();
        }
    }

    /// Notify when we have collected a barrier from all actors.
    fn notify_collected(&mut self) {
        if let Some(tx) = self.collected.take() {
            tx.send(()).ok();
        }
    }
}

type Scheduled = (Command, SmallVec<[Notifier; 1]>);

/// A buffer or queue for scheduling barriers.
struct ScheduledBarriers {
    buffer: RwLock<VecDeque<Scheduled>>,

    /// When `buffer` is not empty anymore, all subscribers of this watcher will be notified.
    changed_tx: watch::Sender<()>,
    metrics: Arc<MetaMetrics>,
}

impl ScheduledBarriers {
    fn new() -> Self {
        Self {
            buffer: RwLock::new(VecDeque::new()),
            changed_tx: watch::channel(()).0,
            metrics: DEFAULT_META_STATS.clone(),
        }
    }

    /// Pop a schduled barrier from the buffer, or a default checkpoint barrier if not exists.
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

    /// Attach `new_notifiers` to the very first schduled barrier. If there's no one scheduled, a
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
pub struct BarrierManager<S>
where
    S: MetaStore,
{
    cluster_manager: StoredClusterManagerRef<S>,

    fragment_manager: FragmentManagerRef<S>,

    epoch_generator: EpochGeneratorRef,

    hummock_manager: Arc<HummockManager<S>>,

    clients: StreamClientsRef,

    scheduled_barriers: ScheduledBarriers,
}

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
    ) -> Self {
        Self {
            cluster_manager,
            fragment_manager,
            epoch_generator,
            clients: env.stream_clients_ref(),
            scheduled_barriers: ScheduledBarriers::new(),
            hummock_manager,
        }
    }

    /// Start an infinite loop to take scheduled barriers and send them.
    pub async fn run(&self) -> Result<()> {
        let mut min_interval = tokio::time::interval(Self::INTERVAL);
        min_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

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
                    .list_worker_node(WorkerType::ComputeNode, Some(Running));
                let all_actor_infos = self
                    .fragment_manager
                    .load_all_actors(command.creating_table_id())?;
                BarrierActorInfo::resolve(all_nodes, all_actor_infos)
            };

            let command_context = CommandContext::new(
                self.fragment_manager.clone(),
                self.clients.clone(),
                &info,
                command,
            );

            let mutation = command_context.to_mutation()?;

            let epoch = self.epoch_generator.generate()?.into_inner();

            let collect_futures = info.node_map.iter().filter_map(|(node_id, node)| {
                let actor_ids_to_send = info.actor_ids_to_send(node_id).collect_vec();
                let actor_ids_to_collect = info.actor_ids_to_collect(node_id).collect_vec();

                if actor_ids_to_send.is_empty() || actor_ids_to_collect.is_empty() {
                    // No need to send barrier for this node.
                    None
                } else {
                    let mutation = mutation.clone();
                    let request_id = Uuid::new_v4().to_string();
                    let barrier = Barrier {
                        epoch,
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

            let mut notifiers = notifiers;
            notifiers.iter_mut().for_each(Notifier::notify_to_send);
            let timer = self
                .scheduled_barriers
                .metrics
                .barrier_latency
                .start_timer();
            // wait all barriers collected
            let collect_result = try_join_all(collect_futures).await;
            timer.observe_duration();
            // TODO #96: This is a temporary implementation of commit epoch. Refactor after hummock
            // shared buffer is deployed.
            match collect_result {
                Ok(_) => {
                    self.hummock_manager.commit_epoch(epoch).await?;
                }
                Err(err) => {
                    self.hummock_manager.abort_epoch(epoch).await?;
                    return Err(err);
                }
            };
            command_context.post_collect().await?; // do some post stuffs
            notifiers.iter_mut().for_each(Notifier::notify_collected);
        }
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
        rx.await.unwrap();
        Ok(())
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
        rx.await.unwrap();
        Ok(())
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
        rx.await.unwrap();
        Ok(())
    }
}

pub type BarrierManagerRef<S> = Arc<BarrierManager<S>>;
