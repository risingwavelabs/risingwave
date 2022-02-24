use std::collections::VecDeque;
use std::iter::once;
use std::sync::Arc;
use std::time::Duration;

use futures::future::try_join_all;
use itertools::Itertools;
use risingwave_common::array::RwError;
use risingwave_common::error::{Result, ToRwResult};
use risingwave_pb::common::WorkerType;
use risingwave_pb::data::Barrier;
use risingwave_pb::stream_service::InjectBarrierRequest;
use smallvec::SmallVec;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::{oneshot, watch, Mutex};
use uuid::Uuid;

pub use self::command::Command;
use self::command::CommandContext;
use self::info::BarrierActorInfo;
use crate::cluster::{StoredClusterManager, StoredClusterManagerRef};
use crate::hummock::HummockManager;
use crate::manager::{EpochGeneratorRef, MetaSrvEnv, StreamClientsRef};
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

type Scheduled = (Command, Notifier);
type NewScheduled = (Command, SmallVec<[Notifier; 1]>);

struct ScheduledBarriers {
    buffer: Mutex<VecDeque<NewScheduled>>,

    changed_tx: watch::Sender<()>,
}

impl ScheduledBarriers {
    fn new() -> Self {
        Self {
            buffer: Mutex::new(VecDeque::new()),
            changed_tx: watch::channel(()).0,
        }
    }

    async fn pop_or_default(&self) -> NewScheduled {
        let mut buffer = self.buffer.lock().await;

        // If no command scheduled, create periodic checkpoint barrier by default.
        buffer
            .pop_front()
            .unwrap_or_else(|| (Command::checkpoint(), Default::default()))
    }

    async fn wait_for_one(&self) {
        let buffer = self.buffer.lock().await;
        if buffer.len() > 0 {
            return;
        }
        let mut rx = self.changed_tx.subscribe();
        drop(buffer);

        rx.changed().await.unwrap();
    }

    async fn push(&self, scheduled: NewScheduled) {
        let mut buffer = self.buffer.lock().await;
        buffer.push_back(scheduled);
        if buffer.len() == 1 {
            self.changed_tx.send(()).ok();
        }
    }

    async fn attach_notifiers(&self, new_notifiers: impl IntoIterator<Item = Notifier>) {
        let mut buffer = self.buffer.lock().await;
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

    /// Send barriers to this channel to schedule them.
    scheduled_barriers_tx: UnboundedSender<Scheduled>,

    /// Used for the long-running loop to take scheduled barrier tasks.
    scheduled_barriers_rx: Mutex<Option<UnboundedReceiver<Scheduled>>>,

    /// Extra notifiers will be taken in the start of next barrier loop, and got merged with the
    /// notifier associated with the scheduled barrier.
    extra_notifiers: Mutex<Vec<Notifier>>,
}

impl<S> BarrierManager<S>
where
    S: MetaStore,
{
    /// Create a new [`BarrierManager`].
    pub fn new(
        env: MetaSrvEnv<S>,
        cluster_manager: Arc<StoredClusterManager<S>>,
        fragment_manager: FragmentManagerRef<S>,
        epoch_generator: EpochGeneratorRef,
        hummock_manager: Arc<HummockManager<S>>,
    ) -> Self {
        let (tx, rx) = unbounded_channel();

        Self {
            cluster_manager,
            fragment_manager,
            epoch_generator,
            clients: env.stream_clients_ref(),
            scheduled_barriers: ScheduledBarriers::new(),
            scheduled_barriers_tx: tx,
            scheduled_barriers_rx: Mutex::new(Some(rx)),
            extra_notifiers: Mutex::new(Default::default()),
            hummock_manager,
        }
    }

    /// Start an infinite loop to take scheduled barriers and send them.
    pub async fn run(&self) -> Result<()> {
        let mut min_interval = tokio::time::interval(Duration::from_millis(5000));
        min_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = min_interval.tick() => {},
                _ = self.scheduled_barriers.wait_for_one() => {}
            }
            let (command, notifiers) = self.scheduled_barriers.pop_or_default().await;

            let all_nodes = self.cluster_manager.list_worker_node(
                WorkerType::ComputeNode,
                Some(risingwave_pb::common::worker_node::State::Running),
            );
            let all_actor_infos = self
                .fragment_manager
                .load_all_actors(command.creating_table_id())?;

            let info = BarrierActorInfo::resolve(&all_nodes, all_actor_infos);

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
                        client.inject_barrier(request).await.to_rw_result()?;

                        Ok::<_, RwError>(())
                    }
                    .into()
                }
            });

            let mut notifiers = notifiers;
            notifiers.iter_mut().for_each(Notifier::notify_to_send);
            // wait all barriers collected
            let collect_result = try_join_all(collect_futures).await;
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
