use std::iter::once;
use std::sync::Arc;
use std::time::Duration;

use futures::future::try_join_all;
use itertools::Itertools;
use risingwave_common::array::RwError;
use risingwave_common::error::{Result, ToRwResult};
use risingwave_pb::data::Barrier;
use risingwave_pb::stream_service::InjectBarrierRequest;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::{oneshot, Mutex};
use uuid::Uuid;

pub use self::command::Command;
use self::command::CommandContext;
use self::info::BarrierActorInfo;
use crate::cluster::StoredClusterManager;
use crate::manager::{EpochGeneratorRef, MetaSrvEnv, StreamClientsRef};
use crate::stream::StreamMetaManagerRef;

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
    /// Nofify when we are about to send a barrier.
    fn notify_to_send(&mut self) {
        if let Some(tx) = self.to_send.take() {
            tx.send(()).ok();
        }
    }

    /// Nofify when we have collected a barrier from all actors.
    fn notify_collected(&mut self) {
        if let Some(tx) = self.collected.take() {
            tx.send(()).ok();
        }
    }
}

type Scheduled = (Command, Notifier);

/// [`BarrierManager`] sends barriers to all registered compute nodes and collect them, with
/// monotonic increasing epoch numbers. On compute nodes, [`LocalBarrierManager`] will serve these
/// requests and dispatch them to source actors.
///
/// Configuration change in our system is achieved by the mutation in the barrier. Thus,
/// [`BarrierManager`] provides a set of interfaces like a state machine, accepting [`Command`] that
/// carries info to build [`Mutation`]. To keep the consistency between barrier manager and meta
/// store, some actions like "drop materialized view" or "create mv on mv" must be done in barrier
/// manager transactionally using [`Command`].
pub struct BarrierManager {
    #[allow(dead_code)]
    cluster_manager: Arc<StoredClusterManager>,

    stream_meta_manager: StreamMetaManagerRef,

    epoch_generator: EpochGeneratorRef,

    clients: StreamClientsRef,

    /// Send barriers to this channel to schedule them.
    scheduled_barriers_tx: UnboundedSender<Scheduled>,

    /// Used for the long-running loop to take scheduled barrier tasks.
    scheduled_barriers_rx: Mutex<Option<UnboundedReceiver<Scheduled>>>,

    /// Extra notifiers will be taken in the start of next barrier loop, and got merged with the
    /// notifier associated with the scheduled barrier.
    extra_notifiers: Mutex<Vec<Notifier>>,
}

impl BarrierManager {
    /// Create a new [`BarrierManager`].
    pub fn new(
        env: MetaSrvEnv,
        cluster_manager: Arc<StoredClusterManager>,
        stream_meta_manager: StreamMetaManagerRef,
        epoch_generator: EpochGeneratorRef,
    ) -> Self {
        let (tx, rx) = unbounded_channel();

        Self {
            cluster_manager,
            stream_meta_manager,
            epoch_generator,
            clients: env.stream_clients_ref(),
            scheduled_barriers_tx: tx,
            scheduled_barriers_rx: Mutex::new(Some(rx)),
            extra_notifiers: Mutex::new(Default::default()),
        }
    }

    /// Start an infinite loop to take scheduled barriers and send them.
    pub async fn run(&self) -> Result<()> {
        let mut rx = self
            .scheduled_barriers_rx
            .lock()
            .await
            .take()
            .expect("barrier manager can only run once");

        let mut min_interval = tokio::time::interval(Duration::from_millis(100));

        loop {
            let scheduled = match rx.try_recv() {
                Ok(scheduled) => Ok(Some(scheduled)),
                Err(TryRecvError::Empty) => Ok(None),
                Err(e) => Err(e),
            }
            .unwrap();

            // Only wait for minimal interval if no command is scheduled.
            if scheduled.is_none() {
                min_interval.tick().await;
            }

            let all_actors = self.stream_meta_manager.load_all_actors().await?;
            let info = BarrierActorInfo::resolve(all_actors);

            let (command_context, mut notifiers) = {
                let (command, notifier) = scheduled.unwrap_or_else(
                    || (Command::checkpoint(), Default::default()), /* default periodic
                                                                     * checkpoint barrier */
                );
                let extra_notifiers = std::mem::take(&mut *self.extra_notifiers.lock().await);
                let notifiers = once(notifier)
                    .chain(extra_notifiers.into_iter())
                    .collect_vec();
                let context = CommandContext::new(
                    self.stream_meta_manager.clone(),
                    self.clients.clone(),
                    &info,
                    command,
                );
                (context, notifiers)
            };

            let mutation = command_context.to_mutation().await?;

            let epoch = self.epoch_generator.generate()?.into_inner();

            let collect_futures = info.node_map.iter().filter_map(|(node_id, node)| {
                let actor_ids_to_send = info.actor_ids_to_send(node_id).collect_vec();
                let actor_ids_to_collect = info.actor_ids_to_collect(node_id).collect_vec();

                if actor_ids_to_collect.is_empty() || actor_ids_to_send.is_empty() {
                    // No need to send barrier for this node.
                    None
                } else {
                    let mutation = mutation.clone();
                    let request_id = Uuid::new_v4().to_string();
                    let barrier = Barrier {
                        epoch,
                        mutation: Some(mutation),
                    };

                    async move {
                        let mut client = self.clients.get(node).await?;

                        let request = InjectBarrierRequest {
                            request_id,
                            barrier: Some(barrier),
                            actor_ids_to_send,
                            actor_ids_to_collect,
                        };
                        client.inject_barrier(request).await.to_rw_result()?;

                        Ok::<_, RwError>(())
                    }
                    .into()
                }
            });

            notifiers.iter_mut().for_each(Notifier::notify_to_send);
            try_join_all(collect_futures).await?; // wait all barriers collected
            command_context.post_collect().await?; // do some post stuffs
            notifiers.iter_mut().for_each(Notifier::notify_collected);
        }
    }
}

impl BarrierManager {
    fn do_schedule(&self, command: Command, notifier: Notifier) -> Result<()> {
        self.scheduled_barriers_tx
            .send((command, notifier))
            .unwrap();
        Ok(())
    }

    /// Schedule a command and return immediately.
    #[allow(dead_code)]
    pub async fn schedule_command(&self, command: Command) -> Result<()> {
        self.do_schedule(command, Default::default())
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
        )?;
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
        )?;
        rx.await.unwrap();
        Ok(())
    }

    /// Wait for the next barrier to finish. Note that the barrier flowing in our stream graph is
    /// ignored, if exists.
    pub async fn wait_for_next_barrier(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.extra_notifiers.lock().await.push(Notifier {
            collected: Some(tx),
            ..Default::default()
        });
        rx.await.unwrap();
        Ok(())
    }
}

pub type BarrierManagerRef = Arc<BarrierManager>;
