use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use futures::future::try_join_all;
use itertools::Itertools;
use risingwave_common::array::RwError;
use risingwave_common::error::{Result, ToRwResult};
use risingwave_pb::data::barrier::Mutation;
use risingwave_pb::data::{Barrier, NothingMutation, StopMutation};
use risingwave_pb::stream_service::InjectBarrierRequest;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::{oneshot, Mutex, RwLock};
use uuid::Uuid;

use crate::cluster::StoredClusterManager;
use crate::manager::{EpochGeneratorRef, MetaSrvEnv, StreamClientsRef};
use crate::stream::StreamMetaManagerRef;

#[derive(Debug, Default)]
struct Notifier {
    /// Get notified when scheduled barrier is about to send.
    to_send: Option<oneshot::Sender<()>>,

    /// Get notified when scheduled barrier is collected / finished.
    collected: Option<oneshot::Sender<()>>,
}

type Scheduled = (Mutation, Notifier);

/// [`BarrierManager`] sends barriers to all registered compute nodes and collect them, with
/// monotonic increasing epoch numbers. On compute nodes, [`LocalBarrierManager`] will serve these
/// requests and dispatch them to source actors.
pub struct BarrierManager {
    cluster_manager: Arc<StoredClusterManager>,

    stream_meta_manager: StreamMetaManagerRef,

    epoch_generator: EpochGeneratorRef,

    clients: StreamClientsRef,

    /// Send barriers to this channel to schedule them.
    scheduled_barriers_tx: UnboundedSender<Scheduled>,

    /// Used for the long-running loop to take scheduled barrier tasks.
    scheduled_barriers_rx: Mutex<Option<UnboundedReceiver<Scheduled>>>,

    /// A workaround to avoid collecting stopped actors.
    /// TODO: remove this when inconsistency of stream meta manager after drop mv is fixed
    stopped_actors: RwLock<HashSet<u32>>,
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
            stopped_actors: RwLock::new(Default::default()),
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

            if scheduled.is_none() {
                min_interval.tick().await;
            }

            let actor_locations = self.stream_meta_manager.load_all_actors().await?;

            // actor_id => node
            let node_map = actor_locations
                .iter()
                .map(|location| location.node.as_ref().unwrap())
                .map(|node| (node.id, node.clone()))
                .collect::<HashMap<_, _>>();

            // actor_id => actor
            let actor_map = {
                let mut actor_map: HashMap<u32, Vec<_>> = HashMap::new();
                for location in actor_locations {
                    let node_id = location.node.unwrap().id;
                    let actors = actor_map.entry(node_id).or_default();
                    actors.extend(location.actors);
                }
                actor_map
            };
            if actor_map.is_empty() {
                continue;
            }

            let (mutation, mut notifier) = scheduled.unwrap_or_else(
        || (Mutation::Nothing(NothingMutation {}), Default::default()), // default periodic checkpoint barrier
      );

            let epoch = self.epoch_generator.generate()?.into_inner();

            let collect_futures = actor_map.into_iter().map(|(node_id, actors)| {
                let node = node_map.get(&node_id).unwrap().clone();
                let mutation = mutation.clone();

                async move {
                    let mut client = self.clients.get(&node).await?;

                    // TODO: shoule only to collect reachable actors, for mv on mv
                    let actor_ids_to_collect = {
                        // TODO: remove this workaround for stopped actors when meta store
                        // inconsistency is fixed
                        let stopped_actors = self.stopped_actors.read().await;
                        actors
                            .into_iter()
                            .map(|a| a.actor_id)
                            .filter(|id| !stopped_actors.contains(id))
                            .collect_vec()
                    };
                    if actor_ids_to_collect.is_empty() {
                        return Ok(());
                    }

                    let request_id = Uuid::new_v4().to_string();
                    let barrier = Barrier {
                        epoch,
                        mutation: Some(mutation),
                    };

                    let request = InjectBarrierRequest {
                        request_id,
                        barrier: Some(barrier),
                        actor_ids_to_collect,
                    };
                    client.inject_barrier(request).await.to_rw_result()?;

                    Ok::<_, RwError>(())
                }
            });

            if let Some(tx) = notifier.to_send.take() {
                tx.send(()).unwrap();
            }

            try_join_all(collect_futures).await?; // wait all barriers collected

            if let Some(tx) = notifier.collected.take() {
                tx.send(()).unwrap();
            }

            // TODO: remove this workaround for stopped actors when meta store inconsistency is
            // fixed
            if let Mutation::Stop(StopMutation { actors }) = &mutation {
                self.stopped_actors.write().await.extend(actors);
            }
        }
    }
}

impl BarrierManager {
    /// Schedule a barrier and return immediately.
    pub async fn schedule_barrier(&self, mutation: Mutation) -> Result<()> {
        self.scheduled_barriers_tx
            .send((mutation, Default::default()))
            .unwrap();
        Ok(())
    }

    /// Schedule a barrier and returns when it's sent.
    pub async fn send_barrier(&self, mutation: Mutation) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.scheduled_barriers_tx
            .send((
                mutation,
                Notifier {
                    to_send: Some(tx),
                    ..Default::default()
                },
            ))
            .unwrap();
        rx.await.unwrap();
        Ok(())
    }

    /// Send a barrier, returns when it's collected / finished by all reachable actors in the graph.
    pub async fn send_barrier_and_collect(&self, mutation: Mutation) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.scheduled_barriers_tx
            .send((
                mutation,
                Notifier {
                    collected: Some(tx),
                    ..Default::default()
                },
            ))
            .unwrap();
        rx.await.unwrap();
        Ok(())
    }
}

pub type BarrierManagerRef = Arc<BarrierManager>;
