use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use futures::future::try_join_all;
use itertools::Itertools;
use risingwave_common::array::RwError;
use risingwave_common::error::{Result, ToRwResult};
use risingwave_pb::common::WorkerNode;
use risingwave_pb::data::barrier::Mutation;
use risingwave_pb::data::{Barrier, NothingMutation, StopMutation};
use risingwave_pb::meta::ActorLocation;
use risingwave_pb::stream_plan::{StreamActor, StreamNode};
use risingwave_pb::stream_service::InjectBarrierRequest;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::{oneshot, Mutex};
use uuid::Uuid;

use crate::cluster::StoredClusterManager;
use crate::manager::{EpochGeneratorRef, MetaSrvEnv, StreamClientsRef};
use crate::stream::StreamMetaManagerRef;

struct BarrierActorInfo {
    /// node_id => node
    node_map: HashMap<u32, WorkerNode>,

    /// node_id => actors
    actor_map: HashMap<u32, Vec<StreamActor>>,
}

impl BarrierActorInfo {
    // TODO: we may resolve this info as graph updating, instead of doing it every time we want to
    // send a barrier
    fn resolve(all_actors: impl IntoIterator<Item = ActorLocation>) -> Self {
        let all_actors = all_actors.into_iter().collect_vec();

        let node_map = all_actors
            .iter()
            .map(|location| location.node.as_ref().unwrap())
            .map(|node| (node.id, node.clone()))
            .collect::<HashMap<_, _>>();

        let actor_map = {
            let mut actor_map: HashMap<u32, Vec<_>> = HashMap::new();
            for location in all_actors {
                let node_id = location.node.unwrap().id;
                let actors = actor_map.entry(node_id).or_default();
                actors.extend(location.actors);
            }
            actor_map
        };

        Self {
            node_map,
            actor_map,
        }
    }

    // TODO: should only collect from reachable actors, for mv on mv
    fn actor_ids_to_collect(&self, node_id: &u32) -> impl Iterator<Item = u32> {
        let actors = self.actor_map.get(node_id).unwrap().clone();
        actors.into_iter().map(|a| a.actor_id)
    }

    fn actor_ids_to_send(&self, node_id: &u32) -> impl Iterator<Item = u32> {
        fn resolve_head_node<'a>(node: &'a StreamNode, head_nodes: &mut Vec<&'a StreamNode>) {
            if node.input.is_empty() {
                head_nodes.push(node);
            } else {
                for node in &node.input {
                    resolve_head_node(node, head_nodes);
                }
            }
        }

        let actors = self.actor_map.get(node_id).unwrap().clone();

        actors
            .into_iter()
            .filter(|actor| {
                let mut head_nodes = vec![];
                resolve_head_node(actor.get_nodes().unwrap(), &mut head_nodes);
                head_nodes.iter().any(|node| {
                    matches!(
                        node.get_node().unwrap(),
                        risingwave_pb::stream_plan::stream_node::Node::TableSourceNode(_)
                    )
                })
            })
            .map(|a| a.actor_id)
    }

    fn is_empty(&self) -> bool {
        self.actor_map.is_empty()
    }
}

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

        // A workaround to avoid sending to or collecting from stopped actors.
        // TODO: remove this when inconsistency of stream meta manager after drop mv is fixed
        let mut stopped_actors: HashSet<u32> = Default::default();

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

            let all_actors = self.stream_meta_manager.load_all_actors().await?;
            let info = BarrierActorInfo::resolve(all_actors);
            if info.is_empty() {
                continue;
            }

            let (mutation, mut notifier) = scheduled.unwrap_or_else(
        || (Mutation::Nothing(NothingMutation {}), Default::default()), // default periodic checkpoint barrier
      );

            let epoch = self.epoch_generator.generate()?.into_inner();

            let collect_futures = info.node_map.iter().filter_map(|(node_id, node)| {
                let mutation = mutation.clone();

                // TODO: remove the filter when inconsistency of stream meta manager after drop mv
                // is fixeds
                let actor_ids_to_send = info
                    .actor_ids_to_send(node_id)
                    .filter(|id| !stopped_actors.contains(id))
                    .collect_vec();
                let actor_ids_to_collect = info
                    .actor_ids_to_collect(node_id)
                    .filter(|id| !stopped_actors.contains(id))
                    .collect_vec();

                if actor_ids_to_collect.is_empty() || actor_ids_to_send.is_empty() {
                    // No need to send barrier for this node.
                    None
                } else {
                    async move {
                        let mut client = self.clients.get(node).await?;

                        let request_id = Uuid::new_v4().to_string();
                        let barrier = Barrier {
                            epoch,
                            mutation: Some(mutation),
                        };

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
                stopped_actors.extend(actors);
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
