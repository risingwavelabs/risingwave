use std::sync::Arc;
use std::time::Duration;

use futures::future::try_join_all;
use risingwave_common::array::RwError;
use risingwave_common::error::{ErrorCode, Result, ToRwResult};
use risingwave_pb::data::barrier::Mutation;
use risingwave_pb::data::{Barrier, NothingMutation};
use risingwave_pb::meta::ClusterType;
use risingwave_pb::stream_service::InjectBarrierRequest;
use uuid::Uuid;

use crate::cluster::WorkerNodeMetaManager;
use crate::manager::{Epoch, EpochGeneratorRef, MetaSrvEnv, StreamClientsRef};

/// [`BarrierManager`] sends barriers to all registered compute nodes, with monotonic increasing
/// epoch number. On compute nodes, [`LocalBarrierManager`] will serve these requests and dispatch
/// them to table actors.
pub struct BarrierManager {
    cluster_manager: Arc<dyn WorkerNodeMetaManager>,

    epoch_generator: EpochGeneratorRef,

    clients: StreamClientsRef,
}

impl BarrierManager {
    pub fn new(
        env: MetaSrvEnv,
        cluster_manager: Arc<dyn WorkerNodeMetaManager>,
        epoch_generator: EpochGeneratorRef,
    ) -> Self {
        Self {
            cluster_manager,
            epoch_generator,
            clients: env.stream_clients_ref(),
        }
    }

    fn checkpoint_barrier(epoch: Epoch) -> Barrier {
        Barrier {
            epoch: epoch.into_inner(),
            mutation: Some(Mutation::Nothing(NothingMutation {})),
        }
    }

    /// Start an infinite loop to send checkpoint barriers.
    pub async fn run(&self) -> Result<()> {
        // TODO: should not trigger barrier periodically, change it when barrier collection is ready
        let mut interval = tokio::time::interval(Duration::from_millis(100));

        loop {
            interval.tick().await;

            // TODO: `list_worker_node` should return empty vec instead of error, if there's none
            let nodes = self
                .cluster_manager
                .list_worker_node(ClusterType::ComputeNode)
                .await
                .or_else(|e| match e.inner() {
                    ErrorCode::ItemNotFound(_) => Ok(vec![]),
                    _ => Err(e),
                })?;

            if nodes.is_empty() {
                continue;
            }

            let epoch = self.epoch_generator.generate()?;

            let futures = nodes.into_iter().map(|node| async move {
                let mut client = self.clients.get(&node).await?;
                let request_id = Uuid::new_v4().to_string();
                let barrier = Self::checkpoint_barrier(epoch);
                let request = InjectBarrierRequest {
                    request_id,
                    barrier: Some(barrier),
                };
                client.inject_barrier(request).await.to_rw_result()?;
                Ok::<_, RwError>(())
            });

            // the rpc request is simple so we make them all concurrent
            try_join_all(futures).await?;
        }
    }
}

pub type BarrierManagerRef = Arc<BarrierManager>;
