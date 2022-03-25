use std::sync::Arc;

use futures::Stream;
use futures_async_stream::try_stream;
use risingwave_common::array::DataChunk;
use risingwave_common::error::{Result, RwError};
use risingwave_pb::common::WorkerNode;
use risingwave_pb::plan::{PlanNode as BatchPlanProst, TaskId, TaskOutputId};
use risingwave_rpc_client::{ComputeClient, ExchangeSource};
use uuid::Uuid;

use crate::meta_client::FrontendMetaClient;
use crate::scheduler::schedule::WorkerNodeManagerRef;
use crate::scheduler::ExecutionContextRef;

struct QueryResultFetcher {
    // TODO: Remove these after implemented worker node level snapshot pinnning
    epoch: u64,
    meta_client: Arc<dyn FrontendMetaClient>,

    task_output_id: TaskOutputId,
    gather_task_node: WorkerNode,
}

/// Manages execution of batch queries.
#[derive(Clone)]
pub struct QueryManager {
    worker_node_manager: WorkerNodeManagerRef,
}

impl QueryManager {
    pub fn new(worker_node_manager: WorkerNodeManagerRef) -> Self {
        Self {
            worker_node_manager,
        }
    }

    /// Schedule query to single node.
    pub async fn schedule_single(
        &self,
        context: ExecutionContextRef,
        plan: BatchPlanProst,
    ) -> Result<impl Stream<Item = Result<DataChunk>>> {
        let session = context.session();
        let worker_node = self.worker_node_manager.next_random();
        let compute_client: ComputeClient =
            ComputeClient::new(worker_node.host.as_ref().unwrap().into()).await?;

        // Build task id and task sink id
        let task_id = TaskId {
            query_id: Uuid::new_v4().to_string(),
            stage_id: 0,
            task_id: 0,
        };
        let task_output_id = TaskOutputId {
            task_id: Some(task_id.clone()),
            output_id: 0,
        };

        // Pin snapshot in meta. Single frontend for now. So context_id is always 0.
        // TODO: hummock snapshot should maintain as cache instead of RPC each query.
        let meta_client = session.env().meta_client_ref();
        let epoch = meta_client.pin_snapshot().await?;

        compute_client
            .create_task(task_id.clone(), plan, epoch)
            .await?;

        let query_result_fetcher = QueryResultFetcher {
            epoch,
            meta_client,
            task_output_id,
            gather_task_node: worker_node,
        };

        Ok(query_result_fetcher.run())
    }
}

impl QueryResultFetcher {
    #[try_stream(ok = DataChunk, error = RwError)]
    async fn run(self) {
        let compute_client: ComputeClient =
            ComputeClient::new(self.gather_task_node.host.as_ref().unwrap().into()).await?;

        let mut source = compute_client.get_data(self.task_output_id).await?;
        while let Some(chunk) = source.take_data().await? {
            yield chunk;
        }

        let epoch = self.epoch;
        // Unpin corresponding snapshot.
        self.meta_client.unpin_snapshot(epoch).await?;
    }
}
