use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use futures::Stream;
use futures_async_stream::try_stream;
use log::debug;
use risingwave_common::array::DataChunk;
use risingwave_common::error::{Result, RwError};
use risingwave_pb::common::HostAddress;
use risingwave_pb::plan::{PlanNode as BatchPlanProst, TaskId, TaskOutputId};
use risingwave_rpc_client::{ComputeClient, ExchangeSource};
use uuid::Uuid;

use crate::meta_client::FrontendMetaClient;
use crate::scheduler::execution::QueryExecution;
use crate::scheduler::plan_fragmenter::Query;
use crate::scheduler::schedule::WorkerNodeManagerRef;
use crate::scheduler::ExecutionContextRef;

pub trait DataChunkStream = Stream<Item = Result<DataChunk>>;

pub struct QueryResultFetcher {
    // TODO: Remove these after implemented worker node level snapshot pinnning
    epoch: u64,
    meta_client: Arc<dyn FrontendMetaClient>,

    task_output_id: TaskOutputId,
    task_host: HostAddress,
}

/// Manages execution of batch queries.
#[derive(Clone)]
pub struct QueryManager {
    worker_node_manager: WorkerNodeManagerRef,
    /// Option to specify how to execute query, in single or distributed mode.
    ///
    /// This should be a session variable, but currently we don't support `set` statement, so we
    /// pass in startup environment.
    ///
    /// TODO: Remove this after we support `set` statement.
    dist_query: bool,
}

impl QueryManager {
    pub fn new(worker_node_manager: WorkerNodeManagerRef, dist_query: bool) -> Self {
        Self {
            worker_node_manager,
            dist_query,
        }
    }

    pub fn dist_query(&self) -> bool {
        self.dist_query
    }

    /// Schedule query to single node.
    pub async fn schedule_single(
        &self,
        context: ExecutionContextRef,
        plan: BatchPlanProst,
    ) -> Result<impl Stream<Item = Result<DataChunk>>> {
        let session = context.session();
        let worker_node_addr = self.worker_node_manager.next_random().host.unwrap();
        let compute_client: ComputeClient = ComputeClient::new((&worker_node_addr).into()).await?;

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

        let meta_client = session.env().meta_client_ref();

        // Pin snapshot in meta.
        // TODO: Hummock snapshot should maintain as cache instead of RPC each query.
        // TODO: Use u64::MAX for `last_pinned` so it always return the greatest current epoch. Use
        // correct `last_pinned` when retrying this RPC.
        let last_pinned = u64::MAX;
        let epoch = meta_client.pin_snapshot(last_pinned).await?;

        compute_client
            .create_task(task_id.clone(), plan, epoch)
            .await?;

        let query_result_fetcher = QueryResultFetcher {
            epoch,
            meta_client,
            task_output_id,
            task_host: worker_node_addr,
        };

        Ok(query_result_fetcher.run())
    }

    pub async fn schedule(
        &self,
        context: ExecutionContextRef,
        query: Query,
    ) -> Result<impl DataChunkStream> {
        // Cheat compiler to resolve type
        let session = context.session();

        let meta_client = session.env().meta_client_ref();

        // Pin snapshot in meta.
        // TODO: Hummock snapshot should maintain as cache instead of RPC each query.
        // TODO: Use u64::MAX for `last_pinned` so it always return the greatest current epoch. Use
        // correct `last_pinned` when retrying this RPC.
        let last_pinned = u64::MAX;
        let epoch = meta_client.pin_snapshot(last_pinned).await?;

        let query_execution = QueryExecution::new(
            query,
            epoch,
            meta_client,
            session.env().worker_node_manager_ref(),
        );

        let query_result_fetcher = query_execution.start().await?;

        Ok(query_result_fetcher.run())
    }
}

impl QueryResultFetcher {
    pub fn new(
        epoch: u64,
        meta_client: Arc<dyn FrontendMetaClient>,
        task_output_id: TaskOutputId,
        task_host: HostAddress,
    ) -> Self {
        Self {
            epoch,
            meta_client,
            task_output_id,
            task_host,
        }
    }

    #[try_stream(ok = DataChunk, error = RwError)]
    async fn run(self) {
        debug!(
            "Starting to run query result fetcher, task output id: {:?}, task_host: {:?}",
            self.task_output_id, self.task_host
        );
        let compute_client: ComputeClient = ComputeClient::new((&self.task_host).into()).await?;

        let mut source = compute_client.get_data(self.task_output_id).await?;
        while let Some(chunk) = source.take_data().await? {
            yield chunk;
        }

        let epoch = self.epoch;
        // Unpin corresponding snapshot.
        self.meta_client.unpin_snapshot(epoch).await?;
    }
}

impl Debug for QueryResultFetcher {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryResultFetcher")
            .field("epoch", &self.epoch)
            .field("task_output_id", &self.task_output_id)
            .field("task_host", &self.task_host)
            .finish()
    }
}
