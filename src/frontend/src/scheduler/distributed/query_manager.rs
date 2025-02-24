// Copyright 2025 RisingWave Labs
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

use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Formatter};
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::task::{Context, Poll};

use futures::Stream;
use pgwire::pg_server::{BoxedError, Session, SessionId};
use risingwave_batch::worker_manager::worker_node_manager::{
    WorkerNodeManagerRef, WorkerNodeSelector,
};
use risingwave_common::array::DataChunk;
use risingwave_common::session_config::QueryMode;
use risingwave_pb::batch_plan::TaskOutputId;
use risingwave_pb::common::HostAddress;
use risingwave_rpc_client::ComputeClientPoolRef;
use tokio::sync::OwnedSemaphorePermit;

use super::QueryExecution;
use super::stats::DistributedQueryMetrics;
use crate::catalog::TableId;
use crate::catalog::catalog_service::CatalogReader;
use crate::scheduler::plan_fragmenter::{Query, QueryId};
use crate::scheduler::{ExecutionContextRef, SchedulerResult};

pub struct DistributedQueryStream {
    chunk_rx: tokio::sync::mpsc::Receiver<SchedulerResult<DataChunk>>,
    // Used for cleaning up `QueryExecution` after all data are polled.
    query_id: QueryId,
    query_execution_info: QueryExecutionInfoRef,
}

impl DistributedQueryStream {
    pub fn query_id(&self) -> &QueryId {
        &self.query_id
    }
}

impl Stream for DistributedQueryStream {
    // TODO(error-handling): use a concrete error type.
    type Item = Result<DataChunk, BoxedError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.chunk_rx.poll_recv(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(chunk) => match chunk {
                Some(chunk_result) => match chunk_result {
                    Ok(chunk) => Poll::Ready(Some(Ok(chunk))),
                    Err(err) => Poll::Ready(Some(Err(Box::new(err)))),
                },
                None => Poll::Ready(None),
            },
        }
    }
}

impl Drop for DistributedQueryStream {
    fn drop(&mut self) {
        // Clear `QueryExecution`. Avoid holding it after execution ends.
        let mut query_execution_info = self.query_execution_info.write().unwrap();
        query_execution_info.delete_query(&self.query_id);
    }
}

pub struct QueryResultFetcher {
    task_output_id: TaskOutputId,
    task_host: HostAddress,

    chunk_rx: tokio::sync::mpsc::Receiver<SchedulerResult<DataChunk>>,

    // `query_id` and `query_execution_info` are used for cleaning up `QueryExecution` after
    // execution.
    query_id: QueryId,
    query_execution_info: QueryExecutionInfoRef,
}

/// [`QueryExecutionInfo`] stores necessary information of query executions. Currently, a
/// `QueryExecution` will be removed right after it ends execution. We might add additional fields
/// in the future.
#[derive(Clone, Default)]
pub struct QueryExecutionInfo {
    query_execution_map: HashMap<QueryId, Arc<QueryExecution>>,
}

impl QueryExecutionInfo {
    #[cfg(test)]
    pub fn new_from_map(query_execution_map: HashMap<QueryId, Arc<QueryExecution>>) -> Self {
        Self {
            query_execution_map,
        }
    }
}

pub type QueryExecutionInfoRef = Arc<RwLock<QueryExecutionInfo>>;

impl QueryExecutionInfo {
    pub fn add_query(&mut self, query_id: QueryId, query_execution: Arc<QueryExecution>) {
        self.query_execution_map.insert(query_id, query_execution);
    }

    pub fn delete_query(&mut self, query_id: &QueryId) {
        self.query_execution_map.remove(query_id);
    }

    pub fn abort_queries(&self, session_id: SessionId) {
        for query in self.query_execution_map.values() {
            // `QueryExecutionInfo` might have queries from different sessions.
            if query.session_id == session_id {
                let query = query.clone();
                // Spawn a task to abort. Avoid await point in this function.
                tokio::spawn(async move { query.abort("cancelled by user".to_owned()).await });
            }
        }
    }
}

/// Manages execution of distributed batch queries.
#[derive(Clone)]
pub struct QueryManager {
    worker_node_manager: WorkerNodeManagerRef,
    compute_client_pool: ComputeClientPoolRef,
    catalog_reader: CatalogReader,
    query_execution_info: QueryExecutionInfoRef,
    pub query_metrics: Arc<DistributedQueryMetrics>,
    /// Limit per session.
    disrtibuted_query_limit: Option<u64>,
    /// Limits the number of concurrent distributed queries.
    distributed_query_semaphore: Option<Arc<tokio::sync::Semaphore>>,
    /// Total permitted distributed query number.
    pub total_distributed_query_limit: Option<u64>,
}

impl QueryManager {
    pub fn new(
        worker_node_manager: WorkerNodeManagerRef,
        compute_client_pool: ComputeClientPoolRef,
        catalog_reader: CatalogReader,
        query_metrics: Arc<DistributedQueryMetrics>,
        disrtibuted_query_limit: Option<u64>,
        total_distributed_query_limit: Option<u64>,
    ) -> Self {
        let distributed_query_semaphore = total_distributed_query_limit
            .map(|limit| Arc::new(tokio::sync::Semaphore::new(limit as usize)));
        Self {
            worker_node_manager,
            compute_client_pool,
            catalog_reader,
            query_execution_info: Arc::new(RwLock::new(QueryExecutionInfo::default())),
            query_metrics,
            disrtibuted_query_limit,
            distributed_query_semaphore,
            total_distributed_query_limit,
        }
    }

    async fn get_permit(&self) -> SchedulerResult<Option<OwnedSemaphorePermit>> {
        match self.distributed_query_semaphore {
            Some(ref semaphore) => {
                let permit = semaphore.clone().acquire_owned().await;
                match permit {
                    Ok(permit) => Ok(Some(permit)),
                    Err(_) => {
                        self.query_metrics.rejected_query_counter.inc();
                        Err(crate::scheduler::SchedulerError::QueryReachLimit(
                            QueryMode::Distributed,
                            self.total_distributed_query_limit
                                .expect("should have distributed query limit"),
                        ))
                    }
                }
            }
            None => Ok(None),
        }
    }

    pub async fn schedule(
        &self,
        context: ExecutionContextRef,
        query: Query,
        read_storage_tables: HashSet<TableId>,
    ) -> SchedulerResult<DistributedQueryStream> {
        if let Some(query_limit) = self.disrtibuted_query_limit
            && self.query_metrics.running_query_num.get() as u64 == query_limit
        {
            self.query_metrics.rejected_query_counter.inc();
            return Err(crate::scheduler::SchedulerError::QueryReachLimit(
                QueryMode::Distributed,
                query_limit,
            ));
        }
        let query_id = query.query_id.clone();
        let permit = self.get_permit().await?;
        let query_execution = Arc::new(QueryExecution::new(query, context.session().id(), permit));

        // Add queries status when begin.
        context
            .session()
            .env()
            .query_manager()
            .add_query(query_id.clone(), query_execution.clone());

        // TODO: if there's no table scan, we don't need to acquire snapshot.
        let pinned_snapshot = context.session().pinned_snapshot();

        let worker_node_manager_reader = WorkerNodeSelector::new(
            self.worker_node_manager.clone(),
            pinned_snapshot.support_barrier_read(),
        );
        // Starts the execution of the query.
        let query_result_fetcher = query_execution
            .start(
                context.clone(),
                worker_node_manager_reader,
                pinned_snapshot.batch_query_epoch(&read_storage_tables)?,
                self.compute_client_pool.clone(),
                self.catalog_reader.clone(),
                self.query_execution_info.clone(),
                self.query_metrics.clone(),
            )
            .await
            .inspect_err(|_| {
                // Clean up query execution on error.
                context
                    .session()
                    .env()
                    .query_manager()
                    .delete_query(&query_id);
            })?;
        Ok(query_result_fetcher.stream_from_channel())
    }

    pub fn cancel_queries_in_session(&self, session_id: SessionId) {
        let query_execution_info = self.query_execution_info.read().unwrap();
        query_execution_info.abort_queries(session_id);
    }

    pub fn add_query(&self, query_id: QueryId, query_execution: Arc<QueryExecution>) {
        let mut query_execution_info = self.query_execution_info.write().unwrap();
        query_execution_info.add_query(query_id, query_execution);
    }

    pub fn delete_query(&self, query_id: &QueryId) {
        let mut query_execution_info = self.query_execution_info.write().unwrap();
        query_execution_info.delete_query(query_id);
    }
}

impl QueryResultFetcher {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        task_output_id: TaskOutputId,
        task_host: HostAddress,
        chunk_rx: tokio::sync::mpsc::Receiver<SchedulerResult<DataChunk>>,
        query_id: QueryId,
        query_execution_info: QueryExecutionInfoRef,
    ) -> Self {
        Self {
            task_output_id,
            task_host,
            chunk_rx,
            query_id,
            query_execution_info,
        }
    }

    fn stream_from_channel(self) -> DistributedQueryStream {
        DistributedQueryStream {
            chunk_rx: self.chunk_rx,
            query_id: self.query_id,
            query_execution_info: self.query_execution_info,
        }
    }
}

impl Debug for QueryResultFetcher {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryResultFetcher")
            .field("task_output_id", &self.task_output_id)
            .field("task_host", &self.task_host)
            .finish()
    }
}
