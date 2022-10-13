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

use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::task::{Context, Poll};

use futures::{Stream, StreamExt};
use futures_async_stream::try_stream;
use pgwire::pg_server::{BoxedError, Session, SessionId};
use risingwave_batch::executor::BoxedDataChunkStream;
use risingwave_common::array::DataChunk;
use risingwave_common::error::RwError;
use risingwave_pb::batch_plan::TaskOutputId;
use risingwave_pb::common::HostAddress;
use risingwave_rpc_client::ComputeClientPoolRef;
use tracing::debug;

use super::QueryExecution;
use crate::catalog::catalog_service::CatalogReader;
use crate::scheduler::plan_fragmenter::{Query, QueryId};
use crate::scheduler::worker_node_manager::WorkerNodeManagerRef;
use crate::scheduler::{ExecutionContextRef, HummockSnapshotManagerRef, SchedulerResult};

pub struct DistributedQueryStream {
    chunk_rx: tokio::sync::mpsc::Receiver<SchedulerResult<DataChunk>>,
    // Used for cleaning up `QueryExecution` after all data have been polled.
    query_id: QueryId,
    query_execution_info: QueryExecutionInfoRef,
}

impl DistributedQueryStream {
    pub fn query_id(&self) -> &QueryId {
        &self.query_id
    }
}

impl Stream for DistributedQueryStream {
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
    // TODO: Remove these after implemented worker node level snapshot pinnning
    epoch: u64,
    hummock_snapshot_manager: HummockSnapshotManagerRef,

    task_output_id: TaskOutputId,
    task_host: HostAddress,
    compute_client_pool: ComputeClientPoolRef,

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
            // Query manager may have queries from different sessions.
            if query.session_id == session_id {
                let query = query.clone();
                // spawn a task to abort. Avoid await point in this function.
                tokio::spawn(async move { query.abort().await });
            }
        }
    }
}

/// Manages execution of distributed batch queries.
#[derive(Clone)]
pub struct QueryManager {
    worker_node_manager: WorkerNodeManagerRef,
    hummock_snapshot_manager: HummockSnapshotManagerRef,
    compute_client_pool: ComputeClientPoolRef,
    catalog_reader: CatalogReader,
    query_execution_info: QueryExecutionInfoRef,
}

type QueryManagerRef = Arc<QueryManager>;

impl QueryManager {
    pub fn new(
        worker_node_manager: WorkerNodeManagerRef,
        hummock_snapshot_manager: HummockSnapshotManagerRef,
        compute_client_pool: ComputeClientPoolRef,
        catalog_reader: CatalogReader,
    ) -> Self {
        Self {
            worker_node_manager,
            hummock_snapshot_manager,
            compute_client_pool,
            catalog_reader,
            query_execution_info: Arc::new(RwLock::new(QueryExecutionInfo::default())),
        }
    }

    pub async fn schedule(
        &self,
        context: ExecutionContextRef,
        query: Query,
    ) -> SchedulerResult<DistributedQueryStream> {
        let query_id = query.query_id().clone();
        let epoch = self
            .hummock_snapshot_manager
            .acquire(&query_id)
            .await?
            .committed_epoch;
        let query_id = query.query_id.clone();
        let query_execution = Arc::new(QueryExecution::new(
            context.clone(),
            query,
            epoch,
            self.worker_node_manager.clone(),
            self.hummock_snapshot_manager.clone(),
            self.compute_client_pool.clone(),
            self.catalog_reader.clone(),
            context.session().id(),
        ));

        // Add queries status when begin.
        context
            .session()
            .env()
            .query_manager()
            .add_query(query_id.clone(), query_execution.clone());

        // Create a oneshot channel for QueryResultFetcher to get failed event.
        let query_result_fetcher = match query_execution
            .start(self.query_execution_info.clone())
            .await
        {
            Ok(query_result_fetcher) => query_result_fetcher,
            Err(e) => {
                self.hummock_snapshot_manager
                    .release(epoch, &query_id)
                    .await;
                return Err(e);
            }
        };

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
}

impl QueryResultFetcher {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        epoch: u64,
        hummock_snapshot_manager: HummockSnapshotManagerRef,
        task_output_id: TaskOutputId,
        task_host: HostAddress,
        compute_client_pool: ComputeClientPoolRef,
        chunk_rx: tokio::sync::mpsc::Receiver<SchedulerResult<DataChunk>>,
        query_id: QueryId,
        query_execution_info: QueryExecutionInfoRef,
    ) -> Self {
        Self {
            epoch,
            hummock_snapshot_manager,
            task_output_id,
            task_host,
            compute_client_pool,
            chunk_rx,
            query_id,
            query_execution_info,
        }
    }

    #[try_stream(ok = DataChunk, error = RwError)]
    async fn run_inner(self) {
        debug!(
            "Starting to run query result fetcher, task output id: {:?}, task_host: {:?}",
            self.task_output_id, self.task_host
        );
        let compute_client = self
            .compute_client_pool
            .get_by_addr((&self.task_host).into())
            .await?;
        let mut stream = compute_client.get_data(self.task_output_id.clone()).await?;
        while let Some(response) = stream.next().await {
            yield DataChunk::from_protobuf(response?.get_record_batch()?)?;
        }
    }

    fn run(self) -> BoxedDataChunkStream {
        Box::pin(self.run_inner())
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
            .field("epoch", &self.epoch)
            .field("task_output_id", &self.task_output_id)
            .field("task_host", &self.task_host)
            .finish()
    }
}
