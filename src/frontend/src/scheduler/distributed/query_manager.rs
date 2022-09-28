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
use std::sync::Arc;

use futures::StreamExt;
use futures_async_stream::try_stream;
use pgwire::pg_server::{BoxedError, Session, SessionId};
use pgwire::types::Row;
use risingwave_batch::executor::BoxedDataChunkStream;
use risingwave_common::array::DataChunk;
use risingwave_common::error::RwError;
use risingwave_pb::batch_plan::TaskOutputId;
use risingwave_pb::common::HostAddress;
use risingwave_rpc_client::ComputeClientPoolRef;
use tracing::debug;

use super::QueryExecution;
use crate::catalog::catalog_service::CatalogReader;
use crate::handler::query::QueryResultSet;
use crate::handler::util::to_pg_rows;
use crate::scheduler::plan_fragmenter::{Query, QueryId};
use crate::scheduler::worker_node_manager::WorkerNodeManagerRef;
use crate::scheduler::{ExecutionContextRef, HummockSnapshotManagerRef, SchedulerResult};

pub struct QueryResultFetcher {
    // TODO: Remove these after implemented worker node level snapshot pinnning
    epoch: u64,
    hummock_snapshot_manager: HummockSnapshotManagerRef,

    task_output_id: TaskOutputId,
    task_host: HostAddress,
    compute_client_pool: ComputeClientPoolRef,

    chunk_rx: tokio::sync::mpsc::Receiver<SchedulerResult<DataChunk>>,
}

/// Manages execution of distributed batch queries.
#[derive(Clone)]
pub struct QueryManager {
    worker_node_manager: WorkerNodeManagerRef,
    hummock_snapshot_manager: HummockSnapshotManagerRef,
    compute_client_pool: ComputeClientPoolRef,
    catalog_reader: CatalogReader,

    /// Shutdown channels map
    /// FIXME: Use weak key hash map to remove query id if query ends.
    query_executions_map: Arc<std::sync::Mutex<HashMap<QueryId, Arc<QueryExecution>>>>,
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
            query_executions_map: Arc::new(std::sync::Mutex::new(HashMap::new())),
        }
    }

    pub async fn schedule(
        &self,
        context: ExecutionContextRef,
        query: Query,
        format: bool,
    ) -> SchedulerResult<QueryResultSet> {
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
        let query_result_fetcher = match query_execution.start().await {
            Ok(query_result_fetcher) => query_result_fetcher,
            Err(e) => {
                self.hummock_snapshot_manager
                    .release(epoch, &query_id)
                    .await;
                return Err(e);
            }
        };

        // TODO: Clean up queries status when ends. This should be done lazily.

        Ok(query_result_fetcher.stream_from_channel(format))
    }

    pub fn cancel_queries_in_session(&self, session_id: SessionId) {
        let write_guard = self.query_executions_map.lock().unwrap();
        let values_iter = write_guard.values();
        for query in values_iter {
            // Query manager may have queries from different sessions.
            if query.session_id == session_id {
                let query = query.clone();
                // spawn a task to abort. Avoid await point in this function.
                tokio::spawn(async move { query.abort().await });
            }
        }

        // Note that just like normal query ends we do not explicitly delete.
    }

    pub fn add_query(&self, query_id: QueryId, query_execution: Arc<QueryExecution>) {
        let mut write_guard = self.query_executions_map.lock().unwrap();
        write_guard.insert(query_id, query_execution);
    }

    pub fn delete_query(&self, query_id: &QueryId) {
        let mut write_guard = self.query_executions_map.lock().unwrap();
        write_guard.remove(query_id);
    }
}

impl QueryResultFetcher {
    pub fn new(
        epoch: u64,
        hummock_snapshot_manager: HummockSnapshotManagerRef,
        task_output_id: TaskOutputId,
        task_host: HostAddress,
        compute_client_pool: ComputeClientPoolRef,
        chunk_rx: tokio::sync::mpsc::Receiver<SchedulerResult<DataChunk>>,
    ) -> Self {
        Self {
            epoch,
            hummock_snapshot_manager,
            task_output_id,
            task_host,
            compute_client_pool,
            chunk_rx,
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

    #[try_stream(ok = Vec<Row>, error = BoxedError)]
    async fn stream_from_channel_inner(mut self, format: bool) {
        while let Some(chunk_inner) = self.chunk_rx.recv().await {
            let chunk = chunk_inner?;
            let rows = to_pg_rows(chunk, format);
            yield rows;
        }
    }

    fn stream_from_channel(self, format: bool) -> QueryResultSet {
        Box::pin(self.stream_from_channel_inner(format))
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
