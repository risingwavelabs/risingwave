// Copyright 2024 RisingWave Labs
// Licensed under the Apache License, Version 2.0 (the "License");
//
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

use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::mem;
use std::sync::Arc;

use anyhow::Context;
use futures::executor::block_on;
use petgraph::dot::{Config, Dot};
use petgraph::Graph;
use pgwire::pg_server::SessionId;
use risingwave_batch::worker_manager::worker_node_manager::WorkerNodeSelector;
use risingwave_common::array::DataChunk;
use risingwave_pb::batch_plan::{TaskId as PbTaskId, TaskOutputId as PbTaskOutputId};
use risingwave_pb::common::{BatchQueryEpoch, HostAddress};
use risingwave_rpc_client::ComputeClientPoolRef;
use thiserror_ext::AsReport;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::{oneshot, RwLock};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn, Instrument};

use super::{DistributedQueryMetrics, QueryExecutionInfoRef, QueryResultFetcher, StageEvent};
use crate::catalog::catalog_service::CatalogReader;
use crate::scheduler::distributed::query::QueryMessage;
use crate::scheduler::distributed::stage::StageEvent::ScheduledRoot;
use crate::scheduler::distributed::StageEvent::Scheduled;
use crate::scheduler::distributed::StageExecution;
use crate::scheduler::plan_fragmenter::{Query, StageId, ROOT_TASK_ID, ROOT_TASK_OUTPUT_ID};
use crate::scheduler::{ExecutionContextRef, SchedulerError, SchedulerResult};

pub struct FastInsertExecution {
    query: Arc<Query>,
    shutdown_tx: Sender<QueryMessage>,
    /// Identified by `process_id`, `secret_key`. Query in the same session should have same key.
    pub session_id: SessionId,
    /// Permit to execute the query. Once query finishes execution, this is dropped.
    pub permit: Option<tokio::sync::OwnedSemaphorePermit>,
}

impl FastInsertExecution {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        query: Query,
        session_id: SessionId,
        permit: Option<tokio::sync::OwnedSemaphorePermit>,
    ) -> Self {
        let query = Arc::new(query);
        let (sender, receiver) = channel(100);

        Self {
            query,
            shutdown_tx: sender,
            session_id,
            permit,
        }
    }

    /// Start execution of this query.
    /// Note the two shutdown channel sender and receivers are not dual.
    /// One is used for propagate error to `QueryResultFetcher`, one is used for listening on
    /// cancel request (from ctrl-c, cli, ui etc).
    #[allow(clippy::too_many_arguments)]
    pub async fn execute(
        self: Arc<Self>,
        context: ExecutionContextRef,
        worker_node_manager: WorkerNodeSelector,
        batch_query_epoch: BatchQueryEpoch,
        compute_client_pool: ComputeClientPoolRef,
        catalog_reader: CatalogReader,
        query_execution_info: QueryExecutionInfoRef,
        query_metrics: Arc<DistributedQueryMetrics>,
    ) -> SchedulerResult<QueryResultFetcher> {
        // Start a timer to cancel the query
        // let mut timeout_abort_task_handle: Option<JoinHandle<()>> = None;
        // if let Some(timeout) = context.timeout() {
        //     let this = self.clone();
        //     timeout_abort_task_handle = Some(tokio::spawn(async move {
        //         tokio::time::sleep(timeout).await;
        //         warn!(
        //             "Query {:?} timeout after {} seconds, sending cancel message.",
        //             this.query.query_id,
        //             timeout.as_secs(),
        //         );
        //         this.abort(format!("timeout after {} seconds", timeout.as_secs()))
        //             .await;
        //     }));
        // }

        // Create a oneshot channel for QueryResultFetcher to get failed event.
        // let (root_stage_sender, root_stage_receiver) =
        //     oneshot::channel::<SchedulerResult<QueryResultFetcher>>();

        let runner = QueryRunner {
            query: self.query.clone(),
            stage_executions,
            msg_receiver,
            root_stage_sender: Some(root_stage_sender),
            scheduled_stages_count: 0,
            query_execution_info,
            query_metrics,
            timeout_abort_task_handle,
        };

        let span = tracing::info_span!(
            "distributed_execute",
            query_id = self.query.query_id.id,
            epoch = ?batch_query_epoch,
        );

        tracing::trace!("Starting query: {:?}", self.query.query_id);

        // Not trace the error here, it will be processed in scheduler.
        // tokio::spawn(async move { runner.run().instrument(span).await });
        run_inner();
        // let root_stage = root_stage_receiver
        //     .await
        //     .context("Starting query execution failed")??;

        tracing::trace!(
            "Received root stage query result fetcher: {:?}, query id: {:?}",
            root_stage,
            self.query.query_id
        );

        tracing::trace!("Query {:?} started.", self.query.query_id);
        Ok(root_stage)
    }

    fn run_inner() {
        let runner = StageRunner {
            epoch: self.epoch,
            stage: self.stage.clone(),
            worker_node_manager: self.worker_node_manager.clone(),
            tasks: self.tasks.clone(),
            msg_sender,
            children: self.children.clone(),
            state: self.state.clone(),
            compute_client_pool: self.compute_client_pool.clone(),
            catalog_reader: self.catalog_reader.clone(),
            ctx: self.ctx.clone(),
        };

        // The channel used for shutdown signal messaging.
        let (sender, receiver) = ShutdownToken::new();
        // Fill the shutdown sender.
        let mut holder = self.shutdown_tx.write().await;
        *holder = Some(sender);

        // Change state before spawn runner.
        *s = StageState::Started;

        let span = tracing::info_span!(
            "stage",
            "otel.name" = format!("Stage {}-{}", self.stage.query_id.id, self.stage.id),
            query_id = self.stage.query_id.id,
            stage_id = self.stage.id,
        );
        self.ctx
            .session()
            .env()
            .compute_runtime()
            .spawn(async move { runner.run(receiver).instrument(span).await });

        tracing::trace!(
            "Stage {:?}-{:?} started.",
            self.stage.query_id.id,
            self.stage.id
        )
    }

    fn schedule() {
        for id in 0..self.stage.parallelism.unwrap() {
            let task_id = PbTaskId {
                query_id: self.stage.query_id.id.clone(),
                stage_id: self.stage.id,
                task_id: id as u64,
            };
            let plan_fragment = self.create_plan_fragment(id as u64, None);
            let worker = self.choose_worker(&plan_fragment, id, self.stage.dml_table_id)?;
            self.call_rpc(
                epoch,
                worker_node_manager,
                task_id,
                plan_fragment,
                compute_client_pool,
                worker,
            );
            // futures.push(self.schedule_task(task_id, plan_fragment, worker, expr_context.clone()));
        }
    }

    fn call_rpc(
        epoch: BatchQueryEpoch,
        worker_node_manager: WorkerNodeSelector,
        task_id: PbTaskId,
        plan_fragment: PlanFragment,
        compute_client_pool: ComputeClientPoolRef,
        worker: Option<WorkerNode>,
    ) {
        let mut worker = worker.unwrap_or(worker_node_manager.next_random_worker()?);
        let worker_node_addr = worker.host.take().unwrap();
        let compute_client = compute_client_pool
            .get_by_addr((&worker_node_addr).into())
            // .inspect_err(|_| self.mask_failed_serving_worker(&worker))
            .map_err(|e| anyhow!(e))?;

        let t_id = task_id.task_id;

        let stream_status: TaskInfoResponse = compute_client
            .fast_insert(task_id, plan_fragment, epoch, expr_context)
            // .inspect_err(|_| self.mask_failed_serving_worker(&worker))
            .map_err(|e| anyhow!(e))?;

        Ok(stream_status)
    }

    // fn mask_failed_serving_worker(&self, worker: &WorkerNode) {
    //     if !worker.property.as_ref().map_or(false, |p| p.is_serving) {
    //         return;
    //     }
    //     let duration = Duration::from_secs(std::cmp::max(
    //         self.ctx
    //             .session
    //             .env()
    //             .batch_config()
    //             .mask_worker_temporary_secs as u64,
    //         1,
    //     ));
    //     self.worker_node_manager
    //         .manager
    //         .mask_worker_node(worker.id, duration);
    // }
}
