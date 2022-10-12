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

use std::cell::RefCell;
use std::collections::HashMap;
use std::mem;
use std::rc::Rc;
use std::sync::Arc;

use anyhow::anyhow;
use arc_swap::ArcSwap;
use futures::{stream, StreamExt};
use futures_async_stream::for_await;
use itertools::Itertools;
use rand::seq::SliceRandom;
use risingwave_batch::executor::ExecutorBuilder;
use risingwave_batch::task::TaskId as TaskIdBatch;
use risingwave_common::array::DataChunk;
use risingwave_common::types::VnodeMapping;
use risingwave_common::util::addr::HostAddr;
use risingwave_common::util::select_all;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::batch_plan::plan_node::NodeBody::{Delete, Insert, Update};
use risingwave_pb::batch_plan::{
    ExchangeNode, ExchangeSource, MergeSortExchangeNode, PlanFragment, PlanNode as PlanNodeProst,
    TaskId as TaskIdProst, TaskOutputId,
};
use risingwave_pb::common::{HostAddress, WorkerNode};
use risingwave_pb::task_service::{AbortTaskRequest, TaskInfoResponse};
use risingwave_rpc_client::ComputeClientPoolRef;
use tokio::spawn;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{oneshot, RwLock};
use tonic::Streaming;
use tracing::{error, warn};
use StageEvent::Failed;

use crate::catalog::catalog_service::CatalogReader;
use crate::catalog::TableId;
use crate::optimizer::plan_node::PlanNodeType;
use crate::scheduler::distributed::stage::StageState::Pending;
use crate::scheduler::distributed::QueryMessage;
use crate::scheduler::plan_fragmenter::{
    ExecutionPlanNode, PartitionInfo, QueryStageRef, StageId, TaskId, ROOT_TASK_ID,
};
use crate::scheduler::worker_node_manager::WorkerNodeManagerRef;
use crate::scheduler::SchedulerError::TaskExecutionError;
use crate::scheduler::{ExecutionContextRef, SchedulerError, SchedulerResult};

const TASK_SCHEDULING_PARALLELISM: usize = 10;

#[derive(Debug)]
enum StageState {
    Pending { msg_sender: Sender<QueryMessage> },
    Started,
    Running,
    Completed,
    Failed,
}

enum StageMessage {
    /// Contains the reason why need to stop (e.g. Execution failure).
    Stop(String),
}

#[derive(Debug)]
pub enum StageEvent {
    Scheduled(StageId),
    ScheduledRoot(Receiver<SchedulerResult<DataChunk>>),
    /// Stage failed.
    Failed {
        id: StageId,
        reason: SchedulerError,
    },
    Completed(StageId),
}

#[derive(Clone)]
pub struct TaskStatus {
    _task_id: TaskId,

    // None before task is scheduled.
    location: Option<HostAddress>,
}

struct TaskStatusHolder {
    inner: ArcSwap<TaskStatus>,
}

pub struct StageExecution {
    epoch: u64,
    stage: QueryStageRef,
    worker_node_manager: WorkerNodeManagerRef,
    tasks: Arc<HashMap<TaskId, TaskStatusHolder>>,
    state: Arc<RwLock<StageState>>,
    shutdown_rx: RwLock<Option<oneshot::Sender<StageMessage>>>,
    /// Children stage executions.
    ///
    /// We use `Vec` here since children's size is usually small.
    children: Vec<Arc<StageExecution>>,
    compute_client_pool: ComputeClientPoolRef,
    catalog_reader: CatalogReader,

    /// Execution context ref
    ctx: ExecutionContextRef,
}

struct StageRunner {
    epoch: u64,
    state: Arc<RwLock<StageState>>,
    stage: QueryStageRef,
    worker_node_manager: WorkerNodeManagerRef,
    tasks: Arc<HashMap<TaskId, TaskStatusHolder>>,
    // Send message to `QueryRunner` to notify stage state change.
    msg_sender: Sender<QueryMessage>,
    children: Vec<Arc<StageExecution>>,
    compute_client_pool: ComputeClientPoolRef,
    catalog_reader: CatalogReader,

    ctx: ExecutionContextRef,
}

impl TaskStatusHolder {
    fn new(task_id: TaskId) -> Self {
        let task_status = TaskStatus {
            _task_id: task_id,
            location: None,
        };

        Self {
            inner: ArcSwap::new(Arc::new(task_status)),
        }
    }

    fn get_status(&self) -> Arc<TaskStatus> {
        self.inner.load_full()
    }
}

impl StageExecution {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        epoch: u64,
        stage: QueryStageRef,
        worker_node_manager: WorkerNodeManagerRef,
        msg_sender: Sender<QueryMessage>,
        children: Vec<Arc<StageExecution>>,
        compute_client_pool: ComputeClientPoolRef,
        catalog_reader: CatalogReader,
        ctx: ExecutionContextRef,
    ) -> Self {
        let tasks = (0..stage.parallelism)
            .into_iter()
            .map(|task_id| (task_id, TaskStatusHolder::new(task_id)))
            .collect();
        Self {
            epoch,
            stage,
            worker_node_manager,
            tasks: Arc::new(tasks),
            state: Arc::new(RwLock::new(Pending { msg_sender })),
            shutdown_rx: RwLock::new(None),
            children,
            compute_client_pool,
            catalog_reader,
            ctx,
        }
    }

    /// Starts execution of this stage, returns error if already started.
    pub async fn start(&self) {
        let mut s = self.state.write().await;
        let cur_state = mem::replace(&mut *s, StageState::Failed);
        match cur_state {
            StageState::Pending { msg_sender } => {
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
                let (sender, receiver) = oneshot::channel();
                // Fill the shutdown sender.
                let mut holder = self.shutdown_rx.write().await;
                *holder = Some(sender);

                // Change state before spawn runner.
                *s = StageState::Started;

                spawn(async move { runner.run(receiver).await });
            }
            _ => {
                unreachable!("Only expect to schedule stage once");
            }
        }
    }

    pub async fn stop(&self, err_str: String) {
        // Send message to tell Stage Runner stop.
        if let Some(shutdown_tx) = self.shutdown_rx.write().await.take() {
            // It's possible that the stage has not been scheduled, so the channel sender is
            // None.
            if shutdown_tx.send(StageMessage::Stop(err_str)).is_err() {
                // The stage runner handle has already closed. so do no-op.
            }
        }
    }

    pub async fn is_scheduled(&self) -> bool {
        let s = self.state.read().await;
        matches!(*s, StageState::Running { .. })
    }

    pub async fn is_pending(&self) -> bool {
        let s = self.state.read().await;
        matches!(*s, StageState::Pending { .. })
    }

    pub fn get_task_status_unchecked(&self, task_id: TaskId) -> Arc<TaskStatus> {
        self.tasks[&task_id].get_status()
    }

    /// Returns all exchange sources for `output_id`. Each `ExchangeSource` is identified by
    /// producer's `TaskId` and `output_id` (consumer's `TaskId`), since each task may produce
    /// output to several channels.
    ///
    /// When this method is called, all tasks should have been scheduled, and their `worker_node`
    /// should have been set.
    pub fn all_exchange_sources_for(&self, output_id: u32) -> Vec<ExchangeSource> {
        self.tasks
            .iter()
            .map(|(task_id, status_holder)| {
                let task_output_id = TaskOutputId {
                    task_id: Some(TaskIdProst {
                        query_id: self.stage.query_id.id.clone(),
                        stage_id: self.stage.id,
                        task_id: *task_id,
                    }),
                    output_id,
                };

                ExchangeSource {
                    task_output_id: Some(task_output_id),
                    host: Some(status_holder.inner.load_full().location.clone().unwrap()),
                    local_execute_plan: None,
                }
            })
            .collect()
    }
}

impl StageRunner {
    async fn run(mut self, shutdown_rx: oneshot::Receiver<StageMessage>) {
        if let Err(e) = self.schedule_tasks_for_all(shutdown_rx).await {
            error!(
                "Stage {:?}-{:?} failed to schedule tasks, error: {:?}",
                self.stage.query_id, self.stage.id, e
            );
            self.send_event(QueryMessage::Stage(Failed {
                id: self.stage.id,
                reason: e,
            }))
            .await;
        }
    }

    /// Send stage event to listener.
    async fn send_event(&self, event: QueryMessage) {
        if let Err(_e) = self.msg_sender.send(event).await {
            warn!("Failed to send event to Query Runner, may be killed by previous failed event");
        }
    }

    /// Schedule all tasks to CN and wait process all status messages from RPC. Note that when all
    /// task is created, it should tell `QueryRunner` to schedule next.
    async fn schedule_tasks(
        &mut self,
        shutdown_rx: oneshot::Receiver<StageMessage>,
    ) -> SchedulerResult<()> {
        let mut futures = vec![];

        if let Some(table_scan_info) = self.stage.table_scan_info.as_ref() && let Some(vnode_bitmaps) = table_scan_info.partitions.as_ref() {
            // If the stage has table scan nodes, we create tasks according to the data distribution
            // and partition of the table.
            // We let each task read one partition by setting the `vnode_ranges` of the scan node in
            // the task.
            // We schedule the task to the worker node that owns the data partition.
            let parallel_unit_ids = vnode_bitmaps.keys().cloned().collect_vec();
            let workers = self.worker_node_manager.get_workers_by_parallel_unit_ids(&parallel_unit_ids)?;

            for (i, (parallel_unit_id, worker)) in parallel_unit_ids
                .into_iter()
                .zip_eq(workers.into_iter())
                .enumerate()
            {
                let task_id = TaskIdProst {
                    query_id: self.stage.query_id.id.clone(),
                    stage_id: self.stage.id,
                    task_id: i as u32,
                };
                let vnode_ranges = vnode_bitmaps[&parallel_unit_id].clone();
                let plan_fragment = self.create_plan_fragment(i as u32, Some(vnode_ranges));
                futures.push(self.schedule_task(task_id, plan_fragment, Some(worker)));
            }
        } else {
            for id in 0..self.stage.parallelism {
                let task_id = TaskIdProst {
                    query_id: self.stage.query_id.id.clone(),
                    stage_id: self.stage.id,
                    task_id: id,
                };
                let plan_fragment = self.create_plan_fragment(id, None);
                let worker = self.choose_worker(&plan_fragment)?;
                futures.push(self.schedule_task(task_id, plan_fragment, worker));
            }
        }

        // Await each future and convert them into a set of streams.
        let mut buffered = stream::iter(futures).buffer_unordered(TASK_SCHEDULING_PARALLELISM);
        let mut buffered_streams = vec![];
        while let Some(result) = buffered.next().await {
            buffered_streams.push(result?);
        }

        // Merge different task streams into a single stream.
        let mut all_streams = select_all(buffered_streams);

        // Process the stream until finished.
        let mut running_task_cnt = 0;
        let mut sent_signal_to_next = false;
        let mut shutdown_rx = shutdown_rx;
        // This loop will stops once receive a stop message, otherwise keep processing status
        // message.
        loop {
            tokio::select! {
                    biased;
                    _ = &mut shutdown_rx => {
                    // Received shutdown signal from query runner, should send abort RPC to all CNs.
                    // change state to aborted. Note that the task cancel can only happen after schedule all these tasks to CN.
                    // This can be an optimization for future: How to stop before schedule tasks.
                    self.abort_all_running_tasks().await?;
                    break;
                }
                status_res = all_streams.next() => {
                        if let Some(stauts_res_inner) = status_res {
                            // The status can be Running, Finished, Failed etc. This stream contains status from
                            // different tasks.
                            //
                            //
                            // Note: For Task execution failure, it now becomes a Rpc Error and will return here.
                            // Do not process this as task status like Running/Finished/ etc.
                            let status = stauts_res_inner.map_err(SchedulerError::from)?;
                            use risingwave_pb::task_service::task_info::TaskStatus as TaskStatusProst;
                            match TaskStatusProst::from_i32(status.task_info.as_ref().unwrap().task_status).unwrap() {
                                TaskStatusProst::Running => {
                                    running_task_cnt += 1;
                                    // The task running count should always less or equal than the registered tasks
                                    // number.
                                    assert!(running_task_cnt <= self.tasks.keys().len());
                                    // All tasks in this stage have been scheduled. Notify query runner to schedule next
                                    // stage.
                                    if running_task_cnt == self.tasks.keys().len() {
                                        self.notify_schedule_next_stage().await;
                                        sent_signal_to_next = true;
                                    }
                                }

                                TaskStatusProst::Finished => {
                                    // if Finished, no-op
                                }

                                status => {
                                    // The remain possible variant is Pending & Aborted, but now they won't be pushed from CN.
                                    unimplemented!("Unexpected task status {:?}", status);
                                }
                            }
                         } else {
                            // After processing all stream status, we must have sent signal (Either Scheduled or
                            // Failed) to Query Runner. If this is not true, query runner will stuck cuz it do not receive any signals.
                            assert!(sent_signal_to_next);
                            break;
                    }
                }
            }
        }
        Ok(())
    }

    async fn schedule_tasks_for_root(
        &mut self,
        shutdown_rx: oneshot::Receiver<StageMessage>,
    ) -> SchedulerResult<()> {
        let root_stage_id = self.stage.id;
        // Currently, the dml should never be root fragment, so the partition is None.
        // And root fragment only contain one task.
        let plan_fragment = self.create_plan_fragment(ROOT_TASK_ID, None);
        let plan_node = plan_fragment.root.unwrap();
        let task_id = TaskIdBatch {
            query_id: self.stage.query_id.id.clone(),
            stage_id: root_stage_id,
            task_id: 0,
        };

        // Notify QueryRunner to poll chunk from result_rx.
        let (result_tx, result_rx) = tokio::sync::mpsc::channel(100);
        self.send_event(QueryMessage::Stage(StageEvent::ScheduledRoot(result_rx)))
            .await;

        let executor = ExecutorBuilder::new(
            &plan_node,
            &task_id,
            self.ctx.to_batch_task_context(),
            self.epoch,
        );

        let executor = executor.build().await?;
        let chunk_stream = executor.execute();
        let mut terminated_chunk_stream = chunk_stream.take_until(shutdown_rx);
        #[for_await]
        for chunk in &mut terminated_chunk_stream {
            if let Err(ref e) = chunk {
                let err_str = e.to_string();
                result_tx
                    .send(chunk.map_err(|e| e.into()))
                    .await
                    .expect("The receiver should always existed! ");
                // Different from below, return this function and report error.
                return Err(SchedulerError::TaskExecutionError(err_str));
            } else {
                result_tx
                    .send(chunk.map_err(|e| e.into()))
                    .await
                    .expect("The receiver should always existed! ");
            }
        }

        if let Some(err) = terminated_chunk_stream.take_result() {
            let stage_message = err.expect("Sender should always existed!");

            // Terminated by other tasks execution error, so no need to return error here.
            match stage_message {
                StageMessage::Stop(err_str) => {
                    // Tell Query Result Fetcher to stop polling and attach failure reason as str.
                    if let Err(_e) = result_tx.send(Err(TaskExecutionError(err_str))).await {
                        warn!("Send task execution failed");
                    }
                }
            }
        }

        Ok(())
    }

    async fn schedule_tasks_for_all(
        &mut self,
        shutdown_rx: oneshot::Receiver<StageMessage>,
    ) -> SchedulerResult<()> {
        // If root, we execute it locally.
        if self.stage.id != 0 {
            self.schedule_tasks(shutdown_rx).await?;
        } else {
            self.schedule_tasks_for_root(shutdown_rx).await?;
        }
        Ok(())
    }

    #[inline(always)]
    fn get_vnode_mapping(&self, table_id: &TableId) -> Option<VnodeMapping> {
        self.catalog_reader
            .read_guard()
            .get_table_by_id(table_id)
            .map(|table| {
                self.worker_node_manager
                    .get_fragment_mapping(&table.fragment_id)
            })
            .ok()
            .flatten()
    }

    fn choose_worker(&self, plan_fragment: &PlanFragment) -> SchedulerResult<Option<WorkerNode>> {
        let node_body = plan_fragment
            .root
            .as_ref()
            .expect("fail to get plan node")
            .node_body
            .as_ref()
            .expect("fail to get node body");

        let vnode_mapping = match node_body {
            Insert(insert_node) => self.get_vnode_mapping(&insert_node.associated_mview_id.into()),
            Update(update_node) => self.get_vnode_mapping(&update_node.associated_mview_id.into()),
            Delete(delete_node) => self.get_vnode_mapping(&delete_node.associated_mview_id.into()),
            _ => None,
        };

        let worker_node = match vnode_mapping {
            Some(mut parallel_unit_ids) => {
                parallel_unit_ids.dedup();
                let candidates = self
                    .worker_node_manager
                    .get_workers_by_parallel_unit_ids(&parallel_unit_ids)?;
                Some(candidates.choose(&mut rand::thread_rng()).unwrap().clone())
            }
            None => None,
        };

        Ok(worker_node)
    }

    /// Write message into channel to notify query runner current stage have been scheduled.
    async fn notify_schedule_next_stage(&self) {
        // If all tasks of this stage is scheduled, tell the query manager to schedule next.
        {
            // Changing state
            let mut s = self.state.write().await;
            let state = mem::replace(&mut *s, StageState::Failed);
            match state {
                StageState::Started => {
                    *s = StageState::Running;
                }
                _ => unreachable!(
                    "The state can not be {:?} for query-{:?}-{:?} to do notify ",
                    state, self.stage.query_id.id, self.stage.id
                ),
            }
        }
        self.send_event(QueryMessage::Stage(StageEvent::Scheduled(self.stage.id)))
            .await;
    }

    /// Abort all registered tasks. Note that here we do not care which part of tasks has already
    /// failed or completed, cuz the abort task will not fail if the task has already die.
    /// See PR (#4560).
    async fn abort_all_running_tasks(&self) -> SchedulerResult<()> {
        // Set state to failed.
        {
            let mut state = self.state.write().await;
            // Ignore if already finished.
            if let &StageState::Completed = &*state {
                return Ok(());
            }
            // FIXME: Be careful for state jump back.
            *state = StageState::Failed
        }

        for (task, task_status) in self.tasks.iter() {
            // 1. Collect task info and client.
            let loc = &task_status.get_status().location;
            let addr = loc.as_ref().expect("Get address should not fail");
            let client = self
                .compute_client_pool
                .get_by_addr(HostAddr::from(addr))
                .await
                .map_err(|e| anyhow!(e))?;

            // 2. Send RPC to each compute node for each task asynchronously.
            let query_id = self.stage.query_id.id.clone();
            let stage_id = self.stage.id;
            let task_id = *task;
            tokio::spawn(async move {
                if let Err(e) = client
                    .abort(AbortTaskRequest {
                        task_id: Some(risingwave_pb::batch_plan::TaskId {
                            query_id: query_id.clone(),
                            stage_id,
                            task_id,
                        }),
                    })
                    .await
                {
                    error!(
                        "Abort task failed, task_id: {}, stage_id: {}, query_id: {}, reason: {}",
                        task_id, stage_id, query_id, e
                    );
                };
            });
        }
        Ok(())
    }

    async fn schedule_task(
        &self,
        task_id: TaskIdProst,
        plan_fragment: PlanFragment,
        worker: Option<WorkerNode>,
    ) -> SchedulerResult<Streaming<TaskInfoResponse>> {
        let worker_node_addr = worker
            .unwrap_or(self.worker_node_manager.next_random()?)
            .host
            .unwrap();

        let compute_client = self
            .compute_client_pool
            .get_by_addr((&worker_node_addr).into())
            .await
            .map_err(|e| anyhow!(e))?;

        let t_id = task_id.task_id;
        let stream_status = compute_client
            .create_task(task_id, plan_fragment, self.epoch)
            .await
            .map_err(|e| anyhow!(e))?;

        self.tasks[&t_id].inner.store(Arc::new(TaskStatus {
            _task_id: t_id,
            location: Some(worker_node_addr),
        }));

        Ok(stream_status)
    }

    pub fn create_plan_fragment(
        &self,
        task_id: TaskId,
        partition: Option<PartitionInfo>,
    ) -> PlanFragment {
        // Used to maintain auto-increment identity_id of a task.
        let identity_id: Rc<RefCell<u64>> = Rc::new(RefCell::new(0));

        let plan_node_prost =
            self.convert_plan_node(&self.stage.root, task_id, partition, identity_id);
        let exchange_info = self.stage.exchange_info.clone();

        PlanFragment {
            root: Some(plan_node_prost),
            exchange_info: Some(exchange_info),
        }
    }

    fn convert_plan_node(
        &self,
        execution_plan_node: &ExecutionPlanNode,
        task_id: TaskId,
        partition: Option<PartitionInfo>,
        identity_id: Rc<RefCell<u64>>,
    ) -> PlanNodeProst {
        // Generate identity
        let identity = {
            let identity_type = execution_plan_node.plan_node_type;
            let id = *identity_id.borrow();
            identity_id.replace(id + 1);
            format!("{:?}-{}", identity_type, id)
        };

        match execution_plan_node.plan_node_type {
            PlanNodeType::BatchExchange => {
                // Find the stage this exchange node should fetch from and get all exchange sources.
                let child_stage = self
                    .children
                    .iter()
                    .find(|child_stage| {
                        child_stage.stage.id == execution_plan_node.source_stage_id.unwrap()
                    })
                    .unwrap();
                let exchange_sources = child_stage.all_exchange_sources_for(task_id);

                match &execution_plan_node.node {
                    NodeBody::Exchange(_exchange_node) => PlanNodeProst {
                        children: vec![],
                        identity,
                        node_body: Some(NodeBody::Exchange(ExchangeNode {
                            sources: exchange_sources,
                            input_schema: execution_plan_node.schema.clone(),
                        })),
                    },
                    NodeBody::MergeSortExchange(sort_merge_exchange_node) => PlanNodeProst {
                        children: vec![],
                        identity,
                        node_body: Some(NodeBody::MergeSortExchange(MergeSortExchangeNode {
                            exchange: Some(ExchangeNode {
                                sources: exchange_sources,
                                input_schema: execution_plan_node.schema.clone(),
                            }),
                            column_orders: sort_merge_exchange_node.column_orders.clone(),
                        })),
                    },
                    _ => unreachable!(),
                }
            }
            PlanNodeType::BatchSeqScan => {
                let node_body = execution_plan_node.node.clone();
                let NodeBody::RowSeqScan(mut scan_node) = node_body else {
                    unreachable!();
                };
                let partition = partition.unwrap();
                scan_node.vnode_bitmap = Some(partition.vnode_bitmap);
                scan_node.scan_ranges = partition.scan_ranges;
                PlanNodeProst {
                    children: vec![],
                    identity,
                    node_body: Some(NodeBody::RowSeqScan(scan_node)),
                }
            }
            PlanNodeType::BatchLookupJoin => {
                let mut node_body = execution_plan_node.node.clone();
                match &mut node_body {
                    NodeBody::LookupJoin(node) => {
                        let side_table_desc = node
                            .inner_side_table_desc
                            .as_ref()
                            .expect("no side table desc");
                        node.inner_side_vnode_mapping = self
                            .get_vnode_mapping(&side_table_desc.table_id.into())
                            .unwrap_or_default();
                        node.worker_nodes = self.worker_node_manager.list_worker_nodes();
                    }
                    _ => unreachable!(),
                }

                let left_child = self.convert_plan_node(
                    &execution_plan_node.children[0],
                    task_id,
                    partition,
                    identity_id,
                );

                PlanNodeProst {
                    children: vec![left_child],
                    identity,
                    node_body: Some(node_body),
                }
            }
            _ => {
                let children = execution_plan_node
                    .children
                    .iter()
                    .map(|e| {
                        self.convert_plan_node(e, task_id, partition.clone(), identity_id.clone())
                    })
                    .collect();

                PlanNodeProst {
                    children,
                    identity,
                    node_body: Some(execution_plan_node.node.clone()),
                }
            }
        }
    }
}

impl TaskStatus {
    pub fn task_host_unchecked(&self) -> HostAddress {
        self.location.clone().unwrap()
    }
}
