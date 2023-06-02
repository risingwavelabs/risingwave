// Copyright 2023 RisingWave Labs
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

use std::collections::{hash_map, HashMap};
use std::net::SocketAddr;
use std::sync::Arc;

use hytra::TrAdder;
use parking_lot::Mutex;
use risingwave_common::config::BatchConfig;
use risingwave_common::error::ErrorCode::{self, TaskNotFound};
use risingwave_common::error::Result;
use risingwave_common::memory::MemoryContext;
use risingwave_common::util::runtime::BackgroundShutdownRuntime;
use risingwave_pb::batch_plan::{PbTaskId, PbTaskOutputId, PlanFragment};
use risingwave_pb::common::BatchQueryEpoch;
use risingwave_pb::task_service::task_info_response::TaskStatus;
use risingwave_pb::task_service::{GetDataResponse, TaskInfoResponse};
use tokio::sync::mpsc::Sender;
use tonic::Status;

use crate::monitor::BatchManagerMetrics;
use crate::rpc::service::exchange::GrpcExchangeWriter;
use crate::task::{
    BatchTaskExecution, ComputeNodeContext, StateReporter, TaskId, TaskOutput, TaskOutputId,
};

/// `BatchManager` is responsible for managing all batch tasks.
#[derive(Clone)]
pub struct BatchManager {
    /// Every task id has a corresponding task execution.
    tasks: Arc<Mutex<HashMap<TaskId, Arc<BatchTaskExecution<ComputeNodeContext>>>>>,

    /// Runtime for the batch manager.
    runtime: Arc<BackgroundShutdownRuntime>,

    /// Batch configuration
    config: BatchConfig,

    /// Total batch memory usage in this CN.
    /// When each task context report their own usage, it will apply the diff into this total mem
    /// value for all tasks.
    total_mem_val: Arc<TrAdder<i64>>,

    /// Memory context used for batch tasks in cn.
    mem_context: MemoryContext,

    /// Metrics for batch manager.
    metrics: BatchManagerMetrics,
}

impl BatchManager {
    pub fn new(config: BatchConfig, metrics: BatchManagerMetrics) -> Self {
        let runtime = {
            let mut builder = tokio::runtime::Builder::new_multi_thread();
            if let Some(worker_threads_num) = config.worker_threads_num {
                builder.worker_threads(worker_threads_num);
            }
            builder
                .thread_name("risingwave-batch-tasks")
                .enable_all()
                .build()
                .unwrap()
        };

        let mem_context = MemoryContext::root(metrics.batch_total_mem.clone());
        BatchManager {
            tasks: Arc::new(Mutex::new(HashMap::new())),
            runtime: Arc::new(runtime.into()),
            config,
            total_mem_val: TrAdder::new().into(),
            metrics,
            mem_context,
        }
    }

    pub fn memory_context_ref(&self) -> MemoryContext {
        self.mem_context.clone()
    }

    pub async fn fire_task(
        self: &Arc<Self>,
        tid: &PbTaskId,
        plan: PlanFragment,
        epoch: BatchQueryEpoch,
        context: ComputeNodeContext,
        state_reporter: StateReporter,
    ) -> Result<()> {
        trace!("Received task id: {:?}, plan: {:?}", tid, plan);
        let task = BatchTaskExecution::new(tid, plan, context, epoch, self.runtime())?;
        let task_id = task.get_task_id().clone();
        let task = Arc::new(task);
        // Here the task id insert into self.tasks is put in front of `.async_execute`, cuz when
        // send `TaskStatus::Running` in `.async_execute`, the query runner may schedule next stage,
        // it's possible do not found parent task id in theory.
        let ret = if let hash_map::Entry::Vacant(e) = self.tasks.lock().entry(task_id.clone()) {
            e.insert(task.clone());
            self.metrics.task_num.inc();

            let this = self.clone();
            let task_id = task_id.clone();
            let state_reporter = state_reporter.clone();
            let heartbeat_join_handle = self.runtime.spawn(async move {
                this.start_task_heartbeat(state_reporter, task_id).await;
            });
            task.set_heartbeat_join_handle(heartbeat_join_handle);

            Ok(())
        } else {
            Err(ErrorCode::InternalError(format!(
                "can not create duplicate task with the same id: {:?}",
                task_id,
            ))
            .into())
        };
        task.async_execute(Some(state_reporter))
            .await
            .inspect_err(|_| {
                self.cancel_task(&task_id.to_prost());
            })?;
        ret
    }

    async fn start_task_heartbeat(&self, mut state_reporter: StateReporter, task_id: TaskId) {
        scopeguard::guard((), |_| {
            tracing::debug!("heartbeat worker for task {:?} stopped", task_id);
            self.metrics.batch_heartbeat_worker_num.dec();
        });
        tracing::debug!("heartbeat worker for task {:?} started", task_id);
        self.metrics.batch_heartbeat_worker_num.inc();
        // The heartbeat is to ensure task cancellation when frontend's cancellation request fails
        // to reach compute node (for any reason like RPC fails, frontend crashes).
        let mut heartbeat_interval = tokio::time::interval(core::time::Duration::from_secs(60));
        heartbeat_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        heartbeat_interval.reset();
        loop {
            heartbeat_interval.tick().await;
            if !self.tasks.lock().contains_key(&task_id) {
                break;
            }
            if state_reporter
                .send(TaskInfoResponse {
                    task_id: Some(task_id.to_prost()),
                    task_status: TaskStatus::Ping.into(),
                    error_message: "".to_string(),
                })
                .await
                .is_err()
            {
                tracing::warn!("try to cancel task {:?} due to heartbeat", task_id);
                // Task may have been cancelled, but it's fine to `cancel_task` again.
                self.cancel_task(&task_id.to_prost());
                break;
            }
        }
    }

    pub fn get_data(
        &self,
        tx: Sender<std::result::Result<GetDataResponse, Status>>,
        peer_addr: SocketAddr,
        pb_task_output_id: &PbTaskOutputId,
    ) -> Result<()> {
        let task_id = TaskOutputId::try_from(pb_task_output_id)?;
        tracing::trace!(target: "events::compute::exchange", peer_addr = %peer_addr, from = ?task_id, "serve exchange RPC");
        let mut task_output = self.take_output(pb_task_output_id)?;
        self.runtime.spawn(async move {
            let mut writer = GrpcExchangeWriter::new(tx.clone());
            match task_output.take_data(&mut writer).await {
                Ok(_) => {
                    tracing::trace!(
                        from = ?task_id,
                        "exchanged {} chunks",
                        writer.written_chunks(),
                    );
                    Ok(())
                }
                Err(e) => tx.send(Err(e.into())).await,
            }
        });
        Ok(())
    }

    pub fn take_output(&self, output_id: &PbTaskOutputId) -> Result<TaskOutput> {
        let task_id = TaskId::from(output_id.get_task_id()?);
        self.tasks
            .lock()
            .get(&task_id)
            .ok_or(TaskNotFound)?
            .get_task_output(output_id)
    }

    pub fn cancel_task(&self, sid: &PbTaskId) {
        let sid = TaskId::from(sid);
        match self.tasks.lock().remove(&sid) {
            Some(task) => {
                tracing::trace!("Removed task: {:?}", task.get_task_id());
                // Use `cancel` rather than `abort` here since this is not an error which should be
                // propagated to upstream.
                task.cancel();
                self.metrics.task_num.dec();
                if let Some(heartbeat_join_handle) = task.heartbeat_join_handle() {
                    heartbeat_join_handle.abort();
                }
            }
            None => {
                warn!("Task {:?} not found for cancel", sid)
            }
        };
    }

    /// Returns error if task is not running.
    pub fn check_if_task_running(&self, task_id: &TaskId) -> Result<()> {
        match self.tasks.lock().get(task_id) {
            Some(task) => task.check_if_running(),
            None => Err(TaskNotFound.into()),
        }
    }

    pub fn check_if_task_aborted(&self, task_id: &TaskId) -> Result<bool> {
        match self.tasks.lock().get(task_id) {
            Some(task) => task.check_if_aborted(),
            None => Err(TaskNotFound.into()),
        }
    }

    #[cfg(test)]
    async fn wait_until_task_aborted(&self, task_id: &TaskId) -> Result<()> {
        use std::time::Duration;
        loop {
            match self.tasks.lock().get(task_id) {
                Some(task) => {
                    let ret = task.check_if_aborted();
                    match ret {
                        Ok(true) => return Ok(()),
                        Ok(false) => {}
                        Err(err) => return Err(err),
                    }
                }
                None => return Err(TaskNotFound.into()),
            }
            tokio::time::sleep(Duration::from_millis(100)).await
        }
    }

    pub fn runtime(&self) -> Arc<BackgroundShutdownRuntime> {
        self.runtime.clone()
    }

    pub fn config(&self) -> &BatchConfig {
        &self.config
    }

    /// Kill batch queries with larges memory consumption per task. Required to maintain task level
    /// memory usage in the struct. Will be called by global memory manager.
    pub fn kill_queries(&self, reason: String) {
        let mut max_mem_task_id = None;
        let mut max_mem = usize::MIN;
        let guard = self.tasks.lock();
        for (t_id, t) in guard.iter() {
            // If the task has been stopped, we should not count this.
            if t.is_end() {
                continue;
            }
            // Alternatively, we can use a bool flag to indicate end of execution.
            // Now we use only store 0 bytes in Context after execution ends.
            let mem_usage = t.mem_usage();
            if mem_usage > max_mem {
                max_mem = mem_usage;
                max_mem_task_id = Some(t_id.clone());
            }
        }
        if let Some(id) = max_mem_task_id {
            let t = guard.get(&id).unwrap();
            // FIXME: `Abort` will not report error but truncated results to user. We should
            // consider throw error.
            t.abort(reason);
        }
    }

    /// Called by global memory manager for total usage of batch tasks. This op is designed to be
    /// light-weight
    pub fn total_mem_usage(&self) -> usize {
        self.total_mem_val.get() as usize
    }

    /// Calculate the diff between this time and last time memory usage report, apply the diff for
    /// the global counter. Due to the limitation of hytra, we need to use i64 type here.
    pub fn apply_mem_diff(&self, diff: i64) {
        self.total_mem_val.inc(diff)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use risingwave_common::config::BatchConfig;
    use risingwave_hummock_sdk::to_committed_batch_query_epoch;
    use risingwave_pb::batch_plan::exchange_info::DistributionMode;
    use risingwave_pb::batch_plan::plan_node::NodeBody;
    use risingwave_pb::batch_plan::{
        ExchangeInfo, PbTaskId, PbTaskOutputId, PlanFragment, PlanNode, ValuesNode,
    };
    use tonic::Code;

    use crate::monitor::BatchManagerMetrics;
    use crate::task::{BatchManager, ComputeNodeContext, StateReporter, TaskId};

    #[test]
    fn test_task_not_found() {
        use tonic::Status;
        let manager = Arc::new(BatchManager::new(
            BatchConfig::default(),
            BatchManagerMetrics::for_test(),
        ));
        let task_id = TaskId {
            task_id: 0,
            stage_id: 0,
            query_id: "abc".to_string(),
        };

        assert_eq!(
            Status::from(manager.check_if_task_running(&task_id).unwrap_err()).code(),
            Code::Internal
        );

        let output_id = PbTaskOutputId {
            task_id: Some(risingwave_pb::batch_plan::TaskId {
                stage_id: 0,
                task_id: 0,
                query_id: "".to_owned(),
            }),
            output_id: 0,
        };
        match manager.take_output(&output_id) {
            Err(e) => assert_eq!(Status::from(e).code(), Code::Internal),
            Ok(_) => unreachable!(),
        };
    }

    #[tokio::test]
    async fn test_task_id_conflict() {
        let manager = Arc::new(BatchManager::new(
            BatchConfig::default(),
            BatchManagerMetrics::for_test(),
        ));
        let plan = PlanFragment {
            root: Some(PlanNode {
                children: vec![],
                identity: "".to_string(),
                node_body: Some(NodeBody::Values(ValuesNode {
                    tuples: vec![],
                    fields: vec![],
                })),
            }),
            exchange_info: Some(ExchangeInfo {
                mode: DistributionMode::Single as i32,
                distribution: None,
            }),
        };
        let context = ComputeNodeContext::for_test();
        let task_id = PbTaskId {
            query_id: "".to_string(),
            stage_id: 0,
            task_id: 0,
        };
        manager
            .fire_task(
                &task_id,
                plan.clone(),
                to_committed_batch_query_epoch(0),
                context.clone(),
                StateReporter::new_with_test(),
            )
            .await
            .unwrap();
        let err = manager
            .fire_task(
                &task_id,
                plan,
                to_committed_batch_query_epoch(0),
                context,
                StateReporter::new_with_test(),
            )
            .await
            .unwrap_err();
        assert!(err
            .to_string()
            .contains("can not create duplicate task with the same id"));
    }

    #[tokio::test]
    async fn test_task_cancel_for_busy_loop() {
        let manager = Arc::new(BatchManager::new(
            BatchConfig::default(),
            BatchManagerMetrics::for_test(),
        ));
        let plan = PlanFragment {
            root: Some(PlanNode {
                children: vec![],
                identity: "".to_string(),
                node_body: Some(NodeBody::BusyLoopExecutor(true)),
            }),
            exchange_info: Some(ExchangeInfo {
                mode: DistributionMode::Single as i32,
                distribution: None,
            }),
        };
        let context = ComputeNodeContext::for_test();
        let task_id = PbTaskId {
            query_id: "".to_string(),
            stage_id: 0,
            task_id: 0,
        };
        manager
            .fire_task(
                &task_id,
                plan.clone(),
                to_committed_batch_query_epoch(0),
                context.clone(),
                StateReporter::new_with_test(),
            )
            .await
            .unwrap();
        manager.cancel_task(&task_id);
        let task_id = TaskId::from(&task_id);
        assert!(!manager.tasks.lock().contains_key(&task_id));
    }

    #[tokio::test]
    async fn test_task_cancel_for_block() {
        let manager = Arc::new(BatchManager::new(
            BatchConfig::default(),
            BatchManagerMetrics::for_test(),
        ));
        let plan = PlanFragment {
            root: Some(PlanNode {
                children: vec![],
                identity: "".to_string(),
                node_body: Some(NodeBody::BlockExecutor(true)),
            }),
            exchange_info: Some(ExchangeInfo {
                mode: DistributionMode::Single as i32,
                distribution: None,
            }),
        };
        let context = ComputeNodeContext::for_test();
        let task_id = PbTaskId {
            query_id: "".to_string(),
            stage_id: 0,
            task_id: 0,
        };
        manager
            .fire_task(
                &task_id,
                plan.clone(),
                to_committed_batch_query_epoch(0),
                context.clone(),
                StateReporter::new_with_test(),
            )
            .await
            .unwrap();
        manager.cancel_task(&task_id);
        let task_id = TaskId::from(&task_id);
        assert!(!manager.tasks.lock().contains_key(&task_id));
    }

    #[tokio::test]
    async fn test_task_abort_for_busy_loop() {
        let manager = Arc::new(BatchManager::new(
            BatchConfig::default(),
            BatchManagerMetrics::for_test(),
        ));
        let plan = PlanFragment {
            root: Some(PlanNode {
                children: vec![],
                identity: "".to_string(),
                node_body: Some(NodeBody::BusyLoopExecutor(true)),
            }),
            exchange_info: Some(ExchangeInfo {
                mode: DistributionMode::Single as i32,
                distribution: None,
            }),
        };
        let context = ComputeNodeContext::for_test();
        let task_id = PbTaskId {
            query_id: "".to_string(),
            stage_id: 0,
            task_id: 0,
        };
        manager
            .fire_task(
                &task_id,
                plan.clone(),
                to_committed_batch_query_epoch(0),
                context.clone(),
                StateReporter::new_with_test(),
            )
            .await
            .unwrap();
        let task_id = TaskId::from(&task_id);
        manager
            .tasks
            .lock()
            .get(&task_id)
            .unwrap()
            .abort("Abort Test".to_owned());
        assert!(manager.wait_until_task_aborted(&task_id).await.is_ok());
    }

    #[tokio::test]
    async fn test_task_abort_for_block() {
        let manager = Arc::new(BatchManager::new(
            BatchConfig::default(),
            BatchManagerMetrics::for_test(),
        ));
        let plan = PlanFragment {
            root: Some(PlanNode {
                children: vec![],
                identity: "".to_string(),
                node_body: Some(NodeBody::BlockExecutor(true)),
            }),
            exchange_info: Some(ExchangeInfo {
                mode: DistributionMode::Single as i32,
                distribution: None,
            }),
        };
        let context = ComputeNodeContext::for_test();
        let task_id = PbTaskId {
            query_id: "".to_string(),
            stage_id: 0,
            task_id: 0,
        };
        manager
            .fire_task(
                &task_id,
                plan.clone(),
                to_committed_batch_query_epoch(0),
                context.clone(),
                StateReporter::new_with_test(),
            )
            .await
            .unwrap();
        let task_id = TaskId::from(&task_id);
        manager
            .tasks
            .lock()
            .get(&task_id)
            .unwrap()
            .abort("Abort Test".to_owned());
        assert!(manager.wait_until_task_aborted(&task_id).await.is_ok());
    }
}
