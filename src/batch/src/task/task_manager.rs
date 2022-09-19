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

use std::collections::{hash_map, HashMap};
use std::net::SocketAddr;
use std::sync::Arc;

use parking_lot::Mutex;
use risingwave_common::error::ErrorCode::{self, TaskNotFound};
use risingwave_common::error::{Result, RwError};
use risingwave_pb::batch_plan::{
    PlanFragment, TaskId as ProstTaskId, TaskOutputId as ProstTaskOutputId,
};
use risingwave_pb::task_service::GetDataResponse;
use tokio::runtime::Runtime;
use tokio::sync::mpsc::Sender;
use tonic::Status;

use crate::rpc::service::exchange::GrpcExchangeWriter;
use crate::rpc::service::task_service::TaskInfoResponseResult;
use crate::task::{BatchTaskExecution, ComputeNodeContext, TaskId, TaskOutput, TaskOutputId};

/// `BatchManager` is responsible for managing all batch tasks.
#[derive(Clone)]
pub struct BatchManager {
    /// Every task id has a corresponding task execution.
    tasks: Arc<Mutex<HashMap<TaskId, Arc<BatchTaskExecution<ComputeNodeContext>>>>>,

    /// Runtime for the batch manager.
    runtime: &'static Runtime,
}

impl BatchManager {
    pub fn new(worker_threads_num: Option<usize>) -> Self {
        let runtime = {
            let mut builder = tokio::runtime::Builder::new_multi_thread();
            if let Some(worker_threads_num) = worker_threads_num {
                builder.worker_threads(worker_threads_num);
            }
            builder
                .thread_name("risingwave-batch-tasks")
                .enable_all()
                .build()
                .unwrap()
        };
        BatchManager {
            tasks: Arc::new(Mutex::new(HashMap::new())),
            // Leak the runtime to avoid runtime shutting-down in the main async context.
            // TODO: may manually shutdown the runtime after we implement graceful shutdown for
            // stream manager.
            runtime: Box::leak(Box::new(runtime)),
        }
    }

    pub async fn fire_task(
        &self,
        tid: &ProstTaskId,
        plan: PlanFragment,
        epoch: u64,
        context: ComputeNodeContext,
    ) -> Result<()> {
        trace!("Received task id: {:?}, plan: {:?}", tid, plan);
        let task = BatchTaskExecution::new(tid, plan, context, epoch, self.runtime)?;
        let task_id = task.get_task_id().clone();
        let task = Arc::new(task);
        // Here the task id insert into self.tasks is put in front of `.async_execute`, cuz when
        // send `TaskStatus::Running` in `.async_execute`, the query runner may schedule next stage,
        // it's possible do not found parent task id in theory.
        let ret = if let hash_map::Entry::Vacant(e) = self.tasks.lock().entry(task_id.clone()) {
            e.insert(task.clone());
            Ok(())
        } else {
            Err(ErrorCode::InternalError(format!(
                "can not create duplicate task with the same id: {:?}",
                task_id,
            ))
            .into())
        };
        task.clone().async_execute().await?;
        ret
    }

    pub fn get_data(
        &self,
        tx: Sender<std::result::Result<GetDataResponse, Status>>,
        peer_addr: SocketAddr,
        pb_task_output_id: &ProstTaskOutputId,
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

    pub fn take_output(&self, output_id: &ProstTaskOutputId) -> Result<TaskOutput> {
        let task_id = TaskId::from(output_id.get_task_id()?);
        self.tasks
            .lock()
            .get(&task_id)
            .ok_or(TaskNotFound)?
            .get_task_output(output_id)
    }

    pub fn abort_task(&self, sid: &ProstTaskId) {
        let sid = TaskId::from(sid);
        match self.tasks.lock().get(&sid) {
            Some(task) => task.abort_task(),
            None => {
                warn!("Task id not found for abort task")
            }
        };
    }

    pub fn remove_task(
        &self,
        sid: &ProstTaskId,
    ) -> Result<Option<Arc<BatchTaskExecution<ComputeNodeContext>>>> {
        let task_id = TaskId::from(sid);
        match self.tasks.lock().remove(&task_id) {
            Some(t) => Ok(Some(t)),
            None => Err(TaskNotFound.into()),
        }
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

    pub fn get_error(&self, task_id: &TaskId) -> Result<Option<RwError>> {
        Ok(self
            .tasks
            .lock()
            .get(task_id)
            .ok_or(TaskNotFound)?
            .get_error())
    }

    /// Return the receivers for streaming RPC.
    pub fn get_task_receiver(
        &self,
        task_id: &TaskId,
    ) -> tokio::sync::mpsc::Receiver<TaskInfoResponseResult> {
        self.tasks.lock().get(task_id).unwrap().state_receiver()
    }

    pub fn runtime(&self) -> &'static Runtime {
        self.runtime
    }
}

impl Default for BatchManager {
    fn default() -> Self {
        BatchManager::new(None)
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::types::DataType;
    use risingwave_expr::expr::make_i32_literal;
    use risingwave_pb::batch_plan::exchange_info::DistributionMode;
    use risingwave_pb::batch_plan::plan_node::NodeBody;
    use risingwave_pb::batch_plan::{
        ExchangeInfo, PlanFragment, PlanNode, TableFunctionNode, TaskId as ProstTaskId,
        TaskOutputId as ProstTaskOutputId, ValuesNode,
    };
    use risingwave_pb::expr::table_function::Type;
    use risingwave_pb::expr::TableFunction;
    use tonic::Code;

    use crate::task::{BatchManager, ComputeNodeContext, TaskId};

    #[test]
    fn test_task_not_found() {
        use tonic::Status;
        let manager = BatchManager::new(None);
        let task_id = TaskId {
            task_id: 0,
            stage_id: 0,
            query_id: "abc".to_string(),
        };

        assert_eq!(
            Status::from(manager.check_if_task_running(&task_id).unwrap_err()).code(),
            Code::Internal
        );

        let output_id = ProstTaskOutputId {
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
        let manager = BatchManager::new(None);
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
        let context = ComputeNodeContext::new_for_test();
        let task_id = ProstTaskId {
            query_id: "".to_string(),
            stage_id: 0,
            task_id: 0,
        };
        manager
            .fire_task(&task_id, plan.clone(), 0, context.clone())
            .await
            .unwrap();
        let err = manager
            .fire_task(&task_id, plan, 0, context)
            .await
            .unwrap_err();
        assert!(err
            .to_string()
            .contains("can not create duplicate task with the same id"));
    }

    #[tokio::test]
    async fn test_task_aborted() {
        let manager = BatchManager::new(None);
        let plan = PlanFragment {
            root: Some(PlanNode {
                children: vec![],
                identity: "".to_string(),
                node_body: Some(NodeBody::TableFunction(TableFunctionNode {
                    table_function: Some(TableFunction {
                        function_type: Type::Generate as i32,
                        args: vec![
                            make_i32_literal(1),
                            make_i32_literal(i32::MAX),
                            make_i32_literal(1),
                        ],
                        // This is a bit hacky as we want to make sure the task lasts long enough
                        // for us to abort it.
                        return_type: Some(DataType::Int32.to_protobuf()),
                    }),
                })),
            }),
            exchange_info: Some(ExchangeInfo {
                mode: DistributionMode::Single as i32,
                distribution: None,
            }),
        };
        let context = ComputeNodeContext::new_for_test();
        let task_id = ProstTaskId {
            query_id: "".to_string(),
            stage_id: 0,
            task_id: 0,
        };
        manager
            .fire_task(&task_id, plan.clone(), 0, context.clone())
            .await
            .unwrap();
        manager.abort_task(&task_id);
        let task_id = TaskId::from(&task_id);
        let res = manager.wait_until_task_aborted(&task_id).await;
        assert_eq!(res, Ok(()));
    }
}
