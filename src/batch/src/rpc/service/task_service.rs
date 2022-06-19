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

use std::convert::Into;
use std::sync::Arc;

use risingwave_pb::batch_plan::TaskOutputId;
use risingwave_pb::task_service::task_service_server::TaskService;
use risingwave_pb::task_service::{
    AbortTaskRequest, AbortTaskResponse, CreateTaskRequest, CreateTaskResponse, ExecuteRequest,
    GetDataResponse, GetTaskInfoRequest, GetTaskInfoResponse, RemoveTaskRequest,
    RemoveTaskResponse,
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::rpc::service::exchange::GrpcExchangeWriter;
use crate::task::{BatchEnvironment, BatchManager, BatchTaskExecution, ComputeNodeContext};

const LOCAL_EXECUTE_BUFFER_SIZE: usize = 64;

#[derive(Clone)]
pub struct BatchServiceImpl {
    mgr: Arc<BatchManager>,
    env: BatchEnvironment,
}

impl BatchServiceImpl {
    pub fn new(mgr: Arc<BatchManager>, env: BatchEnvironment) -> Self {
        BatchServiceImpl { mgr, env }
    }
}

#[async_trait::async_trait]
impl TaskService for BatchServiceImpl {
    type ExecuteStream = ReceiverStream<std::result::Result<GetDataResponse, Status>>;

    #[cfg_attr(coverage, no_coverage)]
    async fn create_task(
        &self,
        request: Request<CreateTaskRequest>,
    ) -> Result<Response<CreateTaskResponse>, Status> {
        let CreateTaskRequest {
            task_id,
            plan,
            epoch,
            vnode_ranges,
        } = request.into_inner();

        let res = self
            .mgr
            .fire_task(
                &task_id.expect("no task id found"),
                plan.expect("no plan found").clone(),
                vnode_ranges,
                epoch,
                ComputeNodeContext::new(self.env.clone()),
            )
            .await;
        match res {
            Ok(_) => Ok(Response::new(CreateTaskResponse { status: None })),
            Err(e) => {
                error!("failed to fire task {}", e);
                Err(e.into())
            }
        }
    }

    #[cfg_attr(coverage, no_coverage)]
    async fn get_task_info(
        &self,
        _: Request<GetTaskInfoRequest>,
    ) -> Result<Response<GetTaskInfoResponse>, Status> {
        todo!()
    }

    #[cfg_attr(coverage, no_coverage)]
    async fn abort_task(
        &self,
        req: Request<AbortTaskRequest>,
    ) -> Result<Response<AbortTaskResponse>, Status> {
        let req = req.into_inner();
        let res = self
            .mgr
            .abort_task(req.get_task_id().expect("no task id found"));
        match res {
            Ok(_) => Ok(Response::new(AbortTaskResponse { status: None })),
            Err(e) => {
                error!("failed to abort task {}", e);
                Err(e.into())
            }
        }
    }

    #[cfg_attr(coverage, no_coverage)]
    async fn remove_task(
        &self,
        req: Request<RemoveTaskRequest>,
    ) -> Result<Response<RemoveTaskResponse>, Status> {
        let req = req.into_inner();
        let res = self
            .mgr
            .remove_task(req.get_task_id().expect("no task id found"));
        match res {
            Ok(_) => Ok(Response::new(RemoveTaskResponse { status: None })),
            Err(e) => {
                error!("failed to remove task {}", e);
                Err(e.into())
            }
        }
    }

    #[cfg_attr(coverage, no_coverage)]
    async fn execute(
        &self,
        req: Request<ExecuteRequest>,
    ) -> Result<Response<Self::ExecuteStream>, Status> {
        let ExecuteRequest {
            task_id,
            plan,
            epoch,
            vnode_ranges,
        } = req.into_inner();
        let task_id = task_id.expect("no task id found");
        let plan = plan.expect("no plan found").clone();
        let context = ComputeNodeContext::new(self.env.clone());
        debug!(
            "local execute request: plan:{:?} with task id:{:?}",
            plan, task_id
        );
        let task = BatchTaskExecution::new(&task_id, vnode_ranges, plan, context, epoch)?;
        let task = Arc::new(task);

        if let Err(e) = task.clone().async_execute().await {
            error!(
                "failed to build executors and trigger execution of Task {:?}: {}",
                task_id, e
            );
            return Err(e.into());
        }

        let pb_task_output_id = TaskOutputId {
            task_id: Some(task_id.clone()),
            // Since this is local execution path, the exchange would follow single distribution,
            // therefore we would only have one data output.
            output_id: 0,
        };

        let mut output = task.get_task_output(&pb_task_output_id).map_err(|e| {
            error!(
                "failed to get task output of Task {:?} in local execution mode",
                task_id
            );
            e
        })?;
        let (tx, rx) = tokio::sync::mpsc::channel(LOCAL_EXECUTE_BUFFER_SIZE);
        let mut writer = GrpcExchangeWriter::new(tx.clone());
        output.take_data(&mut writer).await?;
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
