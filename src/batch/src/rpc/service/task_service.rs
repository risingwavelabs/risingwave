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
    AbortTaskRequest, AbortTaskResponse, CreateTaskRequest, ExecuteRequest, GetDataResponse,
    TaskInfoResponse,
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::rpc::service::exchange::GrpcExchangeWriter;
use crate::task::{
    self, BatchEnvironment, BatchManager, BatchTaskExecution, ComputeNodeContext, TaskId,
};

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
pub(crate) type TaskInfoResponseResult = std::result::Result<TaskInfoResponse, Status>;
#[async_trait::async_trait]
impl TaskService for BatchServiceImpl {
    type CreateTaskStream = ReceiverStream<TaskInfoResponseResult>;
    type ExecuteStream = ReceiverStream<std::result::Result<GetDataResponse, Status>>;

    #[cfg_attr(coverage, no_coverage)]
    async fn create_task(
        &self,
        request: Request<CreateTaskRequest>,
    ) -> Result<Response<Self::CreateTaskStream>, Status> {
        let CreateTaskRequest {
            task_id,
            plan,
            epoch,
        } = request.into_inner();

        let res = self
            .mgr
            .fire_task(
                task_id.as_ref().expect("no task id found"),
                plan.expect("no plan found").clone(),
                epoch,
                ComputeNodeContext::new(
                    self.env.clone(),
                    TaskId::from(task_id.as_ref().expect("no task id found")),
                ),
            )
            .await;
        match res {
            Ok(_) => Ok(Response::new(ReceiverStream::new(
                // Create receiver stream from state receiver.
                // The state receiver is init in `.async_execute()`.
                // Will be used for receive task status update.
                // Note: we introduce this hack cuz `.execute()` do not produce a status stream,
                // but still share `.async_execute()` and `.try_execute()`.
                self.mgr
                    .get_task_receiver(&task::TaskId::from(&task_id.unwrap())),
            ))),
            Err(e) => {
                error!("failed to fire task {}", e);
                Err(e.into())
            }
        }
    }

    #[cfg_attr(coverage, no_coverage)]
    async fn abort_task(
        &self,
        req: Request<AbortTaskRequest>,
    ) -> Result<Response<AbortTaskResponse>, Status> {
        let req = req.into_inner();
        self.mgr
            .abort_task(req.get_task_id().expect("no task id found"));
        Ok(Response::new(AbortTaskResponse { status: None }))
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
        } = req.into_inner();
        let task_id = task_id.expect("no task id found");
        let plan = plan.expect("no plan found").clone();
        let context = ComputeNodeContext::new_for_local(self.env.clone());
        trace!(
            "local execute request: plan:{:?} with task id:{:?}",
            plan,
            task_id
        );
        let task = BatchTaskExecution::new(&task_id, plan, context, epoch, self.mgr.runtime())?;
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
        self.mgr.runtime().spawn(async move {
            match output.take_data(&mut writer).await {
                Ok(_) => Ok(()),
                Err(e) => tx.clone().send(Err(e.into())).await
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
