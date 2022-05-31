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

use std::sync::Arc;

use risingwave_pb::task_service::task_service_server::TaskService;
use risingwave_pb::task_service::{
    AbortTaskRequest, AbortTaskResponse, CreateTaskRequest, CreateTaskResponse, GetTaskInfoRequest,
    GetTaskInfoResponse, RemoveTaskRequest, RemoveTaskResponse,
};
use tonic::{Request, Response, Status};

use crate::task::{BatchEnvironment, BatchManager, ComputeNodeContext};

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
    #[cfg_attr(coverage, no_coverage)]
    async fn create_task(
        &self,
        request: Request<CreateTaskRequest>,
    ) -> Result<Response<CreateTaskResponse>, Status> {
        let req = request.into_inner();

        let res = self
            .mgr
            .fire_task(
                req.get_task_id().expect("no task id found"),
                req.get_plan().expect("no plan found").clone(),
                req.epoch,
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
}
