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

use risingwave_common::error::tonic_err;
use risingwave_common::try_match_expand;
use risingwave_pb::meta::scale_service_server::ScaleService;
use risingwave_pb::meta::{
    AbortTaskRequest, AbortTaskResponse, GetTaskStatusRequest, GetTaskStatusResponse,
    RemoveTaskRequest, RemoveTaskResponse, ScaleTaskRequest, ScaleTaskResponse,
};
use tonic::{Request, Response, Status};

use crate::manager::ScaleManagerRef;
use crate::storage::MetaStore;

pub struct ScaleServiceImpl<S: MetaStore> {
    scale_manager: ScaleManagerRef<S>,
}

impl<S> ScaleServiceImpl<S>
where
    S: MetaStore,
{
    pub fn new(scale_manager: ScaleManagerRef<S>) -> Self {
        ScaleServiceImpl { scale_manager }
    }
}

#[async_trait::async_trait]
impl<S> ScaleService for ScaleServiceImpl<S>
where
    S: MetaStore,
{
    async fn scale_task(
        &self,
        request: Request<ScaleTaskRequest>,
    ) -> Result<Response<ScaleTaskResponse>, Status> {
        let req = request.into_inner();
        let task = try_match_expand!(req.task, Some, "ScaleTaskRequest::task is empty")?;
        task.get_task_type().map_err(tonic_err)?;
        let task_id = self.scale_manager.add_scale_task(task).await?;
        Ok(Response::new(ScaleTaskResponse {
            status: None,
            task_id,
        }))
    }

    async fn get_task_status(
        &self,
        request: Request<GetTaskStatusRequest>,
    ) -> Result<Response<GetTaskStatusResponse>, Status> {
        let req = request.into_inner();
        let task_id = req.task_id;
        Ok(Response::new(GetTaskStatusResponse {
            status: None,
            task_status: self
                .scale_manager
                .get_task_status(task_id)
                .await
                .map_err(tonic_err)? as i32,
        }))
    }

    async fn abort_task(
        &self,
        request: Request<AbortTaskRequest>,
    ) -> Result<Response<AbortTaskResponse>, Status> {
        let req = request.into_inner();
        let task_id = req.task_id;
        self.scale_manager
            .abort_task(task_id)
            .await
            .map_err(tonic_err)?;
        Ok(Response::new(AbortTaskResponse { status: None }))
    }

    async fn remove_task(
        &self,
        request: Request<RemoveTaskRequest>,
    ) -> Result<Response<RemoveTaskResponse>, Status> {
        let req = request.into_inner();
        let task_id = req.task_id;
        self.scale_manager
            .remove_task(task_id)
            .await
            .map_err(tonic_err)?;
        Ok(Response::new(RemoveTaskResponse { status: None }))
    }
}
