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

use risingwave_pb::meta::scale_service_server::ScaleService;
use risingwave_pb::meta::{PauseRequest, PauseResponse, ResumeRequest, ResumeResponse};
use tonic::{Request, Response, Status};

use crate::barrier::{BarrierManagerRef, Command};
use crate::storage::MetaStore;

pub struct ScaleServiceImpl<S: MetaStore> {
    barrier_manager: BarrierManagerRef<S>,
}

impl<S> ScaleServiceImpl<S>
where
    S: MetaStore,
{
    pub fn new(barrier_manager: BarrierManagerRef<S>) -> Self {
        Self { barrier_manager }
    }
}

#[async_trait::async_trait]
impl<S> ScaleService for ScaleServiceImpl<S>
where
    S: MetaStore,
{
    #[cfg_attr(coverage, no_coverage)]
    async fn pause(&self, _: Request<PauseRequest>) -> Result<Response<PauseResponse>, Status> {
        self.barrier_manager.run_command(Command::pause()).await?;
        Ok(Response::new(PauseResponse {}))
    }

    #[cfg_attr(coverage, no_coverage)]
    async fn resume(&self, _: Request<ResumeRequest>) -> Result<Response<ResumeResponse>, Status> {
        self.barrier_manager.run_command(Command::resume()).await?;
        Ok(Response::new(ResumeResponse {}))
    }
}
