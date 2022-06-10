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

use risingwave_pb::meta::barrier_manager_service_server::BarrierManagerService;
use risingwave_pb::meta::{CollectOverRequest, CollectOverResponse};
use tonic::{Request, Response, Status};

use crate::barrier::BarrierManagerRef;
use crate::storage::MetaStore;

#[derive(Clone)]
pub struct BarrierServiceImpl<S>
where
    S: MetaStore,
{
    barrier_manager: BarrierManagerRef<S>,
}

impl<S> BarrierServiceImpl<S>
where
    S: MetaStore,
{
    pub fn new(barrier_manager: BarrierManagerRef<S>) -> Self {
        BarrierServiceImpl { barrier_manager }
    }
}

#[async_trait::async_trait]
impl<S> BarrierManagerService for BarrierServiceImpl<S>
where
    S: MetaStore,
{
    #[cfg_attr(coverage, no_coverage)]
    async fn collect_over(
        &self,
        request: Request<CollectOverRequest>,
    ) -> Result<Response<CollectOverResponse>, Status> {
        let req = request.into_inner();

        self.barrier_manager.collect_over(req).await;
        Ok(Response::new(CollectOverResponse { status: None }))
    }
}
