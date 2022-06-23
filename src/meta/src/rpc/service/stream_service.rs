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

use risingwave_pb::meta::stream_manager_service_server::StreamManagerService;
use risingwave_pb::meta::*;
use tonic::{Request, Response, Status};

use crate::storage::MetaStore;
use crate::stream::GlobalStreamManagerRef;

pub type TonicResponse<T> = Result<Response<T>, Status>;

#[derive(Clone)]
pub struct StreamServiceImpl<S>
where
    S: MetaStore,
{
    global_stream_manager: GlobalStreamManagerRef<S>,
}

impl<S> StreamServiceImpl<S>
where
    S: MetaStore,
{
    pub fn new(global_stream_manager: GlobalStreamManagerRef<S>) -> Self {
        StreamServiceImpl {
            global_stream_manager,
        }
    }
}

#[async_trait::async_trait]
impl<S> StreamManagerService for StreamServiceImpl<S>
where
    S: MetaStore,
{
    #[cfg_attr(coverage, no_coverage)]
    async fn flush(&self, request: Request<FlushRequest>) -> TonicResponse<FlushResponse> {
        let _req = request.into_inner();

        self.global_stream_manager.flush().await?;
        Ok(Response::new(FlushResponse { status: None }))
    }
}
