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

use std::sync::Arc;

use risingwave_common::error::RwError;
use risingwave_hummock_sdk::compact::CompactorRuntimeConfig;
use risingwave_pb::compactor::compactor_service_server::CompactorService;
use risingwave_pb::compactor::{SetRuntimeConfigRequest, SetRuntimeConfigResponse};
use risingwave_rpc_client::MetaClient;
use risingwave_storage::hummock::compactor::CompactorContext;
use tonic::{Request, Response, Status};

pub struct CompactorServiceImpl {
    context: Arc<CompactorContext>,
    meta_client: MetaClient,
}

impl CompactorServiceImpl {
    pub fn new(context: Arc<CompactorContext>, meta_client: MetaClient) -> Self {
        Self {
            context,
            meta_client,
        }
    }
}

#[async_trait::async_trait]
impl CompactorService for CompactorServiceImpl {
    async fn set_runtime_config(
        &self,
        request: Request<SetRuntimeConfigRequest>,
    ) -> Result<Response<SetRuntimeConfigResponse>, Status> {
        // The lock ensures config is synchronized in compactor and meta. Otherwise this may happen:
        // 1. set_compactor_runtime_config succeeds with new config.
        // 2. subscribe_compact_tasks succeeds with old config.
        // 3. Local config is set with new config. But the one in meta is stale.
        let mut local_config = self.context.lock_config().await;
        let new_config = CompactorRuntimeConfig::from(request.into_inner().config.unwrap());
        self.meta_client
            .set_compactor_runtime_config(new_config.clone())
            .await
            .map_err(RwError::from)?;
        *local_config = new_config;
        Ok(Response::new(SetRuntimeConfigResponse {}))
    }
}
