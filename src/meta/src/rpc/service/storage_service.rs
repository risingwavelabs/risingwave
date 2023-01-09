// Copyright 2023 Singularity Data
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

use risingwave_pb::meta::storage_service_server::StorageService;
use risingwave_pb::meta::{GetStateStoreUrlRequest, GetStateStoreUrlResponse};
use tonic::{Request, Response, Status};

pub struct StorageServiceImpl {
    state_store_url: String,
}

impl StorageServiceImpl {
    pub fn new(state_store_url: String) -> Self {
        Self { state_store_url }
    }
}

#[async_trait::async_trait]
impl StorageService for StorageServiceImpl {
    async fn get_state_store_url(
        &self,
        _request: Request<GetStateStoreUrlRequest>,
    ) -> Result<Response<GetStateStoreUrlResponse>, Status> {
        Ok(Response::new(GetStateStoreUrlResponse {
            url: self.state_store_url.clone(),
        }))
    }
}
