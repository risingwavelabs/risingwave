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

use moka::future::Cache;
use risingwave_common::error::Result;
use risingwave_common::util::addr::HostAddr;

use crate::ComputeClient;

#[derive(Clone)]
pub struct ComputeClientPool {
    cache: Cache<HostAddr, ComputeClient>,
}

impl ComputeClientPool {
    pub fn new(cache_capacity: u64) -> Self {
        Self {
            cache: Cache::new(cache_capacity),
        }
    }

    /// Get a compute client from the pool.
    pub async fn get_client_for_addr(&self, addr: HostAddr) -> Result<ComputeClient> {
        self.cache
            .try_get_with(addr.clone(), async { ComputeClient::new(addr).await })
            .await
            .map_err(|e| {
                // TODO: change this to error when we completed failover and error handling
                panic!("failed to create compute client: {:?}", e)
            })
    }
}
