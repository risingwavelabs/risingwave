// Copyright 2025 RisingWave Labs
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

use async_trait::async_trait;
use risingwave_pb::meta::{GetChannelStatsRequest, GetChannelStatsResponse};
use risingwave_rpc_client::error::Result;

/// Trait alias for `FrontendMetaClient` to avoid cyclic dependencies
#[async_trait]
pub trait FrontendMetaClient: Send + Sync {
    async fn get_channel_stats(
        &self,
        request: GetChannelStatsRequest,
    ) -> Result<GetChannelStatsResponse>;
}
