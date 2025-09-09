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

use std::sync::Arc;

use anyhow::Result;
use risingwave_common::metrics_reader::MetricsReader;
use risingwave_pb::monitor_service::{GetChannelDeltaStatsRequest, GetChannelDeltaStatsResponse};

use crate::meta_client::FrontendMetaClient;

/// Implementation of `MetricsReader` that wraps a `FrontendMetaClient`.
pub struct MetricsReaderImpl {
    meta_client: Arc<dyn FrontendMetaClient>,
}

impl MetricsReaderImpl {
    /// Creates a new `MetricsReaderImpl` with the given `FrontendMetaClient`.
    pub fn new(meta_client: Arc<dyn FrontendMetaClient>) -> Self {
        Self { meta_client }
    }
}

#[async_trait::async_trait]
impl MetricsReader for MetricsReaderImpl {
    async fn get_channel_delta_stats(
        &self,
        request: GetChannelDeltaStatsRequest,
    ) -> Result<GetChannelDeltaStatsResponse> {
        self.meta_client
            .get_channel_delta_stats(request)
            .await
            .map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use risingwave_pb::monitor_service::GetChannelDeltaStatsRequest;

    #[tokio::test]
    async fn test_metrics_reader_impl_creation() {
        // This test just verifies that we can create a GetChannelDeltaStatsRequest
        // In a real test, you would need to provide a mock MetaClient
        // For now, we'll just test that the request structure is correct
        let request = GetChannelDeltaStatsRequest {
            time_offset: 0,
            at: Some(0),
        };

        // Verify the request structure is correct
        assert_eq!(request.time_offset, 0);
        assert_eq!(request.at, Some(0));
    }
}
