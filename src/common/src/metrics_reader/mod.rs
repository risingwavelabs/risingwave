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

use anyhow::Result;

/// Entry containing channel delta statistics with fragment IDs.
#[derive(Debug, Clone)]
pub struct ChannelDeltaStatsEntry {
    pub upstream_fragment_id: u32,
    pub downstream_fragment_id: u32,
    pub backpressure_rate: f64,
    pub recv_throughput: f64,
    pub send_throughput: f64,
}

/// Trait for reading metrics from the meta node via RPC calls.
#[async_trait::async_trait]
pub trait MetricsReader: Send + Sync {
    /// Fetches channel delta statistics from the meta node.
    ///
    /// # Arguments
    /// * `at` - Unix timestamp in seconds for the evaluation time. If None, defaults to current Prometheus server time.
    /// * `time_offset` - Time offset for throughput and backpressure rate calculation in seconds. If None, defaults to 60s.
    ///
    /// # Returns
    /// * `Result<Vec<ChannelDeltaStatsEntry>>` - The channel delta stats entries or an error
    async fn get_channel_delta_stats(
        &self,
        at: Option<i64>,
        time_offset: Option<i64>,
    ) -> Result<Vec<ChannelDeltaStatsEntry>>;
}
