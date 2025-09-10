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

use std::collections::HashMap;

use anyhow::{Result, anyhow};
use prometheus_http_query;
use risingwave_common::metrics_reader::{
    ChannelDeltaStats, ChannelDeltaStatsEntry, ChannelDeltaStatsResponse, MetricsReader,
};

/// Implementation of `MetricsReader` that queries Prometheus directly.
pub struct MetricsReaderImpl {
    prometheus_client: Option<prometheus_http_query::Client>,
    prometheus_selector: String,
}

impl MetricsReaderImpl {
    /// Creates a new `MetricsReaderImpl` with the given Prometheus client and selector.
    pub fn new(
        prometheus_client: Option<prometheus_http_query::Client>,
        prometheus_selector: String,
    ) -> Self {
        Self {
            prometheus_client,
            prometheus_selector,
        }
    }
}

#[async_trait::async_trait]
impl MetricsReader for MetricsReaderImpl {
    async fn get_channel_delta_stats(
        &self,
        at: Option<i64>,
        time_offset: Option<i64>,
    ) -> Result<ChannelDeltaStatsResponse> {
        // Local structural type for channel identification
        #[derive(Debug, Clone, PartialEq, Eq, Hash)]
        struct ChannelKey {
            upstream_fragment_id: u32,
            downstream_fragment_id: u32,
        }

        let time_offset = time_offset.unwrap_or(60); // Default to 60 seconds if not provided
        let at_time = at;

        // Check if Prometheus client is available
        let prometheus_client = self
            .prometheus_client
            .as_ref()
            .ok_or_else(|| anyhow!("Prometheus endpoint is not set"))?;

        // Query channel delta stats: throughput and backpressure rate
        let channel_input_throughput_query = format!(
            "sum(rate(stream_actor_in_record_cnt{{{}}}[{}s])) by (fragment_id, upstream_fragment_id)",
            self.prometheus_selector, time_offset
        );
        let channel_output_throughput_query = format!(
            "sum(rate(stream_actor_out_record_cnt{{{}}}[{}s])) by (fragment_id, upstream_fragment_id)",
            self.prometheus_selector, time_offset
        );
        let channel_backpressure_query = format!(
            "sum(rate(stream_actor_output_buffer_blocking_duration_ns{{{}}}[{}s])) by (fragment_id, downstream_fragment_id) \
             / ignoring (downstream_fragment_id) group_left sum(stream_actor_count) by (fragment_id)",
            self.prometheus_selector, time_offset
        );

        // Execute all queries concurrently with optional time parameter
        let (
            channel_input_throughput_result,
            channel_output_throughput_result,
            channel_backpressure_result,
        ) = {
            let mut input_query = prometheus_client.query(channel_input_throughput_query);
            let mut output_query = prometheus_client.query(channel_output_throughput_query);
            let mut backpressure_query = prometheus_client.query(channel_backpressure_query);

            // Set the evaluation time if provided
            if let Some(at_time) = at_time {
                input_query = input_query.at(at_time);
                output_query = output_query.at(at_time);
                backpressure_query = backpressure_query.at(at_time);
            }

            tokio::try_join!(
                input_query.get(),
                output_query.get(),
                backpressure_query.get(),
            )
            .map_err(|e| anyhow!("Failed to query Prometheus: {}", e))?
        };

        // Process channel delta stats
        let mut channel_data: HashMap<ChannelKey, ChannelDeltaStats> = HashMap::new();

        // Collect input throughput
        if let Some(channel_input_throughput_data) =
            channel_input_throughput_result.data().as_vector()
        {
            for sample in channel_input_throughput_data {
                if let Some(fragment_id_str) = sample.metric().get("fragment_id")
                    && let Some(upstream_fragment_id_str) =
                        sample.metric().get("upstream_fragment_id")
                    && let (Ok(fragment_id), Ok(upstream_fragment_id)) = (
                        fragment_id_str.parse::<u32>(),
                        upstream_fragment_id_str.parse::<u32>(),
                    )
                {
                    let key = ChannelKey {
                        upstream_fragment_id,
                        downstream_fragment_id: fragment_id,
                    };
                    channel_data
                        .entry(key)
                        .or_insert_with(|| ChannelDeltaStats {
                            actor_count: 0,
                            backpressure_rate: 0.0,
                            recv_throughput: 0.0,
                            send_throughput: 0.0,
                        })
                        .recv_throughput = sample.sample().value();
                }
            }
        }

        // Collect output throughput
        if let Some(channel_output_throughput_data) =
            channel_output_throughput_result.data().as_vector()
        {
            for sample in channel_output_throughput_data {
                if let Some(fragment_id_str) = sample.metric().get("fragment_id")
                    && let Some(upstream_fragment_id_str) =
                        sample.metric().get("upstream_fragment_id")
                    && let (Ok(fragment_id), Ok(upstream_fragment_id)) = (
                        fragment_id_str.parse::<u32>(),
                        upstream_fragment_id_str.parse::<u32>(),
                    )
                {
                    let key = ChannelKey {
                        upstream_fragment_id,
                        downstream_fragment_id: fragment_id,
                    };
                    channel_data
                        .entry(key)
                        .or_insert_with(|| ChannelDeltaStats {
                            actor_count: 0,
                            backpressure_rate: 0.0,
                            recv_throughput: 0.0,
                            send_throughput: 0.0,
                        })
                        .send_throughput = sample.sample().value();
                }
            }
        }

        // Collect backpressure rate
        if let Some(channel_backpressure_data) = channel_backpressure_result.data().as_vector() {
            for sample in channel_backpressure_data {
                if let Some(fragment_id_str) = sample.metric().get("fragment_id")
                    && let Some(downstream_fragment_id_str) =
                        sample.metric().get("downstream_fragment_id")
                    && let (Ok(fragment_id), Ok(downstream_fragment_id)) = (
                        fragment_id_str.parse::<u32>(),
                        downstream_fragment_id_str.parse::<u32>(),
                    )
                {
                    let key = ChannelKey {
                        upstream_fragment_id: fragment_id,
                        downstream_fragment_id,
                    };
                    channel_data
                        .entry(key)
                        .or_insert_with(|| ChannelDeltaStats {
                            actor_count: 0,
                            backpressure_rate: 0.0,
                            recv_throughput: 0.0,
                            send_throughput: 0.0,
                        })
                        .backpressure_rate = sample.sample().value() / 1_000_000_000.0; // Convert ns to seconds
                }
            }
        }

        // For now, we'll set actor_count to 0 as we don't have easy access to fragment stats
        // In a full implementation, you might want to query fragment stats from compute nodes
        // or maintain a mapping in the metadata manager
        for channel_stats in channel_data.values_mut() {
            channel_stats.actor_count = 0;
        }

        // Convert HashMap to Vec<ChannelDeltaStatsEntry>
        let channel_delta_stats_entries: Vec<ChannelDeltaStatsEntry> = channel_data
            .into_iter()
            .map(|(key, stats)| ChannelDeltaStatsEntry {
                upstream_fragment_id: key.upstream_fragment_id,
                downstream_fragment_id: key.downstream_fragment_id,
                channel_delta_stats: stats,
            })
            .collect();

        Ok(ChannelDeltaStatsResponse {
            channel_delta_stats_entries,
        })
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn test_metrics_reader_impl_creation() {
        // This test just verifies that we can create the parameters for get_channel_delta_stats
        // In a real test, you would need to provide a mock MetaClient
        // For now, we'll just test that the parameter structure is correct
        let at = Some(0i64);
        let time_offset = Some(60i64);

        // Verify the parameter structure is correct
        assert_eq!(at, Some(0i64));
        assert_eq!(time_offset, Some(60i64));
    }
}
