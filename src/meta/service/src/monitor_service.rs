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

use risingwave_meta::manager::MetadataManager;
use risingwave_meta::rpc::await_tree::dump_cluster_await_tree;
use risingwave_pb::monitor_service::monitor_service_server::MonitorService;
use risingwave_pb::monitor_service::{
    self, ChannelDeltaStats, GetChannelDeltaStatsRequest, GetChannelDeltaStatsResponse,
    StackTraceRequest, StackTraceResponse,
};
use tonic::{Request, Response, Status};

/// The [`MonitorService`] implementation for meta node.
///
/// Currently, only [`MonitorService::stack_trace`] is implemented, which returns the await tree of
/// all nodes in the cluster.
pub struct MonitorServiceImpl {
    pub metadata_manager: MetadataManager,
    pub await_tree_reg: await_tree::Registry,
    pub prometheus_client: Option<prometheus_http_query::Client>,
    pub prometheus_selector: String,
}

#[tonic::async_trait]
impl MonitorService for MonitorServiceImpl {
    async fn stack_trace(
        &self,
        request: Request<StackTraceRequest>,
    ) -> Result<Response<StackTraceResponse>, Status> {
        let request = request.into_inner();
        let actor_traces_format = request.actor_traces_format();

        let result = dump_cluster_await_tree(
            &self.metadata_manager,
            &self.await_tree_reg,
            actor_traces_format,
        )
        .await?;

        Ok(Response::new(result))
    }

    async fn profiling(
        &self,
        _request: Request<monitor_service::ProfilingRequest>,
    ) -> Result<Response<monitor_service::ProfilingResponse>, Status> {
        Err(Status::unimplemented("not implemented in meta node"))
    }

    async fn heap_profiling(
        &self,
        _request: Request<monitor_service::HeapProfilingRequest>,
    ) -> Result<Response<monitor_service::HeapProfilingResponse>, Status> {
        Err(Status::unimplemented("not implemented in meta node"))
    }

    async fn list_heap_profiling(
        &self,
        _request: Request<monitor_service::ListHeapProfilingRequest>,
    ) -> Result<Response<monitor_service::ListHeapProfilingResponse>, Status> {
        Err(Status::unimplemented("not implemented in meta node"))
    }

    async fn analyze_heap(
        &self,
        _request: Request<monitor_service::AnalyzeHeapRequest>,
    ) -> Result<Response<monitor_service::AnalyzeHeapResponse>, Status> {
        Err(Status::unimplemented("not implemented in meta node"))
    }

    async fn get_streaming_stats(
        &self,
        _request: Request<monitor_service::GetStreamingStatsRequest>,
    ) -> Result<Response<monitor_service::GetStreamingStatsResponse>, Status> {
        Err(Status::unimplemented("not implemented in meta node"))
    }

    async fn tiered_cache_tracing(
        &self,
        _request: Request<monitor_service::TieredCacheTracingRequest>,
    ) -> Result<Response<monitor_service::TieredCacheTracingResponse>, Status> {
        Err(Status::unimplemented("not implemented in meta node"))
    }

    async fn get_profile_stats(
        &self,
        _request: Request<monitor_service::GetProfileStatsRequest>,
    ) -> Result<Response<monitor_service::GetProfileStatsResponse>, Status> {
        Err(Status::unimplemented("not implemented in meta node"))
    }

    async fn get_channel_delta_stats(
        &self,
        request: Request<GetChannelDeltaStatsRequest>,
    ) -> Result<Response<GetChannelDeltaStatsResponse>, Status> {
        let request = request.into_inner();
        let time_offset = request.time_offset;
        let at_time = request.at;

        // Check if Prometheus client is available
        let prometheus_client = self
            .prometheus_client
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("Prometheus endpoint is not set"))?;

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
            .map_err(|e| Status::internal(format!("Failed to query Prometheus: {}", e)))?
        };

        // Process channel delta stats
        let mut channel_data = HashMap::new();

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
                    let key = format!("{}_{}", upstream_fragment_id, fragment_id);
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
                    let key = format!("{}_{}", upstream_fragment_id, fragment_id);
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
                    let key = format!("{}_{}", fragment_id, downstream_fragment_id);
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

        Ok(Response::new(GetChannelDeltaStatsResponse {
            channel_stats: channel_data,
        }))
    }
}
