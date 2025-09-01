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

use futures_async_stream::try_stream;
use risingwave_common::array::{DataChunk, I32Array, F64Array};
use risingwave_common::catalog::Schema;
use risingwave_common::types::{DataType, ScalarImpl};
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::meta::GetChannelStatsRequest;

use crate::error::{BatchError, Result};
use crate::executor::{
    BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder,
    register_executor,
};
use crate::task::BatchTaskContext;

/// [`ChannelStatsExecutor`] fetches channel statistics from the meta service
pub struct ChannelStatsExecutor {
    schema: Schema,
    identity: String,
    meta_client: Arc<dyn crate::task::FrontendMetaClient>,
}

impl ChannelStatsExecutor {
    pub(crate) fn new(
        schema: Schema,
        identity: String,
        meta_client: Arc<dyn crate::task::FrontendMetaClient>,
    ) -> Self {
        Self {
            schema,
            identity,
            meta_client,
        }
    }
}

impl Executor for ChannelStatsExecutor {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn identity(&self) -> &str {
        &self.identity
    }

    fn execute(self: Box<Self>) -> BoxedDataChunkStream {
        self.do_execute()
    }
}

impl ChannelStatsExecutor {
    #[try_stream(boxed, ok = DataChunk, error = BatchError)]
    async fn do_execute(self: Box<Self>) {
        // Create request to get channel stats
        let request = GetChannelStatsRequest {
            at: None, // Use current time
            time_offset: Some(60), // 60 seconds time offset
        };

        // Call meta service to get channel stats
        let response = self
            .meta_client
            .get_channel_stats(request)
            .await
            .map_err(|e| BatchError::Internal(e.into()))?;

        // Convert response to data chunks
        let mut upstream_fragment_ids = Vec::new();
        let mut downstream_fragment_ids = Vec::new();
        let mut actor_counts = Vec::new();
        let mut backpressure_rates = Vec::new();
        let mut recv_throughputs = Vec::new();
        let mut send_throughputs = Vec::new();

        for row in response.rows {
            upstream_fragment_ids.push(Some(row.upstream_fragment_id as i32));
            downstream_fragment_ids.push(Some(row.downstream_fragment_id as i32));
            actor_counts.push(Some(row.actor_count as i32));
            backpressure_rates.push(Some(row.backpressure_rate));
            recv_throughputs.push(Some(row.recv_throughput));
            send_throughputs.push(Some(row.send_throughput));
        }

        // Create arrays
        let upstream_fragment_id_array = I32Array::from_slice(&upstream_fragment_ids).unwrap();
        let downstream_fragment_id_array = I32Array::from_slice(&downstream_fragment_ids).unwrap();
        let actor_count_array = I32Array::from_slice(&actor_counts).unwrap();
        let backpressure_rate_array = F64Array::from_slice(&backpressure_rates).unwrap();
        let recv_throughput_array = F64Array::from_slice(&recv_throughputs).unwrap();
        let send_throughput_array = F64Array::from_slice(&send_throughputs).unwrap();

        // Create data chunk
        let chunk = DataChunk::new(vec![
            upstream_fragment_id_array.into(),
            downstream_fragment_id_array.into(),
            actor_count_array.into(),
            backpressure_rate_array.into(),
            recv_throughput_array.into(),
            send_throughput_array.into(),
        ]);

        yield chunk;
    }
}

impl BoxedExecutorBuilder for ChannelStatsExecutor {
    async fn new_boxed_executor(
        source: &ExecutorBuilder<'_>,
        _inputs: Vec<BoxedExecutor>,
    ) -> Result<BoxedExecutor> {
        let node_body = source.plan_node.get_node_body()?;
        let channel_stats_node = match node_body {
            NodeBody::ChannelStats(node) => node,
            _ => {
                return Err(BatchError::Internal(
                    "Expected ChannelStatsNode".into(),
                ));
            }
        };

                let schema = Schema::from(channel_stats_node.fields.as_slice());
        let identity = source.plan_node.identity.clone();

        // For now, return an error since meta client is not available in batch environment
        // This should be implemented by adding meta_client to BatchEnvironment
        Err(BatchError::Internal(anyhow::anyhow!("Meta client not available in batch environment").into()))
    }
}

register_executor!(ChannelStats, ChannelStatsExecutor);