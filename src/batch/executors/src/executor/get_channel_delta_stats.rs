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
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::metrics_reader::MetricsReader;
use risingwave_common::types::{DataType, F64, ScalarImpl};
use risingwave_common::{ensure, try_match_expand};
use risingwave_pb::batch_plan::plan_node::NodeBody;

use crate::error::{BatchError, Result};
use crate::executor::{
    BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder,
};

/// [`GetChannelDeltaStatsExecutor`] implements the executor for retrieving channel statistics
/// from the meta node via RPC calls. This executor has no inputs and returns channel stats data.
pub struct GetChannelDeltaStatsExecutor {
    schema: Schema,
    identity: String,
    at_time: Option<u64>,
    time_offset: Option<u64>,
    metrics_reader: Arc<dyn MetricsReader>,
}

impl GetChannelDeltaStatsExecutor {
    pub fn new(
        schema: Schema,
        identity: String,
        at_time: Option<u64>,
        time_offset: Option<u64>,
        metrics_reader: Arc<dyn MetricsReader>,
    ) -> Self {
        Self {
            schema,
            identity,
            at_time,
            time_offset,
            metrics_reader,
        }
    }

    /// Generate channel stats data using the metrics reader
    async fn generate_channel_stats(&self) -> Result<Vec<Vec<Option<ScalarImpl>>>> {
        let stats = self.fetch_channel_stats_from_metrics_reader().await?;
        println!("Using metrics reader data: {} rows", stats.len());
        Ok(stats)
    }

    /// Fetch channel stats from metrics reader
    async fn fetch_channel_stats_from_metrics_reader(
        &self,
    ) -> Result<Vec<Vec<Option<ScalarImpl>>>> {
        // Fetch channel delta stats from meta node
        let response = self
            .metrics_reader
            .get_channel_delta_stats(
                self.at_time.map(|t| t as i64),
                self.time_offset.map(|t| t as i64),
            )
            .await?;

        // Convert response to rows
        let mut rows = Vec::new();
        for entry in response.channel_delta_stats_entries {
            let stats = &entry.channel_delta_stats;
            let row = vec![
                Some(ScalarImpl::Int32(entry.upstream_fragment_id as i32)),
                Some(ScalarImpl::Int32(entry.downstream_fragment_id as i32)),
                Some(ScalarImpl::Int32(stats.actor_count as i32)),
                Some(ScalarImpl::Float64(F64::from(stats.backpressure_rate))),
                Some(ScalarImpl::Float64(F64::from(stats.recv_throughput))),
                Some(ScalarImpl::Float64(F64::from(stats.send_throughput))),
            ];
            println!("Generated row with {} columns: {:?}", row.len(), row);
            rows.push(row);
        }

        println!("Total rows generated: {}", rows.len());
        Ok(rows)
    }
}

impl Executor for GetChannelDeltaStatsExecutor {
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

impl GetChannelDeltaStatsExecutor {
    #[try_stream(boxed, ok = DataChunk, error = BatchError)]
    async fn do_execute(self: Box<Self>) {
        // 1. Read the channel stats from the meta node RPC.
        // 2. Render into rows.

        let rows = self.generate_channel_stats().await?;

        if !rows.is_empty() {
            let mut array_builders = self.schema.create_array_builders(rows.len());

            // Build arrays for each column
            for (col_idx, builder) in array_builders.iter_mut().enumerate() {
                for row in &rows {
                    let value = &row[col_idx];
                    builder.append(value);
                }
            }

            let columns: Vec<_> = array_builders
                .into_iter()
                .map(|b| b.finish().into())
                .collect();

            let chunk = DataChunk::new(columns, rows.len());
            yield chunk;
        }
    }
}

impl BoxedExecutorBuilder for GetChannelDeltaStatsExecutor {
    async fn new_boxed_executor(
        source: &ExecutorBuilder<'_>,
        inputs: Vec<BoxedExecutor>,
    ) -> Result<BoxedExecutor> {
        ensure!(
            inputs.is_empty(),
            "GetChannelDeltaStatsExecutor should have no child!"
        );

        let get_channel_delta_stats_node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::GetChannelDeltaStats
        )?;

        // Create a schema for channel stats
        // This should match the expected schema from table_function.rs
        let fields = vec![
            Field::new("upstream_fragment_id", DataType::Int32),
            Field::new("downstream_fragment_id", DataType::Int32),
            Field::new("upstream_actor_count", DataType::Int32),
            Field::new("backpressure_rate", DataType::Float64),
            Field::new("recv_throughput", DataType::Float64),
            Field::new("send_throughput", DataType::Float64),
        ];

        let schema = Schema { fields };

        // Get the MetricsReader from the batch task context
        let metrics_reader = source.context().metrics_reader();

        Ok(Box::new(Self::new(
            schema,
            source.plan_node().get_identity().clone(),
            get_channel_delta_stats_node.at_time,
            get_channel_delta_stats_node.time_offset,
            metrics_reader,
        )))
    }
}
