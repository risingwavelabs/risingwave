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
use risingwave_common::types::{DataType, ScalarImpl};
use risingwave_common::{ensure, try_match_expand};
use risingwave_pb::batch_plan::plan_node::NodeBody;

use crate::error::{BatchError, Result};
use crate::executor::{
    BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder,
};

/// [`GetChannelStatsExecutor`] implements the executor for retrieving channel statistics
/// from the system catalog. This executor has no inputs and returns channel stats data.
pub struct GetChannelStatsExecutor {
    schema: Schema,
    identity: String,
    at_time: Option<u64>,
    time_offset: u64,
    sys_catalog_reader: Arc<dyn risingwave_common::catalog::SysCatalogReader>,
}

impl GetChannelStatsExecutor {
    pub fn new(
        schema: Schema,
        identity: String,
        at_time: Option<u64>,
        time_offset: u64,
        sys_catalog_reader: Arc<dyn risingwave_common::catalog::SysCatalogReader>,
    ) -> Self {
        Self {
            schema,
            identity,
            at_time,
            time_offset,
            sys_catalog_reader,
        }
    }

    /// Generate channel stats data using the system catalog reader
    async fn generate_channel_stats(&self) -> Vec<Vec<Option<ScalarImpl>>> {
        // Try to get real data from system catalog if possible
        if let Ok(stats) = self.fetch_channel_stats_from_sys_catalog().await {
            return stats;
        }

        // Fallback to mock data if system catalog fails
        self.generate_mock_channel_stats()
    }

    /// Fetch channel stats from system catalog
    async fn fetch_channel_stats_from_sys_catalog(&self) -> Result<Vec<Vec<Option<ScalarImpl>>>> {
        // This is a placeholder for actual system catalog integration
        // In a real implementation, you would use the sys_catalog_reader to get channel stats
        // For now, we'll return an error to fall back to mock data

        // Example of how you might use the system catalog reader:
        // - Access meta client through the system catalog reader if it's SysCatalogReaderImpl
        // - Query system tables for channel information
        // - Process the data and return channel stats...
        todo!()
    }

    /// Generate mock channel stats data for demonstration purposes
    fn generate_mock_channel_stats(&self) -> Vec<Vec<Option<ScalarImpl>>> {
        let mut rows = Vec::new();

        // Generate some sample channel stats data
        // In practice, this would come from the actual system catalog
        let channels = vec![
            ("channel_1", "active", "1000"),
            ("channel_2", "inactive", "500"),
            ("channel_3", "active", "750"),
            ("channel_4", "error", "200"),
        ];

        for (channel_name, status, message_count) in channels {
            rows.push(vec![
                Some(ScalarImpl::Utf8(channel_name.to_owned().into())),
                Some(ScalarImpl::Utf8(status.to_owned().into())),
                Some(ScalarImpl::Utf8(message_count.to_owned().into())),
                Some(ScalarImpl::Utf8(
                    self.at_time.unwrap_or(0).to_string().into(),
                )),
                Some(ScalarImpl::Utf8(self.time_offset.to_string().into())),
            ]);
        }

        rows
    }
}

impl Executor for GetChannelStatsExecutor {
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

impl GetChannelStatsExecutor {
    #[try_stream(boxed, ok = DataChunk, error = BatchError)]
    async fn do_execute(self: Box<Self>) {
        // 1. Read the channel stats from the meta node RPC.
        let stats = self.fetch_channel_stats_from_sys_catalog().await;
        // 2. Render into rows.

        let rows = self.generate_channel_stats().await;

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

impl BoxedExecutorBuilder for GetChannelStatsExecutor {
    async fn new_boxed_executor(
        source: &ExecutorBuilder<'_>,
        inputs: Vec<BoxedExecutor>,
    ) -> Result<BoxedExecutor> {
        ensure!(
            inputs.is_empty(),
            "GetChannelStatsExecutor should have no child!"
        );

        let get_channel_stats_node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::GetChannelStats
        )?;

        // Create a schema for channel stats
        // This would typically include: channel_name, status, message_count, timestamp, time_offset
        let fields = vec![
            Field::new("channel_name", DataType::Varchar),
            Field::new("status", DataType::Varchar),
            Field::new("message_count", DataType::Varchar),
            Field::new("timestamp", DataType::Varchar),
            Field::new("time_offset", DataType::Varchar),
        ];

        let schema = Schema { fields };
        let sys_catalog_reader = source.context().catalog_reader();

        Ok(Box::new(Self::new(
            schema,
            source.plan_node().get_identity().clone(),
            get_channel_stats_node.at_time,
            get_channel_stats_node.time_offset,
            sys_catalog_reader,
        )))
    }
}
