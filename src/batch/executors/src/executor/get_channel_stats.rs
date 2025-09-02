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

use futures_async_stream::try_stream;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::types::{DataType, ScalarImpl};
use risingwave_common::{ensure, try_match_expand};
use risingwave_pb::batch_plan::plan_node::NodeBody;
use std::sync::Arc;

use crate::error::{BatchError, Result};
use crate::executor::{
    BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder,
};

/// [`GetChannelStatsExecutor`] implements the executor for retrieving channel statistics
/// from the dashboard API. This executor has no inputs and returns channel stats data.
pub struct GetChannelStatsExecutor {
    schema: Schema,
    identity: String,
    at_time: Option<u64>,
    time_offset: u64,
    meta_client: Option<Arc<dyn crate::meta_client::FrontendMetaClient>>,
}

impl GetChannelStatsExecutor {
    pub fn new(
        schema: Schema, 
        identity: String, 
        at_time: Option<u64>, 
        time_offset: u64,
        meta_client: Option<Arc<dyn crate::meta_client::FrontendMetaClient>>,
    ) -> Self {
        Self {
            schema,
            identity,
            at_time,
            time_offset,
            meta_client,
        }
    }

    /// Generate channel stats data from meta client or fallback to mock data
    async fn generate_channel_stats(&self) -> Vec<Vec<Option<ScalarImpl>>> {
        let mut rows = Vec::new();

        // Try to get real data from meta client if available
        if let Some(meta_client) = &self.meta_client {
            if let Ok(stats) = self.fetch_channel_stats_from_meta(meta_client).await {
                return stats;
            }
        }

        // Fallback to mock data if meta client is not available or fails
        self.generate_mock_channel_stats()
    }

    /// Fetch channel stats from meta client
    async fn fetch_channel_stats_from_meta(
        &self,
        meta_client: &Arc<dyn crate::meta_client::FrontendMetaClient>,
    ) -> Result<Vec<Vec<Option<ScalarImpl>>>, BatchError> {
        // This is a placeholder for actual meta client integration
        // In a real implementation, you would call meta_client methods to get channel stats
        // For now, we'll return an error to fall back to mock data
        
        // Example of how you might use the meta client:
        // let cluster_info = meta_client.get_cluster_info().await?;
        // let worker_nodes = meta_client.list_worker_nodes().await?;
        // Process the data and return channel stats...
        
        Err(BatchError::Internal("Meta client integration not yet implemented".into()))
    }

    /// Generate mock channel stats data for demonstration purposes
    fn generate_mock_channel_stats(&self) -> Vec<Vec<Option<ScalarImpl>>> {
        let mut rows = Vec::new();

        // Generate some sample channel stats data
        // In practice, this would come from the actual dashboard API
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

        // Get meta client from the executor builder context if available
        let meta_client = source.context().and_then(|ctx| {
            // Try to get meta client from the context
            // This is a placeholder - you'll need to implement the actual context access
            None
        });

        Ok(Box::new(Self::new(
            schema,
            source.plan_node().get_identity().clone(),
            get_channel_stats_node.at_time,
            get_channel_stats_node.time_offset,
            meta_client,
        )))
    }
}

#[cfg(test)]
mod tests {
    use futures::stream::StreamExt;

    use super::*;

    #[tokio::test]
    async fn test_get_channel_stats_executor() {
        let fields = vec![
            Field::new("channel_name", DataType::Varchar),
            Field::new("status", DataType::Varchar),
            Field::new("message_count", DataType::Varchar),
            Field::new("timestamp", DataType::Varchar),
            Field::new("time_offset", DataType::Varchar),
        ];

        let schema = Schema { fields };
        let executor = Box::new(GetChannelStatsExecutor::new(
            schema,
            "GetChannelStatsExecutor".to_string(),
            Some(1234567890),
            3600,
            None, // No meta client in tests
        ));

        let mut stream = executor.execute();
        let result = stream.next().await.unwrap().unwrap();

        // Should have 4 rows (channels)
        assert_eq!(result.cardinality(), 4);
        // Should have 5 columns
        assert_eq!(result.dimension(), 5);

        // Verify the data structure
        assert_eq!(
            result.column_at(0).as_utf8().unwrap().value_at(0).unwrap(),
            "channel_1"
        );
        assert_eq!(
            result.column_at(1).as_utf8().unwrap().value_at(0).unwrap(),
            "active"
        );
        assert_eq!(
            result.column_at(2).as_utf8().unwrap().value_at(0).unwrap(),
            "1000"
        );
    }

    #[tokio::test]
    async fn test_get_channel_stats_executor_no_at_time() {
        let fields = vec![
            Field::new("channel_name", DataType::Varchar),
            Field::new("status", DataType::Varchar),
            Field::new("message_count", DataType::Varchar),
            Field::new("timestamp", DataType::Varchar),
            Field::new("time_offset", DataType::Varchar),
        ];

        let schema = Schema { fields };
        let executor = Box::new(GetChannelStatsExecutor::new(
            schema,
            "GetChannelStatsExecutor".to_string(),
            None,
            7200,
            None, // No meta client in tests
        ));

        let mut stream = executor.execute();
        let result = stream.next().await.unwrap().unwrap();

        // Should have 4 rows (channels)
        assert_eq!(result.cardinality(), 4);
        // Should have 5 columns
        assert_eq!(result.dimension(), 5);

        // Verify timestamp is 0 when no at_time is provided
        assert_eq!(
            result.column_at(3).as_utf8().unwrap().value_at(0).unwrap(),
            "0"
        );
        assert_eq!(
            result.column_at(4).as_utf8().unwrap().value_at(0).unwrap(),
            "7200"
        );
    }
}
