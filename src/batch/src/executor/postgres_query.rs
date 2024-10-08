// Copyright 2024 RisingWave Labs
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

use anyhow::anyhow;
use futures_async_stream::try_stream;
use futures_util::stream::StreamExt;
use parquet::arrow::ProjectionMask;
use risingwave_common::array::arrow::IcebergArrowConvert;
use risingwave_common::catalog::{Field, Schema};
use risingwave_connector::source::iceberg::parquet_file_reader::create_parquet_stream_builder;
use risingwave_pb::batch_plan::plan_node::NodeBody;

use crate::error::BatchError;
use crate::executor::{BoxedExecutor, BoxedExecutorBuilder, DataChunk, Executor, ExecutorBuilder};
use crate::task::BatchTaskContext;

/// S3 file scan executor. Currently only support parquet file format.
pub struct PostgresQueryExecutor {
    schema: Schema,
    identity: String,
}

impl Executor for PostgresQueryExecutor {
    fn schema(&self) -> &risingwave_common::catalog::Schema {
        &self.schema
    }

    fn identity(&self) -> &str {
        &self.identity
    }

    fn execute(self: Box<Self>) -> super::BoxedDataChunkStream {
        self.do_execute().boxed()
    }
}

impl PostgresQueryExecutor {
    pub fn new(schema: Schema, identity: String) -> Self {
        Self { schema, identity }
    }

    #[try_stream(ok = DataChunk, error = BatchError)]
    async fn do_execute(self: Box<Self>) {}
}

pub struct PostgresQueryExecutorBuilder {}

#[async_trait::async_trait]
impl BoxedExecutorBuilder for PostgresQueryExecutorBuilder {
    async fn new_boxed_executor<C: BatchTaskContext>(
        source: &ExecutorBuilder<'_, C>,
        _inputs: Vec<BoxedExecutor>,
    ) -> crate::error::Result<BoxedExecutor> {
        let file_scan_node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::PostgresQuery
        )?;

        Ok(Box::new(PostgresQueryExecutor::new(
            Schema::from_iter(file_scan_node.columns.iter().map(Field::from)),
            source.plan_node().get_identity().clone(),
        )))
    }
}
