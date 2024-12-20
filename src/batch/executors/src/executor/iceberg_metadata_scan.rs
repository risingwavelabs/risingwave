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

use futures_async_stream::try_stream;
use futures_util::stream::StreamExt;
use risingwave_common::array::arrow::IcebergArrowConvert;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::Schema;
use risingwave_connector::source::iceberg::IcebergProperties;
use risingwave_connector::source::ConnectorProperties;
use risingwave_connector::WithOptionsSecResolved;
use risingwave_pb::batch_plan::plan_node::NodeBody;

use crate::error::BatchError;
use crate::executor::{BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder};

pub struct IcebergMetadataScanExecutor {
    schema: Schema,
    identity: String,
    iceberg_properties: IcebergProperties,
}

impl Executor for IcebergMetadataScanExecutor {
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

impl IcebergMetadataScanExecutor {
    #[try_stream(ok = DataChunk, error = BatchError)]
    async fn do_execute(self: Box<Self>) {
        let table = self.iceberg_properties.load_table_v2().await?;
        let snapshot = table.metadata_scan().snapshots()?;
        let chunk = IcebergArrowConvert.chunk_from_record_batch(&snapshot)?;
        yield chunk;

        return Ok(());
    }
}

pub struct IcebergMetadataScanExecutorBuilder {}

impl BoxedExecutorBuilder for IcebergMetadataScanExecutorBuilder {
    async fn new_boxed_executor(
        source: &ExecutorBuilder<'_>,
        _inputs: Vec<BoxedExecutor>,
    ) -> crate::error::Result<BoxedExecutor> {
        let node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::IcebergMetadataScan
        )?;

        let options_with_secret =
            WithOptionsSecResolved::new(node.with_properties.clone(), node.secret_refs.clone());
        let iceberg_properties = if let ConnectorProperties::Iceberg(config) =
            ConnectorProperties::extract(options_with_secret.clone(), true)?
        {
            *config
        } else {
            unreachable!()
        };

        Ok(Box::new(IcebergMetadataScanExecutor {
            iceberg_properties,
            identity: source.plan_node().get_identity().clone(),
            schema: Schema::new(vec![]),
        }))
    }
}
