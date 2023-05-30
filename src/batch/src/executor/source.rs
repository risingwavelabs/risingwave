// Copyright 2023 RisingWave Labs
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
use std::sync::Arc;

use anyhow::anyhow;
use futures::StreamExt;
use futures_async_stream::try_stream;
use risingwave_common::array::{DataChunk, Op, StreamChunk};
use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema, TableId};
use risingwave_common::error::ErrorCode::{ConnectorError, ProtocolError};
use risingwave_common::error::{Result, RwError};
use risingwave_common::types::DataType;
use risingwave_connector::parser::SpecificParserConfig;
use risingwave_connector::source::monitor::SourceMetrics;
use risingwave_connector::source::{
    ConnectorProperties, SourceColumnDesc, SourceContext, SourceFormat, SplitImpl, SplitMetaData,
};
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::plan_common::RowFormatType;
use risingwave_source::connector_source::ConnectorSource;

use super::Executor;
use crate::error::BatchError;
use crate::executor::{BoxedExecutor, BoxedExecutorBuilder, ExecutorBuilder};
use crate::task::BatchTaskContext;

pub struct SourceExecutor {
    connector_source: ConnectorSource,

    // used to create reader
    column_ids: Vec<ColumnId>,
    metrics: Arc<SourceMetrics>,
    source_id: TableId,
    split: SplitImpl,

    schema: Schema,
    identity: String,
}

#[async_trait::async_trait]
impl BoxedExecutorBuilder for SourceExecutor {
    async fn new_boxed_executor<C: BatchTaskContext>(
        source: &ExecutorBuilder<'_, C>,
        inputs: Vec<BoxedExecutor>,
    ) -> Result<BoxedExecutor> {
        ensure!(inputs.is_empty(), "Source should not have input executor!");
        let source_node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::Source
        )?;

        // prepare connector source
        let source_props: HashMap<String, String> =
            HashMap::from_iter(source_node.properties.clone().into_iter());
        let config = ConnectorProperties::extract(source_props)
            .map_err(|e| RwError::from(ConnectorError(e.into())))?;

        let info = source_node.get_info().unwrap();
        let format = match info.get_row_format()? {
            RowFormatType::Json => SourceFormat::Json,
            RowFormatType::Protobuf => SourceFormat::Protobuf,
            RowFormatType::DebeziumJson => SourceFormat::DebeziumJson,
            RowFormatType::Avro => SourceFormat::Avro,
            RowFormatType::Maxwell => SourceFormat::Maxwell,
            RowFormatType::CanalJson => SourceFormat::CanalJson,
            RowFormatType::Native => SourceFormat::Native,
            RowFormatType::DebeziumAvro => SourceFormat::DebeziumAvro,
            RowFormatType::UpsertJson => SourceFormat::UpsertJson,
            _ => unreachable!(),
        };
        if format == SourceFormat::Protobuf && info.row_schema_location.is_empty() {
            return Err(RwError::from(ProtocolError(
                "protobuf file location not provided".to_string(),
            )));
        }

        let parser_config =
            SpecificParserConfig::new(format, info, &source_node.properties).await?;

        let columns: Vec<_> = source_node
            .columns
            .iter()
            .map(|c| SourceColumnDesc::from(&ColumnDesc::from(c.column_desc.as_ref().unwrap())))
            .collect();

        let connector_source = ConnectorSource {
            config,
            columns,
            parser_config,
            connector_message_buffer_size: source
                .context()
                .get_config()
                .developer
                .connector_message_buffer_size,
        };

        let column_ids: Vec<_> = source_node
            .columns
            .iter()
            .map(|column| ColumnId::from(column.get_column_desc().unwrap().column_id))
            .collect();

        let split = SplitImpl::restore_from_bytes(&source_node.split)?;

        let fields = source_node
            .columns
            .iter()
            .map(|prost| {
                let column_desc = prost.column_desc.as_ref().unwrap();
                let data_type = DataType::from(column_desc.column_type.as_ref().unwrap());
                let name = column_desc.name.clone();
                Field::with_name(data_type, name)
            })
            .collect();
        let schema = Schema::new(fields);

        Ok(Box::new(SourceExecutor {
            connector_source,
            column_ids,
            metrics: source.context().source_metrics(),
            source_id: TableId::new(source_node.source_id),
            split,
            schema,
            identity: source.plan_node().get_identity().clone(),
        }))
    }
}

impl Executor for SourceExecutor {
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

impl SourceExecutor {
    #[try_stream(ok = DataChunk, error = RwError)]
    async fn do_execute(self: Box<Self>) {
        let source_ctx = Arc::new(SourceContext::new(
            u32::MAX,
            self.source_id,
            u32::MAX,
            self.metrics,
        ));
        let stream = self
            .connector_source
            .stream_reader(Some(vec![self.split]), self.column_ids, source_ctx)
            .await?;

        #[for_await]
        for chunk in stream {
            match chunk {
                Ok(chunk) => {
                    yield covert_stream_chunk_to_batch_chunk(chunk.chunk)?;
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }
    }
}

fn covert_stream_chunk_to_batch_chunk(chunk: StreamChunk) -> Result<DataChunk> {
    // chunk read from source must be compact
    assert!(chunk.data_chunk().visibility().is_none());

    if chunk.ops().iter().any(|op| *op != Op::Insert) {
        return Err(RwError::from(BatchError::Internal(anyhow!(
            "Only support insert op in batch source executor"
        ))));
    }

    Ok(chunk.data_chunk().clone())
}
