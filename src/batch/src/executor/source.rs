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

use std::sync::Arc;

use futures::StreamExt;
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::array::{DataChunk, Op, StreamChunk};
use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema, TableId};
use risingwave_common::types::DataType;
use risingwave_connector::parser::SpecificParserConfig;
use risingwave_connector::source::monitor::SourceMetrics;
use risingwave_connector::source::reader::reader::SourceReader;
use risingwave_connector::source::{
    ConnectorProperties, SourceColumnDesc, SourceContext, SourceCtrlOpts, SplitImpl, SplitMetaData,
};
use risingwave_connector::WithOptionsSecResolved;
use risingwave_pb::batch_plan::plan_node::NodeBody;

use super::Executor;
use crate::error::{BatchError, Result};
use crate::executor::{BoxedExecutor, BoxedExecutorBuilder, ExecutorBuilder};

pub struct SourceExecutor {
    source: SourceReader,

    // used to create reader
    column_ids: Vec<ColumnId>,
    metrics: Arc<SourceMetrics>,
    source_id: TableId,
    split_list: Vec<SplitImpl>,

    schema: Schema,
    identity: String,

    chunk_size: usize,
}

impl BoxedExecutorBuilder for SourceExecutor {
    async fn new_boxed_executor(
        source: &ExecutorBuilder<'_>,
        inputs: Vec<BoxedExecutor>,
    ) -> Result<BoxedExecutor> {
        ensure!(inputs.is_empty(), "Source should not have input executor!");
        let source_node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::Source
        )?;

        // prepare connector source
        let options_with_secret = WithOptionsSecResolved::new(
            source_node.with_properties.clone(),
            source_node.secret_refs.clone(),
        );
        let config = ConnectorProperties::extract(options_with_secret.clone(), false)
            .map_err(BatchError::connector)?;

        let info = source_node.get_info().unwrap();
        let parser_config = SpecificParserConfig::new(info, &options_with_secret)?;

        let columns: Vec<_> = source_node
            .columns
            .iter()
            .map(|c| SourceColumnDesc::from(&ColumnDesc::from(c.column_desc.as_ref().unwrap())))
            .collect();

        let column_ids: Vec<_> = source_node
            .columns
            .iter()
            .map(|column| ColumnId::from(column.get_column_desc().unwrap().column_id))
            .collect();

        let split_list = source_node
            .split
            .iter()
            .map(|split| SplitImpl::restore_from_bytes(split).unwrap())
            .collect_vec();

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

        assert!(!matches!(config, ConnectorProperties::Iceberg(_)));
        let source_reader = SourceReader {
            config,
            columns,
            parser_config,
            connector_message_buffer_size: source
                .context()
                .get_config()
                .developer
                .connector_message_buffer_size,
        };

        Ok(Box::new(SourceExecutor {
            source: source_reader,
            column_ids,
            metrics: source.context().source_metrics(),
            source_id: TableId::new(source_node.source_id),
            split_list,
            schema,
            identity: source.plan_node().get_identity().clone(),
            chunk_size: source.context().get_config().developer.chunk_size,
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
    #[try_stream(ok = DataChunk, error = BatchError)]
    async fn do_execute(self: Box<Self>) {
        let source_ctx = Arc::new(SourceContext::new(
            u32::MAX,
            self.source_id,
            u32::MAX,
            "NA".to_owned(), // source name was not passed in batch plan
            self.metrics,
            SourceCtrlOpts {
                chunk_size: self.chunk_size,
                split_txn: false,
            },
            ConnectorProperties::default(),
            None,
        ));
        let (stream, _) = self
            .source
            .build_stream(Some(self.split_list), self.column_ids, source_ctx, false)
            .await?;

        #[for_await]
        for chunk in stream {
            let chunk = chunk.map_err(BatchError::connector)?;
            let data_chunk = covert_stream_chunk_to_batch_chunk(chunk)?;
            if data_chunk.capacity() > 0 {
                yield data_chunk;
            }
        }
    }
}

fn covert_stream_chunk_to_batch_chunk(chunk: StreamChunk) -> Result<DataChunk> {
    // chunk read from source must be compact
    assert!(chunk.data_chunk().is_compacted());

    if chunk.ops().iter().any(|op| *op != Op::Insert) {
        bail!("Only support insert op in batch source executor");
    }

    Ok(chunk.data_chunk().clone())
}
