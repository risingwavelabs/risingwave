// Copyright 2023 Singularity Data
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

use futures::future::try_join_all;
use futures::stream::BoxStream;
use futures::StreamExt;
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::catalog::{ColumnDesc, ColumnId, TableId};
use risingwave_common::error::ErrorCode::{ConnectorError, ProtocolError};
use risingwave_common::error::{internal_error, Result, RwError, ToRwResult};
use risingwave_common::types::Datum;
use risingwave_common::util::select_all;
use risingwave_connector::source::{
    Column, ConnectorProperties, ConnectorState, SourceMessage, SourceMeta, SplitId, SplitMetaData,
    SplitReaderImpl,
};
use risingwave_connector::ConnectorParams;
use risingwave_expr::vector_op::cast::i64_to_timestamptz;
use risingwave_pb::catalog::{
    ColumnIndex as ProstColumnIndex, StreamSourceInfo as ProstStreamSourceInfo,
};
use risingwave_pb::plan_common::{
    ColumnCatalog as ProstColumnCatalog, RowFormatType as ProstRowFormatType,
};

use crate::fs_connector_source::FsConnectorSource;
use crate::monitor::SourceMetrics;
use crate::{
    ParserConfig, SourceColumnDesc, SourceFormat, SourceParserImpl, SourceStreamChunkBuilder,
    StreamChunkWithState,
};

pub const DEFAULT_CONNECTOR_MESSAGE_BUFFER_SIZE: usize = 16;

#[derive(Clone, Debug)]
pub struct SourceContext {
    pub actor_id: u32,
    pub source_id: TableId,
}

impl SourceContext {
    pub fn new(actor_id: u32, source_id: TableId) -> Self {
        SourceContext {
            actor_id,
            source_id,
        }
    }
}

fn default_split_id() -> SplitId {
    "None".into()
}

struct InnerConnectorSourceReader {
    reader: SplitReaderImpl,
    // split should be None or only contains one value
    split: ConnectorState,

    metrics: Arc<SourceMetrics>,
    context: SourceContext,
}

/// [`ConnectorSource`] serves as a bridge between external components and streaming or
/// batch processing. [`ConnectorSource`] introduces schema at this level while
/// [`SplitReaderImpl`] simply loads raw content from message queue or file system.
/// Parallel means that multiple [`InnerConnectorSourceReader`] will run in parallel during the
/// `next`, so that 0 or more Splits reads can be handled at the Source level.
pub struct ConnectorSourceReader {
    parser: Arc<SourceParserImpl>,
    columns: Vec<SourceColumnDesc>,

    // merge all streams of inner reader into one
    // TODO: make this static dispatch instead of box
    stream: BoxStream<'static, Result<Vec<SourceMessage>>>,
}

impl InnerConnectorSourceReader {
    async fn new(
        prop: ConnectorProperties,
        split: ConnectorState,
        columns: Vec<SourceColumnDesc>,
        metrics: Arc<SourceMetrics>,
        context: SourceContext,
    ) -> Result<Self> {
        tracing::debug!(
            "Spawning new connector source inner reader with config {:?}, split {:?}",
            prop,
            split
        );

        // Here is a workaround, we now provide the vec with only one element
        let reader = SplitReaderImpl::create(
            prop,
            split.clone(),
            Some(
                columns
                    .iter()
                    .cloned()
                    .map(|col| Column {
                        name: col.name,
                        data_type: col.data_type,
                    })
                    .collect_vec(),
            ),
        )
        .await
        .to_rw_result()?;

        Ok(InnerConnectorSourceReader {
            reader,
            split,
            metrics,
            context,
        })
    }

    #[try_stream(boxed, ok = Vec<SourceMessage>, error = RwError)]
    async fn into_stream(self) {
        let actor_id = self.context.actor_id.to_string();
        let source_id = self.context.source_id.to_string();
        let id = match &self.split {
            Some(splits) => splits[0].id(),
            None => default_split_id(),
        };
        #[for_await]
        for msgs in self.reader.into_stream() {
            let msgs = msgs?;
            self.metrics
                .partition_input_count
                .with_label_values(&[&actor_id, &source_id, &id])
                .inc_by(msgs.len() as u64);
            let sum_bytes = msgs
                .iter()
                .map(|msg| match &msg.payload {
                    None => 0,
                    Some(payload) => payload.len() as u64,
                })
                .sum();
            self.metrics
                .partition_input_bytes
                .with_label_values(&[&actor_id, &source_id, &id])
                .inc_by(sum_bytes);
            yield msgs;
        }
    }
}

impl ConnectorSourceReader {
    #[try_stream(boxed, ok = StreamChunkWithState, error = RwError)]
    pub async fn into_stream(self) {
        #[for_await]
        for batch in self.stream {
            let batch = batch?;
            let mut builder =
                SourceStreamChunkBuilder::with_capacity(self.columns.clone(), batch.len());
            let mut split_offset_mapping: HashMap<SplitId, String> = HashMap::new();

            for msg in batch {
                tracing::warn!("payload: {:?}", msg.payload);
                let payload = Some(br#"{"amount": "1111111111111111111160", "sysTimestamp": 1673000000}"#.as_slice());
                if let Some(content) = payload {
                    split_offset_mapping.insert(msg.split_id, msg.offset);

                    let old_op_num = builder.op_num();

                    if let Err(e) = self
                        .parser
                        .parse(content.as_ref(), builder.row_writer())
                        .await
                    {
                        tracing::warn!("message parsing failed {}, skipping", e.to_string());
                        continue;
                    }

                    let new_op_num = builder.op_num();

                    // new_op_num - old_op_num is the number of rows added to the builder
                    for _ in old_op_num..new_op_num {
                        // TODO: support more kinds of SourceMeta
                        if let SourceMeta::Kafka(kafka_meta) = msg.meta.clone() {
                            let f = |desc: &SourceColumnDesc| -> Option<Datum> {
                                if !desc.is_meta {
                                    return None;
                                }
                                match desc.name.as_str() {
                                    "_rw_kafka_timestamp" => Some(
                                        kafka_meta
                                            .timestamp
                                            .map(|ts| i64_to_timestamptz(ts).unwrap().into()),
                                    ),
                                    _ => unreachable!(
                                        "kafka will not have this meta column: {}",
                                        desc.name
                                    ),
                                }
                            };
                            builder.row_writer().fulfill_meta_column(f)?;
                        }
                    }
                }
            }
            yield StreamChunkWithState {
                chunk: builder.finish(),
                split_offset_mapping: Some(split_offset_mapping),
            };
        }
    }
}

#[derive(Clone, Debug)]
pub struct ConnectorSource {
    pub config: ConnectorProperties,
    pub columns: Vec<SourceColumnDesc>,
    pub parser: Arc<SourceParserImpl>,
    pub connector_message_buffer_size: usize,
}

impl ConnectorSource {
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        format: SourceFormat,
        row_schema_location: &str,
        use_schema_registry: bool,
        proto_message_name: String,
        properties: HashMap<String, String>,
        columns: Vec<SourceColumnDesc>,
        connector_node_addr: Option<String>,
        connector_message_buffer_size: usize,
    ) -> Result<Self> {
        let mut config = ConnectorProperties::extract(properties.clone())
            .map_err(|e| ConnectorError(e.into()))?;
        if let Some(addr) = connector_node_addr {
            // fixme: require source_id
            config.init_properties_for_cdc(0, addr, None)
        }
        let parser = SourceParserImpl::create(
            &format,
            &properties,
            row_schema_location,
            use_schema_registry,
            proto_message_name,
        )
        .await?;
        Ok(Self {
            config,
            columns,
            parser,
            connector_message_buffer_size,
        })
    }

    fn get_target_columns(&self, column_ids: Vec<ColumnId>) -> Result<Vec<SourceColumnDesc>> {
        column_ids
            .iter()
            .map(|id| {
                self.columns
                    .iter()
                    .find(|c| c.column_id == *id)
                    .ok_or_else(|| {
                        internal_error(format!(
                            "Failed to find column id: {} in source: {:?}",
                            id, self
                        ))
                    })
                    .map(|col| col.clone())
            })
            .collect::<Result<Vec<SourceColumnDesc>>>()
    }

    pub async fn stream_reader(
        &self,
        splits: ConnectorState,
        column_ids: Vec<ColumnId>,
        metrics: Arc<SourceMetrics>,
        context: SourceContext,
    ) -> Result<ConnectorSourceReader> {
        let config = self.config.clone();
        let columns = self.get_target_columns(column_ids)?;
        let source_metrics = metrics.clone();

        let to_reader_splits = match splits {
            Some(vec_split_impl) => vec_split_impl
                .into_iter()
                .map(|split| Some(vec![split]))
                .collect::<Vec<ConnectorState>>(),
            None => vec![None],
        };
        let readers =
            try_join_all(to_reader_splits.into_iter().map(|split| {
                tracing::debug!("spawning connector split reader for split {:?}", split);
                let props = config.clone();
                let columns = columns.clone();
                let metrics = source_metrics.clone();
                let context = context.clone();
                async move {
                    InnerConnectorSourceReader::new(props, split, columns, metrics, context).await
                }
            }))
            .await?;

        let stream = select_all(readers.into_iter().map(|r| r.into_stream())).boxed();

        Ok(ConnectorSourceReader {
            parser: self.parser.clone(),
            columns,
            stream,
        })
    }
}

/// `SourceDescV2` describes a stream source.
#[derive(Debug)]
pub struct SourceDescV2 {
    pub source: ConnectorSource,
    pub format: SourceFormat,
    pub columns: Vec<SourceColumnDesc>,
    pub metrics: Arc<SourceMetrics>,
    pub pk_column_ids: Vec<i32>,
}

#[derive(Clone)]
pub struct SourceDescBuilderV2 {
    columns: Vec<ProstColumnCatalog>,
    metrics: Arc<SourceMetrics>,
    pk_column_ids: Vec<i32>,
    row_id_index: Option<ProstColumnIndex>,
    properties: HashMap<String, String>,
    source_info: ProstStreamSourceInfo,
    connector_params: ConnectorParams,
    connector_message_buffer_size: usize,
}

impl SourceDescBuilderV2 {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        columns: Vec<ProstColumnCatalog>,
        metrics: Arc<SourceMetrics>,
        pk_column_ids: Vec<i32>,
        row_id_index: Option<ProstColumnIndex>,
        properties: HashMap<String, String>,
        source_info: ProstStreamSourceInfo,
        connector_params: ConnectorParams,
        connector_message_buffer_size: usize,
    ) -> Self {
        Self {
            columns,
            metrics,
            pk_column_ids,
            row_id_index,
            properties,
            source_info,
            connector_params,
            connector_message_buffer_size,
        }
    }

    pub async fn build(self) -> Result<SourceDescV2> {
        let format = match self.source_info.get_row_format()? {
            ProstRowFormatType::Json => SourceFormat::Json,
            ProstRowFormatType::Protobuf => SourceFormat::Protobuf,
            ProstRowFormatType::DebeziumJson => SourceFormat::DebeziumJson,
            ProstRowFormatType::Avro => SourceFormat::Avro,
            ProstRowFormatType::Maxwell => SourceFormat::Maxwell,
            ProstRowFormatType::CanalJson => SourceFormat::CanalJson,
            ProstRowFormatType::Csv => SourceFormat::Csv,
            ProstRowFormatType::RowUnspecified => unreachable!(),
        };

        if format == SourceFormat::Protobuf && self.source_info.row_schema_location.is_empty() {
            return Err(ProtocolError("protobuf file location not provided".to_string()).into());
        }

        let mut columns: Vec<_> = self
            .columns
            .iter()
            .map(|c| SourceColumnDesc::from(&ColumnDesc::from(c.column_desc.as_ref().unwrap())))
            .collect();
        if let Some(row_id_index) = self.row_id_index.as_ref() {
            columns[row_id_index.index as usize].is_row_id = true;
        }
        assert!(
            !self.pk_column_ids.is_empty(),
            "source should have at least one pk column"
        );

        let source = ConnectorSource::new(
            format.clone(),
            &self.source_info.row_schema_location,
            self.source_info.use_schema_registry,
            self.source_info.proto_message_name,
            self.properties,
            columns.clone(),
            self.connector_params.connector_rpc_endpoint,
            self.connector_message_buffer_size,
        )
        .await?;

        Ok(SourceDescV2 {
            source,
            format,
            columns,
            metrics: self.metrics,
            pk_column_ids: self.pk_column_ids,
        })
    }

    pub fn metrics(&self) -> Arc<SourceMetrics> {
        self.metrics.clone()
    }

    pub fn build_fs_stream_source(&self) -> Result<FsConnectorSource> {
        let format = match self.source_info.get_row_format()? {
            ProstRowFormatType::Csv => SourceFormat::Csv,
            _ => unreachable!(),
        };
        let parser_config = ParserConfig::new(&format, &self.source_info);
        let mut columns: Vec<_> = self
            .columns
            .iter()
            .map(|c| SourceColumnDesc::from(&ColumnDesc::from(c.column_desc.as_ref().unwrap())))
            .collect();
        if let Some(row_id_index) = self.row_id_index.as_ref() {
            columns[row_id_index.index as usize].is_row_id = true;
        }
        FsConnectorSource::new(
            format,
            self.properties.clone(),
            columns,
            self.connector_params.connector_rpc_endpoint.clone(),
            parser_config,
        )
    }
}

pub mod test_utils {
    use std::collections::HashMap;

    use risingwave_common::catalog::{ColumnDesc, ColumnId, Schema};
    use risingwave_pb::catalog::{ColumnIndex, StreamSourceInfo};
    use risingwave_pb::plan_common::ColumnCatalog;

    use super::{SourceDescBuilderV2, DEFAULT_CONNECTOR_MESSAGE_BUFFER_SIZE};

    pub fn create_source_desc_builder(
        schema: &Schema,
        pk_column_ids: Vec<i32>,
        row_id_index: Option<u64>,
        source_info: StreamSourceInfo,
        properties: HashMap<String, String>,
    ) -> SourceDescBuilderV2 {
        let row_id_index = row_id_index.map(|index| ColumnIndex { index });
        let columns = schema
            .fields
            .iter()
            .enumerate()
            .map(|(i, f)| ColumnCatalog {
                column_desc: Some(
                    ColumnDesc {
                        data_type: f.data_type.clone(),
                        column_id: ColumnId::from(i as i32), // use column index as column id
                        name: f.name.clone(),
                        field_descs: vec![],
                        type_name: "".to_string(),
                    }
                    .to_protobuf(),
                ),
                is_hidden: false,
            })
            .collect();
        SourceDescBuilderV2 {
            columns,
            metrics: Default::default(),
            pk_column_ids,
            row_id_index,
            properties,
            source_info,
            connector_params: Default::default(),
            connector_message_buffer_size: DEFAULT_CONNECTOR_MESSAGE_BUFFER_SIZE,
        }
    }
}
