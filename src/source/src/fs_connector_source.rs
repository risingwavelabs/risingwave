// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::sync::Arc;

use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use futures_async_stream::try_stream;
use risingwave_common::catalog::ColumnId;
use risingwave_common::error::ErrorCode::ConnectorError;
use risingwave_common::error::{internal_error, Result, RwError};
use risingwave_connector::source::filesystem::FsSplit;
use risingwave_connector::source::{
    ConnectorProperties, FsSourceMessage, FsSplitReaderImpl, SplitId,
};

use crate::connector_source::SourceContext;
use crate::monitor::SourceMetrics;
use crate::{
    ByteStreamSourceParserImpl, FsStreamChunkWithState, ParserConfig, SourceColumnDesc,
    SourceFormat, SourceStreamChunkBuilder,
};

pub struct FsConnectorSourceReader {
    parser_config: ParserConfig,
    properties: HashMap<String, String>,
    format: SourceFormat,

    columns: Vec<SourceColumnDesc>,

    stream: BoxStream<'static, Result<Vec<FsSourceMessage>>>,
}

impl FsConnectorSourceReader {
    #[try_stream(boxed, ok = FsStreamChunkWithState, error = RwError)]
    pub async fn into_stream(self) {
        let mut parser = ByteStreamSourceParserImpl::create(
            &self.format,
            &self.properties,
            self.parser_config.clone(),
        )
        .await?;

        #[for_await]
        for batch in self.stream {
            let batch = batch?;
            let mut builder =
                SourceStreamChunkBuilder::with_capacity(self.columns.clone(), batch.len() * 2);
            let mut split_offset_mapping: HashMap<SplitId, usize> = HashMap::new();

            for msg in batch {
                if let Some(content) = msg.payload {
                    let mut offset = msg.offset;

                    let mut buff = content.as_ref();
                    let mut prev_buff_len = buff.len();
                    loop {
                        match parser.parse(&mut buff, builder.row_writer()).await {
                            Err(e) => {
                                tracing::warn!(
                                    "message parsing failed {}, skipping",
                                    e.to_string()
                                );
                                continue;
                            }
                            Ok(None) => {
                                break;
                            }
                            Ok(Some(_)) => {
                                offset += prev_buff_len - buff.len();
                                prev_buff_len = buff.len();
                            }
                        }
                    }

                    // If a split is finished reading, recreate a parser
                    if content.len() + msg.offset >= msg.split_size {
                        // the last record in a file may be missing the terminator,
                        // so we need to pass an empty payload to inform the parser.
                        if let Err(e) = parser.parse(&mut buff, builder.row_writer()).await {
                            tracing::warn!("message parsing failed {}, skipping", e.to_string());
                        }

                        parser = ByteStreamSourceParserImpl::create(
                            &self.format,
                            &self.properties,
                            self.parser_config.clone(),
                        )
                        .await?;
                        // Maybe there are still some bytes left that can't be parsed into a
                        // complete record, just discard
                        offset = msg.split_size;
                    }
                    split_offset_mapping.insert(msg.split_id, offset);
                }
            }
            yield FsStreamChunkWithState {
                chunk: builder.finish(),
                split_offset_mapping: Some(split_offset_mapping),
            };
        }
    }
}

#[derive(Clone, Debug)]
pub struct FsConnectorSource {
    pub config: ConnectorProperties,
    pub columns: Vec<SourceColumnDesc>,
    pub properties: HashMap<String, String>,
    pub format: SourceFormat,
    pub parser_config: ParserConfig,
}

impl FsConnectorSource {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        format: SourceFormat,
        properties: HashMap<String, String>,
        columns: Vec<SourceColumnDesc>,
        connector_node_addr: Option<String>,
        parser_config: ParserConfig,
    ) -> Result<Self> {
        // Store the connector node address to properties for later use.
        let mut source_props: HashMap<String, String> =
            HashMap::from_iter(properties.clone().into_iter());
        connector_node_addr
            .map(|addr| source_props.insert("connector_node_addr".to_string(), addr));
        let config =
            ConnectorProperties::extract(source_props).map_err(|e| ConnectorError(e.into()))?;

        Ok(Self {
            config,
            columns,
            properties,
            format,
            parser_config,
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
        splits: Vec<FsSplit>,
        column_ids: Vec<ColumnId>,
        _metrics: Arc<SourceMetrics>,
        _context: SourceContext,
    ) -> Result<FsConnectorSourceReader> {
        let config = self.config.clone();
        let columns = self.get_target_columns(column_ids)?;

        let stream = FsSplitReaderImpl::create(config, splits, None)
            .await?
            .into_stream()
            .map_err(RwError::from)
            .boxed();

        Ok(FsConnectorSourceReader {
            columns,
            stream,
            format: self.format.clone(),
            properties: self.properties.clone(),
            parser_config: self.parser_config.clone(),
        })
    }
}
