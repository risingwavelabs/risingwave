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

use futures::future::try_join_all;
use futures::stream::{select_all, BoxStream};
use futures::StreamExt;
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::catalog::{ColumnId, TableId};
use risingwave_common::error::{internal_error, Result, RwError, ToRwResult};
use risingwave_connector::source::{
    Column, ConnectorProperties, ConnectorState, SourceMessage, SplitId, SplitMetaData,
    SplitReaderImpl,
};

use crate::monitor::SourceMetrics;
use crate::{SourceColumnDesc, SourceParserImpl, SourceStreamChunkBuilder, StreamChunkWithState};

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
    stream: BoxStream<'static, Result<SourceMessage>>,
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

    #[try_stream(boxed, ok = SourceMessage, error = RwError)]
    async fn into_stream(self) {
        let actor_id = self.context.actor_id.to_string();
        let source_id = self.context.source_id.to_string();
        let id = match &self.split {
            Some(splits) => splits[0].id(),
            None => default_split_id(),
        };
        #[for_await]
        for msg in self.reader.into_stream() {
            self.metrics
                .partition_input_count
                .with_label_values(&[&actor_id, &source_id, &id])
                .inc();
            yield msg?;
        }
    }
}

const MAX_CHUNK_SIZE: usize = 1024;

impl ConnectorSourceReader {
    #[try_stream(boxed, ok = StreamChunkWithState, error = RwError)]
    pub async fn into_stream(self) {
        #[for_await]
        for batch in self.stream.ready_chunks(MAX_CHUNK_SIZE) {
            let mut builder =
                SourceStreamChunkBuilder::with_capacity(self.columns.clone(), batch.len());
            let mut split_offset_mapping: HashMap<SplitId, String> = HashMap::new();

            for msg in batch {
                let msg = msg?;
                if let Some(content) = msg.payload {
                    split_offset_mapping.insert(msg.split_id, msg.offset);
                    if let Err(e) = self.parser.parse(content.as_ref(), builder.row_writer()) {
                        tracing::warn!("message parsing failed {}, skipping", e.to_string());
                        continue;
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
