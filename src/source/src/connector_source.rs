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
use std::sync::{Arc, LazyLock};

use futures::future::try_join_all;
use futures::StreamExt;
use futures_async_stream::try_stream;
use futures_concurrency::prelude::*;
use futures_util::stream::BoxStream;
use itertools::Itertools;
use risingwave_common::catalog::{ColumnId, TableId};
use risingwave_common::error::{internal_error, Result, RwError, ToRwResult};
use risingwave_connector::source::{
    Column, ConnectorProperties, ConnectorState, SourceMessage, SplitId, SplitMetaData,
    SplitReaderImpl,
};

use crate::common::SourceChunkBuilder;
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

static DEFAULT_SPLIT_ID: LazyLock<SplitId> = LazyLock::new(|| "None".into());

struct InnerConnectorSourceReader {
    reader: SplitReaderImpl,
    // split should be None or only contains one value
    split: ConnectorState,

    metrics: Arc<SourceMetrics>,
    context: SourceContext,
}

/// [`ConnectorSource`] serves as a bridge between external components and streaming or
/// batch processing. [`ConnectorSource`] introduces schema at this level while
/// [`SplitReaderImpl`] simply loadsF raw content from message queue or file system.
/// Parallel means that multiple [`InnerConnectorSourceReader`] will run in parallel during the
/// `next`, so that 0 or more Splits reads can be handled at the Source level.
#[allow(dead_code)]
pub struct ConnectorSourceReader {
    pub config: ConnectorProperties,
    pub parser: Arc<SourceParserImpl>,
    pub columns: Vec<SourceColumnDesc>,

    // merge all streams of inner reader into one
    all_reader_stream: BoxStream<'static, Result<Vec<SourceMessage>>>,
    metrics: Arc<SourceMetrics>,
    context: SourceContext,
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

    async fn next(&mut self) -> Result<Option<Vec<SourceMessage>>> {
        loop {
            let chunk = self.reader.next().await;

            match chunk.map_err(|e| internal_error(e.to_string())) {
                Err(e) => {
                    return Err(e);
                }
                Ok(None) => {
                    return Ok(None);
                }
                Ok(Some(msg)) => {
                    if msg.is_empty() {
                        continue;
                    }
                    // Avoid occupying too much CPU time if the source is a data generator, like
                    // DataGen or Nexmark.
                    tokio::task::consume_budget().await;

                    return Ok(Some(msg));
                }
            }
        }
    }
}

#[try_stream(ok = Vec<SourceMessage>, error = RwError)]
async fn inner_connector_source_reader_into_stream(mut reader: InnerConnectorSourceReader) {
    let actor_id = reader.context.actor_id.to_string();
    let source_id = reader.context.source_id.to_string();
    let id = match &reader.split {
        Some(splits) => splits[0].id(),
        None => DEFAULT_SPLIT_ID.clone(),
    };
    loop {
        match reader.next().await {
            Ok(None) => {
                tracing::warn!("connector reader {} stream stopped", id);
                break;
            }
            Ok(Some(msg)) => {
                reader
                    .metrics
                    .partition_input_count
                    .with_label_values(&[actor_id.as_str(), source_id.as_str(), &*id])
                    .inc_by(msg.len() as u64);
                yield msg
            }
            Err(e) => {
                tracing::error!("connector reader {} error happened {}", id, e.to_string());
                return Err(e);
            }
        }
    }
}

impl SourceChunkBuilder for ConnectorSourceReader {}

impl ConnectorSourceReader {
    pub async fn next(&mut self) -> Result<StreamChunkWithState> {
        let batch = self.all_reader_stream.next().await.unwrap()?;

        let mut split_offset_mapping: HashMap<SplitId, String> = HashMap::new();

        let mut builder =
            SourceStreamChunkBuilder::with_capacity(self.columns.clone(), batch.len());

        for msg in batch {
            if let Some(content) = msg.payload {
                split_offset_mapping.insert(msg.split_id, msg.offset);
                let writer = builder.row_writer();
                match self.parser.parse(content.as_ref(), writer) {
                    Err(e) => {
                        tracing::warn!("message parsing failed {}, skipping", e.to_string());
                        continue;
                    }
                    Ok(_guard) => {}
                }
            }
        }

        let chunk = builder.finish();

        Ok(StreamChunkWithState {
            chunk,
            split_offset_mapping: Some(split_offset_mapping),
        })
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

        let streams = readers
            .into_iter()
            .map(inner_connector_source_reader_into_stream)
            .collect::<Vec<_>>()
            .merge()
            .into_stream()
            .boxed();

        Ok(ConnectorSourceReader {
            config: self.config.clone(),
            parser: self.parser.clone(),
            columns,
            all_reader_stream: streams,
            metrics: metrics.clone(),
            context: context.clone(),
        })
    }
}
