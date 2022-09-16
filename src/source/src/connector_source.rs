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

use std::borrow::BorrowMut;
use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use futures::future::try_join_all;
use itertools::Itertools;
use risingwave_common::catalog::{ColumnId, TableId};
use risingwave_common::error::{internal_error, Result, ToRwResult};
use risingwave_connector::source::{
    Column, ConnectorProperties, ConnectorState, SourceMessage, SplitId, SplitMetaData,
    SplitReaderImpl,
};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

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

struct InnerConnectorSourceReaderHandle {
    stop_tx: oneshot::Sender<()>,
    join_handle: JoinHandle<()>,
}

const CONNECTOR_MESSAGE_BUFFER_SIZE: usize = 512;

/// [`ConnectorSource`] serves as a bridge between external components and streaming or
/// batch processing. [`ConnectorSource`] introduces schema at this level while
/// [`SplitReaderImpl`] simply loads raw content from message queue or file system.
/// Parallel means that multiple [`InnerConnectorSourceReader`] will run in parallel during the
/// `next`, so that 0 or more Splits reads can be handled at the Source level.
pub struct ConnectorSourceReader {
    pub config: ConnectorProperties,
    pub parser: Arc<SourceParserImpl>,
    pub columns: Vec<SourceColumnDesc>,

    handles: Option<HashMap<SplitId, InnerConnectorSourceReaderHandle>>,
    message_rx: Receiver<Result<Vec<SourceMessage>>>,
    // We need to keep this tx, otherwise the channel will return none with 0 inner readers, and we
    // need to clone this tx when adding new inner readers in the future.
    message_tx: Sender<Result<Vec<SourceMessage>>>,

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

    async fn run(
        &mut self,
        mut stop: oneshot::Receiver<()>,
        output: mpsc::Sender<Result<Vec<SourceMessage>>>,
    ) {
        let actor_id = self.context.actor_id.to_string();
        let source_id = self.context.source_id.to_string();
        let id = match &self.split {
            Some(splits) => splits[0].id(),
            None => DEFAULT_SPLIT_ID.clone(),
        };

        loop {
            let chunk: anyhow::Result<Option<Vec<SourceMessage>>>;
            tokio::select! {
                biased;
                // stop chan has high priority
                _ = stop.borrow_mut() => {
                    tracing::debug!("connector reader {} stop signal received", id);
                    break;
                }

                c = self.reader.next() => {
                    chunk = c;
                }
            }

            match chunk.map_err(|e| internal_error(e.to_string())) {
                Err(e) => {
                    tracing::error!("connector reader {} error happened {}", id, e.to_string());
                    output.send(Err(e)).await.ok();
                    break;
                }
                Ok(None) => {
                    tracing::warn!("connector reader {} stream stopped", id);
                    break;
                }
                Ok(Some(msg)) => {
                    if msg.is_empty() {
                        continue;
                    }
                    // Avoid occupying too much CPU time if the source is a data generator, like
                    // DataGen or Nexmark.
                    tokio::task::consume_budget().await;

                    self.metrics
                        .partition_input_count
                        .with_label_values(&[actor_id.as_str(), source_id.as_str(), &*id])
                        .inc_by(msg.len() as u64);

                    output.send(Ok(msg)).await.ok();
                }
            }
        }
    }
}

impl SourceChunkBuilder for ConnectorSourceReader {}

impl ConnectorSourceReader {
    pub async fn next(&mut self) -> Result<StreamChunkWithState> {
        let batch = self.message_rx.recv().await.unwrap()?;

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

        let chunk = builder.finish()?;

        Ok(StreamChunkWithState {
            chunk,
            split_offset_mapping: Some(split_offset_mapping),
        })
    }
}

impl Drop for ConnectorSourceReader {
    fn drop(&mut self) {
        let handles = self.handles.take().unwrap();

        for (_, handle) in handles {
            handle.join_handle.abort();
        }
    }
}

impl ConnectorSourceReader {
    pub async fn add_split(&mut self, split: ConnectorState) -> Result<()> {
        if let Some(append_splits) = split {
            for split in append_splits {
                let split_id = split.id();

                let mut reader = InnerConnectorSourceReader::new(
                    self.config.clone(),
                    Some(vec![split]),
                    self.columns.clone(),
                    self.metrics.clone(),
                    self.context.clone(),
                )
                .await?;
                let (stop_tx, stop_rx) = oneshot::channel();
                let sender = self.message_tx.clone();
                let join_handle = tokio::spawn(async move { reader.run(stop_rx, sender).await });

                if let Some(handles) = self.handles.as_mut() {
                    handles.insert(
                        split_id,
                        InnerConnectorSourceReaderHandle {
                            stop_tx,
                            join_handle,
                        },
                    );
                }
            }
        }

        Ok(())
    }

    pub async fn drop_split(&mut self, split_id: SplitId) -> Result<()> {
        let handle = self
            .handles
            .as_mut()
            .and_then(|handles| handles.remove(&split_id))
            .ok_or_else(|| internal_error(format!("could not find split {}", split_id)))
            .unwrap();
        handle.stop_tx.send(()).unwrap();
        handle
            .join_handle
            .await
            .map_err(|e| internal_error(e.to_string()))
    }
}

#[derive(Clone, Debug)]
pub struct ConnectorSource {
    pub config: ConnectorProperties,
    pub columns: Vec<SourceColumnDesc>,
    pub parser: Arc<SourceParserImpl>,
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
        let (tx, rx) = mpsc::channel(CONNECTOR_MESSAGE_BUFFER_SIZE);
        let mut handles = HashMap::with_capacity(if let Some(split) = &splits {
            split.len()
        } else {
            1
        });
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

        for mut reader in readers {
            let split_id = match &reader.split {
                Some(s) => s[0].id(),
                None => DEFAULT_SPLIT_ID.clone(),
            };
            let (stop_tx, stop_rx) = oneshot::channel();
            let sender = tx.clone();
            let join_handle = tokio::spawn(async move { reader.run(stop_rx, sender).await });

            handles.insert(
                split_id,
                InnerConnectorSourceReaderHandle {
                    stop_tx,
                    join_handle,
                },
            );
        }

        Ok(ConnectorSourceReader {
            config: self.config.clone(),
            handles: Some(handles),
            message_rx: rx,
            parser: self.parser.clone(),
            columns,
            message_tx: tx,
            metrics: metrics.clone(),
            context: context.clone(),
        })
    }
}
