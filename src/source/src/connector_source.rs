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
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use risingwave_common::array::StreamChunk;

use risingwave_common::error::ErrorCode::ProtocolError;
use risingwave_connector::{state};
use risingwave_storage::StateStore;
use tokio::sync::Mutex;

use risingwave_common::catalog::ColumnId;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{Result, RwError, ToRwResult};
use risingwave_connector::{ConnectorStateV2, Properties, SourceReaderImpl, SplitImpl};


use crate::common::SourceChunkBuilder;
use crate::{Source, SourceColumnDesc, SourceParserImpl, StreamSourceReader};

/// [`ConnectorSource`] serves as a bridge between external components and streaming or batch
/// processing. [`ConnectorSource`] introduces schema at this level while [`SplitReaderImpl`]
/// simply loads raw content from message queue or file system.
#[derive(Clone)]
pub struct ConnectorSource {
    pub config: HashMap<String, String>,
    pub column_descs: Vec<SourceColumnDesc>,
    pub parser: Arc<SourceParserImpl>,
}

impl Debug for ConnectorSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectorSource").finish()
    }
}

impl ConnectorSource {
    fn get_target_columns(&self, column_ids: Vec<ColumnId>) -> Result<Vec<SourceColumnDesc>> {
        column_ids
            .iter()
            .map(|id| {
                self.column_descs
                    .iter()
                    .find(|c| c.column_id == *id)
                    .ok_or_else(|| {
                        RwError::from(InternalError(format!(
                            "Failed to find column id: {} in source: {:?}",
                            id, self
                        )))
                    })
                    .map(|col| col.clone())
            })
            .collect::<Result<Vec<SourceColumnDesc>>>()
    }
}

pub struct ConnectorReaderContext {
    pub(crate) splits: Vec<SplitImpl>,
}

#[async_trait]
impl Source for ConnectorSource {
    type ReaderContext = ConnectorReaderContext;
    type StreamReader = ConnectorStreamSource;

    async fn stream_reader(
        &self,
        context: ConnectorReaderContext,
        column_ids: Vec<ColumnId>,
    ) -> Result<Self::StreamReader> {
        log::debug!(
            "Creating new connector source with config {:?}, splits {:?}",
            self.config,
            context.splits
        );

        let reader = SourceReaderImpl::create(
            Properties::new(self.config.clone()),
            ConnectorStateV2::Splits(context.splits),
        )
        .await
        .to_rw_result()?;

        let columns = self.get_target_columns(column_ids)?;

        Ok(ConnectorStreamSource {
            reader,
            parser: self.parser.clone(),
            column_descs: columns,
        })
    }
}

pub struct ConnectorStreamSource {
    pub reader: SourceReaderImpl,
    pub parser: Arc<SourceParserImpl>,
    pub column_descs: Vec<SourceColumnDesc>,
}

impl SourceChunkBuilder for ConnectorStreamSource {}

#[async_trait]
impl StreamSourceReader for ConnectorStreamSource {
    async fn open(&mut self) -> Result<()> {
        Ok(())
    }

    async fn next(&mut self) -> Result<StreamChunk> {
        match self.reader.next().await.to_rw_result()? {
            None => Ok(StreamChunk::default()),
            Some(batch) => {
                let mut events = Vec::with_capacity(batch.len());

                for msg in batch {
                    if let Some(content) = msg.payload {
                        events.push(self.parser.parse(content.as_ref(), &self.column_descs)?);
                    }
                }
                let mut ops = Vec::with_capacity(events.iter().map(|e| e.ops.len()).sum());
                let mut rows = Vec::with_capacity(events.iter().map(|e| e.rows.len()).sum());

                for event in events {
                    rows.extend(event.rows);
                    ops.extend(event.ops);
                }
                Ok(StreamChunk::new(
                    ops,
                    Self::build_columns(&self.column_descs, rows.as_ref())?,
                    None,
                ))
            }
        }
    }
}
