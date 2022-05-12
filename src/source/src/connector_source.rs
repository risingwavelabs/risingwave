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
use risingwave_common::catalog::ColumnId;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{Result, RwError, ToRwResult};
use risingwave_connector::{ConnectorProperties, ConnectorStateV2, SplitReaderImpl};

use crate::common::SourceChunkBuilder;
use crate::{SourceColumnDesc, SourceParserImpl, StreamChunkWithState, StreamSourceReader};

/// [`ConnectorSource`] serves as a bridge between external components and streaming or batch
/// processing. [`ConnectorSource`] introduces schema at this level while [`SplitReaderImpl`]
/// simply loads raw content from message queue or file system.
#[derive(Clone)]
pub struct ConnectorSource {
    pub config: ConnectorProperties,
    pub columns: Vec<SourceColumnDesc>,
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
                self.columns
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

    /// Create a new stream reader.
    pub async fn stream_reader(
        &self,
        splits: ConnectorStateV2,
        column_ids: Vec<ColumnId>,
    ) -> Result<ConnectorStreamReader> {
        log::debug!(
            "Creating new connector source with config {:?}, splits {:?}",
            self.config,
            splits
        );

        let reader = SplitReaderImpl::create(self.config.clone(), splits)
            .await
            .to_rw_result()?;

        let columns = self.get_target_columns(column_ids)?;

        Ok(ConnectorStreamReader {
            reader,
            parser: self.parser.clone(),
            columns,
        })
    }
}

pub struct ConnectorStreamReader {
    pub reader: SplitReaderImpl,
    pub parser: Arc<SourceParserImpl>,
    pub columns: Vec<SourceColumnDesc>,
}

impl SourceChunkBuilder for ConnectorStreamReader {}

#[async_trait]
impl StreamSourceReader for ConnectorStreamReader {
    async fn next(&mut self) -> Result<StreamChunkWithState> {
        match self.reader.next().await.to_rw_result()? {
            None => Ok(StreamChunkWithState {
                chunk: StreamChunk::default(),
                split_offset_mapping: None,
            }),
            Some(batch) => {
                let mut events = Vec::with_capacity(batch.len());
                let mut split_offset_mapping: HashMap<String, String> = HashMap::new();

                for msg in batch {
                    if let Some(content) = msg.payload {
                        *split_offset_mapping
                            .entry(msg.split_id.clone())
                            .or_insert_with(|| "".to_string()) = msg.offset.to_string();
                        events.push(self.parser.parse(content.as_ref(), &self.columns)?);
                    }
                }
                let mut ops = Vec::with_capacity(events.iter().map(|e| e.ops.len()).sum());
                let mut rows = Vec::with_capacity(events.iter().map(|e| e.rows.len()).sum());

                for event in events {
                    rows.extend(event.rows);
                    ops.extend(event.ops);
                }
                Ok(StreamChunkWithState {
                    chunk: StreamChunk::new(
                        ops,
                        Self::build_columns(&self.columns, rows.as_ref())?,
                        None,
                    ),
                    split_offset_mapping: Some(split_offset_mapping),
                })
            }
        }
    }
}
