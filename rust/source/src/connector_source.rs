use std::fmt::Debug;
use std::marker::Send;
use std::sync::Arc;

use async_trait::async_trait;
use lazy_static::__Deref;
use risingwave_common::array::StreamChunk;
use risingwave_common::error::ErrorCode::{InternalError, ProtocolError};
use risingwave_common::error::{Result, RwError};
use risingwave_connector::base::SourceReader;
use risingwave_connector::state;
use risingwave_storage::memory::MemoryStateStore;
use tokio::sync::Mutex;

use crate::common::SourceChunkBuilder;
use crate::{SourceColumnDesc, SourceParser, StreamSourceReader};

#[derive(Clone)]
pub struct ConnectorSource {
    pub parser: Arc<dyn SourceParser + Send + Sync>,
    pub reader: Arc<Mutex<dyn SourceReader + Send + Sync>>,
    pub column_descs: Vec<SourceColumnDesc>,
}

impl SourceChunkBuilder for ConnectorSource {}

impl Debug for ConnectorSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectorSource").finish()
    }
}

impl ConnectorSource {
    pub fn new(
        parser: Arc<dyn SourceParser + Send + Sync>,
        reader: Arc<Mutex<dyn SourceReader + Send + Sync>>,
        column_descs: Vec<SourceColumnDesc>,
    ) -> Self {
        Self {
            parser,
            reader,
            column_descs,
        }
    }

    pub async fn assign_split<'a>(&mut self, split: &'a [u8]) -> Result<()> {
        self.reader
            .lock()
            .await
            .assign_split(split)
            .await
            .map_err(|e: anyhow::Error| RwError::from(InternalError(e.to_string())))
    }

    pub async fn next(&mut self) -> Result<StreamChunk> {
        let payload = self
            .reader
            .lock()
            .await
            .next()
            .await
            .map_err(|e| RwError::from(ProtocolError(e.to_string())))?;

        match payload {
            None => Ok(StreamChunk::default()),
            Some(batch) => {
                let mut events = Vec::with_capacity(batch.len());
                for msg in batch {
                    if let Some(content) = msg.payload {
                        events.push(self.parser.parse(content.deref(), &self.column_descs)?);
                    }
                }

                let mut ops = vec![];
                let mut rows = vec![];

                for mut event in events {
                    rows.append(&mut event.rows);
                    ops.append(&mut event.ops);
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

#[derive(Debug)]
pub struct ConnectorStreamSource {
    pub source_reader: ConnectorSource,
    pub state_store: state::SourceStateHandler<MemoryStateStore>,
}

#[async_trait]
impl StreamSourceReader for ConnectorStreamSource {
    async fn open(&mut self) -> Result<()> {
        Ok(())
    }

    async fn next(&mut self) -> Result<StreamChunk> {
        self.source_reader.next().await
    }
}
