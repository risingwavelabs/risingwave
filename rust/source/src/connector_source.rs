use std::fmt::Debug;

use lazy_static::__Deref;
use risingwave_common::array::StreamChunk;
use risingwave_common::error::ErrorCode::{InternalError, ProtocolError};
use risingwave_common::error::{Result, RwError};
use risingwave_connector::base::SourceReader;

use crate::{build_columns, SourceColumnDesc, SourceParser};

pub struct ConnectorSource {
    pub parser: Box<dyn SourceParser>,
    pub reader: Box<dyn SourceReader>,
    pub column_descs: Box<Vec<SourceColumnDesc>>,
}

impl Debug for ConnectorSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectorSource").finish()
    }
}

impl ConnectorSource {
    pub fn new(
        parser: Box<dyn SourceParser>,
        reader: Box<dyn SourceReader>,
        column_descs: Box<Vec<SourceColumnDesc>>,
    ) -> Self {
        Self {
            parser,
            reader,
            column_descs,
        }
    }

    pub fn assign_split<'a>(&mut self, split: &'a [u8]) -> Result<()> {
        let mut runtime = tokio::runtime::Runtime::new().expect("Unable to create a runtime");
        runtime
            .block_on(self.reader.assign_split(split))
            .map_err(|e: anyhow::Error| RwError::from(InternalError(e.to_string())))
    }

    pub fn next(&mut self) -> Result<StreamChunk> {
        let mut runtime = tokio::runtime::Runtime::new().expect("Unable to create a runtime");
        let payload = runtime
            .block_on(self.reader.next())
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
                    build_columns(&self.column_descs, rows.as_ref())?,
                    None,
                ))
            }
        }
    }
}
