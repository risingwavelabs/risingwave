use risingwave_common::array::StreamChunk;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{Result, RwError};
use risingwave_connector::base::SourceReader;

use crate::{SourceChunkBuilder, SourceColumnDesc, SourceParser};

pub struct ConnectorSource {
    pub parser: Box<dyn SourceParser>,
    pub reader: Box<dyn SourceReader>,
    pub column_descs: Box<Vec<SourceColumnDesc>>,
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
        self.reader
            .assign_split(split)
            .map_err(|e: anyhow::Error| RwError::from(InternalError(e.to_string())))
    }

    pub fn next(&mut self) -> Result<StreamChunk> {
        let mut runtime = tokio::runtime::Runtime::new().expect("Unable to create a runtime");
        let payload = runtime.block_on(self.reader.next())?;

        match payload {
            None => Ok(StreamChunk::default()),
            Some(batch) => {
                let mut events = Vec::with_capacity(batch.len());
                for msg in batch {
                    events.push(self.parser.parse(&msg, &self.columns)?);
                }

                let mut ops = vec![];
                let mut rows = vec![];

                for mut event in events {
                    rows.append(&mut event.rows);
                    ops.append(&mut event.ops);
                }
                Ok(StreamChunk::new(
                    ops,
                    SourceChunkBuilder::build_columns(&self.columns, rows.as_ref())?,
                    None,
                ))
            }
        }
    }
}
