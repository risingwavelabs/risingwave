use std::sync::Arc;

use crate::array::column::Column;
use crate::array::{ArrayBuilderImpl, DataChunk};
use crate::error::Result;
use crate::source::parser::SourceParser;
use crate::source::{SourceColumnDesc, SourceMessage, SourceReader};

/// `ChunkReader` is the intermediate layer between Executor and Source,
/// responsible for reading data one by one from `SourceReader` by calling next() and generating `DataChunk`
pub struct ChunkReader {
    reader: Box<dyn SourceReader>,
    parser: Box<dyn SourceParser>,
    columns: Vec<SourceColumnDesc>,
    done: bool,
}

impl ChunkReader {
    pub fn new(
        columns: &[SourceColumnDesc],
        reader: Box<dyn SourceReader>,
        parser: Box<dyn SourceParser>,
    ) -> ChunkReader {
        ChunkReader {
            reader,
            parser,
            columns: columns.to_owned(),
            done: false,
        }
    }

    pub async fn next_chunk(&mut self, max_chunk_size: usize) -> Result<Option<DataChunk>> {
        if self.done {
            return Ok(None);
        }

        let mut builders = self
            .columns
            .iter()
            .map(|k| k.data_type.clone().create_array_builder(max_chunk_size))
            .collect::<Result<Vec<ArrayBuilderImpl>>>()?;

        for i in 0..max_chunk_size {
            let next_message = self.reader.next().await?;

            match next_message {
                Some(source_message) => {
                    if let Some(datum_vec) = match source_message {
                        SourceMessage::Kafka(kafka_message) => kafka_message
                            .payload
                            .map(|payload| self.parser.parse(&payload, &self.columns))
                            .transpose()?,
                        SourceMessage::File(file_message) => {
                            Some(self.parser.parse(&file_message.data, &self.columns)?)
                        }
                    } {
                        datum_vec
                            .into_iter()
                            .zip(&mut builders)
                            .try_for_each(|(datum, builder)| builder.append_datum(&datum))?
                    }
                }
                None => {
                    // There is no message any more, this is the case for olap only.
                    // Streaming should be infinite
                    self.done = true;
                    if i == 0 {
                        return Ok(None);
                    }
                    break;
                }
            };
        }

        let columns = builders
            .into_iter()
            .zip(self.columns.iter().map(|c| c.data_type.clone()))
            .map(|(builder, data_type)| {
                builder
                    .finish()
                    .map(|arr| Column::new(Arc::new(arr), data_type.clone()))
            })
            .collect::<Result<Vec<Column>>>()?;

        let ret = DataChunk::builder().columns(columns).build();
        Ok(Some(ret))
    }

    pub async fn cancel(&mut self) -> Result<()> {
        self.reader.cancel().await
    }
}

impl std::fmt::Debug for ChunkReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChunkReader").finish()
    }
}
