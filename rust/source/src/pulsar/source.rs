use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use log::info;
use pulsar::reader::Reader;
use pulsar::TokioExecutor;
use risingwave_common::array::{DataChunk, InternalError, Op, StreamChunk};
use risingwave_common::error::ErrorCode::ProtocolError;
use risingwave_common::error::{Result, RwError};
use risingwave_common::util::chunk_coalesce::DEFAULT_CHUNK_BUFFER_SIZE;

use crate::common::SourceChunkBuilder;
use crate::pulsar::topic::parse_topic;
use crate::pulsar::util::build_pulsar_reader;
use crate::{
    BatchSourceReader, Source, SourceColumnDesc, SourceParser, SourceWriter, StreamSourceReader,
};

/// `PulsarSource` is an abstraction of pulsar, it implements low level Pulsar subscription by
/// wrapping Pulsar Reader, note that the topic here needs to specify the partition
pub struct PulsarSource {
    address: String,
    topic: String,
    parser: Arc<dyn SourceParser>,
    columns: Arc<Vec<SourceColumnDesc>>,
}

impl PulsarSource {
    pub fn new(
        address: String,
        topic: String,
        // TODO (peng): config support
        _config: HashMap<String, String>,
        parser: Arc<dyn SourceParser>,
        columns: Arc<Vec<SourceColumnDesc>>,
    ) -> Result<Self> {
        let parsed_topic = parse_topic(topic)?;

        if parsed_topic.partition_index.is_none() {
            return Err(RwError::from(ProtocolError(
                "pulsar topic partition index is not specified".into(),
            )));
        }

        Ok(PulsarSource {
            address,
            topic: parsed_topic.to_string(),
            parser,
            columns,
        })
    }

    fn get_target_columns(&self, column_ids: Vec<i32>) -> Result<Vec<SourceColumnDesc>> {
        column_ids
            .iter()
            .map(|id| {
                self.columns
                    .iter()
                    .find(|c| c.column_id == *id)
                    .ok_or_else(|| {
                        RwError::from(InternalError(format!("Failed to find column id: {}", id)))
                    })
                    .map(|col| col.clone())
            })
            .collect::<Result<Vec<SourceColumnDesc>>>()
    }
}

/// `PulsarSourceReaderContext` is the context for Pulsar source reader
/// it contains the bound for the batch reader.
#[derive(Default)]
pub struct PulsarReaderContext {
    bound: Option<PulsarReaderBound>,
}

/// `PulsarReaderBound` is the bound for the batch reader
/// It can use `entry_id` and timestamp to limit consumption
pub enum PulsarReaderBound {
    EntryID(u64),
    Timestamp(u64),
}

pub struct PulsarReader {
    address: String,
    topic: String,

    parser: Arc<dyn SourceParser>,
    columns: Arc<Vec<SourceColumnDesc>>,

    reader: Option<Reader<Vec<u8>, TokioExecutor>>,

    bound: Option<PulsarReaderBound>,

    done: bool,
}

pub struct PulsarWriter {}

#[async_trait]
impl SourceWriter for PulsarWriter {
    async fn write(&mut self, _chunk: StreamChunk) -> Result<()> {
        todo!()
    }

    async fn flush(&mut self) -> Result<()> {
        todo!()
    }
}

impl SourceChunkBuilder for PulsarReader {}

#[async_trait]
impl StreamSourceReader for PulsarReader {
    async fn open(&mut self) -> Result<()> {
        self.reader = Some(build_pulsar_reader(self.address.as_str(), self.topic.as_str()).await?);
        Ok(())
    }

    async fn next(&mut self) -> Result<StreamChunk> {
        assert!(self.reader.is_some());

        let mut chunks = self
            .reader
            .as_mut()
            .unwrap()
            .ready_chunks(DEFAULT_CHUNK_BUFFER_SIZE);

        let next = chunks.next().await;
        match next {
            None => Ok(StreamChunk::default()),
            Some(batch) => {
                let mut rows = Vec::with_capacity(batch.len());

                for msg in batch {
                    let msg = msg.map_err(|e| RwError::from(InternalError(e.to_string())))?;

                    if !msg.payload.data.is_empty() {
                        rows.push(
                            self.parser
                                .parse(msg.payload.data.as_ref(), &self.columns)?,
                        );
                    }
                }

                let columns = Self::build_columns(&self.columns, &rows)?;

                Ok(StreamChunk::new(
                    vec![Op::Insert; rows.len()],
                    columns,
                    None,
                ))
            }
        }
    }
}

#[async_trait]
impl BatchSourceReader for PulsarReader {
    async fn open(&mut self) -> Result<()> {
        self.reader = Some(build_pulsar_reader(self.address.as_str(), self.topic.as_str()).await?);
        Ok(())
    }

    async fn next(&mut self) -> Result<Option<DataChunk>> {
        assert!(self.reader.is_some());
        assert!(self.bound.is_some());

        if self.done {
            return Ok(None);
        }

        let mut chunks = self
            .reader
            .as_mut()
            .unwrap()
            .ready_chunks(DEFAULT_CHUNK_BUFFER_SIZE);

        let batch = match chunks.next().await {
            None => return Ok(None),
            Some(batch) => batch,
        };

        let mut rows = Vec::with_capacity(batch.len());
        for msg in batch {
            let msg = msg.map_err(|e| RwError::from(InternalError(e.to_string())))?;

            let bound = self.bound.as_ref().unwrap();
            match bound {
                PulsarReaderBound::EntryID(id) => {
                    let entry_id = msg.message_id.id.entry_id;
                    if &entry_id > id {
                        self.done = true;
                        break;
                    }
                }
                PulsarReaderBound::Timestamp(ts) => {
                    let publish_time = msg.payload.metadata.publish_time;
                    if &publish_time > ts {
                        self.done = true;
                        break;
                    }
                }
            }

            if !msg.payload.data.is_empty() {
                rows.push(
                    self.parser
                        .parse(msg.payload.data.as_ref(), &self.columns)?,
                );
            }
        }

        if rows.is_empty() {
            self.done = true;
            return Ok(None);
        }

        let columns = Self::build_columns(&self.columns, &rows)?;

        return Ok(Some(DataChunk::builder().columns(columns).build()));
    }

    async fn close(&mut self) -> Result<()> {
        info!("closing pulsar batch reader");
        Ok(())
    }
}

#[async_trait]
impl Source for PulsarSource {
    type ReaderContext = PulsarReaderContext;
    type BatchReader = PulsarReader;
    type StreamReader = PulsarReader;
    type Writer = PulsarWriter;

    fn batch_reader(
        &self,
        context: PulsarReaderContext,
        column_ids: Vec<i32>,
    ) -> Result<Self::BatchReader> {
        Ok(PulsarReader {
            address: self.address.clone(),
            topic: self.topic.clone(),
            parser: self.parser.clone(),
            columns: Arc::from(self.get_target_columns(column_ids)?),
            reader: None,
            bound: context.bound,
            done: false,
        })
    }

    fn stream_reader(
        &self,
        _context: PulsarReaderContext,
        column_ids: Vec<i32>,
    ) -> Result<Self::StreamReader> {
        Ok(PulsarReader {
            address: self.address.clone(),
            topic: self.topic.clone(),
            parser: self.parser.clone(),
            columns: Arc::from(self.get_target_columns(column_ids)?),
            reader: None,
            bound: None,
            done: false,
        })
    }

    fn create_writer(&self) -> Result<Self::Writer> {
        todo!()
    }
}
