use std::fmt::Debug;

use async_trait::async_trait;

pub use high_level_kafka::*;
pub use manager::*;
pub use parser::*;
use risingwave_common::array::{DataChunk, StreamChunk};
use risingwave_common::error::ErrorCode::ProtocolError;
use risingwave_common::error::Result;
use risingwave_common::error::RwError;
pub use table::*;

pub mod parser;

mod high_level_kafka;
mod manager;

mod table;

#[derive(Clone, Debug)]
pub enum SourceConfig {
    Kafka(HighLevelKafkaSourceConfig),
}

#[derive(Clone, Debug)]
pub enum SourceFormat {
    Invalid,
    Json,
    Protobuf,
    Avro,
}

#[derive(Debug)]
pub enum SourceImpl {
    HighLevelKafka(HighLevelKafkaSource),
    Table(TableSource),
}

pub enum SourceReaderContext {
    HighLevelKafka(HighLevelKafkaSourceReaderContext),
    Table(TableReaderContext),
}

impl SourceImpl {
    /// Create a stream reader
    pub fn stream_reader(
        &self,
        ctx: SourceReaderContext,
        column_ids: Vec<i32>,
    ) -> Result<Box<dyn StreamSourceReader>> {
        match (self, ctx) {
            (SourceImpl::HighLevelKafka(source), SourceReaderContext::HighLevelKafka(ctx)) => {
                Ok(Box::new(source.stream_reader(ctx, column_ids)?))
            }
            (SourceImpl::Table(source), SourceReaderContext::Table(ctx)) => {
                Ok(Box::new(source.stream_reader(ctx, column_ids)?))
            }
            _ => Err(RwError::from(ProtocolError(
                "context type illegal".to_string(),
            ))),
        }
    }

    /// Create a batch reader
    pub fn batch_reader(&self, ctx: SourceReaderContext) -> Result<Box<dyn BatchSourceReader>> {
        match (self, ctx) {
            (SourceImpl::HighLevelKafka(source), SourceReaderContext::HighLevelKafka(ctx)) => {
                Ok(Box::new(source.batch_reader(ctx)?))
            }
            (SourceImpl::Table(source), SourceReaderContext::Table(ctx)) => {
                Ok(Box::new(source.batch_reader(ctx)?))
            }
            _ => Err(RwError::from(ProtocolError(
                "context type illegal".to_string(),
            ))),
        }
    }
}

#[async_trait]
pub trait Source: Send + Sync + 'static {
    type ReaderContext;
    type BatchReader: BatchSourceReader;
    type StreamReader: StreamSourceReader;
    type Writer: SourceWriter;

    /// Create a batch reader
    fn batch_reader(&self, context: Self::ReaderContext) -> Result<Self::BatchReader>;

    /// Create a stream reader
    fn stream_reader(
        &self,
        context: Self::ReaderContext,
        column_ids: Vec<i32>,
    ) -> Result<Self::StreamReader>;

    /// Create a writer
    fn create_writer(&self) -> Result<Self::Writer>;
}

#[async_trait]
pub trait SourceWriter: Send + Sync + 'static {
    /// `write` a stream chunk into table
    async fn write(&mut self, chunk: StreamChunk) -> Result<()>;

    /// `flush` ensures data flushed and make sure writer can be closed safely
    async fn flush(&mut self) -> Result<()>;
}

#[async_trait]
pub trait BatchSourceReader: Send + Sync + 'static {
    /// `open` is called once to initialize the reader
    async fn open(&mut self) -> Result<()>;

    /// `next` returns a row or `None` immediately if there is no more result
    async fn next(&mut self) -> Result<Option<DataChunk>>;

    /// `close` is called to stop the reader
    async fn close(&mut self) -> Result<()>;
}

#[async_trait]
pub trait StreamSourceReader: Send + Sync + 'static {
    /// `init` is called once to initialize the reader
    async fn open(&mut self) -> Result<()>;

    /// `next` always returns a StreamChunk. If the queue is empty, it will
    /// block until new data coming
    async fn next(&mut self) -> Result<StreamChunk>;
}
