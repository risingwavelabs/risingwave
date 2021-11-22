use std::fmt::Debug;

use async_trait::async_trait;

pub use highlevel_kafka::*;
pub use manager::*;
pub use parser::*;
use risingwave_common::array::DataChunk;
use risingwave_common::error::Result;

use crate::stream_op::StreamChunk;

pub mod parser;

mod highlevel_kafka;
mod manager;

mod table;

#[derive(Clone, Debug)]
pub enum SourceConfig {
    Kafka(HighLevelKafkaSourceConfig),
}

#[derive(Clone, Debug, Default)]
pub struct SourceReaderContext {
    pub(crate) query_id: Option<String>,
    pub(crate) bound_timestamp_ms: Option<i64>,
}

#[derive(Clone, Debug)]
pub enum SourceFormat {
    Json,
    Protobuf,
    Avro,
}

pub enum Source {
    HighLevelKafka(HighLevelKafkaSource),
}

pub trait SourceImpl: Send + Sync + 'static {
    type ReaderContext;
    type BatchReader: BatchSourceReader;
    type StreamReader: StreamSourceReader;
    type Writer: SourceWriter;

    /// Create a batch reader
    fn batch_reader(&self, context: Self::ReaderContext) -> Result<Self::BatchReader>;

    /// Create a stream reader
    fn stream_reader(&self, context: Self::ReaderContext) -> Result<Self::StreamReader>;

    /// Create a writer
    fn create_writer(&self) -> Result<Self::Writer>;
}

#[async_trait]
pub trait SourceWriter: Send + Sync + 'static {
    /// `write` a stream chunk into table
    async fn write(&mut self, chunk: &StreamChunk) -> Result<()>;

    /// `flush` ensures data flushed and make sure writer can be closed safely
    async fn flush(&mut self, chunk: &StreamChunk) -> Result<()>;
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
