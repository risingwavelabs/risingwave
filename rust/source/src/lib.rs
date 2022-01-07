use std::fmt::Debug;

use async_trait::async_trait;
pub use high_level_kafka::*;
pub use manager::*;
pub use parser::*;
use risingwave_common::array::{DataChunk, StreamChunk};
use risingwave_common::error::Result;
pub use table::*;
pub use table_v2::*;

pub mod parser;

mod high_level_kafka;
mod manager;

mod common;
mod pulsar;
mod table;
mod table_v2;

#[derive(Clone, Debug)]
pub enum SourceConfig {
    Kafka(HighLevelKafkaSourceConfig),
}

#[derive(Clone, Debug)]
pub enum SourceFormat {
    Invalid,
    Json,
    Protobuf,
    DebeziumJson,
    Avro,
}

#[derive(Debug)]
pub enum SourceImpl {
    HighLevelKafka(HighLevelKafkaSource),
    Table(TableSource),
    TableV2(TableSourceV2),
}

impl SourceImpl {
    pub fn as_table(&self) -> &TableSource {
        match self {
            SourceImpl::Table(table) => table,
            _ => panic!("not a table source"),
        }
    }

    pub fn as_table_v2(&self) -> &TableSourceV2 {
        match self {
            SourceImpl::TableV2(table) => table,
            _ => panic!("not a table source v2"),
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
    fn batch_reader(
        &self,
        context: Self::ReaderContext,
        column_ids: Vec<i32>,
    ) -> Result<Self::BatchReader>;

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
