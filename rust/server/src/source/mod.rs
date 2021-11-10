use std::fmt::Debug;

use async_trait::async_trait;

pub use chunk_reader::*;
pub use file::*;
pub use kafka::*;
pub use manager::*;
pub use parser::*;

use crate::stream_op::StreamChunk;
use risingwave_common::error::Result;

mod chunk_reader;
mod file;
mod kafka;
mod manager;
mod parser;
mod table;

#[derive(Clone, Debug)]
pub enum SourceConfig {
    Kafka(KafkaSourceConfig),
    File(FileSourceConfig),
}

#[derive(Clone, Debug)]
pub enum SourceFormat {
    Json,
    Protobuf,
    Avro,
}

#[derive(Clone, Debug)]
pub enum SourceMessage {
    Kafka(KafkaMessage),
    File(FileMessage),
    Raw(StreamChunk),
}

/// `Source` is an abstraction of the data source,
/// storing static properties such as the address of Kafka Brokers,
/// to read data you need to call Source.reader() to get the `SourceReader`
pub trait Source: Send + Sync + 'static {
    fn reader(&self) -> Result<Box<dyn SourceReader>>;
}

#[async_trait]
pub trait SourceReader: Debug + Send + Sync + 'static {
    /// `init` is called once to initialize the reader
    async fn init(&mut self) -> Result<()>;

    /// `poll_message` returns a message or `None` immediately if there is no pending message
    async fn poll_message(&mut self) -> Result<Option<SourceMessage>>;

    /// `next_message` always returns a message. If the queue is empty, it will
    /// block until new messages coming
    async fn next_message(&mut self) -> Result<SourceMessage>;

    /// `cancel` is called to stop the reader
    async fn cancel(&mut self) -> Result<()>;
}
