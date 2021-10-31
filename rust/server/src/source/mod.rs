use std::fmt::Debug;

use async_trait::async_trait;

pub use chunk_reader::*;
pub use file::*;
pub use kafka::*;
pub use manager::*;
pub use parser::*;

use risingwave_common::error::Result;

mod chunk_reader;
mod file;
mod kafka;
mod manager;
mod parser;

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
}

/// `Source` is an abstraction of the data source,
/// storing static properties such as the address of Kafka Brokers,
/// to read data you need to call Source.reader() to get the `SourceReader`
pub trait Source: Send + Sync + 'static {
    fn new(config: SourceConfig) -> Result<Self>
    where
        Self: Sized;

    fn reader(&self) -> Result<Box<dyn SourceReader>>;
}

#[async_trait]
pub trait SourceReader: Debug + Send + Sync + 'static {
    async fn next(&mut self) -> Result<Option<SourceMessage>>;
    async fn cancel(&mut self) -> Result<()>;
}
