mod kafka;
pub use kafka::*;
mod file;
pub use file::*;
mod chunk_reader;
pub use chunk_reader::*;
mod manager;
pub use manager::*;
mod parser;
pub use parser::*;

use crate::error::Result;
use crate::types::Datum;
use async_trait::async_trait;
use std::fmt::Debug;

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

pub trait SourceParser: Send + Sync + 'static {
    // parse needs to be a member method because some format like Protobuf needs to be pre-compiled
    fn parse(&self, payload: &[u8], columns: &[SourceColumnDesc]) -> Result<Vec<Datum>>;
}
