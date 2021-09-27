mod kafka;
pub use kafka::*;
mod file;
pub use file::*;
mod manager;
pub use manager::*;

use crate::error::Result;
use std::fmt::Debug;

#[derive(Clone, Debug)]
pub enum SourceConfig {
    Kafka(KafkaSourceConfig),
    File(FileSourceConfig),
}

#[derive(Clone, Debug)]
pub enum SourceMessage {
    Kafka(KafkaMessage),
    File(FileMessage),
}

pub trait Source: Send + Sync + 'static {
    fn new(config: SourceConfig) -> Result<Self>
    where
        Self: Sized;

    fn reader(&self) -> Result<Box<dyn SourceReader>>;
}

#[async_trait::async_trait]
pub trait SourceReader: Send + Sync + 'static {
    async fn next(&mut self) -> Result<Option<SourceMessage>>;
    async fn cancel(&mut self) -> Result<()>;
}
