use anyhow::Result;
use async_trait::async_trait;

pub enum SourceOffset {
    Number(i64),
    String(String),
}

pub trait SourceMessage {
    fn payload(&self) -> Result<Option<&[u8]>>;
    fn offset(&self) -> Result<Option<SourceOffset>>;
}

pub trait SourceSplit {
    fn id(&self) -> String;
}

#[async_trait]
pub trait SourceReader: Sized {
    type Message: SourceMessage + Send + Sync;
    type Split: SourceSplit + Send + Sync;

    async fn next(&mut self) -> Result<Option<Vec<Self::Message>>>;
    async fn assign_split(&mut self, split: Self::Split) -> Result<()>;
}

#[async_trait]
pub trait SplitEnumerator {
    type Split: SourceSplit + Send + Sync;
    async fn list_splits(&mut self) -> Result<Vec<Self::Split>>;
}
