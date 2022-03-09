use anyhow::Result;
use async_trait::async_trait;

pub enum SourceOffset {
    Number(i64),
    String(String),
}

pub trait SourceMessage {
    fn payload(&self) -> Result<Option<&[u8]>>;
    fn offset(&self) -> Result<Option<SourceOffset>>;
    fn serialize(&self) -> Result<String>;
}

pub trait SourceSplit {
    fn id(&self) -> String;
    fn to_string(&self) -> Result<String>;
}

#[async_trait]
pub trait SourceReader: Sized {
    async fn next(&mut self) -> Result<Option<Vec<Vec<u8>>>>;
    async fn assign_split<'a>(&mut self, split: &'a [u8]) -> Result<()>;
}

#[async_trait]
pub trait SplitEnumerator {
    type Split: SourceSplit + Send + Sync;
    async fn list_splits(&mut self) -> Result<Vec<Self::Split>>;
}
