use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

pub enum SourceOffset {
    Number(i64),
    String(String),
}

pub trait SourceMessage {
    fn payload(&self) -> Result<Option<&[u8]>>;
    fn offset(&self) -> Result<Option<SourceOffset>>;
    fn serialize(&self) -> Result<String>;
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct InnerMessage {
    pub payload: Option<Bytes>,
    pub offset: String,
    pub split_id: String,
}

pub trait SourceSplit {
    fn id(&self) -> String;
    fn to_string(&self) -> Result<String>;
}

#[async_trait]
pub trait SourceReader: 'static {
    async fn next(&mut self) -> Result<Option<Vec<InnerMessage>>>;
    async fn assign_split<'a>(&'a mut self, split: &'a [u8]) -> Result<()>;
}

#[async_trait]
pub trait SplitEnumerator {
    type Split: SourceSplit + Send + Sync;
    async fn list_splits(&mut self) -> Result<Vec<Self::Split>>;
}
