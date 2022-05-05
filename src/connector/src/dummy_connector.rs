use anyhow::Result;
use async_trait::async_trait;
use futures::future;

use crate::{ConnectorStateV2, Properties, SourceMessage, SplitReader};

/// [`DummySplitReader`] is a placeholder for source executor that is assigned no split. It will
/// wait forever when calling `next`.
#[derive(Clone, Debug)]
pub struct DummySplitReader;

#[async_trait]
impl SplitReader for DummySplitReader {
    async fn next(&mut self) -> Result<Option<Vec<SourceMessage>>> {
        let pending = future::pending();
        let () = pending.await;

        unreachable!()
    }

    async fn new(_properties: Properties, _state: ConnectorStateV2) -> Result<Self>
    where
        Self: Sized,
    {
        Ok(Self {})
    }
}
