use anyhow::Result;
use async_trait::async_trait;

use crate::{ConnectorStateV2, Properties, SourceMessage, SplitReader};

/// [`DummySplitReader`] is a placeholder for source executor that is assigned no split. It will
/// wait forever when calling `next`.
#[derive(Clone, Debug)]
pub struct DummySplitReader;

#[async_trait]
impl SplitReader for DummySplitReader {
    async fn next(&mut self) -> Result<Option<Vec<SourceMessage>>> {
        // the dead loop is intentional
        #[allow(clippy::empty_loop)]
        loop {}
    }

    async fn new(_properties: Properties, _state: ConnectorStateV2) -> Result<Self>
    where
        Self: Sized,
    {
        Ok(Self {})
    }
}
