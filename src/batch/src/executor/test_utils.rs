use risingwave_common::array::DataChunk;

use crate::error::Result;
use crate::exchange_source::ExchangeSource;
use crate::task::TaskId;

#[derive(Debug, Clone)]
pub struct FakeExchangeSource {
    chunks: Vec<Option<DataChunk>>,
}

impl FakeExchangeSource {
    pub fn new(chunks: Vec<Option<DataChunk>>) -> Self {
        Self { chunks }
    }
}

impl ExchangeSource for FakeExchangeSource {
    async fn take_data(&mut self) -> Result<Option<DataChunk>> {
        if let Some(chunk) = self.chunks.pop() {
            Ok(chunk)
        } else {
            Ok(None)
        }
    }

    fn get_task_id(&self) -> TaskId {
        TaskId::default()
    }
}
