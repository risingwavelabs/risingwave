use crate::array::DataChunk;
use crate::error::Result;
use crate::executor::ExecutorResult::Done;
use crate::executor::{Executor, ExecutorResult};
use std::collections::VecDeque;

// MockExecutor is to mock the input of executor.
// You can bind one or more MockExecutor as the children of the executor to test,
// (HashAgg, e.g), so that allow testing without instantiating real SeqScans and real storage.
pub struct MockExecutor {
    chunks: VecDeque<DataChunk>,
}

impl MockExecutor {
    pub fn new() -> Self {
        Self {
            chunks: VecDeque::new(),
        }
    }

    pub fn add(&mut self, chunk: DataChunk) {
        self.chunks.push_back(chunk);
    }
}

#[async_trait::async_trait]
impl Executor for MockExecutor {
    fn init(&mut self) -> Result<()> {
        Ok(())
    }

    async fn execute(&mut self) -> Result<ExecutorResult> {
        if self.chunks.is_empty() {
            return Ok(Done);
        }
        let chunk = self.chunks.pop_front().unwrap();
        Ok(ExecutorResult::Batch(chunk))
    }

    fn clean(&mut self) -> Result<()> {
        Ok(())
    }
}
