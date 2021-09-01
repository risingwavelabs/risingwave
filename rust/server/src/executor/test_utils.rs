use crate::array::{DataChunk, DataChunkRef};
use crate::error::Result;
use crate::executor::ExecutorResult::Done;
use crate::executor::{Executor, ExecutorResult};
use std::collections::VecDeque;
use std::sync::Arc;

// MockExecutor is to mock the input of executor.
// You can bind one or more MockExecutor as the children of the executor to test,
// (HashAgg, e.g), so that allow testing without instantiating real SeqScans and real storage.
pub(crate) struct MockExecutor {
    chunks: VecDeque<DataChunkRef>,
}

impl MockExecutor {
    pub(crate) fn new() -> Self {
        Self {
            chunks: VecDeque::new(),
        }
    }

    pub(crate) fn add(&mut self, chunk: DataChunk) {
        self.chunks.push_back(Arc::new(chunk));
    }
}

impl Executor for MockExecutor {
    fn init(&mut self) -> Result<()> {
        Ok(())
    }

    fn execute(&mut self) -> Result<ExecutorResult> {
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
