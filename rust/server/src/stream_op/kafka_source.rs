use crate::error::Result;
use crate::executor::StreamScanExecutor;
use crate::stream_op::{Executor, Message, Op, StreamChunk};
use async_trait::async_trait;
use futures::FutureExt;
use std::fmt::{Debug, Formatter};

pub struct KafkaSourceExecutor {
    executor: StreamScanExecutor,
}

impl KafkaSourceExecutor {
    pub fn new(executor: StreamScanExecutor) -> Self {
        Self { executor }
    }
}

#[async_trait]
impl Executor for KafkaSourceExecutor {
    async fn next(&mut self) -> Result<Message> {
        let received = self.executor.next_data_chunk().fuse().await?;
        if let Some(chunk) = received {
            // `capacity` or `cardinality` should be both fine as we just read the data from external sources
            // no visibility map yet
            let capacity = chunk.capacity();
            let ops = vec![Op::Insert; capacity];
            let stream_chunk = StreamChunk::new(ops, chunk.columns().to_vec(), None);
            Ok(Message::Chunk(stream_chunk))
        } else {
            Ok(Message::Terminate)
        }
    }
}

impl Debug for KafkaSourceExecutor {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaSourceExecutor")
            .field("stream_scan_executor", &self.executor)
            .finish()
    }
}
