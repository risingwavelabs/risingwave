use crate::error::Result;
use crate::executor::StreamScanExecutor;
use crate::stream_op::{Message, Op, StreamChunk, StreamOperator};
use async_trait::async_trait;
use futures::FutureExt;
use std::fmt::{Debug, Formatter};

pub struct KafkaDataSource {
    executor: StreamScanExecutor,
}

impl KafkaDataSource {
    pub fn new(executor: StreamScanExecutor) -> Self {
        Self { executor }
    }
}

#[async_trait]
impl StreamOperator for KafkaDataSource {
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

impl Debug for KafkaDataSource {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaDataSource")
            .field("stream_scan_executor", &self.executor)
            .finish()
    }
}
