use crate::error::Result;
use crate::executor::StreamScanExecutor;
use crate::stream_op::{DataSource, Message, Op, Output, StreamChunk};
use async_trait::async_trait;
use futures::channel::oneshot;
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
impl DataSource for KafkaDataSource {
    async fn run(
        &mut self,
        mut output: Box<dyn Output>,
        mut cancel: oneshot::Receiver<()>,
    ) -> Result<()> {
        loop {
            futures::select! {
              _ = cancel => {
                return Ok(())
              },
              received = self.executor.next_data_chunk().fuse() => {
                let received = received?;
                if let Some(chunk) = received {
                  // `capacity` or `cardinality` should be both fine as we just read the data from external sources
                  // no visibility map yet
                  let capacity = chunk.capacity();
                  let ops = vec![Op::Insert; capacity];
                  let stream_chunk = StreamChunk::new(ops, chunk.columns().to_vec(), None);
                  output.collect(Message::Chunk(stream_chunk)).await?;
                } else {
                  output.collect(Message::Terminate).await?;
                }
              }
            }
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
