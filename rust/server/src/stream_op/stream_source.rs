use crate::executor::StreamScanExecutor;
use crate::stream_op::{Executor, Message, Op, StreamChunk};
use async_trait::async_trait;
use futures::FutureExt;
use itertools::Itertools;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::Result;
use std::fmt::{Debug, Formatter};

/// `StreamSourceExecutor` is a streaming source from external systems such as Kafka
pub struct StreamSourceExecutor {
    schema: Schema,
    executor: StreamScanExecutor,
}

impl StreamSourceExecutor {
    pub fn new(executor: StreamScanExecutor) -> Self {
        let fields = executor
            .columns()
            .iter()
            .map(|col| Field {
                data_type: col.data_type.clone(),
            })
            .collect_vec();
        let schema = Schema { fields };
        Self { schema, executor }
    }
}

#[async_trait]
impl Executor for StreamSourceExecutor {
    async fn next(&mut self) -> Result<Message> {
        let received = self.executor.next_data_chunk().fuse().await?;
        if let Some(chunk) = received {
            // `capacity` or `cardinality` should be both fine as we just read the data from
            // external sources no visibility map yet
            let capacity = chunk.capacity();
            let ops = vec![Op::Insert; capacity];
            let stream_chunk = StreamChunk::new(ops, chunk.columns().to_vec(), None);
            Ok(Message::Chunk(stream_chunk))
        } else {
            // TODO: the epoch 0 here is a placeholder. We will use a side channel to inject barrier
            // message in the future.
            Ok(Message::Barrier {
                epoch: 0,
                stop: true,
            })
        }
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

impl Debug for StreamSourceExecutor {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamSourceExecutor")
            .field("stream_scan_executor", &self.executor)
            .finish()
    }
}
