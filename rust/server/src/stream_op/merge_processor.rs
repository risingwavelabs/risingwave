use super::{Result, StreamOperator};
use async_trait::async_trait;
use futures::channel::mpsc::Receiver;
use futures::future::select_all;
use futures::StreamExt;

use super::Message;

/// `MergeOperator` merges data from multiple channels. Dataflow from one channel
/// will be stopped on barrier.
pub struct MergeOperator {
    /// Number of inputs
    num_inputs: usize,

    /// Active channels
    active: Vec<Receiver<Message>>,

    /// Count of terminated channels
    terminated: usize,

    /// Channels that blocked by barriers are parked here. Would be put back
    /// until all barriers reached
    blocked: Vec<Receiver<Message>>,

    /// Current barrier epoch (for assertion)
    next_epoch: u64,
}

impl MergeOperator {
    pub fn new(inputs: Vec<Receiver<Message>>) -> Self {
        Self {
            num_inputs: inputs.len(),
            active: inputs,
            blocked: vec![],
            terminated: 0,
            next_epoch: 0,
        }
    }
}

#[async_trait]
impl StreamOperator for MergeOperator {
    async fn next(&mut self) -> Result<Message> {
        loop {
            // Convert channel receivers to futures here to do `select_all`
            let mut futures = vec![];
            for ch in self.active.drain(..) {
                futures.push(ch.into_future());
            }
            let ((message, from), _id, remains) = select_all(futures).await;
            for fut in remains.into_iter() {
                self.active.push(fut.into_inner().unwrap());
            }

            match message.unwrap() {
                Message::Chunk(chunk) => {
                    self.active.push(from);
                    return Ok(Message::Chunk(chunk));
                }
                Message::Terminate => {
                    // Drop the terminated channel
                    self.terminated += 1;
                }
                Message::Barrier(epoch) => {
                    // Move this channel into the `blocked` list
                    assert_eq!(epoch, self.next_epoch);
                    self.blocked.push(from);
                }
            }

            if self.terminated == self.num_inputs {
                return Ok(Message::Terminate);
            }
            if self.blocked.len() == self.num_inputs {
                // Emit the barrier to downstream once all barriers collected from upstream
                assert!(self.active.is_empty());
                self.active = std::mem::take(&mut self.blocked);
                let epoch = self.next_epoch;
                self.next_epoch += 1;
                return Ok(Message::Barrier(epoch));
            }
            assert!(!self.active.is_empty())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stream_op::integration_tests::MockConsumer;
    use crate::stream_op::{Actor, Op, StreamChunk};
    use futures::SinkExt;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    fn build_test_chunk(epoch: u64) -> StreamChunk {
        let mut chunk = StreamChunk::default();
        // The number of items in `ops` is the epoch count.
        for _i in 0..epoch {
            chunk.ops.push(Op::Insert);
        }
        chunk
    }

    #[tokio::test]
    async fn test_merger() {
        const CHANNEL_NUMBER: usize = 10;
        let mut txs = Vec::with_capacity(CHANNEL_NUMBER);
        let mut rxs = Vec::with_capacity(CHANNEL_NUMBER);
        for _i in 0..CHANNEL_NUMBER {
            let (tx, rx) = futures::channel::mpsc::channel(16);
            txs.push(tx);
            rxs.push(rx);
        }
        let merger = MergeOperator::new(rxs);

        let msgs = Arc::new(Mutex::new(vec![]));
        let consumer = MockConsumer::new(Box::new(merger), msgs.clone());
        let actor = Actor::new(Box::new(consumer));

        let handle = tokio::spawn(actor.run());
        let mut handles = Vec::with_capacity(CHANNEL_NUMBER);

        for mut tx in txs {
            let handle = tokio::spawn(async move {
                for epoch in 0..100 {
                    tx.send(Message::Chunk(build_test_chunk(epoch)))
                        .await
                        .unwrap();
                    tx.send(Message::Barrier(epoch)).await.unwrap();
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
                tx.send(Message::Terminate).await.unwrap();
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await.unwrap();
        }
        handle.await.unwrap().unwrap();

        let data = msgs.lock().unwrap();
        assert_eq!(data.len(), 100 * CHANNEL_NUMBER);

        // check if data are received epoch by epoch
        for (id, item) in data.iter().enumerate() {
            assert_eq!(item.ops.len(), id / CHANNEL_NUMBER);
        }
    }
}
