use super::{Executor, Result};
use async_trait::async_trait;
use futures::channel::mpsc::Receiver;
use futures::future::select_all;
use futures::StreamExt;

use super::Message;

/// `ReceiverExecutor` is used along with a channel. After creating a mpsc channel,
/// there should be a `ReceiverExecutor` running in the background, so as to push
/// messages down to the executors.
pub struct ReceiverExecutor {
    receiver: Receiver<Message>,
}

impl ReceiverExecutor {
    pub fn new(receiver: Receiver<Message>) -> Self {
        Self { receiver }
    }
}

#[async_trait]
impl Executor for ReceiverExecutor {
    async fn next(&mut self) -> Result<Message> {
        let msg = self.receiver.next().await.unwrap(); // TODO: remove unwrap
        Ok(msg)
    }
}

/// `MergeExecutor` merges data from multiple channels. Dataflow from one channel
/// will be stopped on barrier.
pub struct MergeExecutor {
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
    next_epoch: Option<u64>,
}

impl MergeExecutor {
    pub fn new(inputs: Vec<Receiver<Message>>) -> Self {
        Self {
            num_inputs: inputs.len(),
            active: inputs,
            blocked: vec![],
            terminated: 0,
            next_epoch: None,
        }
    }
}

#[async_trait]
impl Executor for MergeExecutor {
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
                Message::Barrier { epoch, stop: _ } => {
                    // Move this channel into the `blocked` list
                    if self.blocked.is_empty() {
                        assert_eq!(self.next_epoch, None);
                        self.next_epoch = Some(epoch);
                    } else {
                        assert_eq!(self.next_epoch, Some(epoch));
                    }
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
                let epoch = self.next_epoch.take().unwrap();
                return Ok(Message::Barrier { epoch, stop: false });
            }
            assert!(!self.active.is_empty())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stream_op::{Op, StreamChunk};
    use assert_matches::assert_matches;
    use futures::SinkExt;
    use itertools::Itertools;
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
        let mut merger = MergeExecutor::new(rxs);

        let mut handles = Vec::with_capacity(CHANNEL_NUMBER);

        let epochs = (10..1000u64).step_by(10).collect_vec();

        for mut tx in txs {
            let epochs = epochs.clone();
            let handle = tokio::spawn(async move {
                for epoch in epochs {
                    tx.send(Message::Chunk(build_test_chunk(epoch)))
                        .await
                        .unwrap();
                    tx.send(Message::Barrier { epoch, stop: false })
                        .await
                        .unwrap();
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
                tx.send(Message::Terminate).await.unwrap();
            });
            handles.push(handle);
        }

        for epoch in epochs {
            // expect n chunks
            for _ in 0..CHANNEL_NUMBER {
                assert_matches!(merger.next().await.unwrap(), Message::Chunk(chunk) => {
                  assert_eq!(chunk.ops.len() as u64, epoch);
                });
            }
            // expect a barrier
            assert_matches!(merger.next().await.unwrap(), Message::Barrier{epoch:barrier_epoch,stop:_} => {
              assert_eq!(barrier_epoch, epoch);
            });
        }
        assert_matches!(merger.next().await.unwrap(), Message::Terminate);

        for handle in handles {
            handle.await.unwrap();
        }
    }
}
