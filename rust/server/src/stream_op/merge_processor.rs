use super::{Processor, Result, UnaryStreamOperator};
use async_stream::stream;
use async_trait::async_trait;
use futures::channel::mpsc::{unbounded, Receiver, UnboundedReceiver};
use futures::{SinkExt, Stream, StreamExt};

use super::Message;

/// `UnaryMergeProcessor` merges data from multiple channels. Dataflow from one channel
/// will be stopped on barrier.
pub struct UnaryMergeProcessor {
    inputs: Vec<Receiver<Message>>,
    operator_head: Box<dyn UnaryStreamOperator>,
}

impl UnaryMergeProcessor {
    pub fn new(
        inputs: Vec<Receiver<Message>>,
        operator_head: Box<dyn UnaryStreamOperator>,
    ) -> Self {
        Self {
            inputs,
            operator_head,
        }
    }
}

/// `barrier_receiver` converts a message stream to a barrier stream.
/// The stream will emit item only when barrier epoch >= current epoch.
///
/// For example, assume `M` is message, and `Bx` is barrier of epoch x.
/// The timeline is as follows.
/// ```plain
/// epoch:     0           1          2
/// recv:      M M M B0    M M M B1   M M M
/// barrier:               B0         B1
/// ```
/// The epoch will only begin when a message from barrier receiver is
/// received.
fn barrier_receiver(
    id: usize,
    recv: Receiver<Message>,
    mut barrier: UnboundedReceiver<u64>,
) -> impl Stream<Item = (usize, Message)> {
    stream! {
      for await value in recv {
        match value {
          Message::Chunk(chunk) => {
            yield (id, Message::Chunk(chunk));
          },
          Message::Terminate => {
            yield (id, Message::Terminate);
          },
          Message::Barrier(epoch) => {
            yield (id, Message::Barrier(epoch));
            while let Some(barrier_epoch) = barrier.next().await {
              if barrier_epoch >= epoch {
                break;
              }
            }
          }
        }
      }
    }
}

#[async_trait]
impl Processor for UnaryMergeProcessor {
    async fn run(mut self) -> Result<()> {
        let mut txs = vec![];
        let mut streams = vec![];

        let channel_cnt = self.inputs.len();

        // once `terminate_count` becomes zero, the runner will be stopped.
        let mut terminate_count = channel_cnt;

        // once `barrier_count` becomes zero, the next epoch will begin.
        let mut barrier_count = channel_cnt;
        let mut next_epoch = 0;

        for (id, input) in self.inputs.into_iter().enumerate() {
            // create a barrier notifier and make a barrier stream
            let (tx, rx) = unbounded();
            let receiver = Box::pin(barrier_receiver(id, input, rx));
            streams.push(receiver);
            txs.push(tx);
        }

        let mut stream = futures::stream::select_all(streams.into_iter());

        while let Some(msg) = stream.next().await {
            match msg {
                (_id, Message::Chunk(chunk)) => {
                    self.operator_head.consume_chunk(chunk).await?;
                }
                (_id, Message::Terminate) => {
                    terminate_count -= 1;
                    if terminate_count == 0 {
                        self.operator_head.consume_terminate().await?;
                        return Ok(());
                    }
                }
                (_id, Message::Barrier(epoch)) => {
                    assert_eq!(epoch, next_epoch);
                    barrier_count -= 1;
                    if barrier_count == 0 {
                        self.operator_head.consume_barrier(next_epoch).await?;
                        barrier_count = channel_cnt;
                        for tx in &mut txs {
                            tx.send(next_epoch).await.unwrap();
                        }
                        next_epoch += 1;
                    }
                }
            }
        }

        unreachable!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stream_op::integration_tests::TestConsumer;
    use crate::stream_op::{Op, StreamChunk};
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
        let msgs = Arc::new(Mutex::new(vec![]));
        let consumer = TestConsumer::new(msgs.clone());
        for _i in 0..CHANNEL_NUMBER {
            let (tx, rx) = futures::channel::mpsc::channel(16);
            txs.push(tx);
            rxs.push(rx);
        }
        let merger = UnaryMergeProcessor {
            inputs: rxs,
            operator_head: Box::new(consumer),
        };

        let handle = tokio::spawn(merger.run());
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
