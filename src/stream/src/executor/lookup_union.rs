// Copyright 2025 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use async_trait::async_trait;
use futures::channel::mpsc;
use futures::future::{Either, join_all, select};
use futures::{FutureExt, SinkExt};
use itertools::Itertools;

use crate::executor::prelude::*;

/// Merges data from multiple inputs with order. If `order = [2, 1, 0]`, then
/// it will first pipe data from the third input; after the third input gets a barrier, it will then
/// pipe the second, and finally the first. In the future we could have more efficient
/// implementation.
pub struct LookupUnionExecutor {
    inputs: Vec<Executor>,
    order: Vec<usize>,
}

impl std::fmt::Debug for LookupUnionExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LookupUnionExecutor").finish()
    }
}

impl LookupUnionExecutor {
    pub fn new(inputs: Vec<Executor>, order: Vec<u32>) -> Self {
        Self {
            inputs,
            order: order.iter().map(|x| *x as _).collect(),
        }
    }
}

#[async_trait]
impl Execute for LookupUnionExecutor {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }
}

impl LookupUnionExecutor {
    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(self) {
        let mut inputs = self.inputs.into_iter().map(Some).collect_vec();
        let mut futures = vec![];
        let mut rxs = vec![];
        for idx in self.order {
            let mut stream = inputs[idx].take().unwrap().execute();
            let (mut tx, rx) = mpsc::channel(1024); // set buffer size to control back pressure
            rxs.push(rx);
            futures.push(
                // construct a future that drives input stream until it is exhausted.
                // the input elements are sent over bounded channel.
                async move {
                    while let Some(ret) = stream.next().await {
                        tx.send(ret).await.unwrap();
                    }
                }
                .boxed(),
            );
        }
        // This future is used to drive all inputs.
        let mut drive_inputs = join_all(futures).fuse();
        let mut end = false;
        while !end {
            end = true; // no message on this turn?
            let mut this_barrier: Option<Barrier> = None;
            for rx in &mut rxs {
                loop {
                    let msg = match select(rx.next(), &mut drive_inputs).await {
                        Either::Left((Some(msg), _)) => msg?,
                        Either::Left((None, _)) => break, // input end
                        Either::Right(_) => continue,
                    };
                    end = false;
                    match msg {
                        Message::Watermark(_) => {
                            // TODO: https://github.com/risingwavelabs/risingwave/issues/6042
                        }

                        msg @ Message::Chunk(_) => yield msg,
                        Message::Barrier(barrier) => {
                            if let Some(this_barrier) = &this_barrier {
                                if this_barrier.epoch != barrier.epoch {
                                    return Err(StreamExecutorError::align_barrier(
                                        this_barrier.clone(),
                                        barrier,
                                    ));
                                }
                            } else {
                                this_barrier = Some(barrier);
                            }
                            break; // move to the next input
                        }
                    }
                }
            }
            if end {
                break;
            } else {
                yield Message::Barrier(this_barrier.take().unwrap());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::TryStreamExt;
    use risingwave_common::catalog::Field;
    use risingwave_common::test_prelude::StreamChunkTestExt;
    use risingwave_common::util::epoch::test_epoch;

    use super::*;
    use crate::executor::test_utils::MockSource;

    #[tokio::test]
    async fn lookup_union() {
        let schema = Schema {
            fields: vec![Field::unnamed(DataType::Int64)],
        };
        let source0 = MockSource::with_messages(vec![
            Message::Chunk(StreamChunk::from_pretty("I\n + 1")),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(1))),
            Message::Chunk(StreamChunk::from_pretty("I\n + 2")),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(2))),
            Message::Chunk(StreamChunk::from_pretty("I\n + 3")),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(3))),
        ])
        .stop_on_finish(false)
        .into_executor(schema.clone(), vec![0]);
        let source1 = MockSource::with_messages(vec![
            Message::Chunk(StreamChunk::from_pretty("I\n + 11")),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(1))),
            Message::Chunk(StreamChunk::from_pretty("I\n + 12")),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(2))),
        ])
        .stop_on_finish(false)
        .into_executor(schema.clone(), vec![0]);
        let source2 = MockSource::with_messages(vec![
            Message::Chunk(StreamChunk::from_pretty("I\n + 21")),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(1))),
            Message::Chunk(StreamChunk::from_pretty("I\n + 22")),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(2))),
        ])
        .stop_on_finish(false)
        .into_executor(schema, vec![0]);

        let executor = LookupUnionExecutor::new(vec![source0, source1, source2], vec![2, 1, 0])
            .boxed()
            .execute();

        let outputs: Vec<_> = executor.try_collect().await.unwrap();
        assert_eq!(
            outputs,
            vec![
                Message::Chunk(StreamChunk::from_pretty("I\n + 21")),
                Message::Chunk(StreamChunk::from_pretty("I\n + 11")),
                Message::Chunk(StreamChunk::from_pretty("I\n + 1")),
                Message::Barrier(Barrier::new_test_barrier(test_epoch(1))),
                Message::Chunk(StreamChunk::from_pretty("I\n + 22")),
                Message::Chunk(StreamChunk::from_pretty("I\n + 12")),
                Message::Chunk(StreamChunk::from_pretty("I\n + 2")),
                Message::Barrier(Barrier::new_test_barrier(test_epoch(2))),
                Message::Chunk(StreamChunk::from_pretty("I\n + 3")),
                Message::Barrier(Barrier::new_test_barrier(test_epoch(3))),
            ]
        );
    }
}
