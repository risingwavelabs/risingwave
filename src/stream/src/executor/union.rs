// Copyright 2023 RisingWave Labs
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

use std::collections::BTreeMap;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::stream::{FusedStream, FuturesUnordered};
use futures::StreamExt;
use futures_async_stream::try_stream;
use pin_project::pin_project;
use risingwave_common::catalog::Schema;

use super::watermark::BufferedWatermarks;
use super::*;
use crate::executor::{BoxedMessageStream, ExecutorInfo};

/// `UnionExecutor` merges data from multiple inputs.
pub struct UnionExecutor {
    inputs: Vec<BoxedExecutor>,
    info: ExecutorInfo,
}

impl std::fmt::Debug for UnionExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnionExecutor")
            .field("schema", &self.info.schema)
            .field("pk_indices", &self.info.pk_indices)
            .finish()
    }
}

impl UnionExecutor {
    pub fn new(pk_indices: PkIndices, inputs: Vec<BoxedExecutor>) -> Self {
        Self {
            info: ExecutorInfo {
                schema: inputs[0].schema().clone(),
                pk_indices,
                identity: "UnionExecutor".to_string(),
            },
            inputs,
        }
    }
}

impl Executor for UnionExecutor {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        let streams = self.inputs.into_iter().map(|e| e.execute()).collect();
        merge(streams).boxed()
    }

    fn schema(&self) -> &Schema {
        &self.info.schema
    }

    fn pk_indices(&self) -> PkIndicesRef<'_> {
        &self.info.pk_indices
    }

    fn identity(&self) -> &str {
        &self.info.identity
    }

    fn info(&self) -> ExecutorInfo {
        self.info.clone()
    }
}

#[pin_project]
struct Input {
    #[pin]
    inner: BoxedMessageStream,
    id: usize,
}

impl Stream for Input {
    type Item = MessageStreamItem;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().inner.poll_next(cx)
    }
}

/// Merges input streams and aligns with barriers.
#[try_stream(ok = Message, error = StreamExecutorError)]
async fn merge(inputs: Vec<BoxedMessageStream>) {
    let input_num = inputs.len();
    let mut active: FuturesUnordered<_> = inputs
        .into_iter()
        .enumerate()
        .map(|(idx, input)| {
            (Input {
                id: idx,
                inner: input,
            })
            .into_future()
        })
        .collect();
    let mut blocked = vec![];
    let mut current_barrier: Option<Barrier> = None;

    // watermark column index -> `BufferedWatermarks`
    let mut watermark_buffers = BTreeMap::<usize, BufferedWatermarks<usize>>::new();

    loop {
        match active.next().await {
            Some((Some(Ok(message)), remaining)) => {
                match message {
                    Message::Chunk(chunk) => {
                        // Continue polling this upstream by pushing it back to `active`.
                        active.push(remaining.into_future());
                        yield Message::Chunk(chunk);
                    }
                    Message::Watermark(watermark) => {
                        let id = remaining.id;
                        // Continue polling this upstream by pushing it back to `active`.
                        active.push(remaining.into_future());
                        let buffers = watermark_buffers
                            .entry(watermark.col_idx)
                            .or_insert_with(|| BufferedWatermarks::with_ids(0..input_num));
                        if let Some(selected_watermark) =
                            buffers.handle_watermark(id, watermark.clone())
                        {
                            yield Message::Watermark(selected_watermark)
                        }
                    }
                    Message::Barrier(barrier) => {
                        // Block this upstream by pushing it to `blocked`.
                        blocked.push(remaining);
                        if let Some(cur_barrier) = current_barrier.as_ref() {
                            if barrier.epoch != cur_barrier.epoch {
                                return Err(StreamExecutorError::align_barrier(
                                    cur_barrier.clone(),
                                    barrier,
                                ));
                            }
                        } else {
                            current_barrier = Some(barrier);
                        }
                    }
                }
            }
            Some((Some(Err(e)), _)) => return Err(e),
            Some((None, remaining)) => {
                // tracing::error!("Union from upstream {} closed unexpectedly", remaining.id);
                return Err(StreamExecutorError::channel_closed(format!(
                    "Union from upstream {} closed unexpectedly",
                    remaining.id,
                )));
            }
            None => {
                assert!(active.is_terminated());
                let barrier = current_barrier.take().unwrap();

                let upstreams = std::mem::take(&mut blocked);
                active.extend(upstreams.into_iter().map(|upstream| upstream.into_future()));

                yield Message::Barrier(barrier)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use async_stream::try_stream;
    use risingwave_common::array::stream_chunk::StreamChunkTestExt;
    use risingwave_common::array::StreamChunk;

    use super::*;

    #[tokio::test]
    async fn union() {
        let streams = vec![
            try_stream! {
                yield Message::Chunk(StreamChunk::from_pretty("I\n + 1"));
                yield Message::Barrier(Barrier::new_test_barrier(1));
                yield Message::Chunk(StreamChunk::from_pretty("I\n + 2"));
                yield Message::Barrier(Barrier::new_test_barrier(2));
                yield Message::Barrier(Barrier::new_test_barrier(3));
                yield Message::Watermark(Watermark::new(0, DataType::Int64, ScalarImpl::Int64(4)));
                yield Message::Barrier(Barrier::new_test_barrier(4));
            }
            .boxed(),
            try_stream! {
                yield Message::Chunk(StreamChunk::from_pretty("I\n + 1"));
                yield Message::Barrier(Barrier::new_test_barrier(1));
                yield Message::Barrier(Barrier::new_test_barrier(2));
                yield Message::Chunk(StreamChunk::from_pretty("I\n + 3"));
                yield Message::Barrier(Barrier::new_test_barrier(3));
                yield Message::Watermark(Watermark::new(0, DataType::Int64, ScalarImpl::Int64(5)));
                yield Message::Barrier(Barrier::new_test_barrier(4));
            }
            .boxed(),
        ];
        let mut output = vec![];
        let mut merged = merge(streams).boxed();

        let result = vec![
            Message::Chunk(StreamChunk::from_pretty("I\n + 1")),
            Message::Chunk(StreamChunk::from_pretty("I\n + 1")),
            Message::Barrier(Barrier::new_test_barrier(1)),
            Message::Chunk(StreamChunk::from_pretty("I\n + 2")),
            Message::Barrier(Barrier::new_test_barrier(2)),
            Message::Chunk(StreamChunk::from_pretty("I\n + 3")),
            Message::Barrier(Barrier::new_test_barrier(3)),
            Message::Watermark(Watermark::new(0, DataType::Int64, ScalarImpl::Int64(4))),
            Message::Barrier(Barrier::new_test_barrier(4)),
        ];
        for _ in 0..result.len() {
            output.push(merged.next().await.unwrap().unwrap());
        }
        assert_eq!(output, result);
    }
}
