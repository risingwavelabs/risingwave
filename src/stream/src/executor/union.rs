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

use std::pin::Pin;
use std::task::{Context, Poll};

use pin_project::pin_project;

use crate::executor::DynamicReceivers;
use crate::executor::exchange::input::Input;
use crate::executor::prelude::*;
use crate::task::InputId;

/// `UnionExecutor` merges data from multiple inputs.
pub struct UnionExecutor {
    inputs: Vec<Executor>,
    metrics: Arc<StreamingMetrics>,
    actor_context: ActorContextRef,
}

impl std::fmt::Debug for UnionExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnionExecutor").finish()
    }
}

impl UnionExecutor {
    pub fn new(
        inputs: Vec<Executor>,
        metrics: Arc<StreamingMetrics>,
        actor_context: ActorContextRef,
    ) -> Self {
        Self {
            inputs,
            metrics,
            actor_context,
        }
    }
}

impl Execute for UnionExecutor {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        let upstreams = self
            .inputs
            .into_iter()
            .map(|e| e.execute())
            .enumerate()
            .map(|(id, input)| {
                Box::pin(UnionExecutorInput { id, inner: input }) as Pin<Box<dyn Input<Item = _>>>
            })
            .collect();

        let barrier_align = self
            .metrics
            .barrier_align_duration
            .with_guarded_label_values(&[
                self.actor_context.id.to_string().as_str(),
                self.actor_context.fragment_id.to_string().as_str(),
                "",
                "Union",
            ]);

        let union_receivers = DynamicReceivers::new(upstreams, Some(barrier_align), None);

        union_receivers.boxed()
    }
}

#[pin_project]
struct UnionExecutorInput {
    id: usize,
    #[pin]
    inner: BoxedMessageStream,
}

impl Input for UnionExecutorInput {
    fn id(&self) -> InputId {
        self.id as InputId
    }
}

impl Stream for UnionExecutorInput {
    type Item = MessageStreamItem;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().inner.poll_next(cx)
    }
}

#[cfg(test)]
mod tests {
    use async_stream::try_stream;
    use risingwave_common::array::stream_chunk::StreamChunkTestExt;
    use risingwave_common::util::epoch::test_epoch;

    use super::*;

    #[tokio::test]
    async fn union() {
        let streams = vec![
            try_stream! {
                yield Message::Chunk(StreamChunk::from_pretty("I\n + 1"));
                yield Message::Barrier(Barrier::new_test_barrier(test_epoch(1)));
                yield Message::Chunk(StreamChunk::from_pretty("I\n + 2"));
                yield Message::Barrier(Barrier::new_test_barrier(test_epoch(2)));
                yield Message::Barrier(Barrier::new_test_barrier(test_epoch(3)));
                yield Message::Watermark(Watermark::new(0, DataType::Int64, ScalarImpl::Int64(4)));
                yield Message::Barrier(Barrier::new_test_barrier(test_epoch(4)));
            }
            .boxed(),
            try_stream! {
                yield Message::Chunk(StreamChunk::from_pretty("I\n + 1"));
                yield Message::Barrier(Barrier::new_test_barrier(test_epoch(1)));
                yield Message::Barrier(Barrier::new_test_barrier(test_epoch(2)));
                yield Message::Chunk(StreamChunk::from_pretty("I\n + 3"));
                yield Message::Barrier(Barrier::new_test_barrier(test_epoch(3)));
                yield Message::Watermark(Watermark::new(0, DataType::Int64, ScalarImpl::Int64(5)));
                yield Message::Barrier(Barrier::new_test_barrier(test_epoch(4)));
            }
            .boxed(),
        ];
        let upstreams = streams
            .into_iter()
            .enumerate()
            .map(|(id, input)| {
                Box::pin(UnionExecutorInput { id, inner: input }) as Pin<Box<dyn Input<Item = _>>>
            })
            .collect();
        let mut output = vec![];
        let mut union = DynamicReceivers::new(upstreams, None, None).boxed();

        let result = vec![
            Message::Chunk(StreamChunk::from_pretty("I\n + 1")),
            Message::Chunk(StreamChunk::from_pretty("I\n + 1")),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(1))),
            Message::Chunk(StreamChunk::from_pretty("I\n + 2")),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(2))),
            Message::Chunk(StreamChunk::from_pretty("I\n + 3")),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(3))),
            Message::Watermark(Watermark::new(0, DataType::Int64, ScalarImpl::Int64(4))),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(4))),
        ];
        for _ in 0..result.len() {
            output.push(union.next().await.unwrap().unwrap());
        }
        assert_eq!(output, result);
    }
}
