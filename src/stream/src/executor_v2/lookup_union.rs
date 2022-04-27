// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::cmp::Ordering;
use std::collections::HashMap;

use async_trait::async_trait;
use futures::channel::mpsc::{unbounded, UnboundedReceiver};
use futures::stream::select_all;
use futures::StreamExt;
use futures_async_stream::try_stream;
use risingwave_common::catalog::Schema;
use risingwave_common::try_match_expand;
use risingwave_pb::stream_plan;
use risingwave_pb::stream_plan::stream_node::Node;
use risingwave_storage::StateStore;

use super::error::StreamExecutorError;
use super::{Barrier, BoxedExecutor, Executor, Message, PkIndicesRef};
use crate::executor::{ExecutorBuilder, PkIndices};
use crate::executor_v2::{BoxedMessageStream, ExecutorInfo};
use crate::task::{ExecutorParams, LocalStreamManagerCore};

/// `LookupUnionExecutor` merges data from multiple inputs. Currently this is done by using
/// [`BarrierAligner`]. In the future we could have more efficient implementation.
pub struct LookupUnionExecutor {
    inputs: Vec<BoxedExecutor>,
    info: ExecutorInfo,
    order: Vec<usize>,
}

impl std::fmt::Debug for LookupUnionExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LookupUnionExecutor")
            .field("schema", &self.info.schema)
            .field("pk_indices", &self.info.pk_indices)
            .finish()
    }
}

impl LookupUnionExecutor {
    pub fn new(pk_indices: PkIndices, inputs: Vec<BoxedExecutor>, order: Vec<u32>) -> Self {
        Self {
            info: ExecutorInfo {
                schema: inputs[0].schema().clone(),
                pk_indices,
                identity: "LookupUnionExecutor".to_string(),
            },
            inputs,
            order: order.iter().map(|x| *x as _).collect(),
        }
    }
}

#[async_trait]
impl Executor for LookupUnionExecutor {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }

    fn schema(&self) -> &Schema {
        &self.info.schema
    }

    fn pk_indices(&self) -> PkIndicesRef {
        &self.info.pk_indices
    }

    fn identity(&self) -> &str {
        &self.info.identity
    }
}

#[try_stream(ok = (usize, Message), error = StreamExecutorError)]
async fn poll_until_barrier(
    mut stream: BoxedMessageStream,
    id: usize,
    mut barrier_notifier: UnboundedReceiver<()>,
) {
    'outer: loop {
        'inner: loop {
            match stream.next().await {
                Some(Ok(Message::Barrier(barrier))) => {
                    yield (id, Message::Barrier(barrier.clone()));
                    break 'inner;
                }
                Some(Ok(x @ Message::Chunk(_))) => yield (id, x),
                Some(Err(e)) => return Err(e),
                None => break 'outer,
            }
        }
        barrier_notifier.next().await;
    }
}

impl LookupUnionExecutor {
    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(self) {
        let total_inputs = self.inputs.len();
        let mut txs = vec![];
        let mut inputs = self
            .inputs
            .into_iter()
            .enumerate()
            .collect::<HashMap<_, _>>();
        let mut streams = vec![];
        for (position, input_id) in self.order.iter().enumerate() {
            let (tx, rx) = unbounded();
            let stream = poll_until_barrier(
                inputs
                    .remove(input_id)
                    .expect("duplicated inputs in order")
                    .execute(),
                position,
                rx,
            );
            streams.push(Box::pin(stream));
            txs.push(tx);
        }
        let mut stream = select_all(streams);
        let mut this_barrier: Option<Barrier> = None;
        let mut buffer = (0..total_inputs)
            .map(|_| Vec::new())
            .collect::<Vec<Vec<Message>>>();
        // As we are piping data one input by one input, we record which input we are piping in
        // `pipe_order`.
        let mut pipe_order = 0;

        while let Some(res) = stream.next().await {
            let (id, msg) = res?;
            match id.cmp(&pipe_order) {
                Ordering::Equal => {
                    // We can directly forward msg of the current pipe
                    match msg {
                        Message::Chunk(chunk) => {
                            yield Message::Chunk(chunk);
                        }
                        Message::Barrier(barrier) => {
                            if this_barrier.is_none() {
                                this_barrier = Some(barrier);
                            }
                            pipe_order += 1;
                            // process other buffers
                            'outer: while pipe_order < total_inputs {
                                for item in buffer[pipe_order].drain(..) {
                                    if let Message::Barrier(ref barrier) = item {
                                        let this_barrier = this_barrier.as_ref().unwrap();
                                        if barrier != this_barrier {
                                            return Err(StreamExecutorError::align_barrier(
                                                this_barrier.clone(),
                                                barrier.clone(),
                                            ));
                                        }
                                        pipe_order += 1;
                                        continue 'outer; // process the next buffer
                                    } else {
                                        yield item;
                                    }
                                }
                                break; // still need to process this pipe
                            }
                            if pipe_order == total_inputs {
                                for buffer in &buffer {
                                    assert!(buffer.is_empty());
                                }
                                yield Message::Barrier(this_barrier.take().unwrap());
                                pipe_order = 0;
                                for tx in &mut txs {
                                    tx.unbounded_send(()).unwrap();
                                }
                            }
                        }
                    }
                }
                Ordering::Less => {
                    unreachable!()
                }
                Ordering::Greater => {
                    // We need to buffer the msg
                    buffer[id].push(msg);
                }
            }
        }
    }
}

pub struct LookupUnionExecutorBuilder {}

impl ExecutorBuilder for LookupUnionExecutorBuilder {
    fn new_boxed_executor(
        params: ExecutorParams,
        node: &stream_plan::StreamNode,
        _store: impl StateStore,
        _stream: &mut LocalStreamManagerCore,
    ) -> risingwave_common::error::Result<BoxedExecutor> {
        let lookup_union = try_match_expand!(node.get_node().unwrap(), Node::LookupUnionNode)?;
        Ok(
            LookupUnionExecutor::new(params.pk_indices, params.input, lookup_union.order.clone())
                .boxed(),
        )
    }
}
