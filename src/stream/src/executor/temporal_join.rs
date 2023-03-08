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

use either::Either;
use futures::stream::{self, PollNext};
use futures::{StreamExt, TryStreamExt};
use futures_async_stream::try_stream;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::catalog::Schema;
use risingwave_common::row::{OwnedRow, Row, RowExt};
use risingwave_hummock_sdk::{HummockEpoch, HummockReadEpoch};
use risingwave_storage::table::batch_table::storage_table::StorageTable;
use risingwave_storage::StateStore;

use super::{Barrier, Executor, Message, MessageStream, StreamExecutorError, StreamExecutorResult};
use crate::cache::ManagedLruCache;
use crate::common::StreamChunkBuilder;

pub struct TemporalJoinExecutor<S: StateStore> {
    left: Box<dyn Executor>,
    right: TemporalSide<S>,
    right_stream: Box<dyn Executor>,
    left_join_keys: Vec<usize>,
    right_join_keys: Vec<usize>,
    output_indices: Vec<usize>,
    schema: Schema,
    chunk_size: usize,
}

struct TemporalSide<S: StateStore> {
    source: StorageTable<S>,
    cache: ManagedLruCache<OwnedRow, Option<OwnedRow>>,
}

impl<S: StateStore> TemporalSide<S> {
    async fn lookup(
        &mut self,
        key: impl Row,
        epoch: HummockEpoch,
    ) -> StreamExecutorResult<Option<OwnedRow>> {
        let key = key.into_owned_row();
        Ok(match self.cache.get(&key) {
            Some(res) => res.clone(),
            None => {
                let res = self
                    .source
                    .get_row(key.clone(), HummockReadEpoch::NoWait(epoch))
                    .await?;
                self.cache.put(key, res.clone());
                res
            }
        })
    }

    fn update(&mut self, payload: Vec<StreamChunk>, join_keys: &[usize]) {
        payload.iter().flat_map(|c| c.rows()).for_each(|(op, row)| {
            let key = row.project(join_keys).into_owned_row();
            if let Some(value) = self.cache.get_mut(&key) {
                match op {
                    Op::Insert | Op::UpdateInsert => *value = Some(row.into_owned_row()),
                    Op::Delete | Op::UpdateDelete => *value = None,
                };
            }
        })
    }
}

enum InternalMessage {
    Chunk(StreamChunk),
    Barrier(Vec<StreamChunk>, Barrier),
}

#[try_stream(ok = StreamChunk, error = StreamExecutorError)]
pub async fn chunks_until_barrier(stream: impl MessageStream, expected_barrier: Barrier) {
    #[for_await]
    for item in stream {
        match item? {
            Message::Watermark(_) => {
                todo!("https://github.com/risingwavelabs/risingwave/issues/6042")
            }
            Message::Chunk(c) => yield c,
            Message::Barrier(b) if b.epoch != expected_barrier.epoch => {
                return Err(StreamExecutorError::align_barrier(expected_barrier, b));
            }
            Message::Barrier(_) => return Ok(()),
        }
    }
}

// Align the left and right inputs according to their barriers,
// such that in the produced stream, an aligned interval starts with
// `InternalMessage::Barrier(right_chunks, barrier)` and followed by
// any number of `InternalMessage::Chunk(left_chunk)`.
// Notice that we collect all chunks from the right side up front,
// prioritizing before any chunks from the left side in the same interval.
#[try_stream(ok = InternalMessage, error = StreamExecutorError)]
async fn align_input(left: Box<dyn Executor>, right: Box<dyn Executor>) {
    let mut left = Box::pin(left.execute());
    let mut right = Box::pin(right.execute());
    // Keep producing intervals until stream exhaustion or errors.
    loop {
        // Produce an aligned interval.
        'inner: loop {
            let mut combined = stream::select_with_strategy(
                left.by_ref().map(Either::Left),
                right.by_ref().map(Either::Right),
                |_: &mut ()| PollNext::Left,
            );
            let mut left_chunks = vec![];
            let mut right_chunks = vec![];
            match combined.next().await {
                Some(Either::Left(Ok(Message::Chunk(c)))) => left_chunks.push(c),
                Some(Either::Right(Ok(Message::Chunk(c)))) => right_chunks.push(c),
                Some(Either::Left(Ok(Message::Barrier(b)))) => {
                    for chunk in left_chunks {
                        yield InternalMessage::Chunk(chunk);
                    }
                    let mut remain = chunks_until_barrier(right.by_ref(), b.clone())
                        .try_collect()
                        .await?;
                    right_chunks.append(&mut remain);
                    yield InternalMessage::Barrier(right_chunks, b);
                    break 'inner;
                }
                Some(Either::Right(Ok(Message::Barrier(b)))) => {
                    #[for_await]
                    for chunk in chunks_until_barrier(left.by_ref(), b.clone()) {
                        yield InternalMessage::Chunk(chunk?);
                    }
                    yield InternalMessage::Barrier(right_chunks, b);
                    break 'inner;
                }
                Some(Either::Left(Err(e)) | Either::Right(Err(e))) => return Err(e),
                Some(
                    Either::Left(Ok(Message::Watermark(_)))
                    | Either::Right(Ok(Message::Watermark(_))),
                ) => todo!("https://github.com/risingwavelabs/risingwave/issues/6042"),
                None => return Ok(()),
            }
        }
    }
}

impl<S: StateStore> TemporalJoinExecutor<S> {
    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn into_stream(mut self) {
        let (left_map, right_map) = StreamChunkBuilder::get_i2o_mapping(
            self.output_indices.iter().cloned(),
            self.left.schema().len(),
            self.right_stream.schema().len(),
        );
        let mut prev_epoch = None;
        #[for_await]
        for msg in align_input(self.left, self.right_stream) {
            match msg? {
                InternalMessage::Chunk(chunk) => {
                    let mut builder = StreamChunkBuilder::new(
                        self.chunk_size,
                        &self.schema.data_types(),
                        left_map.clone(),
                        right_map.clone(),
                    );
                    let epoch = prev_epoch.expect("Chunk data should come after some barrier.");
                    for (op, row) in chunk.rows() {
                        let key = row.project(&self.left_join_keys);
                        if let Some(right_row) = self.right.lookup(key, epoch).await? {
                            if let Some(chunk) = builder.append_row(op, row, &right_row) {
                                yield Message::Chunk(chunk);
                            }
                        }
                    }
                    if let Some(chunk) = builder.take() {
                        yield Message::Chunk(chunk);
                    }
                }
                InternalMessage::Barrier(updates, barrier) => {
                    prev_epoch = Some(barrier.epoch.curr);
                    self.right.update(updates, &self.right_join_keys);
                    yield Message::Barrier(barrier)
                }
            }
        }
    }
}

impl<S: StateStore> Executor for TemporalJoinExecutor<S> {
    fn execute(self: Box<Self>) -> super::BoxedMessageStream {
        self.into_stream().boxed()
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn pk_indices(&self) -> super::PkIndicesRef<'_> {
        self.left.pk_indices()
    }

    fn identity(&self) -> &str {
        "TemporalJoinExecutor"
    }
}
