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

use std::alloc::Global;
use std::sync::Arc;

use either::Either;
use futures::stream::{self, PollNext};
use futures::{StreamExt, TryStreamExt};
use futures_async_stream::try_stream;
use local_stats_alloc::{SharedStatsAlloc, StatsAlloc};
use lru::DefaultHasher;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::catalog::Schema;
use risingwave_common::row::{OwnedRow, Row, RowExt};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_expr::expr::BoxedExpression;
use risingwave_hummock_sdk::{HummockEpoch, HummockReadEpoch};
use risingwave_storage::table::batch_table::storage_table::StorageTable;
use risingwave_storage::StateStore;

use super::{Barrier, Executor, Message, MessageStream, StreamExecutorError, StreamExecutorResult};
use crate::cache::{cache_may_stale, new_with_hasher_in, ManagedLruCache};
use crate::common::metrics::MetricsInfo;
use crate::common::StreamChunkBuilder;
use crate::executor::monitor::StreamingMetrics;
use crate::executor::{ActorContextRef, BoxedExecutor, JoinType, JoinTypePrimitive, PkIndices};
use crate::task::AtomicU64Ref;

pub struct TemporalJoinExecutor<S: StateStore, const T: JoinTypePrimitive> {
    ctx: ActorContextRef,
    left: BoxedExecutor,
    right: BoxedExecutor,
    right_table: TemporalSide<S>,
    left_join_keys: Vec<usize>,
    right_join_keys: Vec<usize>,
    null_safe: Vec<bool>,
    condition: Option<BoxedExpression>,
    output_indices: Vec<usize>,
    pk_indices: PkIndices,
    schema: Schema,
    chunk_size: usize,
    identity: String,
    // TODO: update metrics
    #[allow(dead_code)]
    metrics: Arc<StreamingMetrics>,
}

struct TemporalSide<S: StateStore> {
    source: StorageTable<S>,
    table_output_indices: Vec<usize>,
    cache: ManagedLruCache<OwnedRow, Option<OwnedRow>, DefaultHasher, SharedStatsAlloc<Global>>,
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
                    .await?
                    .map(|row| row.project(&self.table_output_indices).into_owned_row());
                self.cache.put(key, res.clone());
                res
            }
        })
    }

    fn update(&mut self, payload: Vec<StreamChunk>, join_keys: &[usize]) {
        payload.iter().flat_map(|c| c.rows()).for_each(|(op, row)| {
            let key = row.project(join_keys).into_owned_row();
            if let Some(mut value) = self.cache.get_mut(&key) {
                match op {
                    Op::Insert | Op::UpdateInsert => *value = Some(row.into_owned_row()),
                    Op::Delete | Op::UpdateDelete => *value = None,
                };
            }
        });
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
// any number of `InternalMessage::Chunk(left_chunk)` and followed by
// `InternalMessage::Barrier(right_chunks, barrier)`.
#[try_stream(ok = InternalMessage, error = StreamExecutorError)]
async fn align_input(left: Box<dyn Executor>, right: Box<dyn Executor>) {
    let mut left = Box::pin(left.execute());
    let mut right = Box::pin(right.execute());
    // Keep producing intervals until stream exhaustion or errors.
    loop {
        let mut right_chunks = vec![];
        // Produce an aligned interval.
        'inner: loop {
            let mut combined = stream::select_with_strategy(
                left.by_ref().map(Either::Left),
                right.by_ref().map(Either::Right),
                |_: &mut ()| PollNext::Left,
            );
            match combined.next().await {
                Some(Either::Left(Ok(Message::Chunk(c)))) => yield InternalMessage::Chunk(c),
                Some(Either::Right(Ok(Message::Chunk(c)))) => right_chunks.push(c),
                Some(Either::Left(Ok(Message::Barrier(b)))) => {
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

impl<S: StateStore, const T: JoinTypePrimitive> TemporalJoinExecutor<S, T> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        ctx: ActorContextRef,
        left: BoxedExecutor,
        right: BoxedExecutor,
        table: StorageTable<S>,
        left_join_keys: Vec<usize>,
        right_join_keys: Vec<usize>,
        null_safe: Vec<bool>,
        condition: Option<BoxedExpression>,
        pk_indices: PkIndices,
        output_indices: Vec<usize>,
        table_output_indices: Vec<usize>,
        executor_id: u64,
        watermark_epoch: AtomicU64Ref,
        metrics: Arc<StreamingMetrics>,
        chunk_size: usize,
    ) -> Self {
        let schema_fields = [left.schema().fields.clone(), right.schema().fields.clone()].concat();

        let schema: Schema = output_indices
            .iter()
            .map(|&idx| schema_fields[idx].clone())
            .collect();

        let alloc = StatsAlloc::new(Global).shared();

        let metrics_info = MetricsInfo::new(metrics.clone(), table.table_id().table_id, ctx.id);

        let cache = new_with_hasher_in(
            watermark_epoch,
            metrics_info,
            DefaultHasher::default(),
            alloc,
        );

        Self {
            ctx,
            left,
            right,
            right_table: TemporalSide {
                source: table,
                table_output_indices,
                cache,
            },
            left_join_keys,
            right_join_keys,
            null_safe,
            condition,
            output_indices,
            schema,
            chunk_size,
            pk_indices,
            identity: format!("TemporalJoinExecutor {:X}", executor_id),
            metrics,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn into_stream(mut self) {
        let (left_map, right_map) = StreamChunkBuilder::get_i2o_mapping(
            self.output_indices.iter().cloned(),
            self.left.schema().len(),
            self.right.schema().len(),
        );

        let mut prev_epoch = None;
        #[for_await]
        for msg in align_input(self.left, self.right) {
            self.right_table.cache.evict();

            match msg? {
                InternalMessage::Chunk(chunk) => {
                    let mut builder = StreamChunkBuilder::new(
                        self.chunk_size,
                        &self.schema.data_types(),
                        left_map.clone(),
                        right_map.clone(),
                    );
                    let epoch = prev_epoch.expect("Chunk data should come after some barrier.");
                    for (op, left_row) in chunk.rows() {
                        let key = left_row.project(&self.left_join_keys);
                        if key
                            .iter()
                            .zip_eq_fast(self.null_safe.iter())
                            .any(|(datum, can_null)| datum.is_none() && !*can_null)
                        {
                            continue;
                        }
                        if let Some(right_row) = self.right_table.lookup(key, epoch).await? {
                            // check join condition
                            let ok = if let Some(ref mut cond) = self.condition {
                                let concat_row = left_row.chain(&right_row).into_owned_row();
                                cond.eval_row_infallible(&concat_row, |err| {
                                    self.ctx.on_compute_error(err, self.identity.as_str())
                                })
                                .await
                                .map(|s| *s.as_bool())
                                .unwrap_or(false)
                            } else {
                                true
                            };
                            if ok {
                                if let Some(chunk) = builder.append_row(op, left_row, &right_row) {
                                    yield Message::Chunk(chunk);
                                }
                            }
                        } else if T == JoinType::LeftOuter {
                            if let Some(chunk) = builder.append_row_update(op, left_row) {
                                yield Message::Chunk(chunk);
                            }
                        }
                    }
                    if let Some(chunk) = builder.take() {
                        yield Message::Chunk(chunk);
                    }
                }
                InternalMessage::Barrier(updates, barrier) => {
                    if let Some(vnodes) = barrier.as_update_vnode_bitmap(self.ctx.id) {
                        let prev_vnodes =
                            self.right_table.source.update_vnode_bitmap(vnodes.clone());
                        if cache_may_stale(&prev_vnodes, &vnodes) {
                            self.right_table.cache.clear();
                        }
                    }
                    self.right_table.cache.update_epoch(barrier.epoch.curr);
                    self.right_table.update(updates, &self.right_join_keys);
                    prev_epoch = Some(barrier.epoch.curr);
                    yield Message::Barrier(barrier)
                }
            }
        }
    }
}

impl<S: StateStore, const T: JoinTypePrimitive> Executor for TemporalJoinExecutor<S, T> {
    fn execute(self: Box<Self>) -> super::BoxedMessageStream {
        self.into_stream().boxed()
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn pk_indices(&self) -> super::PkIndicesRef<'_> {
        &self.pk_indices
    }

    fn identity(&self) -> &str {
        self.identity.as_str()
    }
}
