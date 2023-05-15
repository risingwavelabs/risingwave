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

// Copyright 2023 Singularity Data
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

use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use futures::{pin_mut, StreamExt};
use futures_async_stream::try_stream;
use itertools::{Group, Itertools};
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::catalog::Schema;
use risingwave_common::row::{self, CompactedRow, OwnedRow, Row, RowExt};
use risingwave_common::types::{ScalarImpl, ScalarRefImpl};
use risingwave_common::util::epoch::EpochPair;
use risingwave_common::util::sort_util::ColumnOrder;
use risingwave_storage::store::PrefetchOptions;
use risingwave_storage::StateStore;

use crate::cache::{new_unbounded, ManagedLruCache};
use crate::common::table::state_table::StateTable;
use crate::error::StreamResult;
use crate::executor::aggregation::{ChunkBuilder, ColumnDeduplicater};
use crate::executor::error::StreamExecutorResult;
use crate::executor::sort_buffer::SortBuffer;
use crate::executor::{
    expect_first_barrier, ActorContextRef, Executor, ExecutorInfo, Message, PkIndices,
    PkIndicesRef, StreamExecutorError, Watermark,
};
use crate::task::AtomicU64Ref;

pub struct EowcGroupTopNExecutor<S: StateStore, const WITH_TIES: bool> {
    input: Box<dyn Executor>,
    ctx: ActorContextRef,

    inner: Inner<S, WITH_TIES>,
}
pub struct Inner<S: StateStore, const WITH_TIES: bool> {
    info: ExecutorInfo,

    /// `LIMIT XXX`. None means no limit.
    limit: usize,

    /// `OFFSET XXX`. `0` means no offset.
    offset: usize,

    /// The storage key indices of the `GroupTopNExecutor`
    storage_key_indices: PkIndices,

    state_table: StateTable<S>,

    /// which column we used to group the data.
    group_by: Vec<usize>,
    order_by: Vec<usize>,

    chunk_size: usize,

    /// Latest watermark on window column.
    window_watermark: Option<Watermark>,

    group_keys: GroupKeys<S>,
}

impl<S: StateStore, const WITH_TIES: bool> EowcGroupTopNExecutor<S, WITH_TIES> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        input: Box<dyn Executor>,
        ctx: ActorContextRef,
        storage_key: Vec<ColumnOrder>,
        offset_and_limit: (usize, usize),
        order_by: Vec<ColumnOrder>,
        executor_id: u64,
        group_by: Vec<usize>,
        state_table: StateTable<S>,
        group_key_table: StateTable<S>,
        watermark_epoch: AtomicU64Ref,
        chunk_size: usize,
    ) -> StreamResult<Self> {
        let ExecutorInfo {
            pk_indices, schema, ..
        } = input.info();

        let group_keys = GroupKeys::new(&watermark_epoch, group_key_table, group_by.clone());

        Ok(Self {
            input,
            ctx,

            inner: Inner {
                info: ExecutorInfo {
                    schema,
                    pk_indices,
                    identity: format!("Inner {:X}", executor_id),
                },
                offset: offset_and_limit.0,
                limit: offset_and_limit.1,
                state_table,
                storage_key_indices: storage_key.into_iter().map(|op| op.column_index).collect(),
                group_by,
                order_by: order_by.into_iter().map(|op| op.column_index).collect(),
                window_watermark: None,
                chunk_size,
                group_keys,
            },
        })
    }
}

impl<S: StateStore, const WITH_TIES: bool> Inner<S, WITH_TIES> {
    #[try_stream(ok = StreamChunk, error = StreamExecutorError)]
    async fn flush_data(&mut self, epoch: EpochPair) {
        if let Some(watermark) = self.window_watermark.as_ref() {
            let mut chunk_builder =
                ChunkBuilder::new(self.chunk_size, &self.info.schema.data_types());

            // For each group_key, call iter_with_pk_prefix with it to get N rows.
            // TODO: can we consume group keys concurrently?
            #[for_await]
            for group_key in self.group_keys.consume(watermark.val.clone()) {
                let group_key = group_key?;

                let state_table_iter = self
                    .state_table
                    .iter_with_pk_prefix(group_key, PrefetchOptions::default())
                    .await?;
                pin_mut!(state_table_iter);

                if !WITH_TIES {
                    let mut idx = 0;
                    while let Some(row) = state_table_iter.next().await {
                        let row: OwnedRow = row?;
                        if idx >= self.offset {
                            if let Some(chunk) = chunk_builder.append_row(Op::Insert, row) {
                                yield chunk;
                            }
                        }
                        idx += 1;
                        if idx >= self.offset + self.limit {
                            break;
                        }
                    }
                } else {
                    assert!(self.offset == 0, "offset is not supported with ties yet");
                    assert!(self.limit > 0, "limit must be positive");

                    let row = state_table_iter.next().await;
                    let row: OwnedRow = row.unwrap()?;
                    if let Some(chunk) = chunk_builder.append_row(Op::Insert, row.clone()) {
                        yield chunk;
                    }

                    // rank, idx both start from 0
                    let mut rank;
                    let mut idx = 1;
                    let mut prev_row = row;

                    while let Some(row) = state_table_iter.next().await {
                        let row: OwnedRow = row?;

                        // get the rank of the current row. If ties, rank unchanged.
                        if row.as_ref().project(&self.order_by)
                            != prev_row.as_ref().project(&self.order_by)
                        {
                            rank = idx;
                            if rank >= self.limit {
                                break;
                            }
                        }

                        if let Some(chunk) = chunk_builder.append_row(Op::Insert, row.clone()) {
                            yield chunk;
                        }

                        prev_row = row;
                        idx += 1;
                    }
                }
            }

            if let Some(chunk) = chunk_builder.take() {
                yield chunk;
            }

            // TODO(rc): Need something like `table.range_delete()`. Here we call
            // `update_watermark(watermark, true)` as an alternative to
            // `range_delete((..watermark))`.
            self.state_table
                .update_watermark(watermark.val.clone(), true);
        }

        self.group_keys.flush();

        self.state_table.commit(epoch).await?;
    }

    async fn init(&mut self, epoch: EpochPair) -> StreamExecutorResult<()> {
        self.state_table.init_epoch(epoch);
        Ok(())
    }
}

impl<S: StateStore, const WITH_TIES: bool> EowcGroupTopNExecutor<S, WITH_TIES> {
    /// We remark that topN executor diffs from aggregate executor as it must output diffs
    /// whenever it applies a batch of input data. Therefore, topN executor flushes data only
    /// instead of computing diffs and flushing when receiving a barrier.
    #[try_stream(ok = Message, error = StreamExecutorError)]
    pub(crate) async fn top_n_executor_execute(self: Box<Self>) {
        let input = self.input;
        let mut inner = self.inner;

        let mut input = input.execute();
        let barrier = expect_first_barrier(&mut input).await?;
        inner.init(barrier.epoch).await?;

        yield Message::Barrier(barrier);

        #[for_await]
        for msg in input {
            let msg = msg?;
            match msg {
                Message::Watermark(watermark) => {
                    if watermark.col_idx == inner.group_by[0] {
                        inner.window_watermark = Some(watermark.with_idx(0));
                    }
                }
                Message::Chunk(chunk) => {
                    inner.group_keys.apply_chunk(&chunk).await?;
                    inner.state_table.write_chunk(chunk);
                }
                Message::Barrier(barrier) => {
                    #[for_await]
                    for chunk in inner.flush_data(barrier.epoch) {
                        yield Message::Chunk(chunk?);
                    }

                    if let Some(watermark) = &inner.window_watermark {
                        yield Message::Watermark(watermark.clone());
                    }

                    // Update the vnode bitmap, only used by Group Top-N.
                    if let Some(vnode_bitmap) = barrier.as_update_vnode_bitmap(self.ctx.id) {
                        let (_previous_vnode_bitmap, _cache_may_stale) =
                            inner.state_table.update_vnode_bitmap(vnode_bitmap);
                    }

                    yield Message::Barrier(barrier)
                }
            };
        }
    }
}

impl<S: StateStore, const WITH_TIES: bool> Executor for EowcGroupTopNExecutor<S, WITH_TIES> {
    fn execute(self: Box<Self>) -> crate::executor::BoxedMessageStream {
        self.top_n_executor_execute().boxed()
    }

    fn schema(&self) -> &Schema {
        &self.inner.info.schema
    }

    fn pk_indices(&self) -> PkIndicesRef<'_> {
        &self.inner.info.pk_indices
    }

    fn identity(&self) -> &str {
        &self.inner.info.identity
    }

    fn info(&self) -> ExecutorInfo {
        self.inner.info.clone()
    }
}

/// Maintains a state table for getting all the group keys
struct GroupKeys<S: StateStore> {
    dedup: ColumnDeduplicater<S>,

    group_key_table: StateTable<S>,
    group_key: Vec<usize>,
    /// XXX: Does this sort_buffer make sense? It's only used in `consume`.
    sort_buffer: SortBuffer<S>,
}

impl<S: StateStore> GroupKeys<S> {
    fn new(
        watermark_epoch: &Arc<AtomicU64>,
        group_key_table: StateTable<S>,
        group_key: Vec<usize>,
    ) -> Self {
        Self {
            dedup: ColumnDeduplicater::new(watermark_epoch),
            sort_buffer: SortBuffer::new(0, &group_key_table),
            group_key_table,
            group_key,
        }
    }

    async fn apply_chunk(&mut self, chunk: &StreamChunk) -> StreamExecutorResult<()> {
        let columns = self
            .group_key
            .iter()
            .map(|i| chunk.column_at(*i).clone())
            .collect_vec();
        let mut vis = chunk.vis().clone();
        self.dedup
            .dedup(
                chunk.ops(),
                &columns,
                vec![&mut vis],
                &mut self.group_key_table,
                None,
            )
            .await
    }

    /// Flush the deduplication table.
    fn flush(&mut self) {
        self.dedup.flush(&mut self.group_key_table);
    }

    /// Consume all group keys under `watermark`.
    #[try_stream(ok = OwnedRow, error = StreamExecutorError)]
    pub async fn consume<'a>(&'a mut self, watermark: ScalarImpl) {
        #[for_await]
        for row in self
            .sort_buffer
            .consume(watermark, &mut self.group_key_table)
        {
            yield row?;
        }
    }
}

mod tests {
    // TODO:
}
