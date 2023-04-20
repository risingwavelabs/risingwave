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

use futures::{pin_mut, StreamExt};
use futures_async_stream::try_stream;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::catalog::Schema;
use risingwave_common::row::{OwnedRow, Row, RowExt};
use risingwave_common::types::ScalarImpl;
use risingwave_common::util::epoch::EpochPair;
use risingwave_common::util::sort_util::ColumnOrder;
use risingwave_storage::store::PrefetchOptions;
use risingwave_storage::StateStore;

use crate::common::table::state_table::StateTable;
use crate::common::StreamChunkBuilder;
use crate::error::StreamResult;
use crate::executor::aggregation::ChunkBuilder;
use crate::executor::error::StreamExecutorResult;
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
    order_by: Vec<ColumnOrder>,

    chunk_size: usize,

    /// Latest watermark on window column.
    window_watermark: Option<Watermark>,
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
        _watermark_epoch: AtomicU64Ref,
        chunk_size: usize,
    ) -> StreamResult<Self> {
        let ExecutorInfo {
            pk_indices, schema, ..
        } = input.info();

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
                order_by,
                window_watermark: None,
                chunk_size,
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

            // iterate over the state table
            {
                let state_table_iter = self
                    .state_table
                    .iter(PrefetchOptions {
                        exhaust_iter: false,
                    })
                    .await?;
                pin_mut!(state_table_iter);

                let mut current_group_key: Option<OwnedRow> = None;
                'outer: loop {
                    let mut cnt = 0;
                    while let Some(item) = state_table_iter.next().await {
                        let item: OwnedRow = item?;
                        let group_key = item.as_ref().project(&self.group_by);

                        if current_group_key.is_none()
                            || current_group_key.as_ref().is_some_and(|current_group_key| {
                                !Row::eq(current_group_key, group_key)
                            })
                        {
                            // new group key found.
                            let timestamp_val = group_key
                                .datum_at(0)
                                .expect("watermark column is expected to be non-null");
                            if timestamp_val > watermark.val.as_scalar_ref_impl() {
                                break 'outer;
                            }
                            current_group_key = Some(group_key.into_owned_row());
                            cnt = 0;
                        }

                        // check whether to insert the row.
                        let insert = if WITH_TIES {
                            assert!(self.offset == 0, "offset is not supported with ties yet");
                            todo!()
                        } else {
                            self.offset <= cnt && cnt <= self.limit
                        };

                        if insert {
                            cnt += 1;
                            if let Some(chunk) = chunk_builder.append_row(Op::Insert, item) {
                                yield chunk;
                            }
                        } else {
                            // TODO: should we create a new state_table_iter to skip to the next
                            // group key?
                            // And how? iter_with_pk_range requires a vnode.
                        }
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
                Message::Chunk(chunk) => inner.state_table.write_chunk(chunk),
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

mod tests {
    // TODO:
}
