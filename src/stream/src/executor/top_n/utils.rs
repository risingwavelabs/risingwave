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

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::Schema;
use risingwave_common::row::{CompactedRow, Row, RowDeserializer};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common::util::epoch::EpochPair;
use risingwave_common::util::ordered::OrderedRowSerde;
use risingwave_common::util::sort_util::ColumnOrder;

use super::top_n_cache::CacheKey;
use crate::executor::error::{StreamExecutorError, StreamExecutorResult};
use crate::executor::{
    expect_first_barrier, ActorContextRef, BoxedExecutor, BoxedMessageStream, Executor,
    ExecutorInfo, Message, PkIndicesRef, Watermark,
};

#[async_trait]
pub trait TopNExecutorBase: Send + 'static {
    /// Apply the chunk to the dirty state and get the diffs.
    async fn apply_chunk(&mut self, chunk: StreamChunk) -> StreamExecutorResult<StreamChunk>;

    /// Flush the buffered chunk to the storage backend.
    async fn flush_data(&mut self, epoch: EpochPair) -> StreamExecutorResult<()>;

    fn info(&self) -> &ExecutorInfo;

    /// See [`Executor::schema`].
    fn schema(&self) -> &Schema {
        &self.info().schema
    }

    /// See [`Executor::pk_indices`].
    fn pk_indices(&self) -> PkIndicesRef<'_> {
        self.info().pk_indices.as_ref()
    }

    /// See [`Executor::identity`].
    fn identity(&self) -> &str {
        &self.info().identity
    }

    /// Update the vnode bitmap for the state table and manipulate the cache if necessary, only used
    /// by Group Top-N since it's distributed.
    fn update_vnode_bitmap(&mut self, _vnode_bitmap: Arc<Bitmap>) {
        unreachable!()
    }

    fn evict(&mut self) {}
    async fn init(&mut self, epoch: EpochPair) -> StreamExecutorResult<()>;

    /// Handle incoming watermarks
    async fn handle_watermark(&mut self, watermark: Watermark) -> Option<Watermark>;
}

/// The struct wraps a [`TopNExecutorBase`]
pub struct TopNExecutorWrapper<E> {
    pub(super) input: BoxedExecutor,
    pub(super) ctx: ActorContextRef,
    pub(super) inner: E,
}

impl<E> Executor for TopNExecutorWrapper<E>
where
    E: TopNExecutorBase,
{
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.top_n_executor_execute().boxed()
    }

    fn schema(&self) -> &Schema {
        self.inner.schema()
    }

    fn pk_indices(&self) -> PkIndicesRef<'_> {
        self.inner.pk_indices()
    }

    fn identity(&self) -> &str {
        self.inner.identity()
    }

    fn info(&self) -> ExecutorInfo {
        self.inner.info().clone()
    }
}

impl<E> TopNExecutorWrapper<E>
where
    E: TopNExecutorBase,
{
    /// We remark that topN executor diffs from aggregate executor as it must output diffs
    /// whenever it applies a batch of input data. Therefore, topN executor flushes data only
    /// instead of computing diffs and flushing when receiving a barrier.
    #[try_stream(ok = Message, error = StreamExecutorError)]
    pub(crate) async fn top_n_executor_execute(mut self: Box<Self>) {
        let mut input = self.input.execute();

        let barrier = expect_first_barrier(&mut input).await?;
        self.inner.init(barrier.epoch).await?;

        yield Message::Barrier(barrier);

        #[for_await]
        for msg in input {
            let msg = msg?;
            match msg {
                Message::Watermark(watermark) => {
                    if let Some(output_watermark) = self.inner.handle_watermark(watermark).await {
                        yield Message::Watermark(output_watermark);
                    }
                }
                Message::Chunk(chunk) => yield Message::Chunk(self.inner.apply_chunk(chunk).await?),
                Message::Barrier(barrier) => {
                    self.inner.flush_data(barrier.epoch).await?;

                    // Update the vnode bitmap, only used by Group Top-N.
                    if let Some(vnode_bitmap) = barrier.as_update_vnode_bitmap(self.ctx.id) {
                        self.inner.update_vnode_bitmap(vnode_bitmap);
                    }
                    self.inner.evict();
                    yield Message::Barrier(barrier)
                }
            };
        }
    }
}

pub fn generate_output(
    new_rows: Vec<CompactedRow>,
    new_ops: Vec<Op>,
    schema: &Schema,
) -> StreamExecutorResult<StreamChunk> {
    if !new_rows.is_empty() {
        let mut data_chunk_builder = DataChunkBuilder::new(schema.data_types(), new_rows.len() + 1);
        let row_deserializer = RowDeserializer::new(schema.data_types());
        for compacted_row in new_rows {
            let res = data_chunk_builder
                .append_one_row(row_deserializer.deserialize(compacted_row.row.as_ref())?);
            debug_assert!(res.is_none());
        }
        // since `new_rows` is not empty, we unwrap directly
        let new_data_chunk = data_chunk_builder.consume_all().unwrap();
        let new_stream_chunk = StreamChunk::new(new_ops, new_data_chunk.columns().to_vec(), None);
        Ok(new_stream_chunk)
    } else {
        let columns = schema
            .create_array_builders(0)
            .into_iter()
            .map(|x| x.finish().into())
            .collect_vec();
        Ok(StreamChunk::new(vec![], columns, None))
    }
}

/// For a given pk (Row), it can be split into `order_key` and `additional_pk` according to
/// `order_by_len`, and the two split parts are serialized separately.
pub fn serialize_pk_to_cache_key(pk: impl Row, cache_key_serde: &CacheKeySerde) -> CacheKey {
    // TODO(row trait): may support splitting row
    let pk = pk.into_owned_row().into_inner();
    let (cache_key_first, cache_key_second) = pk.split_at(cache_key_serde.2);
    (
        cache_key_first.memcmp_serialize(&cache_key_serde.0),
        cache_key_second.memcmp_serialize(&cache_key_serde.1),
    )
}

/// See [`CacheKey`].
///
/// The last `usize` is the length of `order_by`, i.e., the first part of the key.
pub type CacheKeySerde = (OrderedRowSerde, OrderedRowSerde, usize);

pub fn create_cache_key_serde(
    storage_key: &[ColumnOrder],
    pk_indices: PkIndicesRef<'_>,
    schema: &Schema,
    order_by: &[ColumnOrder],
    group_by: &[usize],
) -> CacheKeySerde {
    {
        // validate storage_key = group_by + order_by + additional_pk
        for i in 0..group_by.len() {
            assert_eq!(storage_key[i].column_index, group_by[i]);
        }
        for i in group_by.len()..(group_by.len() + order_by.len()) {
            assert_eq!(storage_key[i], order_by[i - group_by.len()]);
        }
        let pk_indices = pk_indices.iter().copied().collect::<HashSet<_>>();
        for i in (group_by.len() + order_by.len())..storage_key.len() {
            assert!(
                pk_indices.contains(&storage_key[i].column_index),
                "storage_key = {:?}, pk_indices = {:?}",
                storage_key,
                pk_indices
            );
        }
    }

    let (cache_key_data_types, cache_key_order_types): (Vec<_>, Vec<_>) = storage_key
        [group_by.len()..]
        .iter()
        .map(|o| (schema[o.column_index].data_type(), o.order_type))
        .unzip();

    let order_by_len = order_by.len();
    let (first_key_data_types, second_key_data_types) = cache_key_data_types.split_at(order_by_len);
    let (first_key_order_types, second_key_order_types) =
        cache_key_order_types.split_at(order_by_len);
    let first_key_serde = OrderedRowSerde::new(
        first_key_data_types.to_vec(),
        first_key_order_types.to_vec(),
    );
    let second_key_serde = OrderedRowSerde::new(
        second_key_data_types.to_vec(),
        second_key_order_types.to_vec(),
    );
    (first_key_serde, second_key_serde, order_by_len)
}
