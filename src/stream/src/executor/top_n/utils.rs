// Copyright 2024 RisingWave Labs
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

use std::future::Future;

use risingwave_common::bitmap::Bitmap;
use risingwave_common::util::epoch::EpochPair;
use risingwave_common::util::row_serde::OrderedRowSerde;
use risingwave_common::util::sort_util::ColumnOrder;

use super::top_n_cache::CacheKey;
use crate::executor::prelude::*;

pub trait TopNExecutorBase: Send + 'static {
    /// Apply the chunk to the dirty state and get the diffs.
    /// TODO(rc): There can be a 2 times amplification in terms of the chunk size, so we may need to
    /// allow `apply_chunk` return a stream of chunks. Motivation is not quite strong though.
    fn apply_chunk(
        &mut self,
        chunk: StreamChunk,
    ) -> impl Future<Output = StreamExecutorResult<Option<StreamChunk>>> + Send;

    /// Flush the buffered chunk to the storage backend.
    fn flush_data(
        &mut self,
        epoch: EpochPair,
    ) -> impl Future<Output = StreamExecutorResult<()>> + Send;

    /// Flush the buffered chunk to the storage backend.
    fn try_flush_data(&mut self) -> impl Future<Output = StreamExecutorResult<()>> + Send;

    /// Update the vnode bitmap for the state table and manipulate the cache if necessary, only used
    /// by Group Top-N since it's distributed.
    fn update_vnode_bitmap(&mut self, _vnode_bitmap: Arc<Bitmap>) {
        unreachable!()
    }

    fn evict(&mut self) {}

    fn init(&mut self, epoch: EpochPair) -> impl Future<Output = StreamExecutorResult<()>> + Send;

    /// Handle incoming watermarks
    fn handle_watermark(
        &mut self,
        watermark: Watermark,
    ) -> impl Future<Output = Option<Watermark>> + Send;
}

/// The struct wraps a [`TopNExecutorBase`]
pub struct TopNExecutorWrapper<E> {
    pub(super) input: Executor,
    pub(super) ctx: ActorContextRef,
    pub(super) inner: E,
}

impl<E> Execute for TopNExecutorWrapper<E>
where
    E: TopNExecutorBase,
{
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.top_n_executor_execute().boxed()
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
        let barrier_epoch = barrier.epoch;
        yield Message::Barrier(barrier);
        self.inner.init(barrier_epoch).await?;

        #[for_await]
        for msg in input {
            self.inner.evict();
            let msg = msg?;
            match msg {
                Message::Watermark(watermark) => {
                    if let Some(output_watermark) = self.inner.handle_watermark(watermark).await {
                        yield Message::Watermark(output_watermark);
                    }
                }
                Message::Chunk(chunk) => {
                    if let Some(output_chunk) = self.inner.apply_chunk(chunk).await? {
                        yield Message::Chunk(output_chunk);
                    }
                    self.inner.try_flush_data().await?;
                }
                Message::Barrier(barrier) => {
                    self.inner.flush_data(barrier.epoch).await?;

                    // Update the vnode bitmap, only used by Group Top-N.
                    if let Some(vnode_bitmap) = barrier.as_update_vnode_bitmap(self.ctx.id) {
                        self.inner.update_vnode_bitmap(vnode_bitmap);
                    }

                    yield Message::Barrier(barrier)
                }
            };
        }
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

use risingwave_common::row;
pub trait GroupKey = row::Row + Send + Sync;
pub const NO_GROUP_KEY: Option<row::Empty> = None;
