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

use std::marker::PhantomData;
use std::ops::Bound;

use futures::stream;
use futures_async_stream::{for_await, try_stream};
use risingwave_common::array::stream_record::Record;
use risingwave_common::array::StreamChunk;
use risingwave_common::hash::VnodeBitmapExt;
use risingwave_common::row::{self, OwnedRow, Row, RowExt};
use risingwave_common::types::{OrdScalarImpl, ScalarImpl, ToOwnedDatum};
use risingwave_storage::store::PrefetchOptions;
use risingwave_storage::StateStore;

use super::{StreamExecutorError, StreamExecutorResult};
use crate::common::cache::{OrderedStateCache, StateCache, StateCacheFiller};
use crate::common::table::state_table::StateTable;

// TODO(rc): This should be a struct in `memcmp_encoding` module. See #8606.
type MemcmpEncoded = Box<[u8]>;

type CacheKey = (
    OrdScalarImpl, // sort (watermark) column value
    MemcmpEncoded, // memcmp-encoded pk
);

fn row_to_cache_key<S: StateStore>(
    sort_column_index: usize,
    row: impl Row,
    buffer_table: &StateTable<S>,
) -> CacheKey {
    let timestamp_val = row
        .datum_at(sort_column_index)
        .to_owned_datum()
        .expect("watermark column is expected to be non-null");
    let mut pk = vec![];
    buffer_table
        .pk_serde()
        .serialize((&row).project(buffer_table.pk_indices()), &mut pk);
    (timestamp_val.into(), pk.into_boxed_slice())
}

/// [`SortBuffer`] is a common component that consume an unordered stream and produce an ordered
/// stream by watermark. This component maintains a buffer table passed in, whose schema is same as
/// [`SortBuffer`]'s input and output. Generally, the component acts as a buffer that output the
/// data it received with a delay, commonly used to implement emit-on-window-close policy.
pub struct SortBuffer<S: StateStore> {
    /// The timestamp column to sort on.
    sort_column_index: usize,

    /// Cache of buffer table. `TopNStateCache` also works, may switch to it if needed later.
    cache: OrderedStateCache<CacheKey, OwnedRow>,

    _phantom: PhantomData<S>,
}

impl<S: StateStore> SortBuffer<S> {
    /// Create a new [`SortBuffer`].
    pub fn new(sort_column_index: usize, buffer_table: &StateTable<S>) -> Self {
        assert_eq!(
            sort_column_index,
            buffer_table.pk_indices()[0],
            "the column to sort on must be the first pk column of the buffer table"
        );

        Self {
            sort_column_index,
            cache: OrderedStateCache::new(),
            _phantom: PhantomData,
        }
    }

    /// Recover a [`SortBuffer`] from a buffer table.
    #[allow(dead_code)]
    pub async fn recover(
        sort_column_index: usize,
        buffer_table: &StateTable<S>,
    ) -> StreamExecutorResult<Self> {
        let mut this = Self::new(sort_column_index, buffer_table);
        this.refill_cache(None, buffer_table).await?;
        Ok(this)
    }

    /// Insert a new row into the buffer.
    pub fn insert(&mut self, new_row: impl Row, buffer_table: &mut StateTable<S>) {
        buffer_table.insert(&new_row);
        let key = row_to_cache_key(self.sort_column_index, &new_row, buffer_table);
        self.cache.insert(key, new_row.into_owned_row());
    }

    /// Delete a row from the buffer.
    pub fn delete(&mut self, old_row: impl Row, buffer_table: &mut StateTable<S>) {
        buffer_table.delete(&old_row);
        let key = row_to_cache_key(self.sort_column_index, &old_row, buffer_table);
        self.cache.delete(&key);
    }

    /// Update a row in the buffer.
    pub fn update(
        &mut self,
        old_row: impl Row,
        new_row: impl Row,
        buffer_table: &mut StateTable<S>,
    ) {
        buffer_table.update(&old_row, &new_row);
        let key = row_to_cache_key(self.sort_column_index, &old_row, buffer_table);
        self.cache.delete(&key);
        self.cache.insert(key, new_row.into_owned_row());
    }

    /// Apply a change to the buffer, insert/delete/update.
    pub fn apply_change(&mut self, change: Record<impl Row>, buffer_table: &mut StateTable<S>) {
        match change {
            Record::Insert { new_row } => self.insert(new_row, buffer_table),
            Record::Delete { old_row } => self.delete(old_row, buffer_table),
            Record::Update { old_row, new_row } => self.update(old_row, new_row, buffer_table),
        }
    }

    /// Apply a stream chunk to the buffer.
    pub fn apply_chunk(&mut self, chunk: StreamChunk, buffer_table: &mut StateTable<S>) {
        for record in chunk.records() {
            self.apply_change(record, buffer_table);
        }
    }

    /// Consume rows under `watermark` from the buffer.
    #[try_stream(ok = OwnedRow, error = StreamExecutorError)]
    pub async fn consume<'a>(
        &'a mut self,
        watermark: ScalarImpl,
        buffer_table: &'a mut StateTable<S>,
    ) {
        let mut last_timestamp = None;
        loop {
            if !self.cache.is_synced() {
                // Refill the cache, then consume from the cache, to ensure strong row ordering
                // and prefetch for the next watermark.
                self.refill_cache(last_timestamp.take(), buffer_table)
                    .await?;
            }

            #[for_await]
            for res in self.consume_from_cache(&watermark) {
                let ((timestamp_val, _), row) = res?;
                last_timestamp = Some(timestamp_val.into_inner());
                yield row;
            }

            if self.cache.is_synced() {
                // The cache is still synced after consuming, meaning that there is no more rows
                // under the watermark to yield.
                break;
            }
        }

        // TODO(rc): Need something like `table.range_delete()`. Here we call
        // `update_watermark(watermark, true)` as an alternative to `range_delete((..watermark))`.
        buffer_table.update_watermark(watermark, true);
    }

    #[try_stream(ok = (CacheKey, OwnedRow), error = StreamExecutorError)]
    async fn consume_from_cache<'a>(&'a mut self, watermark: &'a ScalarImpl) {
        assert!(self.cache.is_synced());
        while let Some(key) = self.cache.first_key_value().map(|(k, _)| k.clone()) {
            if key.0.as_ref() < watermark.as_scalar_ref_impl().into() {
                let row = self.cache.delete(&key).unwrap();
                yield (key, row);
            } else {
                break;
            }
        }
    }

    /// Clear the cache and refill it with the current content of the buffer table.
    pub async fn refill_cache(
        &mut self,
        last_timestamp: Option<ScalarImpl>,
        buffer_table: &StateTable<S>,
    ) -> StreamExecutorResult<()> {
        let mut filler = self.cache.begin_syncing();

        let pk_range = (
            last_timestamp
                .as_ref()
                .map(|v| Bound::Excluded([Some(v.as_scalar_ref_impl())]))
                .unwrap_or(Bound::Unbounded),
            Bound::<row::Empty>::Unbounded,
        );

        let streams =
            futures::future::try_join_all(buffer_table.vnode_bitmap().iter_vnodes().map(|vnode| {
                buffer_table.iter_with_pk_range(
                    &pk_range,
                    vnode,
                    PrefetchOptions::new_for_exhaust_iter(),
                )
            }))
            .await?
            .into_iter()
            .map(Box::pin);

        #[for_await]
        for row in stream::select_all(streams) {
            // NOTE: The rows may not appear in order.
            let row: OwnedRow = row?;
            let key = row_to_cache_key(self.sort_column_index, &row, buffer_table);
            filler.insert_unchecked(key, row);
        }

        filler.finish();
        Ok(())
    }
}
