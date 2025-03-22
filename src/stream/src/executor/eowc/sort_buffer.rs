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

use std::collections::BTreeSet;
use std::marker::PhantomData;
use std::ops::Bound;

use anyhow::Context;
use bytes::Bytes;
use futures::StreamExt;
use futures_async_stream::{for_await, try_stream};
use risingwave_common::array::StreamChunk;
use risingwave_common::array::stream_record::Record;
use risingwave_common::hash::VnodeBitmapExt;
use risingwave_common::row::{self, OwnedRow, Row, RowExt};
use risingwave_common::types::{
    DefaultOrd, DefaultOrdered, ScalarImpl, ScalarRefImpl, ToOwnedDatum,
};
use risingwave_common::util::memcmp_encoding::MemcmpEncoded;
use risingwave_storage::StateStore;
use risingwave_storage::store::PrefetchOptions;
use risingwave_storage::table::KeyedRow;
use risingwave_storage::table::merge_sort::merge_sort;

use crate::common::state_cache::{StateCache, StateCacheFiller, TopNStateCache};
use crate::common::table::state_table::StateTable;
use crate::executor::{StreamExecutorError, StreamExecutorResult};

type CacheKey = (
    DefaultOrdered<ScalarImpl>, // sort (watermark) column value
    MemcmpEncoded,              // memcmp-encoded pk
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
    (timestamp_val.into(), pk.into())
}

// TODO(rc): need to make this configurable?
const CACHE_CAPACITY: usize = 2048;

/// [`SortBuffer`] is a common component that consume an unordered stream and produce an ordered
/// stream by watermark. This component maintains a buffer table passed in, whose schema is same as
/// [`SortBuffer`]'s input and output. Generally, the component acts as a buffer that output the
/// data it received with a delay, commonly used to implement emit-on-window-close policy.
pub struct SortBuffer<S: StateStore> {
    /// The timestamp column to sort on.
    sort_column_index: usize,

    /// Cache of buffer table.
    cache: TopNStateCache<CacheKey, OwnedRow>,

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
            cache: TopNStateCache::new(CACHE_CAPACITY),
            _phantom: PhantomData,
        }
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
        let mut last_table_pk = None;
        loop {
            if !self.cache.is_synced() {
                // Refill the cache, then consume from the cache, to ensure strong row ordering
                // and prefetch for the next watermark.
                self.refill_cache(last_table_pk.take(), buffer_table)
                    .await?;
            }

            #[for_await]
            for res in self.consume_from_cache(watermark.as_scalar_ref_impl()) {
                let row = res?;
                last_table_pk = Some((&row).project(buffer_table.pk_indices()).into_owned_row());
                yield row;
            }

            if self.cache.is_synced() {
                // The cache is still synced after consuming, meaning that there is no more rows
                // under the watermark to yield.
                break;
            }
        }

        // TODO(rc): Need something like `table.range_delete()`. Here we call
        // `update_watermark(watermark)` as an alternative to `range_delete((..watermark))`.
        buffer_table.update_watermark(watermark);
    }

    #[try_stream(ok = OwnedRow, error = StreamExecutorError)]
    async fn consume_from_cache<'a>(&'a mut self, watermark: ScalarRefImpl<'a>) {
        while self.cache.is_synced() {
            let Some(key) = self.cache.first_key_value().map(|(k, _)| k.clone()) else {
                break;
            };
            if key.0.as_scalar_ref_impl().default_cmp(&watermark).is_lt() {
                let row = self.cache.delete(&key).unwrap();
                yield row;
            } else {
                break;
            }
        }
    }

    /// Clear the cache and refill it with the current content of the buffer table.
    pub async fn refill_cache(
        &mut self,
        last_table_pk: Option<OwnedRow>,
        buffer_table: &StateTable<S>,
    ) -> StreamExecutorResult<()> {
        let mut filler = self.cache.begin_syncing();

        let pk_range = (
            last_table_pk
                .map(Bound::Excluded)
                .unwrap_or(Bound::Unbounded),
            Bound::<row::Empty>::Unbounded,
        );

        let streams: Vec<_> =
            futures::future::try_join_all(buffer_table.vnodes().iter_vnodes().map(|vnode| {
                buffer_table.iter_keyed_row_with_vnode(
                    vnode,
                    &pk_range,
                    PrefetchOptions::new(filler.capacity().is_none(), false),
                )
            }))
            .await?
            .into_iter()
            .map(Box::pin)
            .collect();

        #[for_await]
        for kv in merge_sort(streams).take(filler.capacity().unwrap_or(usize::MAX)) {
            let row = key_value_to_full_row(kv?, buffer_table)?;
            let key = row_to_cache_key(self.sort_column_index, &row, buffer_table);
            filler.insert_unchecked(key, row);
        }

        filler.finish();
        Ok(())
    }
}

/// Merge the key part and value part of a row into a full row. This is needed for state table with
/// non-None value indices.
fn key_value_to_full_row<S: StateStore>(
    keyed_row: KeyedRow<Bytes>,
    table: &StateTable<S>,
) -> StreamExecutorResult<OwnedRow> {
    let Some(val_indices) = table.value_indices() else {
        return Ok(keyed_row.into_owned_row());
    };
    let pk_indices = table.pk_indices();
    let indices: BTreeSet<_> = val_indices
        .iter()
        .chain(pk_indices.iter())
        .copied()
        .collect();
    let len = indices.iter().max().unwrap() + 1;
    assert!(indices.iter().copied().eq(0..len));

    let mut row = vec![None; len];
    let key = table
        .pk_serde()
        .deserialize(keyed_row.key())
        .context("failed to deserialize pk")?;
    for (i, v) in key.into_iter().enumerate() {
        row[pk_indices[i]] = v;
    }
    for (i, v) in keyed_row.into_owned_row().into_iter().enumerate() {
        row[val_indices[i]] = v;
    }
    Ok(OwnedRow::new(row))
}
