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
use futures_async_stream::try_stream;
use risingwave_common::array::stream_record::Record;
use risingwave_common::hash::VnodeBitmapExt;
use risingwave_common::row::{self, OwnedRow, Row};
use risingwave_common::types::ScalarImpl;
use risingwave_storage::store::PrefetchOptions;
use risingwave_storage::StateStore;

use super::StreamExecutorError;
use crate::common::table::state_table::StateTable;

// TODO(rc): Now it assumes first column is the watermark column and has no in-memory cache,
// later we will support specifying watermark column index and in-memory cache.
/// [`SortBuffer`] is a common component that consume an unordered stream and produce an ordered
/// stream by watermark. This component maintains a buffer table passed in, whose schema is same as
/// [`SortBuffer`]'s input and output. Generally, the component acts as a buffer that output the
/// data it received with a delay, commonly used to implement emit-on-window-close policy.
pub struct SortBuffer<S: StateStore> {
    _phantom: PhantomData<S>,
}

impl<S: StateStore> SortBuffer<S> {
    pub fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }

    /// Insert a new row into the buffer.
    pub fn insert(&mut self, new_row: impl Row, buffer_table: &mut StateTable<S>) {
        buffer_table.insert(new_row);
    }

    /// Delete a row from the buffer.
    pub fn delete(&mut self, old_row: impl Row, buffer_table: &mut StateTable<S>) {
        buffer_table.delete(old_row);
    }

    /// Update a row in the buffer.
    pub fn update(
        &mut self,
        old_row: impl Row,
        new_row: impl Row,
        buffer_table: &mut StateTable<S>,
    ) {
        buffer_table.update(old_row, new_row);
    }

    /// Apply a change to the buffer, insert/delete/update.
    pub fn apply_change(&mut self, change: &Record<OwnedRow>, buffer_table: &mut StateTable<S>) {
        match change {
            Record::Insert { new_row } => self.insert(new_row, buffer_table),
            Record::Delete { old_row } => self.delete(old_row, buffer_table),
            Record::Update { old_row, new_row } => self.update(old_row, new_row, buffer_table),
        }
    }

    /// Consume rows under `watermark` from the buffer.
    #[try_stream(ok = OwnedRow, error = StreamExecutorError)]
    pub async fn consume<'a>(
        &'a mut self,
        watermark: ScalarImpl,
        buffer_table: &'a mut StateTable<S>,
    ) {
        let pk_range = (
            Bound::<row::Empty>::Unbounded,
            Bound::Excluded([Some(watermark.as_scalar_ref_impl())]),
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
            let row: OwnedRow = row?;
            yield row;
        }

        // TODO(rc): Need something like `table.range_delete()`. Here we call
        // `update_watermark(watermark, true)` as an alternative to `range_delete((..watermark))`.
        buffer_table.update_watermark(watermark, true);
    }
}
