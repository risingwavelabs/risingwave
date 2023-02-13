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

use std::collections::HashMap;
use std::marker::PhantomData;

use itertools::Itertools;
use risingwave_common::array::column::Column;
use risingwave_common::array::{ArrayImpl, Op, Vis, VisRef};
use risingwave_common::buffer::{Bitmap, BitmapBuilder};
use risingwave_common::row::{self, OwnedRow, Row, RowExt};
use risingwave_common::types::ScalarImpl;
use risingwave_storage::StateStore;

use super::AggCall;
use crate::common::table::state_table::StateTable;
use crate::executor::StreamExecutorResult;

/// Deduplicater for one distinct column.
struct Deduplicater<S: StateStore> {
    agg_call_indices: Vec<usize>,
    _phantom: PhantomData<S>,
}

impl<S: StateStore> Deduplicater<S> {
    fn new(indices_and_calls: Vec<(usize, &AggCall)>) -> Self {
        let agg_call_indices = indices_and_calls
            .into_iter()
            .map(|(call_idx, _)| call_idx)
            .collect();
        Self {
            agg_call_indices,
            _phantom: PhantomData,
        }
    }

    /// Get the indices of agg calls that distinct on this column.
    /// The index is the position of the agg call in the original agg call list.
    fn agg_call_indices(&self) -> &[usize] {
        &self.agg_call_indices
    }

    /// Update the `visibilities` of distinct agg calls that distinct on the `column`,
    /// according to the counts of distinct keys for each call.
    ///
    /// * `ops`: Ops for each datum in `column`.
    /// * `column`: The column to distinct on.
    /// * `visibilities` - Visibilities for agg calls that distinct on the this column.
    /// * `dedup_table` - The deduplication table for this distinct column.
    async fn dedup(
        &mut self,
        ops: &[Op],
        column: &ArrayImpl,
        mut visibilities: Vec<&mut Vis>,
        dedup_table: &mut StateTable<S>,
        group_key: Option<&OwnedRow>,
    ) -> StreamExecutorResult<()> {
        assert_eq!(visibilities.len(), self.agg_call_indices.len());

        // TODO(rc): move to field of `Deduplicater`
        let mut cache = HashMap::new();
        let mut old_rows = HashMap::new();

        // inverted masks for visibilities, 1 means hidden, 0 means visible
        let mut vis_masks_inv = (0..visibilities.len())
            .map(|_| BitmapBuilder::zeroed(column.len()))
            .collect_vec();
        for (datum_idx, (op, datum)) in ops.iter().zip(column.iter()).enumerate() {
            // get counts of the distinct key of all agg calls that distinct on this column
            let counts = if let Some(counts) = cache.get_mut(&datum) {
                counts
            } else {
                let counts_row: Option<OwnedRow> = dedup_table
                    .get_row(group_key.chain(row::once(datum)))
                    .await?;
                let counts = counts_row.map_or_else(
                    || vec![0; self.agg_call_indices.len()],
                    |r| {
                        old_rows.insert(datum, r.clone());
                        r.iter()
                            .map(|d| if let Some(d) = d { d.into_int64() } else { 0 })
                            .collect()
                    },
                );
                cache.insert(datum, counts);
                cache.get_mut(&datum).unwrap()
            };
            debug_assert_eq!(counts.len(), visibilities.len());

            match op {
                Op::Insert | Op::UpdateInsert => {
                    // iterate over vis of each distinct agg call, count up for visible datum
                    for (i, vis) in visibilities.iter().enumerate() {
                        if vis.is_set(datum_idx) {
                            counts[i] += 1;
                            if counts[i] > 1 {
                                // duplicate, hide this one
                                vis_masks_inv[i].set(datum_idx, true);
                            }
                        }
                    }
                }
                Op::Delete | Op::UpdateDelete => {
                    // iterate over vis of each distinct agg call, count down for visible datum
                    for (i, vis) in visibilities.iter().enumerate() {
                        if vis.is_set(datum_idx) {
                            counts[i] -= 1;
                            debug_assert!(counts[i] >= 0);
                            if counts[i] > 0 {
                                // still exists at least one duplicate, hide this one
                                vis_masks_inv[i].set(datum_idx, true);
                            }
                        }
                    }
                }
            }
        }

        cache.into_iter().for_each(|(key, counts)| {
            let new_row = group_key.chain(row::once(key)).chain(OwnedRow::new(
                counts.into_iter().map(ScalarImpl::from).map(Some).collect(),
            ));
            if let Some(old_row) = old_rows.remove(&key) {
                dedup_table.update(group_key.chain(row::once(key)).chain(old_row), new_row)
            } else {
                dedup_table.insert(new_row)
            }
        });

        for (vis, vis_mask_inv) in visibilities.iter_mut().zip(vis_masks_inv.into_iter()) {
            let mask = !vis_mask_inv.finish();
            if !mask.all() {
                // update visibility is needed
                **vis = vis.as_ref() & VisRef::from(&mask);
            }
        }

        Ok(())
    }

    /// Flush the deduplication table.
    fn flush(&self, _dedup_table: &mut StateTable<S>) {
        // TODO(rc): now we flush the table in `dedup` method.
    }
}

/// # Safety
///
/// There must not be duplicate items in `indices`.
unsafe fn get_many_mut_from_slice<'a, T>(slice: &'a mut [T], indices: &[usize]) -> Vec<&'a mut T> {
    let mut res = Vec::with_capacity(indices.len());
    let ptr = slice.as_mut_ptr();
    for &idx in indices {
        res.push(&mut *ptr.add(idx));
    }
    res
}

pub struct DistinctDeduplicater<S: StateStore> {
    /// Key: distinct column index, value: deduplicater for the column.
    deduplicaters: HashMap<usize, Deduplicater<S>>,

    _phantom: PhantomData<S>,
}

impl<S: StateStore> DistinctDeduplicater<S> {
    pub fn new(agg_calls: &[AggCall]) -> Self {
        let deduplicaters: HashMap<_, _> = agg_calls
            .iter()
            .enumerate()
            .filter(|(_, call)| call.distinct) // only distinct agg calls need dedup table
            .into_group_map_by(|(_, call)| call.args.val_indices()[0])
            .into_iter()
            .map(|(k, v)| (k, Deduplicater::new(v)))
            .collect();
        Self {
            deduplicaters,
            _phantom: PhantomData,
        }
    }

    /// Deduplicate the chunk for each agg call, by returning new visibilities
    /// that hide duplicate rows.
    pub async fn dedup_chunk(
        &mut self,
        ops: &[Op],
        columns: &[Column],
        visibilities: Vec<Option<Bitmap>>,
        dedup_tables: &mut HashMap<usize, StateTable<S>>,
        group_key: Option<&OwnedRow>,
    ) -> StreamExecutorResult<Vec<Option<Bitmap>>> {
        // convert `Option<Bitmap>` to `Vis` for convenience
        let mut visibilities = visibilities
            .into_iter()
            .map(|v| match v {
                Some(bitmap) => Vis::from(bitmap),
                None => Vis::from(ops.len()),
            })
            .collect_vec();
        for (distinct_col, deduplicater) in &mut self.deduplicaters {
            let column = columns[*distinct_col].array_ref();
            let dedup_table = dedup_tables.get_mut(distinct_col).unwrap();
            // Select visibilities (as mutable references) of distinct agg calls that distinct on
            // `distinct_col` so that `Deduplicater` doesn't need to care about index mapping.
            // Safety: all items in `agg_call_indices` are unique by nature.
            let visibilities = unsafe {
                get_many_mut_from_slice(&mut visibilities, deduplicater.agg_call_indices())
            };
            deduplicater
                .dedup(ops, column, visibilities, dedup_table, group_key)
                .await?;
        }
        Ok(visibilities
            .into_iter()
            .map(|v| v.into_visibility())
            .collect())
    }

    /// Flush dedup state caches to dedup tables.
    pub fn flush(
        &self,
        dedup_tables: &mut HashMap<usize, StateTable<S>>,
    ) -> StreamExecutorResult<()> {
        for (distinct_col, deduplicater) in &self.deduplicaters {
            let dedup_table = dedup_tables.get_mut(distinct_col).unwrap();
            deduplicater.flush(dedup_table);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::StreamChunk;
    use risingwave_common::catalog::{ColumnDesc, ColumnId, TableId};
    use risingwave_common::test_prelude::StreamChunkTestExt;
    use risingwave_common::types::DataType;
    use risingwave_common::util::epoch::EpochPair;
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_storage::memory::MemoryStateStore;

    use super::*;
    use crate::executor::aggregation::{AggArgs, AggCall, AggKind};

    fn count_agg_call(kind: AggKind, col_idx: usize, distinct: bool) -> AggCall {
        AggCall {
            kind,
            args: AggArgs::Unary(DataType::Int64, col_idx),
            return_type: DataType::Int64,
            distinct,

            order_pairs: vec![],
            append_only: false,
            filter: None,
        }
    }

    async fn infer_dedup_tables<S: StateStore>(
        agg_calls: &[AggCall],
        group_key_types: &[DataType],
        store: S,
    ) -> HashMap<usize, StateTable<S>> {
        // corresponding to `Agg::infer_distinct_dedup_table` in frontend
        let mut dedup_tables = HashMap::new();

        for (distinct_col, indices_and_calls) in agg_calls
            .iter()
            .enumerate()
            .filter(|(_, call)| call.distinct) // only distinct agg calls need dedup table
            .into_group_map_by(|(_, call)| call.args.val_indices()[0])
            .into_iter()
        {
            let mut columns = vec![];
            let mut order_types = vec![];

            let mut next_column_id = 0;
            let mut add_column_desc = |data_type: DataType| {
                columns.push(ColumnDesc::unnamed(
                    ColumnId::new(next_column_id),
                    data_type,
                ));
                next_column_id += 1;
            };

            // group key columns
            for data_type in group_key_types {
                add_column_desc(data_type.clone());
                order_types.push(OrderType::Ascending);
            }

            // distinct key column
            add_column_desc(indices_and_calls[0].1.args.arg_types()[0].clone());
            order_types.push(OrderType::Ascending);

            // count columns
            for (_, _) in indices_and_calls {
                add_column_desc(DataType::Int64);
            }

            let n_columns = columns.len();
            let table = StateTable::new_without_distribution_with_value_indices(
                store.clone(),
                TableId::new(2333),
                columns,
                order_types,
                (0..(group_key_types.len() + 1)).collect(),
                ((group_key_types.len() + 1)..n_columns).collect(),
            )
            .await;
            dedup_tables.insert(distinct_col, table);
        }

        dedup_tables
    }

    fn option_bitmap_to_vec_bool(bm: &Option<Bitmap>, size: usize) -> Vec<bool> {
        match bm {
            Some(bm) => bm.iter().take(size).collect(),
            None => vec![true; size],
        }
    }

    #[tokio::test]
    async fn test_distinct_deduplicater() {
        // Schema:
        // a: int, b int, c int
        // Agg calls:
        // count(a), count(distinct a), sum(distinct a), count(distinct b)
        // Group keys:
        // empty

        let agg_calls = [
            // count(a)
            count_agg_call(AggKind::Count, 0, false),
            // count(distinct a)
            count_agg_call(AggKind::Count, 0, true),
            // sum(distinct a)
            count_agg_call(AggKind::Sum, 0, true),
            // count(distinct b)
            count_agg_call(AggKind::Count, 1, true),
        ];

        let store = MemoryStateStore::new();
        let mut epoch = EpochPair::new_test_epoch(1);
        let mut dedup_tables = infer_dedup_tables(&agg_calls, &[], store).await;
        dedup_tables
            .values_mut()
            .for_each(|table| table.init_epoch(epoch));

        let mut deduplicater = DistinctDeduplicater::new(&agg_calls);

        // --- chunk 1 ---

        let chunk = StreamChunk::from_pretty(
            " I   I     I
            + 1  10   100
            + 1  11   101",
        );
        let (ops, columns, visibility) = chunk.into_inner();

        let visibilities = std::iter::repeat(visibility)
            .take(agg_calls.len())
            .collect_vec();
        let visibilities = deduplicater
            .dedup_chunk(&ops, &columns, visibilities, &mut dedup_tables, None)
            .await
            .unwrap();
        assert_eq!(
            option_bitmap_to_vec_bool(&visibilities[0], ops.len()),
            vec![true, true] // same as original chunk
        );
        assert_eq!(
            option_bitmap_to_vec_bool(&visibilities[1], ops.len()),
            vec![true, false] // distinct on a
        );
        assert_eq!(
            option_bitmap_to_vec_bool(&visibilities[2], ops.len()),
            vec![true, false] // distinct on a, same as above
        );
        assert_eq!(
            option_bitmap_to_vec_bool(&visibilities[3], ops.len()),
            vec![true, true] // distinct on b
        );

        deduplicater.flush(&mut dedup_tables).unwrap();

        epoch.inc();
        for table in dedup_tables.values_mut() {
            table.commit(epoch).await.unwrap();
        }

        // --- chunk 2 ---

        let chunk = StreamChunk::from_pretty(
            " I   I     I
            + 1  11  -102
            + 2  12   103  D
            + 2  12  -104",
        );
        let (ops, columns, visibility) = chunk.into_inner();

        let visibilities = std::iter::repeat(visibility)
            .take(agg_calls.len())
            .collect_vec();
        let visibilities = deduplicater
            .dedup_chunk(&ops, &columns, visibilities, &mut dedup_tables, None)
            .await
            .unwrap();
        assert_eq!(
            option_bitmap_to_vec_bool(&visibilities[0], ops.len()),
            vec![true, false, true] // same as original chunk
        );
        assert_eq!(
            option_bitmap_to_vec_bool(&visibilities[1], ops.len()),
            vec![false, false, true] // distinct on a
        );
        assert_eq!(
            option_bitmap_to_vec_bool(&visibilities[2], ops.len()),
            vec![false, false, true] // distinct on a, same as above
        );
        assert_eq!(
            option_bitmap_to_vec_bool(&visibilities[3], ops.len()),
            vec![false, false, true] // distinct on b
        );

        deduplicater.flush(&mut dedup_tables).unwrap();

        epoch.inc();
        for table in dedup_tables.values_mut() {
            table.commit(epoch).await.unwrap();
        }

        // test recovery
        let mut deduplicater = DistinctDeduplicater::new(&agg_calls);

        // --- chunk 3 ---

        let chunk = StreamChunk::from_pretty(
            " I   I     I
            - 1  10   100  D
            - 1  11   101
            - 1  11  -102",
        );
        let (ops, columns, visibility) = chunk.into_inner();

        let visibilities = std::iter::repeat(visibility)
            .take(agg_calls.len())
            .collect_vec();
        let visibilities = deduplicater
            .dedup_chunk(&ops, &columns, visibilities, &mut dedup_tables, None)
            .await
            .unwrap();
        assert_eq!(
            option_bitmap_to_vec_bool(&visibilities[0], ops.len()),
            vec![false, true, true] // same as original chunk
        );
        assert_eq!(
            option_bitmap_to_vec_bool(&visibilities[1], ops.len()),
            // distinct on a
            vec![
                false, // hidden in original chunk
                false, // not the last one
                false, // not the last one
            ]
        );
        assert_eq!(
            option_bitmap_to_vec_bool(&visibilities[2], ops.len()),
            // distinct on a, same as above
            vec![
                false, // hidden in original chunk
                false, // not the last one
                false, // not the last one
            ]
        );
        assert_eq!(
            option_bitmap_to_vec_bool(&visibilities[3], ops.len()),
            // distinct on b
            vec![
                false, // hidden in original chunk
                false, // not the last one
                true,  // is the last one
            ]
        );

        deduplicater.flush(&mut dedup_tables).unwrap();

        epoch.inc();
        for table in dedup_tables.values_mut() {
            table.commit(epoch).await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_distinct_deduplicater_with_group() {
        // Schema:
        // a: int, b int, c int
        // Agg calls:
        // count(a), count(distinct a), count(distinct b)
        // Group keys:
        // c

        let agg_calls = [
            // count(a)
            count_agg_call(AggKind::Count, 0, false),
            // count(distinct a)
            count_agg_call(AggKind::Count, 0, true),
            // count(distinct b)
            count_agg_call(AggKind::Count, 1, true),
        ];

        let group_key_types = [DataType::Int64];
        let group_key = OwnedRow::new(vec![Some(100.into())]);

        let store = MemoryStateStore::new();
        let mut epoch = EpochPair::new_test_epoch(1);
        let mut dedup_tables = infer_dedup_tables(&agg_calls, &group_key_types, store).await;
        dedup_tables
            .values_mut()
            .for_each(|table| table.init_epoch(epoch));

        let mut deduplicater = DistinctDeduplicater::new(&agg_calls);

        let chunk = StreamChunk::from_pretty(
            " I   I     I
            + 1  10   100
            + 1  11   100
            + 1  11   100
            + 2  12   200  D
            + 2  12   100",
        );
        let (ops, columns, visibility) = chunk.into_inner();

        let visibilities = std::iter::repeat(visibility)
            .take(agg_calls.len())
            .collect_vec();
        let visibilities = deduplicater
            .dedup_chunk(
                &ops,
                &columns,
                visibilities,
                &mut dedup_tables,
                Some(&group_key),
            )
            .await
            .unwrap();
        assert_eq!(
            option_bitmap_to_vec_bool(&visibilities[0], ops.len()),
            vec![true, true, true, false, true] // same as original chunk
        );
        assert_eq!(
            option_bitmap_to_vec_bool(&visibilities[1], ops.len()),
            vec![true, false, false, false, true] // distinct on a
        );
        assert_eq!(
            option_bitmap_to_vec_bool(&visibilities[2], ops.len()),
            vec![true, true, false, false, true] // distinct on b
        );

        deduplicater.flush(&mut dedup_tables).unwrap();

        epoch.inc();
        for table in dedup_tables.values_mut() {
            table.commit(epoch).await.unwrap();
        }

        let chunk = StreamChunk::from_pretty(
            " I   I     I
            - 1  10   100  D
            - 1  11   100
            - 1  11   100",
        );
        let (ops, columns, visibility) = chunk.into_inner();

        let visibilities = std::iter::repeat(visibility)
            .take(agg_calls.len())
            .collect_vec();
        let visibilities = deduplicater
            .dedup_chunk(
                &ops,
                &columns,
                visibilities,
                &mut dedup_tables,
                Some(&group_key),
            )
            .await
            .unwrap();
        assert_eq!(
            option_bitmap_to_vec_bool(&visibilities[0], ops.len()),
            vec![false, true, true] // same as original chunk
        );
        assert_eq!(
            option_bitmap_to_vec_bool(&visibilities[1], ops.len()),
            // distinct on a
            vec![
                false, // hidden in original chunk
                false, // not the last one
                false, // not the last one
            ]
        );
        assert_eq!(
            option_bitmap_to_vec_bool(&visibilities[2], ops.len()),
            // distinct on b
            vec![
                false, // hidden in original chunk
                false, // not the last one
                true,  // is the last one
            ]
        );

        deduplicater.flush(&mut dedup_tables).unwrap();

        epoch.inc();
        for table in dedup_tables.values_mut() {
            table.commit(epoch).await.unwrap();
        }
    }
}
