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

use std::collections::HashSet;
use std::ops::Deref as _;

use bytes::Bytes;
use futures::{StreamExt as _, stream};
use itertools::Itertools as _;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::catalog::{ConflictBehavior, checked_conflict_behaviors};
use risingwave_common::row::{CompactedRow, OwnedRow, Row as _};
use risingwave_common::types::ScalarImpl;
use risingwave_common::util::iter_util::ZipEqFast as _;
use risingwave_common::util::sort_util::{OrderType, cmp_datum};
use risingwave_common::util::value_encoding::{BasicSerde, ValueRowSerializer as _};
use risingwave_storage::StateStore;
use risingwave_storage::row_serde::value_serde::ValueRowSerde;

use crate::cache::ManagedLruCache;
use crate::common::metrics::MetricsInfo;
use crate::common::table::state_table::StateTableInner;
use crate::executor::StreamExecutorResult;
use crate::executor::monitor::MaterializeCacheMetrics;
use crate::task::AtomicU64Ref;

/// A cache for materialize executors.
pub struct MaterializeCache {
    lru_cache: ManagedLruCache<Vec<u8>, CacheValue>,
    row_serde: BasicSerde,
    version_column_indices: Vec<u32>,
    conflict_behavior: ConflictBehavior,
    toastable_column_indices: Option<Vec<usize>>,
    metrics: MaterializeCacheMetrics,
}

type CacheValue = Option<CompactedRow>;
type ChangeBuffer = crate::common::change_buffer::ChangeBuffer<Vec<u8>, OwnedRow>;

fn row_below_committed_watermark<S: StateStore, SD: ValueRowSerde>(
    row_serde: &BasicSerde,
    table: &StateTableInner<S, SD>,
    row: &CompactedRow,
) -> StreamExecutorResult<bool> {
    let Some(clean_watermark_index) = table.clean_watermark_index else {
        return Ok(false);
    };
    let Some(committed_watermark) = table.get_committed_watermark() else {
        return Ok(false);
    };
    let deserialized = row_serde.deserializer.deserialize(row.row.clone())?;
    Ok(cmp_datum(
        deserialized.datum_at(clean_watermark_index),
        Some(committed_watermark.as_scalar_ref_impl()),
        OrderType::ascending(),
    ) == std::cmp::Ordering::Less)
}

impl MaterializeCache {
    /// Create a new `MaterializeCache`.
    ///
    /// Returns `None` if the conflict behavior is `NoCheck`.
    pub fn new(
        watermark_sequence: AtomicU64Ref,
        metrics_info: MetricsInfo,
        row_serde: BasicSerde,
        version_column_indices: Vec<u32>,
        conflict_behavior: ConflictBehavior,
        toastable_column_indices: Option<Vec<usize>>,
        materialize_cache_metrics: MaterializeCacheMetrics,
    ) -> Option<Self> {
        match conflict_behavior {
            checked_conflict_behaviors!() => {}
            ConflictBehavior::NoCheck => {
                assert!(
                    toastable_column_indices.is_none(),
                    "when there are toastable columns, conflict handling must be enabled"
                );
                return None;
            }
        }

        let lru_cache: ManagedLruCache<Vec<u8>, CacheValue> =
            ManagedLruCache::unbounded(watermark_sequence, metrics_info);
        Some(Self {
            lru_cache,
            row_serde,
            version_column_indices,
            conflict_behavior,
            toastable_column_indices,
            metrics: materialize_cache_metrics,
        })
    }

    /// First populate the cache from `table`, and then calculate a [`ChangeBuffer`].
    /// `table` will not be written in this method.
    pub async fn handle_new<S: StateStore, SD: ValueRowSerde>(
        &mut self,
        chunk: StreamChunk,
        table: &StateTableInner<S, SD>,
    ) -> StreamExecutorResult<ChangeBuffer> {
        if table.value_indices().is_some() {
            // TODO(st1page): when materialize partial columns(), we should
            // construct some columns in the pk
            unimplemented!("materialize cache cannot handle conflicts with partial table values");
        };

        let (data_chunk, ops) = chunk.clone().into_parts();
        let values = data_chunk.serialize();

        let key_chunk = data_chunk.project(table.pk_indices());

        let pks = {
            let mut pks = vec![vec![]; data_chunk.capacity()];
            key_chunk
                .rows_with_holes()
                .zip_eq_fast(pks.iter_mut())
                .for_each(|(r, vnode_and_pk)| {
                    if let Some(r) = r {
                        table.pk_serde().serialize(r, vnode_and_pk);
                    }
                });
            pks
        };
        let (_, vis) = key_chunk.into_parts();
        let row_ops = ops
            .iter()
            .zip_eq_fast(pks.into_iter())
            .zip_eq_fast(values.into_iter())
            .zip_eq_fast(vis.iter())
            .filter_map(|(((op, k), v), vis)| vis.then_some((*op, k, v)))
            .collect_vec();

        self.handle_inner(row_ops, table).await
    }

    async fn handle_inner<S: StateStore, SD: ValueRowSerde>(
        &mut self,
        row_ops: Vec<(Op, Vec<u8>, Bytes)>,
        table: &StateTableInner<S, SD>,
    ) -> StreamExecutorResult<ChangeBuffer> {
        let key_set: HashSet<Box<[u8]>> = row_ops
            .iter()
            .map(|(_, k, _)| k.as_slice().into())
            .collect();

        // Populate the LRU cache with the keys in input chunk.
        // For new keys, row values are set to None.
        self.fetch_keys(key_set.iter().map(|v| v.deref()), table)
            .await?;

        let mut change_buffer = ChangeBuffer::new();
        let row_serde = self.row_serde.clone();
        let version_column_indices = self.version_column_indices.clone();
        for (op, key, row) in row_ops {
            // Use a macro instead of method to workaround partial borrow.
            macro_rules! get_expected {
                () => {
                    self.lru_cache.get(&key).unwrap_or_else(|| {
                        panic!(
                            "the key {:?} has not been fetched in the materialize executor's cache",
                            key
                        )
                    })
                };
            }

            match op {
                Op::Insert | Op::UpdateInsert => {
                    let Some(old_row) = get_expected!() else {
                        // not exists before, meaning no conflict, simply insert
                        let new_row_deserialized =
                            row_serde.deserializer.deserialize(row.clone())?;
                        change_buffer.insert(key.clone(), new_row_deserialized);
                        self.lru_cache.put(key, Some(CompactedRow { row }));
                        continue;
                    };

                    // now conflict happens, handle it according to the specified behavior
                    match self.conflict_behavior {
                        ConflictBehavior::Overwrite => {
                            let old_row_deserialized =
                                row_serde.deserializer.deserialize(old_row.row.clone())?;
                            let new_row_deserialized =
                                row_serde.deserializer.deserialize(row.clone())?;

                            let need_overwrite = if !version_column_indices.is_empty() {
                                versions_are_newer_or_equal(
                                    &old_row_deserialized,
                                    &new_row_deserialized,
                                    &version_column_indices,
                                )
                            } else {
                                // no version column specified, just overwrite
                                true
                            };

                            if need_overwrite {
                                if let Some(toastable_indices) = &self.toastable_column_indices {
                                    // For TOAST-able columns, replace Debezium's unavailable value placeholder with old row values.
                                    let final_row = toast::handle_toast_columns_for_postgres_cdc(
                                        &old_row_deserialized,
                                        &new_row_deserialized,
                                        toastable_indices,
                                    );

                                    change_buffer.update(
                                        key.clone(),
                                        old_row_deserialized,
                                        final_row.clone(),
                                    );
                                    let final_row_bytes =
                                        Bytes::from(row_serde.serializer.serialize(final_row));
                                    self.lru_cache.put(
                                        key.clone(),
                                        Some(CompactedRow {
                                            row: final_row_bytes,
                                        }),
                                    );
                                } else {
                                    // No TOAST columns, use the original row bytes directly to avoid unnecessary serialization
                                    change_buffer.update(
                                        key.clone(),
                                        old_row_deserialized,
                                        new_row_deserialized,
                                    );
                                    self.lru_cache
                                        .put(key.clone(), Some(CompactedRow { row: row.clone() }));
                                }
                            };
                        }
                        ConflictBehavior::IgnoreConflict => {
                            // ignore conflict, do nothing
                        }
                        ConflictBehavior::DoUpdateIfNotNull => {
                            // In this section, we compare the new row and old row column by column and perform `DoUpdateIfNotNull` replacement.

                            let old_row_deserialized =
                                row_serde.deserializer.deserialize(old_row.row.clone())?;
                            let new_row_deserialized =
                                row_serde.deserializer.deserialize(row.clone())?;
                            let need_overwrite = if !version_column_indices.is_empty() {
                                versions_are_newer_or_equal(
                                    &old_row_deserialized,
                                    &new_row_deserialized,
                                    &version_column_indices,
                                )
                            } else {
                                true
                            };

                            if need_overwrite {
                                let mut row_deserialized_vec =
                                    old_row_deserialized.clone().into_inner().into_vec();
                                replace_if_not_null(
                                    &mut row_deserialized_vec,
                                    new_row_deserialized.clone(),
                                );
                                let mut updated_row = OwnedRow::new(row_deserialized_vec);

                                // Apply TOAST column fix for CDC tables with TOAST columns
                                if let Some(toastable_indices) = &self.toastable_column_indices {
                                    // Note: we need to use old_row_deserialized again, but it was moved above
                                    // So we re-deserialize the old row
                                    let old_row_deserialized_again =
                                        row_serde.deserializer.deserialize(old_row.row.clone())?;
                                    updated_row = toast::handle_toast_columns_for_postgres_cdc(
                                        &old_row_deserialized_again,
                                        &updated_row,
                                        toastable_indices,
                                    );
                                }

                                change_buffer.update(
                                    key.clone(),
                                    old_row_deserialized,
                                    updated_row.clone(),
                                );
                                let updated_row_bytes =
                                    Bytes::from(row_serde.serializer.serialize(updated_row));
                                self.lru_cache.put(
                                    key.clone(),
                                    Some(CompactedRow {
                                        row: updated_row_bytes,
                                    }),
                                );
                            }
                        }
                        ConflictBehavior::NoCheck => unreachable!(),
                    };
                }

                Op::UpdateDelete
                    if matches!(
                        self.conflict_behavior,
                        ConflictBehavior::Overwrite | ConflictBehavior::DoUpdateIfNotNull
                    ) =>
                {
                    // For `UpdateDelete`s, we skip processing them but directly handle the following `UpdateInsert`
                    // instead. This is because...
                    //
                    // - For `Overwrite`, we only care about the new row.
                    // - For `DoUpdateIfNotNull`, we don't want the whole row to be deleted, but instead perform
                    //   column-wise replacement when handling the `UpdateInsert`.
                    //
                    // However, for `IgnoreConflict`, we still need to delete the old row first, otherwise the row
                    // cannot be updated at all.
                }

                Op::Delete | Op::UpdateDelete => {
                    if let Some(old_row) = get_expected!() {
                        let old_row_deserialized =
                            row_serde.deserializer.deserialize(old_row.row.clone())?;
                        change_buffer.delete(key.clone(), old_row_deserialized);
                        // put a None into the cache to represent deletion
                        self.lru_cache.put(key, None);
                    } else {
                        // delete a non-existent value
                        // this is allowed in the case of mview conflict, so ignore
                    }
                }
            }
        }
        Ok(change_buffer)
    }

    async fn fetch_keys<'a, S: StateStore, SD: ValueRowSerde>(
        &mut self,
        keys: impl Iterator<Item = &'a [u8]>,
        table: &StateTableInner<S, SD>,
    ) -> StreamExecutorResult<()> {
        let mut futures = vec![];
        for key in keys {
            self.metrics.materialize_cache_total_count.inc();

            if let Some(cached) = self.lru_cache.get(key) {
                self.metrics.materialize_cache_hit_count.inc();
                if let Some(row) = cached {
                    if row_below_committed_watermark(&self.row_serde, table, row)? {
                        self.lru_cache.put(key.to_vec(), None);
                    } else {
                        self.metrics.materialize_data_exist_count.inc();
                    }
                }
                continue;
            }

            let row_serde = self.row_serde.clone();
            let committed_watermark = table.get_committed_watermark().cloned();
            let clean_watermark_index = table.clean_watermark_index;
            futures.push(async move {
                let key_row = table.pk_serde().deserialize(key).unwrap();
                let row = table.get_row(key_row).await?.map(CompactedRow::from);
                let row = match row {
                    Some(row)
                        if committed_watermark.is_some() && clean_watermark_index.is_some() =>
                    {
                        let committed_watermark = committed_watermark.as_ref().unwrap();
                        let clean_watermark_index = clean_watermark_index.unwrap();
                        let deserialized = row_serde.deserializer.deserialize(row.row.clone())?;
                        let below = cmp_datum(
                            deserialized.datum_at(clean_watermark_index),
                            Some(committed_watermark.as_scalar_ref_impl()),
                            OrderType::ascending(),
                        ) == std::cmp::Ordering::Less;
                        (!below).then_some(row)
                    }
                    other => other,
                };
                StreamExecutorResult::Ok((key.to_vec(), row))
            });
        }

        let mut buffered = stream::iter(futures).buffer_unordered(10).fuse();
        while let Some(result) = buffered.next().await {
            let (key, row) = result?;
            if row.is_some() {
                self.metrics.materialize_data_exist_count.inc();
            }
            // for keys that are not in the table, `value` is None
            match self.conflict_behavior {
                checked_conflict_behaviors!() => self.lru_cache.put(key, row),
                _ => unreachable!(),
            };
        }

        Ok(())
    }

    /// Evict the LRU cache entries that are lower than the watermark.
    pub fn evict(&mut self) {
        self.lru_cache.evict()
    }

    /// Clear the LRU cache.
    pub fn clear(&mut self) {
        self.lru_cache.clear()
    }
}

/// Replace columns in an existing row with the corresponding columns in a replacement row, if the
/// column value in the replacement row is not null.
///
/// # Example
///
/// ```ignore
/// let mut row = vec![Some(1), None, Some(3)];
/// let replacement = vec![Some(10), Some(20), None];
/// replace_if_not_null(&mut row, replacement);
/// ```
///
/// After the call, `row` will be `[Some(10), Some(20), Some(3)]`.
fn replace_if_not_null(row: &mut Vec<Option<ScalarImpl>>, replacement: OwnedRow) {
    for (old_col, new_col) in row.iter_mut().zip_eq_fast(replacement) {
        if let Some(new_value) = new_col {
            *old_col = Some(new_value);
        }
    }
}

/// Compare multiple version columns lexicographically.
/// Returns true if `new_row` has a newer or equal version compared to `old_row`.
fn versions_are_newer_or_equal(
    old_row: &OwnedRow,
    new_row: &OwnedRow,
    version_column_indices: &[u32],
) -> bool {
    if version_column_indices.is_empty() {
        // No version columns specified, always consider new version as newer
        return true;
    }

    for &idx in version_column_indices {
        let old_value = old_row.datum_at(idx as usize);
        let new_value = new_row.datum_at(idx as usize);

        match cmp_datum(old_value, new_value, OrderType::ascending_nulls_first()) {
            std::cmp::Ordering::Less => return true,     // new is newer
            std::cmp::Ordering::Greater => return false, // old is newer
            std::cmp::Ordering::Equal => continue,       // equal, check next column
        }
    }

    // All version columns are equal, consider new version as equal (should overwrite)
    true
}

/// TOAST column handling for CDC tables with TOAST columns.
mod toast {
    use risingwave_common::row::Row as _;
    use risingwave_common::types::DEBEZIUM_UNAVAILABLE_VALUE;

    use super::*;

    /// Fast string comparison to check if a string equals `DEBEZIUM_UNAVAILABLE_VALUE`.
    /// Optimized by checking length first to avoid expensive string comparison.
    fn is_unavailable_value_str(s: &str) -> bool {
        s.len() == DEBEZIUM_UNAVAILABLE_VALUE.len() && s == DEBEZIUM_UNAVAILABLE_VALUE
    }

    /// Check if a datum represents Debezium's unavailable value placeholder.
    /// This function handles both scalar types and one-dimensional arrays.
    fn is_debezium_unavailable_value(
        datum: &Option<risingwave_common::types::ScalarRefImpl<'_>>,
    ) -> bool {
        match datum {
            Some(risingwave_common::types::ScalarRefImpl::Utf8(val)) => {
                is_unavailable_value_str(val)
            }
            Some(risingwave_common::types::ScalarRefImpl::Jsonb(jsonb_ref)) => {
                // For jsonb type, check if it's a string containing the unavailable value
                jsonb_ref
                    .as_str()
                    .map(is_unavailable_value_str)
                    .unwrap_or(false)
            }
            Some(risingwave_common::types::ScalarRefImpl::Bytea(bytea)) => {
                // For bytea type, we need to check if it contains the string bytes of DEBEZIUM_UNAVAILABLE_VALUE
                // This is because when processing bytea from Debezium, we convert the base64-encoded string
                // to `DEBEZIUM_UNAVAILABLE_VALUE` in the json.rs parser to maintain consistency
                if let Ok(bytea_str) = std::str::from_utf8(bytea) {
                    is_unavailable_value_str(bytea_str)
                } else {
                    false
                }
            }
            Some(risingwave_common::types::ScalarRefImpl::List(list_ref)) => {
                // For list type, check if it contains exactly one element with the unavailable value
                // This is because when any element in an array triggers TOAST, Debezium treats the entire
                // array as unchanged and sends a placeholder array with only one element
                if list_ref.len() == 1 {
                    if let Some(Some(element)) = list_ref.get(0) {
                        // Recursively check the array element
                        is_debezium_unavailable_value(&Some(element))
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
            _ => false,
        }
    }

    /// Fix TOAST columns by replacing unavailable values with old row values.
    pub fn handle_toast_columns_for_postgres_cdc(
        old_row: &OwnedRow,
        new_row: &OwnedRow,
        toastable_indices: &[usize],
    ) -> OwnedRow {
        let mut fixed_row_data = new_row.as_inner().to_vec();

        for &toast_idx in toastable_indices {
            // Check if the new value is Debezium's unavailable value placeholder
            let is_unavailable = is_debezium_unavailable_value(&new_row.datum_at(toast_idx));
            if is_unavailable {
                // Replace with old row value if available
                if let Some(old_datum_ref) = old_row.datum_at(toast_idx) {
                    fixed_row_data[toast_idx] = Some(old_datum_ref.into_scalar_impl());
                }
            }
        }

        OwnedRow::new(fixed_row_data)
    }
}
