// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use async_trait::async_trait;
use bytes::Bytes;
use itertools::Itertools;
use madsim::collections::BTreeMap;
use risingwave_common::array::stream_chunk::{Op, Ops};
use risingwave_common::array::ArrayImpl;
use risingwave_common::buffer::Bitmap;
use risingwave_common::error::Result;
use risingwave_common::types::{DataType, Datum, ScalarImpl};
use risingwave_common::util::ordered::OrderedArraysSerializer;
use risingwave_common::util::value_encoding::{deserialize_cell, serialize_cell};
use risingwave_storage::storage_value::StorageValue;
use risingwave_storage::write_batch::WriteBatch;
use risingwave_storage::{Keyspace, StateStore};

use super::super::flush_status::BtreeMapFlushStatus as FlushStatus;
use super::ManagedTableState;

pub struct ManagedStringAggState<S: StateStore> {
    cache: BTreeMap<Bytes, FlushStatus<ScalarImpl>>,

    /// A cached result.
    result: Option<String>,

    /// Marks whether there are modifications, i.e. cache != storage
    dirty: bool,

    /// Number of items in the state.
    total_count: usize,

    /// Sort key indices.
    /// We remark two things:
    /// 1. it is possible that `string_agg(...)` does not have `order by` in it.
    /// 2. the primary key is not necessarily equal to columns in the `order by`
    /// For example, `select string_agg(c1, order by c2 DESC) from t`. The primary key
    /// should be `row_id` of table t, and the sort key is `c2`. However, `sort_key_indices`
    /// would be the indices of `c2` and `row_id`. And the ordering embedded in
    /// `sort_key_serializer` would be `Descending` and `Ascending`(as default).
    sort_key_indices: Vec<usize>,

    /// Value index.
    /// If concatenating multiple columns is needed such as `select string_agg('a' || c1 || 'b')
    /// from t`, `concat` should first be done in a project node.
    /// `ManagedStringAggState` require only one column as the value.
    value_index: usize,

    /// Delimiter.
    delimiter: String,

    /// The keyspace to operate on.
    keyspace: Keyspace<S>,

    /// Serializer to get the bytes of sorted columns.
    sorted_arrays_serializer: OrderedArraysSerializer,
}

impl<S: StateStore> ManagedStringAggState<S> {
    /// Create a managed string agg state based on `Keyspace`.
    // TODO: enable string agg state
    #[allow(dead_code)]
    pub async fn new(
        keyspace: Keyspace<S>,
        row_count: usize,
        sort_key_indices: Vec<usize>,
        value_index: usize,
        delimiter: String,
        sort_key_serializer: OrderedArraysSerializer,
    ) -> Result<Self> {
        Ok(Self {
            cache: BTreeMap::new(),
            result: None,
            dirty: false,
            total_count: row_count,
            sort_key_indices,
            value_index,
            delimiter,
            keyspace,
            sorted_arrays_serializer: sort_key_serializer,
        })
    }

    #[cfg(test)]
    pub fn get_row_count(&self) -> usize {
        self.total_count
    }

    #[allow(dead_code)]
    pub fn clear_cache(&mut self) {
        assert!(
            !self.is_dirty(),
            "cannot clear cache while string agg state is dirty"
        );
        self.cache.clear();
    }
}

impl<S: StateStore> ManagedStringAggState<S> {
    async fn read_all_into_memory(&mut self, epoch: u64) -> Result<()> {
        // We cannot read from storage into memory when the cache has not been flushed onto the
        // storage.
        assert!(!self.is_dirty());
        // Read all.
        let all_data = self.keyspace.scan(None, epoch).await?;
        for (raw_key, mut raw_value) in all_data {
            // We only need to deserialize the value, and keep the key as bytes.
            let value = deserialize_cell(&mut raw_value, &DataType::Varchar)?.unwrap();
            let value_string: String = value.into_utf8();
            self.cache.insert(
                raw_key,
                // Here we abuse the semantics of `DeleteInsert` for those values already existed
                // on the storage, and now we are loading them into memory.
                FlushStatus::DeleteInsert(value_string.into()),
            );
        }
        self.dirty = false;
        Ok(())
    }

    fn concat_strings_in_cache_into_result(&mut self) {
        if self.result.is_some() {
            return;
        }
        if self.total_count == 0 {
            return;
        }
        let res = self
            .cache
            .values()
            .filter_map(|value| value.as_option())
            .map(|scalar| scalar.as_utf8())
            .join(&self.delimiter);
        self.result = Some(res);
    }

    fn get_result(&self) -> Datum {
        self.result.as_ref().map(|res| res.clone().into())
    }
}

#[async_trait]
impl<S: StateStore> ManagedTableState<S> for ManagedStringAggState<S> {
    async fn apply_batch(
        &mut self,
        ops: Ops<'_>,
        visibility: Option<&Bitmap>,
        data: &[&ArrayImpl],
        epoch: u64,
    ) -> Result<()> {
        debug_assert!(super::verify_batch(ops, visibility, data));
        for sort_key_index in &self.sort_key_indices {
            debug_assert!(*sort_key_index < data.len());
        }
        debug_assert!(self.value_index < data.len());

        if self.total_count > self.cache.len() {
            assert_eq!(self.cache.len(), 0);
            // The current policy is all-or-nothing, so no values in the memory.
            // It means the cache gets flushed onto disk.
            self.read_all_into_memory(epoch).await?;
        }

        let mut row_keys = vec![];
        self.sorted_arrays_serializer.serialize(data, &mut row_keys);

        for (row_idx, (op, key_bytes)) in ops.iter().zip_eq(row_keys.into_iter()).enumerate() {
            let visible = visibility
                .map(|x| x.is_set(row_idx).unwrap())
                .unwrap_or(true);
            if !visible {
                continue;
            }

            let value = match data[self.value_index].datum_at(row_idx) {
                Some(scalar) => scalar.into_utf8(),
                None => "".to_string(),
            };
            match op {
                Op::Insert | Op::UpdateInsert => {
                    FlushStatus::do_insert(self.cache.entry(key_bytes.into()), value.into());
                    self.total_count += 1;
                }
                Op::Delete | Op::UpdateDelete => {
                    FlushStatus::do_delete(self.cache.entry(key_bytes.into()));
                    self.total_count -= 1;
                }
            }
            // TODO: This can be further optimized as `Delete` and `Insert` may cancel each other.
            self.dirty = true;
            self.result = None;
        }
        Ok(())
    }

    async fn get_output(&mut self, epoch: u64) -> Result<Datum> {
        // We allow people to get output when the data is dirty.
        // As this is easier compared to `ManagedMinState` as we have a all-or-nothing cache policy
        // here.
        if !self.is_dirty() {
            // If we have already cached the result, we return it directly.
            if let Some(res) = &self.result {
                return Ok(Some(res.clone().into()));
            } else if self.total_count == 0 {
                // If there is simply no data, we return empty string.
                return Ok(None);
            } else if !self.cache.is_empty() {
                // Since we have a all-or-nothing policy, cache must either contain all the values
                // or be empty.
                self.concat_strings_in_cache_into_result();
                return Ok(Some(self.result.clone().unwrap().into()));
            }
        }
        if self.is_dirty() {
            // If the state is dirty, we must have a non-empty cache.
            // do nothing
        } else {
            // or we don't have the state in memory,
            // then we need to load all the state from the memory.
            self.read_all_into_memory(epoch).await?;
        }
        self.concat_strings_in_cache_into_result();
        Ok(self.get_result())
    }

    fn is_dirty(&self) -> bool {
        self.dirty
    }

    fn flush(&mut self, write_batch: &mut WriteBatch<S>) -> Result<()> {
        if !self.is_dirty() {
            return Ok(());
        }

        let mut local = write_batch.prefixify(&self.keyspace);

        for (key, value) in std::mem::take(&mut self.cache) {
            let value = value.into_option();
            match value {
                Some(val) => {
                    // TODO(Yuanxin): Implement value meta
                    local.put(
                        key,
                        StorageValue::new_default_put(serialize_cell(&Some(val))?),
                    );
                }
                None => {
                    local.delete(key);
                }
            }
        }
        self.dirty = false;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::{I64Array, Op, Utf8Array};
    use risingwave_common::types::ScalarImpl;
    use risingwave_common::util::sort_util::{OrderPair, OrderType};
    use risingwave_storage::{Keyspace, StateStore};

    use super::super::ManagedTableState;
    use super::*;
    use crate::executor::test_utils::create_in_memory_keyspace;

    async fn create_managed_state<S: StateStore>(
        keyspace: &Keyspace<S>,
        row_count: usize,
    ) -> ManagedStringAggState<S> {
        let sort_key_indices = vec![0, 1];
        let value_index = 0;
        let orderings = vec![OrderType::Descending, OrderType::Ascending];
        let order_pairs = orderings
            .clone()
            .into_iter()
            .zip_eq(sort_key_indices.clone().into_iter())
            .map(|(ord, idx)| OrderPair::new(idx, ord))
            .collect::<Vec<_>>();
        let sort_key_serializer = OrderedArraysSerializer::new(order_pairs);

        ManagedStringAggState::new(
            keyspace.clone(),
            row_count,
            sort_key_indices,
            value_index,
            "||".to_string(),
            sort_key_serializer,
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn test_managed_string_agg_state() {
        let keyspace = create_in_memory_keyspace();
        let store = keyspace.state_store();
        let mut managed_state = create_managed_state(&keyspace, 0).await;
        assert!(!managed_state.is_dirty());
        let mut epoch: u64 = 0;

        // Insert.
        managed_state
            .apply_batch(
                &[Op::Insert, Op::Insert, Op::Insert],
                None,
                &[
                    &Utf8Array::from_slice(&[Some("abc"), Some("def"), Some("ghi")])
                        .unwrap()
                        .into(),
                    &I64Array::from_slice(&[Some(0), Some(1), Some(2)])
                        .unwrap()
                        .into(),
                ],
                epoch,
            )
            .await
            .unwrap();
        assert!(managed_state.is_dirty());

        // Check output after insertion.
        assert_eq!(
            managed_state.get_output(epoch).await.unwrap(),
            Some(ScalarImpl::Utf8("ghi||def||abc".to_string()))
        );

        let mut write_batch = store.start_write_batch();
        managed_state.flush(&mut write_batch).unwrap();
        write_batch.ingest(epoch).await.unwrap();
        assert!(!managed_state.is_dirty());

        // Insert and delete.
        managed_state
            .apply_batch(
                &[Op::Insert, Op::Delete, Op::Insert],
                None,
                &[
                    &Utf8Array::from_slice(&[Some("def"), Some("abc"), Some("abc")])
                        .unwrap()
                        .into(),
                    &I64Array::from_slice(&[Some(3), Some(0), Some(4)])
                        .unwrap()
                        .into(),
                ],
                epoch,
            )
            .await
            .unwrap();
        assert!(managed_state.is_dirty());

        // Check output after insertion and deletion.
        assert_eq!(
            managed_state.get_output(epoch).await.unwrap(),
            Some(ScalarImpl::Utf8("ghi||def||def||abc".to_string()))
        );

        epoch += 1;
        let mut write_batch = store.start_write_batch();
        managed_state.flush(&mut write_batch).unwrap();
        write_batch.ingest(epoch).await.unwrap();
        assert!(!managed_state.is_dirty());

        // Deletion.
        managed_state
            .apply_batch(
                &[Op::Delete, Op::Delete, Op::Delete],
                None,
                &[
                    &Utf8Array::from_slice(&[Some("def"), Some("def"), Some("abc")])
                        .unwrap()
                        .into(),
                    &I64Array::from_slice(&[Some(3), Some(1), Some(4)])
                        .unwrap()
                        .into(),
                ],
                epoch,
            )
            .await
            .unwrap();

        assert!(managed_state.is_dirty());

        // Check output after deletion.
        assert_eq!(
            managed_state.get_output(epoch).await.unwrap(),
            Some(ScalarImpl::Utf8("ghi".to_string()))
        );

        epoch += 1;
        let mut write_batch = store.start_write_batch();
        managed_state.flush(&mut write_batch).unwrap();
        write_batch.ingest(epoch).await.unwrap();
        assert!(!managed_state.is_dirty());

        // Check output after flush.
        assert_eq!(
            managed_state.get_output(epoch).await.unwrap(),
            Some(ScalarImpl::Utf8("ghi".to_string()))
        );

        // Drop the state like machine crashes.
        let row_count = managed_state.get_row_count();
        drop(managed_state);

        // Recover the state by `row_count`.
        let mut managed_state = create_managed_state(&keyspace, row_count).await;
        assert!(!managed_state.is_dirty());
        // Get the output after recovery
        assert_eq!(
            managed_state.get_output(epoch).await.unwrap(),
            Some(ScalarImpl::Utf8("ghi".to_string()))
        );

        // Insert and delete the same string.
        managed_state
            .apply_batch(
                &[Op::Insert, Op::Delete, Op::Insert],
                None,
                &[
                    &Utf8Array::from_slice(&[Some("ghi"), Some("ghi"), Some("ghi")])
                        .unwrap()
                        .into(),
                    &I64Array::from_slice(&[Some(5), Some(2), Some(6)])
                        .unwrap()
                        .into(),
                ],
                epoch,
            )
            .await
            .unwrap();
        assert!(managed_state.is_dirty());
        assert_eq!(
            managed_state.get_output(epoch).await.unwrap(),
            Some(ScalarImpl::Utf8("ghi||ghi".to_string()))
        );
        // Check dirtiness after getting the output.
        // Since no flushing happened, it is still dirty.
        assert!(managed_state.is_dirty());

        // Delete all the strings.
        managed_state
            .apply_batch(
                &[Op::Delete, Op::Delete],
                None,
                &[
                    &Utf8Array::from_slice(&[Some("ghi"), Some("ghi")])
                        .unwrap()
                        .into(),
                    &I64Array::from_slice(&[Some(5), Some(6)]).unwrap().into(),
                ],
                epoch,
            )
            .await
            .unwrap();
        assert!(managed_state.is_dirty());
        assert_eq!(managed_state.get_output(epoch).await.unwrap(), None,);
        assert_eq!(managed_state.get_row_count(), 0);

        managed_state
            .apply_batch(
                &[Op::Insert, Op::Insert],
                None,
                &[
                    &Utf8Array::from_slice(&[Some("code"), Some("miko")])
                        .unwrap()
                        .into(),
                    &I64Array::from_slice(&[Some(7), Some(8)]).unwrap().into(),
                ],
                epoch,
            )
            .await
            .unwrap();

        epoch += 1;
        let mut write_batch = store.start_write_batch();
        managed_state.flush(&mut write_batch).unwrap();
        write_batch.ingest(epoch).await.unwrap();
        assert!(!managed_state.is_dirty());
        let row_count = managed_state.get_row_count();

        drop(managed_state);
        let mut managed_state = create_managed_state(&keyspace, row_count).await;
        // Delete right after recovery.
        managed_state
            .apply_batch(
                &[Op::Delete, Op::Insert],
                None,
                &[
                    &Utf8Array::from_slice(&[Some("code"), Some("miko")])
                        .unwrap()
                        .into(),
                    &I64Array::from_slice(&[Some(7), Some(9)]).unwrap().into(),
                ],
                epoch,
            )
            .await
            .unwrap();
        assert_eq!(
            managed_state.get_output(epoch).await.unwrap(),
            Some(ScalarImpl::Utf8("miko||miko".to_string()))
        );

        epoch += 1;
        let mut write_batch = store.start_write_batch();
        managed_state.flush(&mut write_batch).unwrap();
        write_batch.ingest(epoch).await.unwrap();
        assert!(!managed_state.is_dirty());

        let row_count = managed_state.get_row_count();

        drop(managed_state);
        let mut managed_state = create_managed_state(&keyspace, row_count).await;
        assert_eq!(
            managed_state.get_output(epoch).await.unwrap(),
            Some(ScalarImpl::Utf8("miko||miko".to_string()))
        );

        // Insert and Delete but not flush before crash.
        managed_state
            .apply_batch(
                &[Op::Insert, Op::Delete, Op::Insert],
                None,
                &[
                    &Utf8Array::from_slice(&[Some("naive"), Some("miko"), Some("simple")])
                        .unwrap()
                        .into(),
                    &I64Array::from_slice(&[Some(10), Some(9), Some(11)])
                        .unwrap()
                        .into(),
                ],
                epoch,
            )
            .await
            .unwrap();
        assert_eq!(
            managed_state.get_output(epoch).await.unwrap(),
            Some(ScalarImpl::Utf8("simple||naive||miko".to_string()))
        );

        let row_count = managed_state.get_row_count();

        drop(managed_state);
        let mut managed_state = create_managed_state(&keyspace, row_count).await;
        // As we didn't flush the changes, the result should be the same as the result before last
        // changes.
        assert_eq!(
            managed_state.get_output(epoch).await.unwrap(),
            Some(ScalarImpl::Utf8("miko||miko".to_string()))
        );
    }
}
