use async_trait::async_trait;
use bytes::Bytes;

use std::collections::BTreeMap;

use risingwave_common::array::stream_chunk::Op;
use risingwave_common::array::stream_chunk::Ops;
use risingwave_common::array::ArrayImpl;
use risingwave_common::buffer::Bitmap;
use risingwave_common::error::Result;

use crate::stream_op::OrderedArraysSerializer;
use risingwave_common::types::{
    deserialize_datum_not_null_from, serialize_datum_not_null_into, DataTypeKind, Datum, Scalar,
};

use crate::stream_op::state_aggregation::ManagedExtremeState;

use super::super::keyspace::{Keyspace, StateStore};

pub struct ManagedStringAggState<S: StateStore> {
    cache: BTreeMap<Bytes, String>,

    /// A cached result.
    result: Option<String>,

    /// Marks whether there are modifications.
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
            result: Some(String::from("")),
            dirty: false,
            total_count: row_count,
            sort_key_indices,
            value_index,
            delimiter,
            keyspace,
            sorted_arrays_serializer: sort_key_serializer,
        })
    }
}

impl<S: StateStore> ManagedStringAggState<S> {
    async fn read_all_into_memory(&mut self) -> Result<()> {
        // Read all.
        let all_data = self.keyspace.scan(None).await?;
        for (raw_key, raw_value) in all_data {
            // We only need to deserialize the value, and keep the key as bytes.
            let mut deserializer = memcomparable::Deserializer::from_slice(&raw_value[..]);
            let value =
                deserialize_datum_not_null_from(&DataTypeKind::Char, &mut deserializer)?.unwrap();
            let value_string: String = value.into_utf8();
            self.cache.insert(raw_key, value_string.clone());
        }
        self.dirty = false;
        Ok(())
    }

    fn concat_strings_in_cache_into_result(&mut self) {
        use itertools::Itertools;
        let res = self.cache.values().join(&self.delimiter);
        self.result = Some(res);
    }
}

#[async_trait]
impl<S: StateStore> ManagedExtremeState<S> for ManagedStringAggState<S> {
    async fn apply_batch(
        &mut self,
        ops: Ops<'_>,
        visibility: Option<&Bitmap>,
        data: &[&ArrayImpl],
    ) -> Result<()> {
        debug_assert!(super::verify_batch(ops, visibility, data));
        for sort_key_index in &self.sort_key_indices {
            debug_assert!(*sort_key_index < data.len());
        }
        debug_assert!(self.value_index < data.len());

        if self.total_count > self.cache.len() {
            assert_eq!(self.cache.len(), 0);
            // Not all the state is in the memory.
            // The current policy is all-or-nothing.
            self.read_all_into_memory().await?;
        }

        let mut row_keys = vec![];
        self.sorted_arrays_serializer
            .order_based_scehmaed_serialize(data, &mut row_keys);

        for (row_idx, (op, key_bytes)) in ops.iter().zip(row_keys.into_iter()).enumerate() {
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
                    self.cache.insert(key_bytes.into(), value);
                    self.total_count += 1;
                }
                Op::Delete | Op::UpdateDelete => {
                    self.cache.remove::<Bytes>(&(key_bytes.into()));
                    self.total_count -= 1;
                }
            }
            // TODO: This can be further optimized as `Delete` and `Insert` may cancel each other.
            self.dirty = true;
            self.result = None;
        }
        Ok(())
    }

    async fn get_output(&mut self) -> Result<Datum> {
        // We allow people to get output when the data is dirty.
        // As this is easier compared to `ManagedMinState` as we have a all-or-nothing cache policy
        // here.
        if !self.dirty {
            // If we have already cached the result, we return it directly.
            if let Some(res) = &self.result {
                return Ok(Some(res.clone().into()));
            } else {
                // If there is simply no data, we return empty string.
                if self.total_count == 0 {
                    return Ok(Some(String::from("").into()));
                } else if !self.cache.is_empty() {
                    self.concat_strings_in_cache_into_result();
                    return Ok(Some(self.result.clone().unwrap().into()));
                }
            }
        }
        // If the state is dirty or we don't have the state in memory,
        // then we need to load all the state from the memory.
        self.read_all_into_memory().await?;
        self.concat_strings_in_cache_into_result();
        Ok(Some(self.result.clone().unwrap().into()))
    }

    fn is_dirty(&self) -> bool {
        self.dirty
    }

    fn flush(&mut self, write_batch: &mut Vec<(Bytes, Option<Bytes>)>) -> Result<()> {
        if !self.dirty {
            return Ok(());
        }

        for (key, value) in std::mem::take(&mut self.cache) {
            let key_encoded = [self.keyspace.prefix(), &key[..]].concat();
            let mut serializer = memcomparable::Serializer::default();
            serialize_datum_not_null_into(&Some(value.to_scalar_value()), &mut serializer)?;
            let value = serializer.into_inner();
            write_batch.push((key_encoded.into(), Some(value.into())));
        }
        self.dirty = false;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::stream_op::state_aggregation::ordered_serializer::OrderedArraysSerializer;
    use crate::stream_op::state_aggregation::string_agg::ManagedStringAggState;
    use crate::stream_op::state_aggregation::ManagedExtremeState;
    use crate::stream_op::StateStore;
    use crate::stream_op::{Keyspace, MemoryStateStore};
    use risingwave_common::array::{I64Array, Op, Utf8Array};
    use risingwave_common::types::ScalarImpl;
    use risingwave_common::util::sort_util::OrderType;

    #[tokio::test]
    async fn test_managed_string_agg_state() {
        let store = MemoryStateStore::new();
        let keyspace = Keyspace::new(store.clone(), b"233333".to_vec());
        let sort_key_indices = vec![0, 1];
        let value_index = 0;
        let orderings = vec![OrderType::Descending, OrderType::Ascending];
        let order_pairs = orderings
            .clone()
            .into_iter()
            .zip(sort_key_indices.clone().into_iter())
            .collect::<Vec<_>>();
        let sort_key_serializer = OrderedArraysSerializer::new(order_pairs);
        let mut managed_state = ManagedStringAggState::new(
            keyspace,
            0,
            sort_key_indices,
            value_index,
            "|".to_string(),
            sort_key_serializer,
        )
        .await
        .unwrap();
        assert!(!managed_state.is_dirty());

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
            )
            .await
            .unwrap();
        assert!(managed_state.is_dirty());

        assert_eq!(
            managed_state.get_output().await.unwrap(),
            Some(ScalarImpl::Utf8("ghi|def|abc".to_string()))
        );

        let mut write_batch = vec![];
        managed_state.flush(&mut write_batch).unwrap();
        store.ingest_batch(write_batch).await.unwrap();
        assert!(!managed_state.is_dirty());

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
            )
            .await
            .unwrap();
        assert!(managed_state.is_dirty());

        assert_eq!(
            managed_state.get_output().await.unwrap(),
            Some(ScalarImpl::Utf8("ghi|def|def|abc".to_string()))
        );

        let mut write_batch = vec![];
        managed_state.flush(&mut write_batch).unwrap();
        store.ingest_batch(write_batch).await.unwrap();
        assert!(!managed_state.is_dirty());

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
            )
            .await
            .unwrap();

        assert!(managed_state.is_dirty());

        assert_eq!(
            managed_state.get_output().await.unwrap(),
            Some(ScalarImpl::Utf8("ghi".to_string()))
        );

        let mut write_batch = vec![];
        managed_state.flush(&mut write_batch).unwrap();
        store.ingest_batch(write_batch).await.unwrap();
        assert!(!managed_state.is_dirty());

        // Get the output after flush.
        assert_eq!(
            managed_state.get_output().await.unwrap(),
            Some(ScalarImpl::Utf8("ghi".to_string()))
        );
    }
}
