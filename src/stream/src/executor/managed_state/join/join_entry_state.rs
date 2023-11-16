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

use risingwave_common::estimate_size::KvSize;

use super::*;

/// We manages a `HashMap` in memory for all entries belonging to a join key.
/// When evicted, `cached` does not hold any entries.
///
/// If a `JoinEntryState` exists for a join key, the all records under this
/// join key will be presented in the cache.
#[derive(Default)]
pub struct JoinEntryState {
    /// The full copy of the state.
    cached: join_row_set::JoinRowSet<PkType, StateValueType>,
    kv_heap_size: KvSize,
}

impl EstimateSize for JoinEntryState {
    fn estimated_heap_size(&self) -> usize {
        // TODO: Add btreemap internal size.
        // https://github.com/risingwavelabs/risingwave/issues/9713
        self.kv_heap_size.size()
    }
}

impl JoinEntryState {
    /// Insert into the cache.
    pub fn insert(&mut self, key: PkType, value: StateValueType) {
        self.kv_heap_size.add(&key, &value);
        self.cached.try_insert(key, value).unwrap();
    }

    /// Delete from the cache.
    pub fn remove(&mut self, pk: PkType) {
        if let Some(value) = self.cached.remove(&pk) {
            self.kv_heap_size.sub(&pk, &value);
        } else {
            panic!("pk {:?} should be in the cache", pk);
        }
    }

    /// Note: the first item in the tuple is the mutable reference to the value in this entry, while
    /// the second item is the decoded value. To mutate the degree, one **must not** forget to apply
    /// the changes to the first item.
    ///
    /// WARNING: Should not change the heap size of `StateValueType` with the mutable reference.
    pub fn values_mut<'a>(
        &'a mut self,
        data_types: &'a [DataType],
    ) -> impl Iterator<
        Item = (
            &'a mut StateValueType,
            StreamExecutorResult<JoinRow<OwnedRow>>,
        ),
    > + 'a {
        self.cached.values_mut().map(|encoded| {
            let decoded = encoded.decode(data_types);
            (encoded, decoded)
        })
    }

    pub fn len(&self) -> usize {
        self.cached.len()
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use risingwave_common::array::*;
    use risingwave_common::types::ScalarImpl;
    use risingwave_common::util::iter_util::ZipEqDebug;

    use super::*;

    fn insert_chunk(
        managed_state: &mut JoinEntryState,
        pk_indices: &[usize],
        data_chunk: &DataChunk,
    ) {
        for row_ref in data_chunk.rows() {
            let row: OwnedRow = row_ref.into_owned_row();
            let value_indices = (0..row.len() - 1).collect_vec();
            let pk = pk_indices.iter().map(|idx| row[*idx].clone()).collect_vec();
            // Pk is only a `i64` here, so encoding method does not matter.
            let pk = OwnedRow::new(pk).project(&value_indices).value_serialize();
            let join_row = JoinRow { row, degree: 0 };
            managed_state.insert(pk, join_row.encode());
        }
    }

    fn check(
        managed_state: &mut JoinEntryState,
        col_types: &[DataType],
        col1: &[i64],
        col2: &[i64],
    ) {
        for ((_, matched_row), (d1, d2)) in managed_state
            .values_mut(col_types)
            .zip_eq_debug(col1.iter().zip_eq_debug(col2.iter()))
        {
            let matched_row = matched_row.unwrap();
            assert_eq!(matched_row.row[0], Some(ScalarImpl::Int64(*d1)));
            assert_eq!(matched_row.row[1], Some(ScalarImpl::Int64(*d2)));
            assert_eq!(matched_row.degree, 0);
        }
    }

    #[tokio::test]
    async fn test_managed_all_or_none_state() {
        let mut managed_state = JoinEntryState::default();
        let col_types = vec![DataType::Int64, DataType::Int64];
        let pk_indices = [0];

        let col1 = [3, 2, 1];
        let col2 = [4, 5, 6];
        let data_chunk1 = DataChunk::from_pretty(
            "I I
             3 4
             2 5
             1 6",
        );

        // `Vec` in state
        insert_chunk(&mut managed_state, &pk_indices, &data_chunk1);
        check(&mut managed_state, &col_types, &col1, &col2);

        // `BtreeMap` in state
        let col1 = [1, 2, 3, 4, 5];
        let col2 = [6, 5, 4, 9, 8];
        let data_chunk2 = DataChunk::from_pretty(
            "I I
             5 8
             4 9",
        );
        insert_chunk(&mut managed_state, &pk_indices, &data_chunk2);
        check(&mut managed_state, &col_types, &col1, &col2);
    }
}
