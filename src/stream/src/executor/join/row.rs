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

use risingwave_common::row::{self, CompactedRow, OwnedRow, Row, RowExt};
use risingwave_common::types::{DataType, ScalarImpl};
use risingwave_common_estimate_size::EstimateSize;

use crate::executor::StreamExecutorResult;

pub trait JoinEncoding: 'static + Send + Sync + Default {
    type EncodedRow: CachedJoinRow + Default;
    fn encode<R: Row>(row: &JoinRow<R>) -> Self::EncodedRow;
}

#[derive(Default)]
pub struct CpuEncoding {}

impl JoinEncoding for CpuEncoding {
    type EncodedRow = JoinRow<OwnedRow>;

    fn encode<R: Row>(row: &JoinRow<R>) -> JoinRow<OwnedRow> {
        JoinRow::new(row.row.to_owned_row(), row.degree)
    }
}

#[derive(Default)]
pub struct MemoryEncoding {}

impl JoinEncoding for MemoryEncoding {
    type EncodedRow = EncodedJoinRow;

    fn encode<R: Row>(row: &JoinRow<R>) -> EncodedJoinRow {
        EncodedJoinRow {
            compacted_row: (&row.row).into(),
            degree: row.degree,
        }
    }
}

/// This is a row with a match degree
#[derive(Clone, Debug)]
pub struct JoinRow<R: Row> {
    pub row: R,
    pub degree: DegreeType,
}

impl<R: Row> JoinRow<R> {
    pub fn new(row: R, degree: DegreeType) -> Self {
        Self { row, degree }
    }

    pub fn is_zero_degree(&self) -> bool {
        self.degree == 0
    }

    /// Return row and degree in `Row` format. The degree part will be inserted in degree table
    /// later, so a pk prefix will be added.
    ///
    /// * `state_order_key_indices` - the order key of `row`
    pub fn to_table_rows<'a>(
        &'a self,
        state_order_key_indices: &'a [usize],
    ) -> (&'a R, impl Row + 'a) {
        let order_key = (&self.row).project(state_order_key_indices);
        let degree = build_degree_row(order_key, self.degree);
        (&self.row, degree)
    }
}

pub type DegreeType = u64;

fn build_degree_row(order_key: impl Row, degree: DegreeType) -> impl Row {
    order_key.chain(row::once(Some(ScalarImpl::Int64(degree as i64))))
}

pub trait CachedJoinRow: EstimateSize + Default + Send + Sync {
    fn decode(&self, data_types: &[DataType]) -> StreamExecutorResult<JoinRow<OwnedRow>>;

    fn increase_degree(&mut self);

    fn decrease_degree(&mut self);
}

#[derive(Clone, Debug, EstimateSize, Default)]
pub struct EncodedJoinRow {
    pub compacted_row: CompactedRow,
    pub degree: DegreeType,
}

impl CachedJoinRow for EncodedJoinRow {
    fn decode(&self, data_types: &[DataType]) -> StreamExecutorResult<JoinRow<OwnedRow>> {
        let row = self.compacted_row.deserialize(data_types)?;
        Ok(JoinRow::new(row, self.degree))
    }

    fn increase_degree(&mut self) {
        self.degree += 1;
    }

    fn decrease_degree(&mut self) {
        self.degree -= 1;
    }
}

impl Default for JoinRow<OwnedRow> {
    fn default() -> JoinRow<OwnedRow> {
        Self {
            row: OwnedRow::default(),
            degree: DegreeType::default(),
        }
    }
}

impl EstimateSize for JoinRow<OwnedRow> {
    fn estimated_heap_size(&self) -> usize {
        self.row.estimated_heap_size()
    }
}

impl CachedJoinRow for JoinRow<OwnedRow> {
    fn decode(&self, _data_types: &[DataType]) -> StreamExecutorResult<JoinRow<OwnedRow>> {
        Ok(self.clone())
    }

    fn increase_degree(&mut self) {
        self.degree += 1;
    }

    fn decrease_degree(&mut self) {
        self.degree -= 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cached_join_row_sizes() {
        let encoded_size = size_of::<EncodedJoinRow>();
        let unencoded_size = size_of::<JoinRow<OwnedRow>>();

        assert_eq!(encoded_size, 40);
        assert_eq!(unencoded_size, 24);
    }
}
