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

use risingwave_common::row::{self, CompactedRow, OwnedRow, Row, RowExt};
use risingwave_common::types::{DataType, ScalarImpl};
use risingwave_common_estimate_size::EstimateSize;

use crate::executor::StreamExecutorResult;

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

    pub fn to_degree_row<'a>(&'a self, state_order_key_indices: &'a [usize]) -> impl Row + 'a {
        self.to_table_rows(state_order_key_indices).1
    }

    pub fn encode(&self) -> EncodedJoinRow {
        EncodedJoinRow {
            compacted_row: (&self.row).into(),
            degree: self.degree,
        }
    }
}

pub type DegreeType = u64;

fn build_degree_row(order_key: impl Row, degree: DegreeType) -> impl Row {
    order_key.chain(row::once(Some(ScalarImpl::Int64(degree as i64))))
}

#[derive(Clone, Debug, EstimateSize)]
pub struct EncodedJoinRow {
    pub compacted_row: CompactedRow,
    pub degree: DegreeType,
}

impl EncodedJoinRow {
    pub fn decode(&self, data_types: &[DataType]) -> StreamExecutorResult<JoinRow<OwnedRow>> {
        Ok(JoinRow {
            row: self.decode_row(data_types)?,
            degree: self.degree,
        })
    }

    fn decode_row(&self, data_types: &[DataType]) -> StreamExecutorResult<OwnedRow> {
        let row = self.compacted_row.deserialize(data_types)?;
        Ok(row)
    }
}
