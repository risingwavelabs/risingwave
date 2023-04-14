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

use std::collections::HashSet;

use anyhow::anyhow;
use num_traits::{CheckedAdd, Zero};
use risingwave_common::array::{
    Array, ArrayBuilder, ArrayBuilderImpl, ArrayImpl, DataChunk, Int256Array,
};
use risingwave_common::bail;
use risingwave_common::types::num256::Int256;
use risingwave_common::types::{DataType, Scalar, ScalarRef};

use crate::vector_op::agg::aggregator::Aggregator;
use crate::Result;

#[derive(Clone)]
pub struct Int256Sum {
    input_col_idx: usize,
    result: Int256,
    distinct: bool,
    exists: HashSet<Option<Int256>>,
}

impl Int256Sum {
    pub fn new(input_col_idx: usize, distinct: bool) -> Self {
        Self {
            input_col_idx,
            distinct,
            result: Int256::zero(),
            exists: HashSet::new(),
        }
    }

    fn accumulate_value_at(&mut self, array: &Int256Array, row_id: usize) -> Result<()> {
        let value = array
            .value_at(row_id)
            .map(|scalar_ref| scalar_ref.to_owned_scalar());

        if !self.distinct || self.exists.insert(value.clone()) {
            if let Some(scalar) = value {
                self.result = self
                    .result
                    .checked_add(&scalar)
                    .ok_or_else(|| anyhow!("Overflow when summing up Int256 values."))?;
            }
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl Aggregator for Int256Sum {
    fn return_type(&self) -> DataType {
        DataType::Int256
    }

    async fn update_single(&mut self, input: &DataChunk, row_id: usize) -> Result<()> {
        if let ArrayImpl::Int256(array) = input.column_at(self.input_col_idx).array_ref() {
            self.accumulate_value_at(array, row_id)?;
        }
        Ok(())
    }

    async fn update_multi(
        &mut self,
        input: &DataChunk,
        start_row_id: usize,
        end_row_id: usize,
    ) -> Result<()> {
        if let ArrayImpl::Int256(array) = input.column_at(self.input_col_idx).array_ref() {
            for row_id in start_row_id..end_row_id {
                self.accumulate_value_at(array, row_id)?;
            }
            Ok(())
        } else {
            bail!("Unexpected array type for sum(int256).")
        }
    }

    fn output(&mut self, builder: &mut ArrayBuilderImpl) -> Result<()> {
        match builder {
            ArrayBuilderImpl::Int256(b) => {
                b.append(Some(self.result.as_scalar_ref()));
                Ok(())
            }
            _ => bail!("Unexpected builder for sum(int256)."),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use itertools::Itertools;
    use risingwave_common::array::column::Column;
    use risingwave_common::array::Int256ArrayBuilder;

    use super::*;

    #[tokio::test]
    async fn vec_sum_int256() -> Result<()> {
        let numbers = vec![1, 2, 3, 4, 1];
        let values = numbers.iter().cloned().map(Int256::from).collect_vec();
        let input = Int256Array::from_iter(values.iter().map(|r| Some(r.as_scalar_ref())));

        let input_len = input.len();
        let array_ref = Arc::new(input.into());
        let input_chunk = DataChunk::new(vec![Column::new(array_ref)], input_len);

        let actual = sum(&input_chunk, false)
            .await?
            .iter()
            .map(|v| v.map(|v| v.to_owned_scalar()))
            .collect_vec();

        assert_eq!(
            actual,
            vec![Some(Int256::from(numbers.iter().sum::<i64>()))]
        );

        let actual = sum(&input_chunk, true)
            .await?
            .iter()
            .map(|v| v.map(|v| v.to_owned_scalar()))
            .collect_vec();

        assert_eq!(
            actual,
            vec![Some(Int256::from(
                numbers.into_iter().unique().sum::<i64>()
            ))]
        );

        Ok(())
    }

    async fn sum(input_chunk: &DataChunk, distinct: bool) -> Result<Int256Array> {
        let mut builder = ArrayBuilderImpl::Int256(Int256ArrayBuilder::new(0));

        let mut agg_state = Int256Sum::new(0, distinct);

        agg_state
            .update_multi(input_chunk, 0, input_chunk.cardinality() - 1)
            .await?;

        agg_state
            .update_single(input_chunk, input_chunk.cardinality() - 1)
            .await?;

        agg_state.output(&mut builder)?;

        Ok(builder.finish().into())
    }
}
