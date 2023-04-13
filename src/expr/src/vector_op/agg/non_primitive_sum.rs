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

use anyhow::anyhow;
use num_traits::Zero;
use risingwave_common::array::{Array, ArrayBuilder, ArrayBuilderImpl, ArrayImpl, DataChunk};
use risingwave_common::bail;
use risingwave_common::types::num256::Int256;
use risingwave_common::types::{CheckedAdd, DataType, Scalar, ScalarRef};

use crate::vector_op::agg::aggregator::Aggregator;

#[derive(Clone)]
pub struct Int256Sum {
    input_col_idx: usize,
    result: Int256,
}

impl Int256Sum {
    pub fn new(input_col_idx: usize) -> Self {
        Self {
            input_col_idx,
            result: Int256::zero(),
        }
    }
}

#[async_trait::async_trait]
impl Aggregator for Int256Sum {
    fn return_type(&self) -> DataType {
        DataType::Int256
    }

    async fn update_single(&mut self, input: &DataChunk, row_id: usize) -> crate::Result<()> {
        if let ArrayImpl::Int256(array) = input.column_at(self.input_col_idx).array_ref() {
            self.result = array
                .value_at(row_id)
                .and_then(|scalar_ref| {
                    self.result
                        .clone()
                        .checked_add(scalar_ref.to_owned_scalar())
                })
                .ok_or_else(|| anyhow!("Overflow when summing up Int256 values."))?;
        }
        Ok(())
    }

    async fn update_multi(
        &mut self,
        input: &DataChunk,
        start_row_id: usize,
        end_row_id: usize,
    ) -> crate::Result<()> {
        if let ArrayImpl::Int256(array) = input.column_at(self.input_col_idx).array_ref() {
            let mut cur = self.result.clone();
            for row_id in start_row_id..end_row_id {
                cur = array
                    .value_at(row_id)
                    .and_then(|scalar_ref| cur.checked_add(scalar_ref.to_owned_scalar()))
                    .ok_or_else(|| anyhow!("Overflow when summing up Int256 values."))?;
            }
            self.result = cur;
            Ok(())
        } else {
            bail!("Unexpected array type for sum(int256).")
        }
    }

    fn output(&mut self, builder: &mut ArrayBuilderImpl) -> crate::Result<()> {
        match builder {
            ArrayBuilderImpl::Int256(b) => {
                b.append(Some(self.result.as_scalar_ref()));
                Ok(())
            }
            _ => bail!("Unexpected builder for sum(int256)."),
        }
    }
}
