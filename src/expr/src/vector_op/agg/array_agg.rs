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

use risingwave_common::array::{ArrayBuilder, ArrayBuilderImpl, DataChunk, ListValue};
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::{DataType, Datum, Scalar};
use risingwave_common::util::sort_util::OrderPair;

use crate::vector_op::agg::aggregator::Aggregator;

pub struct ArrayAggUnordered {
    return_type: DataType,
    agg_col_idx: usize,
    result: Vec<Datum>,
}

impl ArrayAggUnordered {
    pub fn new(return_type: DataType, agg_col_idx: usize) -> Self {
        debug_assert!(matches!(return_type, DataType::List { datatype: _ }));
        ArrayAggUnordered {
            return_type,
            agg_col_idx,
            result: Vec::new(),
        }
    }

    fn get_result(&self) -> ListValue {
        ListValue::new(self.result.clone())
    }

    fn get_result_and_reset(&mut self) -> ListValue {
        let res = self.get_result();
        self.result.clear();
        res
    }
}

impl Aggregator for ArrayAggUnordered {
    fn return_type(&self) -> DataType {
        self.return_type.clone()
    }

    fn update_single(&mut self, input: &DataChunk, row_id: usize) -> Result<()> {
        let array = input.column_at(self.agg_col_idx).array_ref();
        self.result.push(array.datum_at(row_id));
        Ok(())
    }

    fn update_multi(
        &mut self,
        input: &DataChunk,
        start_row_id: usize,
        end_row_id: usize,
    ) -> Result<()> {
        let array = input.column_at(self.agg_col_idx).array_ref();
        for row_id in start_row_id..end_row_id {
            self.result.push(array.datum_at(row_id));
        }
        Ok(())
    }

    fn output(&self, builder: &mut ArrayBuilderImpl) -> Result<()> {
        if let ArrayBuilderImpl::List(builder) = builder {
            builder
                .append(Some(self.get_result().as_scalar_ref()))
                .map_err(Into::into)
        } else {
            Err(
                ErrorCode::InternalError(format!("Builder fail to match {}.", stringify!(Utf8)))
                    .into(),
            )
        }
    }

    fn output_and_reset(&mut self, builder: &mut ArrayBuilderImpl) -> Result<()> {
        if let ArrayBuilderImpl::List(builder) = builder {
            builder
                .append(Some(self.get_result_and_reset().as_scalar_ref()))
                .map_err(Into::into)
        } else {
            Err(
                ErrorCode::InternalError(format!("Builder fail to match {}.", stringify!(Utf8)))
                    .into(),
            )
        }
    }
}

pub fn create_array_agg_state(
    return_type: DataType,
    agg_col_idx: usize,
    order_pairs: Vec<OrderPair>,
    _order_col_types: Vec<DataType>,
) -> Result<Box<dyn Aggregator>> {
    if order_pairs.is_empty() {
        Ok(Box::new(ArrayAggUnordered::new(return_type, agg_col_idx)))
    } else {
        Err(
            ErrorCode::InternalError("ArrayAgg with order by clause is not supported yet".into())
                .into(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_array_agg_basic() -> Result<()> {
        // TODO(rc)
        Ok(())
    }
}
