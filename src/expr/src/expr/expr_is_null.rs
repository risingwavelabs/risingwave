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

use std::sync::Arc;

use risingwave_common::array::{ArrayImpl, ArrayRef, BoolArray, DataChunk};
use risingwave_common::buffer::Bitmap;
use risingwave_common::row::Row;
use risingwave_common::types::{DataType, Datum, Scalar};

use crate::expr::{BoxedExpression, Expression};
use crate::Result;

#[derive(Debug)]
pub struct IsNullExpression {
    child: BoxedExpression,
    return_type: DataType,
}

#[derive(Debug)]
pub struct IsNotNullExpression {
    child: BoxedExpression,
    return_type: DataType,
}

impl IsNullExpression {
    pub(crate) fn new(child: BoxedExpression) -> Self {
        Self {
            child,
            return_type: DataType::Boolean,
        }
    }
}

impl IsNotNullExpression {
    pub(crate) fn new(child: BoxedExpression) -> Self {
        Self {
            child,
            return_type: DataType::Boolean,
        }
    }
}

impl Expression for IsNullExpression {
    fn return_type(&self) -> DataType {
        self.return_type.clone()
    }

    fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        let child_arr = self.child.eval_checked(input)?;
        let arr = BoolArray::new(
            Bitmap::all_high_bits(input.capacity()),
            !child_arr.null_bitmap(),
        );

        Ok(Arc::new(ArrayImpl::Bool(arr)))
    }

    fn eval_row(&self, input: &Row) -> Result<Datum> {
        let result = self.child.eval_row(input)?;
        let is_null = result.is_none();
        Ok(Some(is_null.to_scalar_value()))
    }
}

impl Expression for IsNotNullExpression {
    fn return_type(&self) -> DataType {
        self.return_type.clone()
    }

    fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        let child_arr = self.child.eval_checked(input)?;
        let null_bitmap = match Arc::try_unwrap(child_arr) {
            Ok(child_arr) => child_arr.into_null_bitmap(),
            Err(child_arr) => child_arr.null_bitmap().clone(),
        };
        let arr = BoolArray::new(Bitmap::all_high_bits(input.capacity()), null_bitmap);

        Ok(Arc::new(ArrayImpl::Bool(arr)))
    }

    fn eval_row(&self, input: &Row) -> Result<Datum> {
        let result = self.child.eval_row(input)?;
        let is_not_null = result.is_some();
        Ok(Some(is_not_null.to_scalar_value()))
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use std::sync::Arc;

    use risingwave_common::array::column::Column;
    use risingwave_common::array::{ArrayBuilder, ArrayImpl, DataChunk, DecimalArrayBuilder};
    use risingwave_common::row::Row;
    use risingwave_common::types::{DataType, Decimal};

    use crate::expr::expr_is_null::{IsNotNullExpression, IsNullExpression};
    use crate::expr::{BoxedExpression, InputRefExpression};
    use crate::Result;

    fn do_test(
        expr: BoxedExpression,
        expected_eval_result: Vec<bool>,
        expected_eval_row_result: Vec<bool>,
    ) -> Result<()> {
        let input_array = {
            let mut builder = DecimalArrayBuilder::new(3);
            builder.append(Some(Decimal::from_str("0.1").unwrap()))?;
            builder.append(Some(Decimal::from_str("-0.1").unwrap()))?;
            builder.append(None)?;
            builder.finish()?
        };

        let input_chunk = DataChunk::new(
            vec![Column::new(Arc::new(ArrayImpl::Decimal(input_array)))],
            3,
        );
        let result_array = expr.eval(&input_chunk).unwrap();
        assert_eq!(3, result_array.len());
        for (i, v) in expected_eval_result.iter().enumerate() {
            assert_eq!(
                *v,
                bool::try_from(result_array.value_at(i).unwrap()).unwrap()
            );
        }

        let rows = vec![
            Row::new(vec![Some(1.into()), Some(2.into())]),
            Row::new(vec![None, Some(2.into())]),
        ];

        for (i, row) in rows.iter().enumerate() {
            let result = expr.eval_row(row).unwrap().unwrap();
            assert_eq!(expected_eval_row_result[i], result.into_bool());
        }

        Ok(())
    }

    #[test]
    fn test_is_null() -> Result<()> {
        let expr = IsNullExpression::new(Box::new(InputRefExpression::new(DataType::Decimal, 0)));
        do_test(Box::new(expr), vec![false, false, true], vec![false, true]).unwrap();
        Ok(())
    }

    #[test]
    fn test_is_not_null() -> Result<()> {
        let expr =
            IsNotNullExpression::new(Box::new(InputRefExpression::new(DataType::Decimal, 0)));
        do_test(Box::new(expr), vec![true, true, false], vec![true, false]).unwrap();
        Ok(())
    }
}
