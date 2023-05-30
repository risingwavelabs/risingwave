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

use std::sync::Arc;

use risingwave_common::array::{ArrayImpl, ArrayRef, BoolArray, DataChunk};
use risingwave_common::buffer::Bitmap;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, Datum, Scalar};
use risingwave_expr_macro::build_function;

use crate::expr::{BoxedExpression, Expression};
use crate::Result;

#[derive(Debug)]
pub struct IsNullExpression {
    child: BoxedExpression,
}

#[derive(Debug)]
pub struct IsNotNullExpression {
    child: BoxedExpression,
}

impl IsNullExpression {
    fn new(child: BoxedExpression) -> Self {
        Self { child }
    }
}

impl IsNotNullExpression {
    fn new(child: BoxedExpression) -> Self {
        Self { child }
    }
}

#[async_trait::async_trait]
impl Expression for IsNullExpression {
    fn return_type(&self) -> DataType {
        DataType::Boolean
    }

    async fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        let child_arr = self.child.eval_checked(input).await?;
        let arr = BoolArray::new(!child_arr.null_bitmap(), Bitmap::ones(input.capacity()));

        Ok(Arc::new(ArrayImpl::Bool(arr)))
    }

    async fn eval_row(&self, input: &OwnedRow) -> Result<Datum> {
        let result = self.child.eval_row(input).await?;
        let is_null = result.is_none();
        Ok(Some(is_null.to_scalar_value()))
    }
}

#[async_trait::async_trait]
impl Expression for IsNotNullExpression {
    fn return_type(&self) -> DataType {
        DataType::Boolean
    }

    async fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        let child_arr = self.child.eval_checked(input).await?;
        let null_bitmap = match Arc::try_unwrap(child_arr) {
            Ok(child_arr) => child_arr.into_null_bitmap(),
            Err(child_arr) => child_arr.null_bitmap().clone(),
        };
        let arr = BoolArray::new(null_bitmap, Bitmap::ones(input.capacity()));

        Ok(Arc::new(ArrayImpl::Bool(arr)))
    }

    async fn eval_row(&self, input: &OwnedRow) -> Result<Datum> {
        let result = self.child.eval_row(input).await?;
        let is_not_null = result.is_some();
        Ok(Some(is_not_null.to_scalar_value()))
    }
}

#[build_function("is_null(*) -> boolean")]
fn build_is_null_expr(_: DataType, children: Vec<BoxedExpression>) -> Result<BoxedExpression> {
    Ok(Box::new(IsNullExpression::new(
        children.into_iter().next().unwrap(),
    )))
}

#[build_function("is_not_null(*) -> boolean")]
fn build_is_not_null_expr(_: DataType, children: Vec<BoxedExpression>) -> Result<BoxedExpression> {
    Ok(Box::new(IsNotNullExpression::new(
        children.into_iter().next().unwrap(),
    )))
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use risingwave_common::array::{Array, ArrayBuilder, DataChunk, DecimalArrayBuilder};
    use risingwave_common::row::OwnedRow;
    use risingwave_common::types::{DataType, Decimal};

    use crate::expr::expr_is_null::{IsNotNullExpression, IsNullExpression};
    use crate::expr::{BoxedExpression, InputRefExpression};
    use crate::Result;

    async fn do_test(
        expr: BoxedExpression,
        expected_eval_result: Vec<bool>,
        expected_eval_row_result: Vec<bool>,
    ) -> Result<()> {
        let input_array = {
            let mut builder = DecimalArrayBuilder::new(3);
            builder.append(Some(Decimal::from_str("0.1").unwrap()));
            builder.append(Some(Decimal::from_str("-0.1").unwrap()));
            builder.append(None);
            builder.finish()
        };

        let input_chunk = DataChunk::new(vec![input_array.into_ref()], 3);
        let result_array = expr.eval(&input_chunk).await.unwrap();
        assert_eq!(3, result_array.len());
        for (i, v) in expected_eval_result.iter().enumerate() {
            assert_eq!(
                *v,
                bool::try_from(result_array.value_at(i).unwrap()).unwrap()
            );
        }

        let rows = vec![
            OwnedRow::new(vec![Some(1.into()), Some(2.into())]),
            OwnedRow::new(vec![None, Some(2.into())]),
        ];

        for (i, row) in rows.iter().enumerate() {
            let result = expr.eval_row(row).await.unwrap().unwrap();
            assert_eq!(expected_eval_row_result[i], result.into_bool());
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_is_null() -> Result<()> {
        let expr = IsNullExpression::new(Box::new(InputRefExpression::new(DataType::Decimal, 0)));
        do_test(Box::new(expr), vec![false, false, true], vec![false, true])
            .await
            .unwrap();
        Ok(())
    }

    #[tokio::test]
    async fn test_is_not_null() -> Result<()> {
        let expr =
            IsNotNullExpression::new(Box::new(InputRefExpression::new(DataType::Decimal, 0)));
        do_test(Box::new(expr), vec![true, true, false], vec![true, false])
            .await
            .unwrap();
        Ok(())
    }
}
