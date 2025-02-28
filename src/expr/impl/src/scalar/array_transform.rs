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

use std::sync::Arc;

use async_trait::async_trait;
use risingwave_common::array::{ArrayRef, DataChunk};
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, Datum, ListValue, ScalarImpl};
use risingwave_expr::expr::{BoxedExpression, Expression};
use risingwave_expr::{Result, build_function};

#[derive(Debug)]
struct ArrayTransformExpression {
    array: BoxedExpression,
    lambda: BoxedExpression,
}

#[async_trait]
impl Expression for ArrayTransformExpression {
    fn return_type(&self) -> DataType {
        DataType::List(Box::new(self.lambda.return_type()))
    }

    async fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        let lambda_input = self.array.eval(input).await?;
        let lambda_input = Arc::unwrap_or_clone(lambda_input).into_list();
        let new_list = lambda_input
            .map_inner(|flatten_input| async move {
                let flatten_len = flatten_input.len();
                let chunk = DataChunk::new(vec![Arc::new(flatten_input)], flatten_len);
                self.lambda.eval(&chunk).await.map(Arc::unwrap_or_clone)
            })
            .await?;
        Ok(Arc::new(new_list.into()))
    }

    async fn eval_row(&self, input: &OwnedRow) -> Result<Datum> {
        let lambda_input = self.array.eval_row(input).await?;
        let lambda_input = lambda_input.map(ScalarImpl::into_list);
        if let Some(lambda_input) = lambda_input {
            let len = lambda_input.len();
            let chunk = DataChunk::new(vec![Arc::new(lambda_input.into_array())], len);
            let new_vals = self.lambda.eval(&chunk).await?;
            let new_list = ListValue::new(Arc::unwrap_or_clone(new_vals));
            Ok(Some(new_list.into()))
        } else {
            Ok(None)
        }
    }
}

#[build_function("array_transform(anyarray, any) -> anyarray")]
fn build(_: DataType, children: Vec<BoxedExpression>) -> Result<BoxedExpression> {
    let [array, lambda] = <[BoxedExpression; 2]>::try_from(children).unwrap();
    Ok(Box::new(ArrayTransformExpression { array, lambda }))
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::{DataChunk, DataChunkTestExt};
    use risingwave_common::row::Row;
    use risingwave_common::types::ToOwnedDatum;
    use risingwave_common::util::iter_util::ZipEqDebug;
    use risingwave_expr::expr::build_from_pretty;

    #[tokio::test]
    async fn test_array_transform() {
        let expr =
            build_from_pretty("(array_transform:int4[] $0:int4[] (multiply:int4 $0:int4 2:int4))");
        let (input, expected) = DataChunk::from_pretty(
            "i[]     i[]
             {1,2,3} {2,4,6}",
        )
        .split_column_at(1);

        // test eval
        let output = expr.eval(&input).await.unwrap();
        assert_eq!(&output, expected.column_at(0));

        // test eval_row
        for (row, expected) in input.rows().zip_eq_debug(expected.rows()) {
            let result = expr.eval_row(&row.to_owned_row()).await.unwrap();
            assert_eq!(result, expected.datum_at(0).to_owned_datum());
        }
    }
}
