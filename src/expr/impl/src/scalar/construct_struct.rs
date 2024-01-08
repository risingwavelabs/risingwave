// Copyright 2024 RisingWave Labs
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

use risingwave_common::array::{ArrayImpl, ArrayRef, DataChunk, StructArray};
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, Datum, ScalarImpl, StructValue};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_expr::expr::{BoxedExpression, Expression};
use risingwave_expr::{build_function, Result};

#[derive(Debug)]
pub struct ConstructStructExpression {
    return_type: DataType,
    children: Vec<BoxedExpression>,
}

#[async_trait::async_trait]
impl Expression for ConstructStructExpression {
    fn return_type(&self) -> DataType {
        self.return_type.clone()
    }

    async fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        let mut struct_cols = Vec::with_capacity(self.children.len());
        for child in &self.children {
            let res = child.eval(input).await?;
            struct_cols.push(res);
        }
        Ok(Arc::new(ArrayImpl::Struct(StructArray::new(
            self.return_type.as_struct().clone(),
            struct_cols,
            input.visibility().clone(),
        ))))
    }

    async fn eval_row(&self, input: &OwnedRow) -> Result<Datum> {
        let mut datums = Vec::with_capacity(self.children.len());
        for child in &self.children {
            let res = child.eval_row(input).await?;
            datums.push(res);
        }
        Ok(Some(ScalarImpl::Struct(StructValue::new(datums))))
    }
}

#[build_function("construct_struct(...) -> struct", type_infer = "panic")]
fn build(return_type: DataType, children: Vec<BoxedExpression>) -> Result<BoxedExpression> {
    assert!(return_type.is_struct());
    return_type
        .as_struct()
        .types()
        .zip_eq_fast(children.iter())
        .for_each(|(ty, child)| {
            assert_eq!(*ty, child.return_type());
        });

    Ok(Box::new(ConstructStructExpression {
        return_type,
        children,
    }))
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::DataChunk;
    use risingwave_common::row::Row;
    use risingwave_common::test_prelude::DataChunkTestExt;
    use risingwave_common::types::ToOwnedDatum;
    use risingwave_common::util::iter_util::ZipEqDebug;
    use risingwave_expr::expr::build_from_pretty;

    #[tokio::test]
    async fn test_construct_struct_expr() {
        let expr = build_from_pretty(
            "(construct_struct:struct<a_int4,b_int4,c_int4> $0:int4 $1:int4 $2:int4)",
        );
        let (input, expected) = DataChunk::from_pretty(
            "i i i <i,i,i>
             1 2 3 (1,2,3)
             4 2 1 (4,2,1)
             9 1 3 (9,1,3)
             1 1 1 (1,1,1)",
        )
        .split_column_at(3);

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
