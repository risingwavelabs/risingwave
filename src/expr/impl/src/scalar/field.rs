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
use risingwave_common::array::{ArrayImpl, ArrayRef, DataChunk};
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, Datum, ScalarImpl};
use risingwave_expr::expr::{
    AsyncExpression, AsyncExpressionBoxExt, BoxedExpression, ExpressionInfo, SyncExpression,
    SyncExpressionBoxExt,
};
use risingwave_expr::{Result, build_function};

/// `FieldExpression` access a field from a struct.
#[derive(Debug)]
pub struct FieldExpression<E> {
    return_type: DataType,
    input: E,
    index: usize,
}

impl<E: ExpressionInfo> ExpressionInfo for FieldExpression<E> {
    fn return_type(&self) -> DataType {
        self.return_type.clone()
    }
}

macro_rules! eval_field {
    ($mode:ident, $this:expr, $input:expr) => {{
        let array = risingwave_expr::forward!($mode, $this.input, eval($input))?;
        if let ArrayImpl::Struct(struct_array) = array.as_ref() {
            Ok(struct_array.field_at($this.index).clone())
        } else {
            Err(anyhow!("expects a struct array ref").into())
        }
    }};
}

macro_rules! eval_row_field {
    ($mode:ident, $this:expr, $input:expr) => {{
        let struct_datum = risingwave_expr::forward!($mode, $this.input, eval_row($input))?;
        struct_datum
            .map(|s| match s {
                ScalarImpl::Struct(v) => Ok(v.fields()[$this.index].clone()),
                _ => Err(anyhow!("expects a struct array ref").into()),
            })
            .transpose()
            .map(|x| x.flatten())
    }};
}

impl<E: SyncExpression> SyncExpression for FieldExpression<E> {
    fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        eval_field!(sync, self, input)
    }

    fn eval_row(&self, input: &OwnedRow) -> Result<Datum> {
        eval_row_field!(sync, self, input)
    }
}

impl<E: AsyncExpression> AsyncExpression for FieldExpression<E> {
    async fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        eval_field!(async, self, input)
    }

    async fn eval_row(&self, input: &OwnedRow) -> Result<Datum> {
        eval_row_field!(async, self, input)
    }
}

#[build_function("field(struct, int4) -> any", type_infer = "unreachable")]
fn build(return_type: DataType, children: Vec<BoxedExpression>) -> Result<BoxedExpression> {
    // Field `func_call_node` have 2 child nodes, the first is Field `FuncCall` or
    // `InputRef`, the second is i32 `Literal`.
    let [input, index]: [_; 2] = children.try_into().unwrap();
    let index = index.eval_const()?.unwrap().into_int32() as usize;
    Ok(match input {
        BoxedExpression::Sync(input) => FieldExpression {
            return_type,
            input,
            index,
        }
        .boxed(),
        input @ BoxedExpression::Async(_) => FieldExpression {
            return_type,
            input,
            index,
        }
        .boxed(),
    })
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::{DataChunk, DataChunkTestExt};
    use risingwave_common::row::Row;
    use risingwave_common::types::ToOwnedDatum;
    use risingwave_common::util::iter_util::ZipEqDebug;
    use risingwave_expr::expr::build_from_pretty;

    #[tokio::test]
    async fn test_field_expr() {
        let expr = build_from_pretty("(field:int4 $0:struct<a_int4,b_float4> 0:int4)");
        let (input, expected) = DataChunk::from_pretty(
            "<i,f>   i
             (1,2.0) 1
             (2,2.0) 2
             (3,2.0) 3",
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
