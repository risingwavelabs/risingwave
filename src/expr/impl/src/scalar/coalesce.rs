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

use std::ops::BitAnd;
use std::sync::Arc;

use risingwave_common::array::{ArrayRef, DataChunk};
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, Datum};
use risingwave_expr::expr::{BoxedExpression, Expression};
use risingwave_expr::{Result, build_function};

#[derive(Debug)]
pub struct CoalesceExpression {
    return_type: DataType,
    children: Vec<BoxedExpression>,
}

#[async_trait::async_trait]
impl Expression for CoalesceExpression {
    fn return_type(&self) -> DataType {
        self.return_type.clone()
    }

    async fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        let init_vis = input.visibility();
        let mut input = input.clone();
        let len = input.capacity();
        let mut selection: Vec<Option<usize>> = vec![None; len];
        let mut children_array = Vec::with_capacity(self.children.len());
        for (child_idx, child) in self.children.iter().enumerate() {
            let res = child.eval(&input).await?;
            let res_bitmap = res.null_bitmap();
            let orig_vis = input.visibility();
            for pos in orig_vis.bitand(res_bitmap).iter_ones() {
                selection[pos] = Some(child_idx);
            }
            let new_vis = orig_vis & !res_bitmap;
            input.set_visibility(new_vis);
            children_array.push(res);
        }
        let mut builder = self.return_type.create_array_builder(len);
        for (i, sel) in selection.iter().enumerate() {
            if init_vis.is_set(i)
                && let Some(child_idx) = sel
            {
                builder.append(children_array[*child_idx].value_at(i));
            } else {
                builder.append_null()
            }
        }
        Ok(Arc::new(builder.finish()))
    }

    async fn eval_row(&self, input: &OwnedRow) -> Result<Datum> {
        for child in &self.children {
            let datum = child.eval_row(input).await?;
            if datum.is_some() {
                return Ok(datum);
            }
        }
        Ok(None)
    }
}

#[build_function("coalesce(...) -> any", type_infer = "unreachable")]
fn build(return_type: DataType, children: Vec<BoxedExpression>) -> Result<BoxedExpression> {
    Ok(Box::new(CoalesceExpression {
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
    async fn test_coalesce_expr() {
        let expr = build_from_pretty("(coalesce:int4 $0:int4 $1:int4 $2:int4)");
        let (input, expected) = DataChunk::from_pretty(
            "i i i i
             1 . . 1
             . 2 . 2
             . . 3 3
             . . . .",
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
