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

use std::collections::HashSet;
use std::fmt::Debug;
use std::sync::Arc;

use futures_util::FutureExt;
use risingwave_common::array::{ArrayBuilder, ArrayRef, BoolArrayBuilder, DataChunk};
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, Datum, Scalar, ToOwnedDatum};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_expr::expr::{BoxedExpression, Expression};
use risingwave_expr::{Result, build_function};

#[derive(Debug)]
pub struct InExpression {
    left: BoxedExpression,
    set: HashSet<Datum>,
    return_type: DataType,
}

impl InExpression {
    pub fn new(
        left: BoxedExpression,
        data: impl Iterator<Item = Datum>,
        return_type: DataType,
    ) -> Self {
        Self {
            left,
            set: data.collect(),
            return_type,
        }
    }

    // Returns true if datum exists in set, null if datum is null or datum does not exist in set
    // but null does, and false if neither datum nor null exists in set.
    fn exists(&self, datum: &Datum) -> Option<bool> {
        if datum.is_none() {
            None
        } else if self.set.contains(datum) {
            Some(true)
        } else if self.set.contains(&None) {
            None
        } else {
            Some(false)
        }
    }
}

#[async_trait::async_trait]
impl Expression for InExpression {
    fn return_type(&self) -> DataType {
        self.return_type.clone()
    }

    async fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        let input_array = self.left.eval(input).await?;
        let mut output_array = BoolArrayBuilder::new(input_array.len());
        for (data, vis) in input_array.iter().zip_eq_fast(input.visibility().iter()) {
            if vis {
                // TODO: avoid `to_owned_datum()`
                let ret = self.exists(&data.to_owned_datum());
                output_array.append(ret);
            } else {
                output_array.append(None);
            }
        }
        Ok(Arc::new(output_array.finish().into()))
    }

    async fn eval_row(&self, input: &OwnedRow) -> Result<Datum> {
        let data = self.left.eval_row(input).await?;
        let ret = self.exists(&data);
        Ok(ret.map(|b| b.to_scalar_value()))
    }
}

#[build_function("in(any, ...) -> boolean")]
fn build(return_type: DataType, children: Vec<BoxedExpression>) -> Result<BoxedExpression> {
    let mut iter = children.into_iter();
    let left_expr = iter.next().unwrap();
    let mut data = Vec::with_capacity(iter.size_hint().0);
    let data_chunk = DataChunk::new_dummy(1);
    for child in iter {
        let array = child
            .eval(&data_chunk)
            .now_or_never()
            .expect("constant expression should not be async")?;
        let datum = array.value_at(0).to_owned_datum();
        data.push(datum);
    }
    Ok(Box::new(InExpression::new(
        left_expr,
        data.into_iter(),
        return_type,
    )))
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::DataChunk;
    use risingwave_common::row::Row;
    use risingwave_common::test_prelude::DataChunkTestExt;
    use risingwave_common::types::ToOwnedDatum;
    use risingwave_common::util::iter_util::ZipEqDebug;
    use risingwave_expr::expr::{Expression, build_from_pretty};

    #[tokio::test]
    async fn test_in_expr() {
        let expr = build_from_pretty("(in:boolean $0:varchar abc:varchar def:varchar)");
        let (input, expected) = DataChunk::from_pretty(
            "T   B
             abc t
             a   f
             def t
             abc t
             .   .",
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

    #[tokio::test]
    async fn test_in_expr_null() {
        let expr = build_from_pretty("(in:boolean $0:varchar abc:varchar null:varchar)");
        let (input, expected) = DataChunk::from_pretty(
            "T   B
             abc t
             a   .
             .   .",
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
