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

use std::collections::HashMap;
use std::sync::Arc;

use risingwave_common::array::{ArrayRef, DataChunk};
use risingwave_common::bail;
use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::types::{DataType, Datum, ScalarImpl};
use risingwave_expr::expr::{BoxedExpression, Expression};
use risingwave_expr::{Result, build_function};

#[derive(Debug)]
struct WhenClause {
    when: BoxedExpression,
    then: BoxedExpression,
}

#[derive(Debug)]
struct CaseExpression {
    return_type: DataType,
    when_clauses: Vec<WhenClause>,
    else_clause: Option<BoxedExpression>,
}

impl CaseExpression {
    fn new(
        return_type: DataType,
        when_clauses: Vec<WhenClause>,
        else_clause: Option<BoxedExpression>,
    ) -> Self {
        Self {
            return_type,
            when_clauses,
            else_clause,
        }
    }
}

#[async_trait::async_trait]
impl Expression for CaseExpression {
    fn return_type(&self) -> DataType {
        self.return_type.clone()
    }

    async fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        let mut input = input.clone();
        let input_len = input.capacity();
        let mut selection = vec![None; input_len];
        let when_len = self.when_clauses.len();
        let mut result_array = Vec::with_capacity(when_len + 1);
        for (when_idx, WhenClause { when, then }) in self.when_clauses.iter().enumerate() {
            let input_vis = input.visibility().clone();
            // note that evaluated result from when clause may contain bits that are not visible,
            // so we need to mask it with input visibility.
            let calc_then_vis = when.eval(&input).await?.as_bool().to_bitmap() & &input_vis;
            input.set_visibility(calc_then_vis.clone());
            let then_res = then.eval(&input).await?;
            calc_then_vis
                .iter_ones()
                .for_each(|pos| selection[pos] = Some(when_idx));
            input.set_visibility(&input_vis & (!calc_then_vis));
            result_array.push(then_res);
        }
        if let Some(ref else_expr) = self.else_clause {
            let else_res = else_expr.eval(&input).await?;
            input
                .visibility()
                .iter_ones()
                .for_each(|pos| selection[pos] = Some(when_len));
            result_array.push(else_res);
        }
        let mut builder = self.return_type().create_array_builder(input.capacity());
        for (i, sel) in selection.into_iter().enumerate() {
            if let Some(when_idx) = sel {
                builder.append(result_array[when_idx].value_at(i));
            } else {
                builder.append_null();
            }
        }
        Ok(Arc::new(builder.finish()))
    }

    async fn eval_row(&self, input: &OwnedRow) -> Result<Datum> {
        for WhenClause { when, then } in &self.when_clauses {
            if when.eval_row(input).await?.is_some_and(|w| w.into_bool()) {
                return then.eval_row(input).await;
            }
        }
        if let Some(ref else_expr) = self.else_clause {
            else_expr.eval_row(input).await
        } else {
            Ok(None)
        }
    }
}

/// With large scale of simple form match arms in case-when expression,
/// we could optimize the `CaseExpression` to `ConstantLookupExpression`,
/// which could significantly facilitate the evaluation of case-when.
#[derive(Debug)]
struct ConstantLookupExpression {
    return_type: DataType,
    arms: HashMap<ScalarImpl, BoxedExpression>,
    fallback: Option<BoxedExpression>,
    /// `operand` must exist at present
    operand: BoxedExpression,
}

impl ConstantLookupExpression {
    fn new(
        return_type: DataType,
        arms: HashMap<ScalarImpl, BoxedExpression>,
        fallback: Option<BoxedExpression>,
        operand: BoxedExpression,
    ) -> Self {
        Self {
            return_type,
            arms,
            fallback,
            operand,
        }
    }

    /// Evaluate the fallback arm with the given input
    async fn eval_fallback(&self, input: &OwnedRow) -> Result<Datum> {
        let Some(ref fallback) = self.fallback else {
            return Ok(None);
        };
        let Ok(res) = fallback.eval_row(input).await else {
            bail!("failed to evaluate the input for fallback arm");
        };
        Ok(res)
    }

    /// The actual lookup & evaluation logic
    /// used in both `eval_row` & `eval`
    async fn lookup(&self, datum: Datum, input: &OwnedRow) -> Result<Datum> {
        if datum.is_none() {
            return self.eval_fallback(input).await;
        }

        if let Some(expr) = self.arms.get(datum.as_ref().unwrap()) {
            let Ok(res) = expr.eval_row(input).await else {
                bail!("failed to evaluate the input for normal arm");
            };
            Ok(res)
        } else {
            // Fallback arm goes here
            self.eval_fallback(input).await
        }
    }
}

#[async_trait::async_trait]
impl Expression for ConstantLookupExpression {
    fn return_type(&self) -> DataType {
        self.return_type.clone()
    }

    async fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        let input_len = input.capacity();
        let mut builder = self.return_type().create_array_builder(input_len);

        // Evaluate the input DataChunk at first
        let eval_result = self.operand.eval(input).await?;

        for i in 0..input_len {
            let datum = eval_result.datum_at(i);
            let (row, vis) = input.row_at(i);

            // Check for visibility
            if !vis {
                builder.append_null();
                continue;
            }

            // Note that the `owned_row` here is extracted from input
            // rather than from `eval_result`
            let owned_row = row.into_owned_row();

            // Lookup and evaluate the current input datum
            if let Ok(datum) = self.lookup(datum, &owned_row).await {
                builder.append(datum.as_ref());
            } else {
                bail!("failed to lookup and evaluate the expression in `eval`");
            }
        }

        Ok(Arc::new(builder.finish()))
    }

    async fn eval_row(&self, input: &OwnedRow) -> Result<Datum> {
        let datum = self.operand.eval_row(input).await?;
        self.lookup(datum, input).await
    }
}

#[build_function("constant_lookup(...) -> any", type_infer = "unreachable")]
fn build_constant_lookup_expr(
    return_type: DataType,
    children: Vec<BoxedExpression>,
) -> Result<BoxedExpression> {
    if children.is_empty() {
        bail!("children expression must not be empty for constant lookup expression");
    }

    let mut children = children;

    let operand = children.remove(0);

    let mut arms = HashMap::new();

    // Build the `arms` with iterating over `when` & `then` clauses
    let mut iter = children.into_iter().array_chunks();
    for [when, then] in iter.by_ref() {
        let Ok(Some(s)) = when.eval_const() else {
            bail!("expect when expression to be const");
        };
        arms.insert(s, then);
    }

    let fallback = if let Some(else_clause) = iter.into_remainder().unwrap().next() {
        if else_clause.return_type() != return_type {
            bail!("Type mismatched between else and case.");
        }
        Some(else_clause)
    } else {
        None
    };

    Ok(Box::new(ConstantLookupExpression::new(
        return_type,
        arms,
        fallback,
        operand,
    )))
}

#[build_function("case(...) -> any", type_infer = "unreachable")]
fn build_case_expr(
    return_type: DataType,
    children: Vec<BoxedExpression>,
) -> Result<BoxedExpression> {
    // children: (when, then)+, (else_clause)?
    let len = children.len();
    let mut when_clauses = Vec::with_capacity(len / 2);
    let mut iter = children.into_iter().array_chunks();
    for [when, then] in iter.by_ref() {
        if when.return_type() != DataType::Boolean {
            bail!("Type mismatched between when clause and condition");
        }
        if then.return_type() != return_type {
            bail!("Type mismatched between then clause and case");
        }
        when_clauses.push(WhenClause { when, then });
    }
    let else_clause = if let Some(else_clause) = iter.into_remainder().unwrap().next() {
        if else_clause.return_type() != return_type {
            bail!("Type mismatched between else and case.");
        }
        Some(else_clause)
    } else {
        None
    };

    Ok(Box::new(CaseExpression::new(
        return_type,
        when_clauses,
        else_clause,
    )))
}

#[cfg(test)]
mod tests {
    use risingwave_common::test_prelude::DataChunkTestExt;
    use risingwave_common::types::ToOwnedDatum;
    use risingwave_common::util::iter_util::ZipEqDebug;
    use risingwave_expr::expr::build_from_pretty;

    use super::*;

    #[tokio::test]
    async fn test_eval_searched_case() {
        // when x then 1 else 2
        let case = build_from_pretty("(case:int4 $0:boolean 1:int4 2:int4)");
        let (input, expected) = DataChunk::from_pretty(
            "B i
             t 1
             f 2
             t 1
             t 1
             f 2",
        )
        .split_column_at(1);

        // test eval
        let output = case.eval(&input).await.unwrap();
        assert_eq!(&output, expected.column_at(0));

        // test eval_row
        for (row, expected) in input.rows().zip_eq_debug(expected.rows()) {
            let result = case.eval_row(&row.to_owned_row()).await.unwrap();
            assert_eq!(result, expected.datum_at(0).to_owned_datum());
        }
    }

    #[tokio::test]
    async fn test_eval_without_else() {
        // when x then 1 when y then 2
        let case = build_from_pretty("(case:int4 $0:boolean 1:int4 $1:boolean 2:int4)");
        let (input, expected) = DataChunk::from_pretty(
            "B B i
             f f .
             f t 2
             t f 1
             t t 1",
        )
        .split_column_at(2);

        // test eval
        let output = case.eval(&input).await.unwrap();
        assert_eq!(&output, expected.column_at(0));

        // test eval_row
        for (row, expected) in input.rows().zip_eq_debug(expected.rows()) {
            let result = case.eval_row(&row.to_owned_row()).await.unwrap();
            assert_eq!(result, expected.datum_at(0).to_owned_datum());
        }
    }
}
