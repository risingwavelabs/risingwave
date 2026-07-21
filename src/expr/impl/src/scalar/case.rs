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

use std::collections::HashMap;
use std::sync::Arc;

use risingwave_common::array::{ArrayRef, DataChunk};
use risingwave_common::bail;
use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::types::{DataType, Datum, ScalarImpl};
use risingwave_expr::expr::{
    AsyncExpression, AsyncExpressionBoxExt, BoxedExpression, ExpressionInfo, SyncExpression,
    SyncExpressionBoxExt, try_convert_all,
};
use risingwave_expr::{Result, build_function};

#[derive(Debug)]
struct WhenClause<E> {
    when: E,
    then: E,
}

#[derive(Debug)]
struct CaseExpression<E> {
    return_type: DataType,
    when_clauses: Vec<WhenClause<E>>,
    else_clause: Option<E>,
}

impl<E> CaseExpression<E> {
    fn new(
        return_type: DataType,
        when_clauses: Vec<WhenClause<E>>,
        else_clause: Option<E>,
    ) -> Self {
        Self {
            return_type,
            when_clauses,
            else_clause,
        }
    }
}

impl<E: ExpressionInfo> ExpressionInfo for CaseExpression<E> {
    fn return_type(&self) -> DataType {
        self.return_type.clone()
    }
}

macro_rules! eval_case {
    ($mode:ident, $this:expr, $input:expr) => {{
        let mut input = $input.clone();
        let input_len = input.capacity();
        let mut selection = vec![None; input_len];
        let when_len = $this.when_clauses.len();
        let mut result_array = Vec::with_capacity(when_len + 1);
        for (when_idx, WhenClause { when, then }) in $this.when_clauses.iter().enumerate() {
            let input_vis = input.visibility().clone();
            // note that evaluated result from when clause may contain bits that are not visible,
            // so we need to mask it with input visibility.
            let calc_then_vis = risingwave_expr::forward!($mode, when, eval(&input))?
                .as_bool()
                .to_bitmap()
                & &input_vis;
            input.set_visibility(calc_then_vis.clone());
            let then_res = risingwave_expr::forward!($mode, then, eval(&input))?;
            calc_then_vis
                .iter_ones()
                .for_each(|pos| selection[pos] = Some(when_idx));
            input.set_visibility(&input_vis & (!calc_then_vis));
            result_array.push(then_res);
        }
        if let Some(ref else_expr) = $this.else_clause {
            let else_res = risingwave_expr::forward!($mode, else_expr, eval(&input))?;
            input
                .visibility()
                .iter_ones()
                .for_each(|pos| selection[pos] = Some(when_len));
            result_array.push(else_res);
        }
        let mut builder = $this.return_type().create_array_builder(input.capacity());
        for (i, sel) in selection.into_iter().enumerate() {
            if let Some(when_idx) = sel {
                builder.append(result_array[when_idx].value_at(i));
            } else {
                builder.append_null();
            }
        }
        Ok(Arc::new(builder.finish()))
    }};
}

macro_rules! eval_row_case {
    ($mode:ident, $this:expr, $input:expr) => {{
        for WhenClause { when, then } in &$this.when_clauses {
            if risingwave_expr::forward!($mode, when, eval_row($input))?
                .is_some_and(|w| w.into_bool())
            {
                return risingwave_expr::forward!($mode, then, eval_row($input));
            }
        }
        if let Some(ref else_expr) = $this.else_clause {
            risingwave_expr::forward!($mode, else_expr, eval_row($input))
        } else {
            Ok(None)
        }
    }};
}

impl<E: SyncExpression> SyncExpression for CaseExpression<E> {
    fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        eval_case!(sync, self, input)
    }

    fn eval_row(&self, input: &OwnedRow) -> Result<Datum> {
        eval_row_case!(sync, self, input)
    }
}

impl<E: AsyncExpression> AsyncExpression for CaseExpression<E> {
    async fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        eval_case!(async, self, input)
    }

    async fn eval_row(&self, input: &OwnedRow) -> Result<Datum> {
        eval_row_case!(async, self, input)
    }
}

/// With large scale of simple form match arms in case-when expression,
/// we could optimize the `CaseExpression` to `ConstantLookupExpression`,
/// which could significantly facilitate the evaluation of case-when.
#[derive(Debug)]
struct ConstantLookupExpression<E> {
    return_type: DataType,
    arms: HashMap<ScalarImpl, E>,
    fallback: Option<E>,
    /// `operand` must exist at present
    operand: E,
}

impl<E> ConstantLookupExpression<E> {
    #[expect(
        clippy::mutable_key_type,
        reason = "constant lookup stores immutable scalar constants as expression keys"
    )]
    fn new(
        return_type: DataType,
        arms: HashMap<ScalarImpl, E>,
        fallback: Option<E>,
        operand: E,
    ) -> Self {
        Self {
            return_type,
            arms,
            fallback,
            operand,
        }
    }
}

impl<E: ExpressionInfo> ExpressionInfo for ConstantLookupExpression<E> {
    fn return_type(&self) -> DataType {
        self.return_type.clone()
    }
}

macro_rules! eval_fallback_lookup {
    ($mode:ident, $this:expr, $input:expr) => {{
        if let Some(ref fallback) = $this.fallback {
            let Ok(res) = risingwave_expr::forward!($mode, fallback, eval_row($input)) else {
                bail!("failed to evaluate the input for fallback arm");
            };
            Ok::<Datum, risingwave_expr::ExprError>(res)
        } else {
            Ok::<Datum, risingwave_expr::ExprError>(None)
        }
    }};
}

macro_rules! lookup {
    ($mode:ident, $this:expr, $datum:expr, $input:expr) => {{
        match $datum.as_ref() {
            Some(datum) => {
                if let Some(expr) = $this.arms.get(datum) {
                    let Ok(res) = risingwave_expr::forward!($mode, expr, eval_row($input)) else {
                        bail!("failed to evaluate the input for normal arm");
                    };
                    Ok(res)
                } else {
                    eval_fallback_lookup!($mode, $this, $input)
                }
            }
            None => eval_fallback_lookup!($mode, $this, $input),
        }
    }};
}

macro_rules! eval_constant_lookup {
    ($mode:ident, $this:expr, $input:expr) => {{
        let input_len = $input.capacity();
        let mut builder = $this.return_type().create_array_builder(input_len);

        // Evaluate the input DataChunk at first
        let eval_result = risingwave_expr::forward!($mode, $this.operand, eval($input))?;

        for i in 0..input_len {
            let datum = eval_result.datum_at(i);
            let (row, vis) = $input.row_at(i);

            // Check for visibility
            if !vis {
                builder.append_null();
                continue;
            }

            // Note that the `owned_row` here is extracted from input
            // rather than from `eval_result`
            let owned_row = row.into_owned_row();

            // Lookup and evaluate the current input datum
            if let Ok(datum) = lookup!($mode, $this, datum, &owned_row) {
                builder.append(datum.as_ref());
            } else {
                bail!("failed to lookup and evaluate the expression in `eval`");
            }
        }

        Ok(Arc::new(builder.finish()))
    }};
}

impl<E: SyncExpression> SyncExpression for ConstantLookupExpression<E> {
    fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        eval_constant_lookup!(sync, self, input)
    }

    fn eval_row(&self, input: &OwnedRow) -> Result<Datum> {
        let datum = self.operand.eval_row(input)?;
        lookup!(sync, self, datum, input)
    }
}

impl<E: AsyncExpression> AsyncExpression for ConstantLookupExpression<E> {
    async fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        eval_constant_lookup!(async, self, input)
    }

    async fn eval_row(&self, input: &OwnedRow) -> Result<Datum> {
        let datum = self.operand.eval_row(input).await?;
        lookup!(async, self, datum, input)
    }
}

#[expect(
    clippy::mutable_key_type,
    reason = "constant lookup builds an immutable scalar-keyed dispatch table"
)]
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

    let fallback = if let Some(else_clause) = iter.into_remainder().next() {
        if else_clause.return_type() != return_type {
            bail!("Type mismatched between else and case.");
        }
        Some(else_clause)
    } else {
        None
    };

    let BoxedExpression::Sync(operand) = operand else {
        return Ok(ConstantLookupExpression::new(return_type, arms, fallback, operand).boxed());
    };
    let arms: Vec<_> = arms.into_iter().collect();
    let sync_arms: HashMap<ScalarImpl, Arc<dyn SyncExpression>> = match try_convert_all(
        arms,
        |(key, expr)| match expr {
            BoxedExpression::Sync(expr) => Ok((key, expr)),
            expr @ BoxedExpression::Async(_) => Err((key, expr)),
        },
        |(key, expr)| (key, BoxedExpression::Sync(expr)),
    ) {
        Ok(arms) => arms.into_iter().collect(),
        Err(arms) => {
            return Ok(ConstantLookupExpression::new(
                return_type,
                arms.into_iter().collect(),
                fallback,
                BoxedExpression::Sync(operand),
            )
            .boxed());
        }
    };
    let fallback = match fallback {
        Some(BoxedExpression::Sync(expr)) => Some(expr),
        Some(expr @ BoxedExpression::Async(_)) => {
            let arms = sync_arms
                .into_iter()
                .map(|(key, expr)| (key, BoxedExpression::Sync(expr)))
                .collect();
            return Ok(ConstantLookupExpression::new(
                return_type,
                arms,
                Some(expr),
                BoxedExpression::Sync(operand),
            )
            .boxed());
        }
        None => None,
    };
    Ok(ConstantLookupExpression::new(return_type, sync_arms, fallback, operand).boxed())
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
    let else_clause = if let Some(else_clause) = iter.into_remainder().next() {
        if else_clause.return_type() != return_type {
            bail!("Type mismatched between else and case.");
        }
        Some(else_clause)
    } else {
        None
    };

    let sync_when_clauses = match try_convert_all(
        when_clauses,
        |WhenClause { when, then }| match (when, then) {
            (BoxedExpression::Sync(when), BoxedExpression::Sync(then)) => {
                Ok(WhenClause { when, then })
            }
            (when, then) => Err(WhenClause { when, then }),
        },
        |WhenClause { when, then }| WhenClause {
            when: BoxedExpression::Sync(when),
            then: BoxedExpression::Sync(then),
        },
    ) {
        Ok(when_clauses) => when_clauses,
        Err(when_clauses) => {
            return Ok(CaseExpression::new(return_type, when_clauses, else_clause).boxed());
        }
    };
    let else_clause = match else_clause {
        Some(BoxedExpression::Sync(expr)) => Some(expr),
        Some(expr @ BoxedExpression::Async(_)) => {
            let when_clauses = sync_when_clauses
                .into_iter()
                .map(|WhenClause { when, then }| WhenClause {
                    when: BoxedExpression::Sync(when),
                    then: BoxedExpression::Sync(then),
                })
                .collect();
            return Ok(CaseExpression::new(return_type, when_clauses, Some(expr)).boxed());
        }
        None => None,
    };
    Ok(CaseExpression::new(return_type, sync_when_clauses, else_clause).boxed())
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
