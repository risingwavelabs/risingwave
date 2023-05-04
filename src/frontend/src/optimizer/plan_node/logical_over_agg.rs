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

use std::fmt;

use fixedbitset::FixedBitSet;
use itertools::Itertools;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::DataType;
use risingwave_common::util::sort_util::ColumnOrder;
use risingwave_expr::function::window::{Frame, FrameBound, WindowFuncKind};

use super::generic::{OverWindow, PlanWindowFunction};
use super::{
    gen_filter_and_pushdown, ColPrunable, ExprRewritable, LogicalProject, PlanBase, PlanRef,
    PlanTreeNodeUnary, PredicatePushdown, ToBatch, ToStream,
};
use crate::expr::{Expr, ExprImpl, InputRef, WindowFunction};
use crate::optimizer::plan_node::{
    ColumnPruningContext, PredicatePushdownContext, RewriteStreamContext, ToStreamContext,
};
use crate::utils::{ColIndexMapping, Condition};

/// `LogicalOverAgg` performs `OVER` window aggregates ([`WindowFunction`]) to its input.
///
/// The output schema is the input schema plus the window functions.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalOverAgg {
    pub base: PlanBase,
    core: OverWindow<PlanRef>,
}

impl LogicalOverAgg {
    fn new(calls: Vec<PlanWindowFunction>, input: PlanRef) -> Self {
        let core = OverWindow::new(calls, input);
        let base = PlanBase::new_logical_with_core(&core);
        Self { base, core }
    }

    pub fn create(
        input: PlanRef,
        mut select_exprs: Vec<ExprImpl>,
    ) -> Result<(PlanRef, Vec<ExprImpl>)> {
        let input_len = input.schema().len();
        let mut window_funcs = vec![];
        for expr in &mut select_exprs {
            if let ExprImpl::WindowFunction(_) = expr {
                let new_expr =
                    InputRef::new(input_len + window_funcs.len(), expr.return_type().clone())
                        .into();
                let f = std::mem::replace(expr, new_expr)
                    .into_window_function()
                    .unwrap();
                window_funcs.push(*f);
            }
            if expr.has_window_function() {
                return Err(ErrorCode::NotImplemented(
                    format!("window function in expression: {:?}", expr),
                    None.into(),
                )
                .into());
            }
        }
        for f in &window_funcs {
            if f.kind.is_rank() {
                if f.order_by.sort_exprs.is_empty() {
                    return Err(ErrorCode::InvalidInputSyntax(format!(
                        "window rank function without order by: {:?}",
                        f
                    ))
                    .into());
                }
                if f.kind == WindowFuncKind::DenseRank {
                    return Err(ErrorCode::NotImplemented(
                        format!("window rank function: {}", f.kind),
                        4847.into(),
                    )
                    .into());
                }
            }
        }

        let plan_window_funcs = window_funcs
            .into_iter()
            .map(Self::convert_window_function)
            .try_collect()?;

        let over_agg = Self::new(plan_window_funcs, input);
        Ok((over_agg.into(), select_exprs))
    }

    fn convert_window_function(window_function: WindowFunction) -> Result<PlanWindowFunction> {
        // TODO: rewrite expressions in `ORDER BY`, `PARTITION BY` and arguments to `InputRef` like
        // in `LogicalAgg`
        let order_by: Vec<_> = window_function
            .order_by
            .sort_exprs
            .into_iter()
            .map(|e| match e.expr.as_input_ref() {
                Some(i) => Ok(ColumnOrder::new(i.index(), e.order_type)),
                None => Err(ErrorCode::NotImplemented(
                    "ORDER BY expression in window function".to_string(),
                    None.into(),
                )),
            })
            .try_collect()?;
        let partition_by: Vec<_> = window_function
            .partition_by
            .into_iter()
            .map(|e| match e.as_input_ref() {
                Some(i) => Ok(*i.clone()),
                None => Err(ErrorCode::NotImplemented(
                    "PARTITION BY expression in window function".to_string(),
                    None.into(),
                )),
            })
            .try_collect()?;

        let mut args = window_function.args;
        let frame = match window_function.kind {
            WindowFuncKind::RowNumber | WindowFuncKind::Rank | WindowFuncKind::DenseRank => {
                // ignore user-defined frame for rank functions
                Frame::Rows(
                    FrameBound::UnboundedPreceding,
                    FrameBound::UnboundedFollowing,
                )
            }
            WindowFuncKind::Lag | WindowFuncKind::Lead => {
                let offset = if args.len() > 1 {
                    let offset_expr = args.remove(1);
                    if !offset_expr.return_type().is_int() {
                        return Err(ErrorCode::InvalidInputSyntax(format!(
                            "the `offset` of `{}` function should be integer",
                            window_function.kind
                        ))
                        .into());
                    }
                    offset_expr
                        .cast_implicit(DataType::Int64)?
                        .try_fold_const()
                        .transpose()?
                        .flatten()
                        .map(|v| *v.as_int64() as usize)
                        .unwrap_or(1usize)
                } else {
                    1usize
                };

                // override the frame
                // TODO(rc): We can only do the optimization for constant offset.
                if window_function.kind == WindowFuncKind::Lag {
                    Frame::Rows(FrameBound::Preceding(offset), FrameBound::CurrentRow)
                } else {
                    Frame::Rows(FrameBound::CurrentRow, FrameBound::Following(offset))
                }
            }
            WindowFuncKind::Aggregate(_) => window_function.frame.unwrap_or({
                // FIXME(rc): The following 2 cases should both be `Frame::Range(Unbounded,
                // CurrentRow)` but we don't support yet.
                if order_by.is_empty() {
                    Frame::Rows(
                        FrameBound::UnboundedPreceding,
                        FrameBound::UnboundedFollowing,
                    )
                } else {
                    Frame::Rows(FrameBound::UnboundedPreceding, FrameBound::CurrentRow)
                }
            }),
        };

        let args = args
            .into_iter()
            .map(|e| match e.as_input_ref() {
                Some(i) => Ok(*i.clone()),
                None => Err(ErrorCode::NotImplemented(
                    "expression arguments in window function".to_string(),
                    None.into(),
                )),
            })
            .try_collect()?;

        Ok(PlanWindowFunction {
            kind: window_function.kind,
            return_type: window_function.return_type,
            args,
            partition_by,
            order_by,
            frame,
        })
    }

    pub fn window_functions(&self) -> &[PlanWindowFunction] {
        &self.core.window_functions
    }

    #[must_use]
    fn rewrite_with_input_and_window(
        &self,
        input: PlanRef,
        window_functions: &[PlanWindowFunction],
        input_col_change: ColIndexMapping,
    ) -> Self {
        let window_functions = window_functions
            .iter()
            .cloned()
            .map(|mut window_function| {
                window_function.args.iter_mut().for_each(|i| {
                    *i = InputRef::new(input_col_change.map(i.index()), i.return_type())
                });
                window_function.order_by.iter_mut().for_each(|o| {
                    o.column_index = input_col_change.map(o.column_index);
                });
                window_function.partition_by.iter_mut().for_each(|i| {
                    *i = InputRef::new(input_col_change.map(i.index()), i.return_type())
                });
                window_function
            })
            .collect();
        Self::new(window_functions, input)
    }
}

impl PlanTreeNodeUnary for LogicalOverAgg {
    fn input(&self) -> PlanRef {
        self.core.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(self.core.window_functions.clone(), input)
    }

    #[must_use]
    fn rewrite_with_input(
        &self,
        input: PlanRef,
        input_col_change: ColIndexMapping,
    ) -> (Self, ColIndexMapping) {
        let input_len = self.core.input_len();
        let new_input_len = input.schema().len();
        let output_len = self.core.output_len();
        let new_output_len = new_input_len + self.window_functions().len();
        let output_col_change = {
            let mut mapping = ColIndexMapping::empty(output_len, new_output_len);
            for win_func_idx in 0..self.window_functions().len() {
                mapping.put(input_len + win_func_idx, Some(new_input_len + win_func_idx));
            }
            mapping.union(&input_col_change)
        };
        let new_self =
            self.rewrite_with_input_and_window(input, self.window_functions(), input_col_change);
        (new_self, output_col_change)
    }
}

impl_plan_tree_node_for_unary! { LogicalOverAgg }

impl fmt::Display for LogicalOverAgg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.core.fmt_with_name(f, "LogicalOverAgg")
    }
}

impl ColPrunable for LogicalOverAgg {
    fn prune_col(&self, required_cols: &[usize], ctx: &mut ColumnPruningContext) -> PlanRef {
        let input_cnt = self.input().schema().len();
        let raw_required_cols = {
            let mut tmp = FixedBitSet::with_capacity(input_cnt);
            required_cols
                .iter()
                .filter(|&&index| index < input_cnt)
                .for_each(|&index| tmp.set(index, true));
            tmp
        };

        let (window_function_required_cols, window_functions) = {
            let mut tmp = FixedBitSet::with_capacity(input_cnt);
            let new_window_functions = required_cols
                .iter()
                .filter(|&&index| index >= input_cnt)
                .map(|&index| {
                    let index = index - input_cnt;
                    let window_function = self.window_functions()[index].clone();
                    tmp.extend(window_function.args.iter().map(|x| x.index()));
                    tmp.extend(window_function.partition_by.iter().map(|x| x.index()));
                    tmp.extend(window_function.order_by.iter().map(|x| x.column_index));
                    window_function
                })
                .collect_vec();
            (tmp, new_window_functions)
        };

        let input_required_cols = {
            let mut tmp = FixedBitSet::with_capacity(input_cnt);
            tmp.union_with(&raw_required_cols);
            tmp.union_with(&window_function_required_cols);
            tmp.ones().collect_vec()
        };
        let input_col_change =
            ColIndexMapping::with_remaining_columns(&input_required_cols, input_cnt);
        let new_over_agg = {
            let input = self.input().prune_col(&input_required_cols, ctx);
            self.rewrite_with_input_and_window(input, &window_functions, input_col_change)
        };
        if new_over_agg.schema().len() == required_cols.len() {
            // current schema perfectly fit the required columns
            new_over_agg.into()
        } else {
            // some columns are not needed so we did a projection to remove the columns.
            let mut new_output_cols = input_required_cols.clone();
            new_output_cols.extend(required_cols.iter().filter(|&&x| x >= input_cnt));
            let mapping =
                &ColIndexMapping::with_remaining_columns(&new_output_cols, self.schema().len());
            let output_required_cols = required_cols
                .iter()
                .map(|&idx| mapping.map(idx))
                .collect_vec();
            let src_size = new_over_agg.schema().len();
            LogicalProject::with_mapping(
                new_over_agg.into(),
                ColIndexMapping::with_remaining_columns(&output_required_cols, src_size),
            )
            .into()
        }
    }
}

impl ExprRewritable for LogicalOverAgg {}

impl PredicatePushdown for LogicalOverAgg {
    fn predicate_pushdown(
        &self,
        predicate: Condition,
        ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        let mut window_col = FixedBitSet::with_capacity(self.schema().len());
        window_col.insert_range(self.core.input.schema().len()..self.schema().len());
        let (window_pred, other_pred) = predicate.split_disjoint(&window_col);
        gen_filter_and_pushdown(self, window_pred, other_pred, ctx)
    }
}

impl ToBatch for LogicalOverAgg {
    fn to_batch(&self) -> Result<PlanRef> {
        Err(ErrorCode::NotImplemented("OverAgg to batch".to_string(), 9124.into()).into())
    }
}

impl ToStream for LogicalOverAgg {
    fn to_stream(&self, _ctx: &mut ToStreamContext) -> Result<PlanRef> {
        Err(ErrorCode::NotImplemented("OverAgg to stream".to_string(), 9124.into()).into())
    }

    fn logical_rewrite_for_stream(
        &self,
        ctx: &mut RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        let (input, input_col_change) = self.core.input.logical_rewrite_for_stream(ctx)?;
        let (new_self, output_col_change) = self.rewrite_with_input(input, input_col_change);
        Ok((new_self.into(), output_col_change))
    }
}
