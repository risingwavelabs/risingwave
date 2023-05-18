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
use risingwave_common::util::sort_util::{ColumnOrder, OrderType};
use risingwave_expr::function::window::{Frame, FrameBound, WindowFuncKind};

use super::generic::{GenericPlanRef, OverWindow, PlanWindowFunction, ProjectBuilder};
use super::{
    gen_filter_and_pushdown, ColPrunable, ExprRewritable, LogicalProject, PlanBase, PlanRef,
    PlanTreeNodeUnary, PredicatePushdown, StreamEowcOverWindow, StreamSort, ToBatch, ToStream,
};
use crate::expr::{Expr, ExprImpl, InputRef, WindowFunction};
use crate::optimizer::plan_node::{
    ColumnPruningContext, PredicatePushdownContext, RewriteStreamContext, ToStreamContext,
};
use crate::optimizer::property::{Order, RequiredDist};
use crate::utils::{ColIndexMapping, Condition};

/// `LogicalOverWindow` performs `OVER` window functions to its input.
///
/// The output schema is the input schema plus the window functions.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalOverWindow {
    pub base: PlanBase,
    core: OverWindow<PlanRef>,
}

impl LogicalOverWindow {
    fn new(calls: Vec<PlanWindowFunction>, input: PlanRef) -> Self {
        let core = OverWindow::new(calls, input);
        let base = PlanBase::new_logical_with_core(&core);
        Self { base, core }
    }

    pub fn create(
        input: PlanRef,
        mut select_exprs: Vec<ExprImpl>,
    ) -> Result<(PlanRef, Vec<ExprImpl>)> {
        let mut input_proj_builder = ProjectBuilder::default();
        for (idx, field) in input.schema().fields().iter().enumerate() {
            input_proj_builder
                .add_expr(&InputRef::new(idx, field.data_type()).into())
                .map_err(|err| {
                    ErrorCode::NotImplemented(format!("{err} inside input"), None.into())
                })?;
        }
        let mut input_len = input.schema().len();
        for expr in &select_exprs {
            if let ExprImpl::WindowFunction(window_function) = expr {
                let input_idx_in_order_by: Vec<_> = window_function
                    .order_by
                    .sort_exprs
                    .iter()
                    .map(|x| input_proj_builder.add_expr(&x.expr))
                    .try_collect()
                    .map_err(|err| {
                        ErrorCode::NotImplemented(format!("{err} inside order_by"), None.into())
                    })?;
                let input_idx_in_partition_by: Vec<_> = window_function
                    .partition_by
                    .iter()
                    .map(|x| input_proj_builder.add_expr(x))
                    .try_collect()
                    .map_err(|err| {
                        ErrorCode::NotImplemented(format!("{err} inside partition_by"), None.into())
                    })?;
                input_len = input_len
                    .max(*input_idx_in_order_by.iter().max().unwrap_or(&0) + 1)
                    .max(*input_idx_in_partition_by.iter().max().unwrap_or(&0) + 1);
            }
        }

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
            .map(|x| Self::convert_window_function(x, &input_proj_builder))
            .try_collect()?;

        Ok((
            Self::new(
                plan_window_funcs,
                LogicalProject::with_core(input_proj_builder.build(input)).into(),
            )
            .into(),
            select_exprs,
        ))
    }

    fn convert_window_function(
        window_function: WindowFunction,
        input_proj_builder: &ProjectBuilder,
    ) -> Result<PlanWindowFunction> {
        let order_by = window_function
            .order_by
            .sort_exprs
            .into_iter()
            .map(|e| {
                ColumnOrder::new(
                    input_proj_builder.expr_index(&e.expr).unwrap(),
                    e.order_type,
                )
            })
            .collect_vec();
        let partition_by = window_function
            .partition_by
            .into_iter()
            .map(|e| InputRef::new(input_proj_builder.expr_index(&e).unwrap(), e.return_type()))
            .collect_vec();

        let mut args = window_function.args;
        let frame = match window_function.kind {
            WindowFuncKind::RowNumber | WindowFuncKind::Rank | WindowFuncKind::DenseRank => {
                // ignore user-defined frame for rank functions
                Frame::rows(
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
                    Frame::rows(FrameBound::Preceding(offset), FrameBound::CurrentRow)
                } else {
                    Frame::rows(FrameBound::CurrentRow, FrameBound::Following(offset))
                }
            }
            WindowFuncKind::Aggregate(_) => window_function.frame.unwrap_or({
                // FIXME(rc): The following 2 cases should both be `Frame::Range(Unbounded,
                // CurrentRow)` but we don't support yet.
                if order_by.is_empty() {
                    Frame::rows(
                        FrameBound::UnboundedPreceding,
                        FrameBound::UnboundedFollowing,
                    )
                } else {
                    Frame::rows(FrameBound::UnboundedPreceding, FrameBound::CurrentRow)
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

    pub fn split_with_rule(&self, group_rule: Vec<usize>) -> PlanRef {
        assert!(group_rule.len() == self.window_functions().len());

        let mut output_proj_builder = ProjectBuilder::default();
        for (idx, field) in self.input().schema().fields().iter().enumerate() {
            let _ = output_proj_builder
                .add_expr(&InputRef::new(idx, field.data_type()).into())
                .unwrap();
        }
        group_rule
            .iter()
            .enumerate()
            .map(|(idx, &group_id)| (group_id, idx))
            .sorted()
            .enumerate()
            .map(|(input_idx, (_, output_idx))| (output_idx, input_idx))
            .sorted()
            .for_each(|(output_idx, input_idx)| {
                let _ = output_proj_builder
                    .add_expr(
                        &InputRef::new(
                            input_idx,
                            self.window_functions()[output_idx].return_type.clone(),
                        )
                        .into(),
                    )
                    .unwrap();
            });

        let mut cur_input = self.input();
        let mut cur_node = self.clone();
        group_rule.iter().unique().sorted().for_each(|group_id| {
            let cur_group = group_rule
                .iter()
                .enumerate()
                .filter(|(_, x)| x == &group_id)
                .map(|(idx, _)| idx)
                .collect_vec();
            cur_input = cur_node.clone().into();
            cur_node = Self::new(
                cur_group
                    .iter()
                    .map(|&idx| self.window_functions()[idx].clone())
                    .collect_vec(),
                cur_input.clone(),
            );
        });
        LogicalProject::with_core(output_proj_builder.build(cur_node.into())).into()
    }
}

impl PlanTreeNodeUnary for LogicalOverWindow {
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

impl_plan_tree_node_for_unary! { LogicalOverWindow }

impl fmt::Display for LogicalOverWindow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.core.fmt_with_name(f, "LogicalOverWindow")
    }
}

impl ColPrunable for LogicalOverWindow {
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
        let new_self = {
            let input = self.input().prune_col(&input_required_cols, ctx);
            self.rewrite_with_input_and_window(input, &window_functions, input_col_change)
        };
        if new_self.schema().len() == required_cols.len() {
            // current schema perfectly fit the required columns
            new_self.into()
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
            let src_size = new_self.schema().len();
            LogicalProject::with_mapping(
                new_self.into(),
                ColIndexMapping::with_remaining_columns(&output_required_cols, src_size),
            )
            .into()
        }
    }
}

impl ExprRewritable for LogicalOverWindow {}

impl PredicatePushdown for LogicalOverWindow {
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

impl ToBatch for LogicalOverWindow {
    fn to_batch(&self) -> Result<PlanRef> {
        Err(ErrorCode::NotImplemented(
            "Batch over window is not implemented yet".to_string(),
            9124.into(),
        )
        .into())
    }
}

impl ToStream for LogicalOverWindow {
    fn to_stream(&self, ctx: &mut ToStreamContext) -> Result<PlanRef> {
        let stream_input = self.core.input.to_stream(ctx)?;
        stream_input.watermark_columns();

        if ctx.emit_on_window_close() {
            if !self.core.funcs_have_same_partition_and_order() {
                return Err(ErrorCode::InvalidInputSyntax(
                    "All window functions must have the same PARTITION BY and ORDER BY".to_string(),
                )
                .into());
            }

            let order_by = &self.window_functions()[0].order_by;
            if order_by.len() != 1
                || !stream_input
                    .watermark_columns()
                    .contains(order_by[0].column_index)
                || order_by[0].order_type != OrderType::ascending()
            {
                return Err(ErrorCode::InvalidInputSyntax(
                    "Only support window functions order by single watermark column in ascending order"
                        .to_string(),
                )
                .into());
            }
            let order_key_index = order_by[0].column_index;

            let partition_key_indices = self.window_functions()[0]
                .partition_by
                .iter()
                .map(|e| e.index())
                .collect_vec();
            if partition_key_indices.is_empty() {
                return Err(ErrorCode::NotImplemented(
                    "Window function with empty PARTITION BY is not supported yet".to_string(),
                    None.into(),
                )
                .into());
            }

            let sort_input =
                RequiredDist::shard_by_key(stream_input.schema().len(), &partition_key_indices)
                    .enforce_if_not_satisfies(stream_input, &Order::any())?;
            let sort = StreamSort::new(sort_input, order_key_index);

            let mut logical = self.core.clone();
            logical.input = sort.into();
            return Ok(StreamEowcOverWindow::new(logical).into());
        }

        Err(ErrorCode::NotImplemented(
            "General version of streaming over window is not implemented yet".to_string(),
            9124.into(),
        )
        .into())
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
