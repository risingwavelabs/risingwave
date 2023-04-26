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
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::DataType;
use risingwave_common::util::sort_util::{ColumnOrder, ColumnOrderDisplay};
use risingwave_expr::function::window::{Frame, FrameBound, WindowFuncKind};

use super::{
    gen_filter_and_pushdown, ColPrunable, ExprRewritable, LogicalProject, PlanBase, PlanRef,
    PlanTreeNodeUnary, PredicatePushdown, ToBatch, ToStream,
};
use crate::expr::{Expr, ExprImpl, InputRef, InputRefDisplay, WindowFunction};
use crate::optimizer::plan_node::{
    ColumnPruningContext, PredicatePushdownContext, RewriteStreamContext, ToStreamContext,
};
use crate::utils::{ColIndexMapping, ColIndexMappingRewriteExt, Condition};

/// Rewritten version of [`WindowFunction`] which uses `InputRef` instead of `ExprImpl`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PlanWindowFunction {
    pub kind: WindowFuncKind,
    pub return_type: DataType,
    pub args: Vec<InputRef>,
    pub partition_by: Vec<InputRef>,
    pub order_by: Vec<ColumnOrder>,
    pub frame: Option<Frame>,
}

struct PlanWindowFunctionDisplay<'a> {
    pub window_function: &'a PlanWindowFunction,
    pub input_schema: &'a Schema,
}

impl<'a> std::fmt::Debug for PlanWindowFunctionDisplay<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let window_function = self.window_function;
        if f.alternate() {
            f.debug_struct("WindowFunction")
                .field("kind", &window_function.kind)
                .field("return_type", &window_function.return_type)
                .field("partition_by", &window_function.partition_by)
                .field("order_by", &window_function.order_by)
                .field("frame", &window_function.frame)
                .finish()
        } else {
            write!(f, "{}() OVER(", window_function.kind)?;

            let mut delim = "";
            if !window_function.partition_by.is_empty() {
                delim = " ";
                write!(
                    f,
                    "PARTITION BY {}",
                    window_function
                        .partition_by
                        .iter()
                        .format_with(", ", |input_ref, f| {
                            f(&InputRefDisplay {
                                input_ref,
                                input_schema: self.input_schema,
                            })
                        })
                )?;
            }
            if !window_function.order_by.is_empty() {
                write!(
                    f,
                    "{delim}ORDER BY {}",
                    window_function.order_by.iter().format_with(", ", |o, f| {
                        f(&ColumnOrderDisplay {
                            column_order: o,
                            input_schema: self.input_schema,
                        })
                    })
                )?;
            }
            if let Some(frame) = &window_function.frame {
                write!(f, "{delim}{}", frame)?;
            }
            f.write_str(")")?;

            Ok(())
        }
    }
}

/// `LogicalOverAgg` performs `OVER` window aggregates ([`WindowFunction`]) to its input.
///
/// The output schema is the input schema plus the window functions.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalOverAgg {
    pub base: PlanBase,
    pub window_functions: Vec<PlanWindowFunction>,
    input: PlanRef,
}

impl LogicalOverAgg {
    fn new(window_functions: Vec<PlanWindowFunction>, input: PlanRef) -> Self {
        let ctx = input.ctx();
        let mut schema = input.schema().clone();
        window_functions.iter().for_each(|func| {
            schema.fields.push(Field::with_name(
                func.return_type.clone(),
                func.kind.to_string(),
            ));
        });

        let logical_pk = input.logical_pk().to_vec();

        let mapping = ColIndexMapping::identity_or_none(input.schema().len(), schema.len());
        let fd_set = input.functional_dependency().clone();
        let fd_set = mapping.rewrite_functional_dependency_set(fd_set);

        let base = PlanBase::new_logical(ctx, schema, logical_pk, fd_set);

        Self {
            base,
            window_functions,
            input,
        }
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
                // ignore frame for rank functions
                None
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
                        .eval_row_const()?
                        .map(|v| *v.as_int64() as usize)
                        .unwrap_or(1usize)
                } else {
                    1usize
                };

                // override the frame
                // TODO(rc): We can only do the optimization for constant offset.
                Some(if window_function.kind == WindowFuncKind::Lag {
                    Frame::Rows(FrameBound::Preceding(offset), FrameBound::CurrentRow)
                } else {
                    Frame::Rows(FrameBound::CurrentRow, FrameBound::Following(offset))
                })
            }
            _ => window_function.frame,
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

    #[must_use]
    fn rewrite_with_input_window(
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
        LogicalOverAgg::new(window_functions, input)
    }
}

impl PlanTreeNodeUnary for LogicalOverAgg {
    fn input(&self) -> PlanRef {
        self.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(self.window_functions.clone(), input)
    }
}

impl_plan_tree_node_for_unary! { LogicalOverAgg }

impl fmt::Display for LogicalOverAgg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut builder = f.debug_struct("LogicalOverAgg");
        let window_funcs_display = self
            .window_functions
            .iter()
            .map(|func| PlanWindowFunctionDisplay {
                window_function: func,
                input_schema: self.input.schema(),
            })
            .collect::<Vec<_>>();
        builder.field("window_functions", &window_funcs_display);
        builder.finish()
    }
}

impl ColPrunable for LogicalOverAgg {
    fn prune_col(&self, required_cols: &[usize], ctx: &mut ColumnPruningContext) -> PlanRef {
        let input_cnt = self.input().schema().fields().len();
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
                    let window_function = self.window_functions[index].clone();
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
        let input_col_change = ColIndexMapping::with_remaining_columns(
            &input_required_cols,
            self.input().schema().len(),
        );
        let new_over_agg = {
            let input = self.input().prune_col(&input_required_cols, ctx);
            self.rewrite_with_input_window(input, &window_functions, input_col_change)
        };
        if new_over_agg.schema().len() == required_cols.len() {
            // current schema perfectly fit the required columns
            new_over_agg.into()
        } else {
            // some columns are not needed so we did a projection to remove the columns.
            let mut new_output_cols = input_required_cols.clone();
            new_output_cols.extend(
                required_cols
                    .iter()
                    .filter(|&&x| x >= self.input().schema().len()),
            );
            let mapping =
                &ColIndexMapping::with_remaining_columns(&new_output_cols, self.schema().len());
            let output_required_cols = required_cols
                .iter()
                .map(|&idx| mapping.map(idx))
                .collect_vec();
            LogicalProject::with_mapping(
                new_over_agg.into(),
                ColIndexMapping::with_remaining_columns(
                    &output_required_cols,
                    new_over_agg.schema().len(),
                ),
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
        window_col.insert_range(self.input.schema().len()..self.schema().len());
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
        _ctx: &mut RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        Err(ErrorCode::NotImplemented("OverAgg to stream".to_string(), 9124.into()).into())
    }
}
