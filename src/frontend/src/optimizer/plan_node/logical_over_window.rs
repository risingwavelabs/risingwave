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

use fixedbitset::FixedBitSet;
use itertools::Itertools;
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_common::types::{DataType, Datum, ScalarImpl};
use risingwave_common::util::sort_util::{ColumnOrder, OrderType};
use risingwave_expr::aggregate::AggKind;
use risingwave_expr::window_function::{Frame, FrameBound, WindowFuncKind};

use super::generic::{GenericPlanRef, OverWindow, PlanWindowFunction, ProjectBuilder};
use super::utils::impl_distill_by_unit;
use super::{
    gen_filter_and_pushdown, BatchOverWindow, ColPrunable, ExprRewritable, Logical, LogicalProject,
    PlanBase, PlanRef, PlanTreeNodeUnary, PredicatePushdown, StreamEowcOverWindow, StreamEowcSort,
    StreamOverWindow, ToBatch, ToStream,
};
use crate::expr::{
    Expr, ExprImpl, ExprRewriter, ExprType, ExprVisitor, FunctionCall, InputRef, WindowFunction,
};
use crate::optimizer::plan_node::{
    ColumnPruningContext, Literal, PredicatePushdownContext, RewriteStreamContext, ToStreamContext,
};
use crate::optimizer::property::{Order, RequiredDist};
use crate::utils::{ColIndexMapping, Condition, IndexSet};

struct LogicalOverWindowBuilder<'a> {
    /// the builder of the input Project
    input_proj_builder: &'a ProjectBuilder,
    /// the window functions
    window_functions: &'a mut Vec<WindowFunction>,
    /// the error during the expression rewriting
    error: Option<RwError>,
}

impl<'a> LogicalOverWindowBuilder<'a> {
    fn new(
        input_proj_builder: &'a ProjectBuilder,
        window_functions: &'a mut Vec<WindowFunction>,
    ) -> Result<Self> {
        Ok(Self {
            input_proj_builder,
            window_functions,
            error: None,
        })
    }

    fn rewrite_selected_items(&mut self, selected_items: Vec<ExprImpl>) -> Result<Vec<ExprImpl>> {
        let mut rewritten_items = vec![];
        for expr in selected_items {
            let rewritten_expr = self.rewrite_expr(expr);
            if let Some(error) = self.error.take() {
                return Err(error);
            } else {
                rewritten_items.push(rewritten_expr);
            }
        }
        Ok(rewritten_items)
    }

    fn schema_over_window_start_offset(&self) -> usize {
        self.input_proj_builder.exprs_len()
    }

    fn push_window_func(&mut self, window_func: WindowFunction) -> InputRef {
        if let Some((pos, existing)) = self
            .window_functions
            .iter()
            .find_position(|&w| w == &window_func)
        {
            return InputRef::new(
                self.schema_over_window_start_offset() + pos,
                existing.return_type.clone(),
            );
        }
        let index = self.schema_over_window_start_offset() + self.window_functions.len();
        let data_type = window_func.return_type.clone();
        self.window_functions.push(window_func);
        InputRef::new(index, data_type)
    }

    fn try_rewrite_window_function(&mut self, window_func: WindowFunction) -> Result<ExprImpl> {
        let (kind, args, return_type, partition_by, order_by, frame) = (
            window_func.kind,
            window_func.args,
            window_func.return_type,
            window_func.partition_by,
            window_func.order_by,
            window_func.frame,
        );

        if let WindowFuncKind::Aggregate(agg_kind) = kind
            && matches!(
                agg_kind,
                AggKind::Avg
                    | AggKind::StddevPop
                    | AggKind::StddevSamp
                    | AggKind::VarPop
                    | AggKind::VarSamp
            )
        {
            // Refer to `LogicalAggBuilder::try_rewrite_agg_call`
            match agg_kind {
                AggKind::Avg => {
                    assert_eq!(args.len(), 1);
                    let left_ref = ExprImpl::from(self.push_window_func(WindowFunction::new(
                        WindowFuncKind::Aggregate(AggKind::Sum),
                        partition_by.clone(),
                        order_by.clone(),
                        args.clone(),
                        frame.clone(),
                    )?))
                    .cast_explicit(return_type)?;
                    let right_ref = ExprImpl::from(self.push_window_func(WindowFunction::new(
                        WindowFuncKind::Aggregate(AggKind::Count),
                        partition_by,
                        order_by,
                        args,
                        frame,
                    )?));

                    let new_expr = ExprImpl::from(FunctionCall::new(
                        ExprType::Divide,
                        vec![left_ref, right_ref],
                    )?);
                    Ok(new_expr)
                }
                AggKind::StddevPop | AggKind::StddevSamp | AggKind::VarPop | AggKind::VarSamp => {
                    let input = args.first().unwrap();
                    let squared_input_expr = ExprImpl::from(FunctionCall::new(
                        ExprType::Multiply,
                        vec![input.clone(), input.clone()],
                    )?);

                    let sum_of_squares_expr =
                        ExprImpl::from(self.push_window_func(WindowFunction::new(
                            WindowFuncKind::Aggregate(AggKind::Sum),
                            partition_by.clone(),
                            order_by.clone(),
                            vec![squared_input_expr],
                            frame.clone(),
                        )?))
                        .cast_explicit(return_type.clone())?;

                    let sum_expr = ExprImpl::from(self.push_window_func(WindowFunction::new(
                        WindowFuncKind::Aggregate(AggKind::Sum),
                        partition_by.clone(),
                        order_by.clone(),
                        args.clone(),
                        frame.clone(),
                    )?))
                    .cast_explicit(return_type.clone())?;

                    let count_expr = ExprImpl::from(self.push_window_func(WindowFunction::new(
                        WindowFuncKind::Aggregate(AggKind::Count),
                        partition_by,
                        order_by,
                        args.clone(),
                        frame,
                    )?));

                    let square_of_sum_expr = ExprImpl::from(FunctionCall::new(
                        ExprType::Multiply,
                        vec![sum_expr.clone(), sum_expr],
                    )?);

                    let numerator_expr = ExprImpl::from(FunctionCall::new(
                        ExprType::Subtract,
                        vec![
                            sum_of_squares_expr,
                            ExprImpl::from(FunctionCall::new(
                                ExprType::Divide,
                                vec![square_of_sum_expr, count_expr.clone()],
                            )?),
                        ],
                    )?);

                    let denominator_expr = match agg_kind {
                        AggKind::StddevPop | AggKind::VarPop => count_expr.clone(),
                        AggKind::StddevSamp | AggKind::VarSamp => {
                            ExprImpl::from(FunctionCall::new(
                                ExprType::Subtract,
                                vec![
                                    count_expr.clone(),
                                    ExprImpl::from(Literal::new(
                                        Datum::from(ScalarImpl::Int64(1)),
                                        DataType::Int64,
                                    )),
                                ],
                            )?)
                        }
                        _ => unreachable!(),
                    };

                    let mut target_expr = ExprImpl::from(FunctionCall::new(
                        ExprType::Divide,
                        vec![numerator_expr, denominator_expr],
                    )?);

                    if matches!(agg_kind, AggKind::StddevPop | AggKind::StddevSamp) {
                        target_expr = ExprImpl::from(
                            FunctionCall::new(ExprType::Sqrt, vec![target_expr]).unwrap(),
                        );
                    }

                    match agg_kind {
                        AggKind::VarPop | AggKind::StddevPop => Ok(target_expr),
                        AggKind::StddevSamp | AggKind::VarSamp => {
                            let less_than_expr = ExprImpl::from(FunctionCall::new(
                                ExprType::LessThanOrEqual,
                                vec![
                                    count_expr,
                                    ExprImpl::from(Literal::new(
                                        Datum::from(ScalarImpl::Int64(1)),
                                        DataType::Int64,
                                    )),
                                ],
                            )?);
                            let null_expr = ExprImpl::from(Literal::new(None, return_type));

                            let case_expr = ExprImpl::from(FunctionCall::new(
                                ExprType::Case,
                                vec![less_than_expr, null_expr, target_expr],
                            )?);
                            Ok(case_expr)
                        }
                        _ => unreachable!(),
                    }
                }
                _ => unreachable!(),
            }
        } else {
            let new_expr = ExprImpl::from(self.push_window_func(WindowFunction::new(
                kind,
                partition_by,
                order_by,
                args,
                frame,
            )?));
            Ok(new_expr)
        }
    }
}

impl<'a> ExprRewriter for LogicalOverWindowBuilder<'a> {
    fn rewrite_window_function(&mut self, window_func: WindowFunction) -> ExprImpl {
        let dummy = Literal::new(None, window_func.return_type()).into();
        match self.try_rewrite_window_function(window_func) {
            Ok(expr) => expr,
            Err(err) => {
                self.error = Some(err);
                dummy
            }
        }
    }

    fn rewrite_input_ref(&mut self, input_ref: InputRef) -> ExprImpl {
        let input_expr = input_ref.into();
        let index = self.input_proj_builder.expr_index(&input_expr).unwrap();
        ExprImpl::from(InputRef::new(index, input_expr.return_type()))
    }
}

/// Build columns from window function `args` / `partition_by` / `order_by`
struct OverWindowProjectBuilder<'a> {
    builder: &'a mut ProjectBuilder,
    error: Option<ErrorCode>,
}

impl<'a> OverWindowProjectBuilder<'a> {
    fn new(builder: &'a mut ProjectBuilder) -> Self {
        Self {
            builder,
            error: None,
        }
    }

    fn try_visit_window_function(
        &mut self,
        window_function: &WindowFunction,
    ) -> std::result::Result<(), ErrorCode> {
        if let WindowFuncKind::Aggregate(agg_kind) = window_function.kind
            && matches!(
                agg_kind,
                AggKind::StddevPop | AggKind::StddevSamp | AggKind::VarPop | AggKind::VarSamp
            )
        {
            let input = window_function.args.iter().exactly_one().unwrap();
            let squared_input_expr = ExprImpl::from(
                FunctionCall::new(ExprType::Multiply, vec![input.clone(), input.clone()]).unwrap(),
            );
            self.builder.add_expr(&squared_input_expr).map_err(|err| {
                ErrorCode::NotImplemented(format!("{err} inside args"), None.into())
            })?;
        }
        for arg in &window_function.args {
            self.builder.add_expr(arg).map_err(|err| {
                ErrorCode::NotImplemented(format!("{err} inside args"), None.into())
            })?;
        }
        for partition_by in &window_function.partition_by {
            self.builder.add_expr(partition_by).map_err(|err| {
                ErrorCode::NotImplemented(format!("{err} inside partition_by"), None.into())
            })?;
        }
        for order_by in window_function.order_by.sort_exprs.iter().map(|e| &e.expr) {
            self.builder.add_expr(order_by).map_err(|err| {
                ErrorCode::NotImplemented(format!("{err} inside order_by"), None.into())
            })?;
        }
        Ok(())
    }
}

impl<'a> ExprVisitor for OverWindowProjectBuilder<'a> {
    type Result = ();

    fn merge(_a: (), _b: ()) {}

    fn visit_window_function(&mut self, window_function: &WindowFunction) {
        if let Err(e) = self.try_visit_window_function(window_function) {
            self.error = Some(e);
        }
    }
}

/// `LogicalOverWindow` performs `OVER` window functions to its input.
///
/// The output schema is the input schema plus the window functions.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalOverWindow {
    pub base: PlanBase<Logical>,
    core: OverWindow<PlanRef>,
}

impl LogicalOverWindow {
    pub fn new(calls: Vec<PlanWindowFunction>, input: PlanRef) -> Self {
        let core = OverWindow::new(calls, input);
        let base = PlanBase::new_logical_with_core(&core);
        Self { base, core }
    }

    fn build_input_proj(input: PlanRef, select_exprs: &[ExprImpl]) -> Result<ProjectBuilder> {
        let mut input_proj_builder = ProjectBuilder::default();
        // Add and check input columns
        for (idx, field) in input.schema().fields().iter().enumerate() {
            input_proj_builder
                .add_expr(&InputRef::new(idx, field.data_type()).into())
                .map_err(|err| {
                    ErrorCode::NotImplemented(format!("{err} inside input"), None.into())
                })?;
        }
        let mut build_input_proj_visitor = OverWindowProjectBuilder::new(&mut input_proj_builder);
        for expr in select_exprs {
            build_input_proj_visitor.visit_expr(expr);
            if let Some(error) = build_input_proj_visitor.error.take() {
                return Err(error.into());
            }
        }
        Ok(input_proj_builder)
    }

    pub fn create(input: PlanRef, select_exprs: Vec<ExprImpl>) -> Result<(PlanRef, Vec<ExprImpl>)> {
        let input_proj_builder = Self::build_input_proj(input.clone(), &select_exprs)?;

        let mut window_functions = vec![];
        let mut over_window_builder =
            LogicalOverWindowBuilder::new(&input_proj_builder, &mut window_functions)?;

        let rewritten_selected_items = over_window_builder.rewrite_selected_items(select_exprs)?;

        for window_func in &window_functions {
            if window_func.kind.is_rank() && window_func.order_by.sort_exprs.is_empty() {
                return Err(ErrorCode::InvalidInputSyntax(format!(
                    "window rank function without order by: {:?}",
                    window_func
                ))
                .into());
            }
        }

        let plan_window_funcs = window_functions
            .into_iter()
            .map(|x| Self::convert_window_function(x, &input_proj_builder))
            .try_collect()?;

        Ok((
            Self::new(
                plan_window_funcs,
                LogicalProject::with_core(input_proj_builder.build(input)).into(),
            )
            .into(),
            rewritten_selected_items,
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
        let (kind, frame) = match window_function.kind {
            WindowFuncKind::RowNumber | WindowFuncKind::Rank | WindowFuncKind::DenseRank => {
                // ignore user-defined frame for rank functions, also, rank functions only care
                // about the rows before current row
                (
                    window_function.kind,
                    Frame::rows(FrameBound::UnboundedPreceding, FrameBound::CurrentRow),
                )
            }
            WindowFuncKind::Lag | WindowFuncKind::Lead => {
                // `lag(x, const offset N) over ()`
                //     == `first_value(x) over (rows between N preceding and N preceding)`
                // `lead(x, const offset N) over ()`
                //     == `first_value(x) over (rows between N following and N following)`

                let offset = if args.len() > 1 {
                    let offset_expr = args.remove(1);
                    if !offset_expr.return_type().is_int() {
                        return Err(ErrorCode::InvalidInputSyntax(format!(
                            "the `offset` of `{}` function should be integer",
                            window_function.kind
                        ))
                        .into());
                    }
                    let const_offset = offset_expr.cast_implicit(DataType::Int64)?.try_fold_const();
                    if const_offset.is_none() {
                        // should already be checked in `WindowFunction::infer_return_type`,
                        // but just in case
                        return Err(ErrorCode::NotImplemented(
                            "non-const `offset` of `lag`/`lead` is not supported yet".to_string(),
                            None.into(),
                        )
                        .into());
                    }
                    const_offset.unwrap()?.map(|v| *v.as_int64()).unwrap_or(1)
                } else {
                    1
                };
                let sign = if window_function.kind == WindowFuncKind::Lag {
                    -1
                } else {
                    1
                };
                let abs_offset = offset.unsigned_abs() as usize;
                let frame = if sign * offset <= 0 {
                    Frame::rows(
                        FrameBound::Preceding(abs_offset),
                        FrameBound::Preceding(abs_offset),
                    )
                } else {
                    Frame::rows(
                        FrameBound::Following(abs_offset),
                        FrameBound::Following(abs_offset),
                    )
                };

                (WindowFuncKind::Aggregate(AggKind::FirstValue), frame)
            }
            WindowFuncKind::Aggregate(_) => {
                let frame = window_function.frame.unwrap_or({
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
                });
                (window_function.kind, frame)
            }
        };

        let args = args
            .into_iter()
            .map(|e| InputRef::new(input_proj_builder.expr_index(&e).unwrap(), e.return_type()))
            .collect_vec();

        Ok(PlanWindowFunction {
            kind,
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

    pub fn split_with_rule(&self, groups: Vec<Vec<usize>>) -> PlanRef {
        assert!(groups.iter().flatten().all_unique());
        assert!(groups
            .iter()
            .flatten()
            .all(|&idx| idx < self.window_functions().len()));

        let input_len = self.input().schema().len();
        let original_out_fields = (0..input_len + self.window_functions().len()).collect_vec();
        let mut out_fields = original_out_fields.clone();
        let mut cur_input = self.input();
        let mut cur_node = self.clone();
        let mut cur_win_func_pos = input_len;
        for func_indices in &groups {
            cur_node = Self::new(
                func_indices
                    .iter()
                    .map(|&idx| {
                        let func = &self.window_functions()[idx];
                        out_fields[input_len + idx] = cur_win_func_pos;
                        cur_win_func_pos += 1;
                        func.clone()
                    })
                    .collect_vec(),
                cur_input.clone(),
            );
            cur_input = cur_node.clone().into();
        }
        if out_fields == original_out_fields {
            cur_node.into()
        } else {
            LogicalProject::with_out_col_idx(cur_node.into(), out_fields.into_iter()).into()
        }
    }

    pub fn decompose(self) -> (PlanRef, Vec<PlanWindowFunction>) {
        self.core.decompose()
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
impl_distill_by_unit!(LogicalOverWindow, core, "LogicalOverWindow");

impl ColPrunable for LogicalOverWindow {
    fn prune_col(&self, required_cols: &[usize], ctx: &mut ColumnPruningContext) -> PlanRef {
        let input_len = self.input().schema().len();

        let (req_cols_input_part, req_cols_win_func_part) = {
            let mut in_input = required_cols.to_vec();
            let in_win_funcs: IndexSet = in_input.extract_if(|i| *i >= input_len).collect();
            (IndexSet::from(in_input), in_win_funcs)
        };

        if req_cols_win_func_part.is_empty() {
            // no window function is needed
            return self.input().prune_col(&req_cols_input_part.to_vec(), ctx);
        }

        let (input_cols_required_by_this, window_functions) = {
            let mut tmp = IndexSet::empty();
            let new_window_functions = req_cols_win_func_part
                .indices()
                .map(|idx| self.window_functions()[idx - input_len].clone())
                .map(|func| {
                    tmp.extend(func.args.iter().map(|x| x.index()));
                    tmp.extend(func.partition_by.iter().map(|x| x.index()));
                    tmp.extend(func.order_by.iter().map(|x| x.column_index));
                    func
                })
                .collect_vec();
            (tmp, new_window_functions)
        };

        let input_required_cols = (req_cols_input_part | input_cols_required_by_this).to_vec();
        let input_col_change =
            ColIndexMapping::with_remaining_columns(&input_required_cols, input_len);
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
            new_output_cols.extend(required_cols.iter().filter(|&&x| x >= input_len));
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
        if !self.core.funcs_have_same_partition_and_order() {
            return Err(ErrorCode::InvalidInputSyntax(
                "All window functions must have the same PARTITION BY and ORDER BY".to_string(),
            )
            .into());
        }

        // TODO(rc): Let's not introduce too many cases at once. Later we may decide to support
        // empty PARTITION BY by simply removing the following check.
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

        let input = self.input().to_batch()?;
        let new_logical = OverWindow {
            input,
            ..self.core.clone()
        };
        Ok(BatchOverWindow::new(new_logical).into())
    }
}

impl ToStream for LogicalOverWindow {
    fn to_stream(&self, ctx: &mut ToStreamContext) -> Result<PlanRef> {
        use super::stream::prelude::*;

        let stream_input = self.core.input.to_stream(ctx)?;

        if ctx.emit_on_window_close() {
            // Emit-On-Window-Close case

            if !self.core.funcs_have_same_partition_and_order() {
                return Err(ErrorCode::InvalidInputSyntax(
                    "All window functions must have the same PARTITION BY and ORDER BY".to_string(),
                )
                .into());
            }

            let order_by = &self.window_functions()[0].order_by;
            if order_by.len() != 1 || order_by[0].order_type != OrderType::ascending() {
                return Err(ErrorCode::InvalidInputSyntax(
                    "Only support window functions order by single column and in ascending order"
                        .to_string(),
                )
                .into());
            }
            if !stream_input
                .watermark_columns()
                .contains(order_by[0].column_index)
            {
                return Err(ErrorCode::InvalidInputSyntax(
                    "The column ordered by must be a watermark column".to_string(),
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
            let sort = StreamEowcSort::new(sort_input, order_key_index);

            let mut core = self.core.clone();
            core.input = sort.into();
            Ok(StreamEowcOverWindow::new(core).into())
        } else {
            // General (Emit-On-Update) case

            if !self.core.funcs_have_same_partition_and_order() {
                return Err(ErrorCode::InvalidInputSyntax(
                    "All window functions must have the same PARTITION BY and ORDER BY".to_string(),
                )
                .into());
            }

            // TODO(rc): Let's not introduce too many cases at once. Later we may decide to support
            // empty PARTITION BY by simply removing the following check.
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

            let new_input =
                RequiredDist::shard_by_key(stream_input.schema().len(), &partition_key_indices)
                    .enforce_if_not_satisfies(stream_input, &Order::any())?;
            let mut core = self.core.clone();
            core.input = new_input;
            Ok(StreamOverWindow::new(core).into())
        }
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
