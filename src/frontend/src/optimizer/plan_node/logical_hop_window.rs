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
use std::num::NonZeroUsize;

use fixedbitset::FixedBitSet;
use itertools::Itertools;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::Result;
use risingwave_common::types::{DataType, IntervalUnit};
use risingwave_expr::ExprError;

use super::generic::GenericPlanNode;
use super::{
    gen_filter_and_pushdown, generic, BatchHopWindow, ColPrunable, ExprRewritable, LogicalFilter,
    PlanBase, PlanRef, PlanTreeNodeUnary, PredicatePushdown, StreamHopWindow, ToBatch, ToStream,
};
use crate::expr::{ExprImpl, ExprType, FunctionCall, InputRef, Literal};
use crate::optimizer::plan_node::{
    ColumnPruningContext, PredicatePushdownContext, RewriteStreamContext, ToStreamContext,
};
use crate::optimizer::property::Order;
use crate::utils::{ColIndexMapping, ColIndexMappingRewriteExt, Condition};

/// `LogicalHopWindow` implements Hop Table Function.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalHopWindow {
    pub base: PlanBase,
    pub(super) core: generic::HopWindow<PlanRef>,
}

impl LogicalHopWindow {
    /// just used in optimizer and the function will not check if the `time_col`'s value is NULL
    /// compared with `LogicalHopWindow::create`
    fn new(
        input: PlanRef,
        time_col: InputRef,
        window_slide: IntervalUnit,
        window_size: IntervalUnit,
        output_indices: Option<Vec<usize>>,
    ) -> Self {
        // if output_indices is not specified, use default output_indices
        let output_indices =
            output_indices.unwrap_or_else(|| (0..input.schema().len() + 2).collect_vec());
        let output_type = DataType::window_of(&time_col.data_type).unwrap();
        let original_schema: Schema = input
            .schema()
            .clone()
            .into_fields()
            .into_iter()
            .chain([
                Field::with_name(output_type.clone(), "window_start"),
                Field::with_name(output_type, "window_end"),
            ])
            .collect();
        let window_start_index = output_indices
            .iter()
            .position(|&idx| idx == input.schema().len());
        let window_end_index = output_indices
            .iter()
            .position(|&idx| idx == input.schema().len() + 1);

        let core = generic::HopWindow {
            input,
            time_col,
            window_slide,
            window_size,
            output_indices,
        };

        let schema = core.schema();
        let pk_indices = core.logical_pk();
        let ctx = core.ctx();
        let functional_dependency = {
            let mut fd_set =
                ColIndexMapping::identity_or_none(core.input.schema().len(), original_schema.len())
                    .composite(&ColIndexMapping::with_remaining_columns(
                        &core.output_indices,
                        original_schema.len(),
                    ))
                    .rewrite_functional_dependency_set(core.input.functional_dependency().clone());
            if let Some(start_idx) = window_start_index && let Some(end_idx) = window_end_index {
                fd_set.add_functional_dependency_by_column_indices(&[start_idx], &[end_idx]);
                fd_set.add_functional_dependency_by_column_indices(&[end_idx], &[start_idx]);
            }
            fd_set
        };

        // NOTE(st1page): add join keys in the pk_indices a work around before we really have stream
        // key.
        // let pk_indices = match pk_indices {
        //     Some(pk_indices) if functional_dependency.is_key(&pk_indices) => {
        //         functional_dependency.minimize_key(&pk_indices)
        //     }
        //     _ => pk_indices.unwrap_or_default(),
        // };

        let base = PlanBase::new_logical(
            ctx,
            schema,
            pk_indices.unwrap_or_default(),
            functional_dependency,
        );

        LogicalHopWindow { base, core }
    }

    pub fn into_parts(self) -> (PlanRef, InputRef, IntervalUnit, IntervalUnit, Vec<usize>) {
        self.core.into_parts()
    }

    /// used for binder and planner. The function will add a filter operator to ignore records with
    /// NULL time value. <https://github.com/risingwavelabs/risingwave/issues/8130>
    pub fn create(
        input: PlanRef,
        time_col: InputRef,
        window_slide: IntervalUnit,
        window_size: IntervalUnit,
    ) -> PlanRef {
        let input = LogicalFilter::create_with_expr(
            input,
            FunctionCall::new(ExprType::IsNotNull, vec![time_col.clone().into()])
                .unwrap()
                .into(),
        );
        Self::new(input, time_col, window_slide, window_size, None).into()
    }

    pub fn window_start_col_idx(&self) -> usize {
        self.input().schema().len()
    }

    pub fn window_end_col_idx(&self) -> usize {
        self.window_start_col_idx() + 1
    }

    pub fn o2i_col_mapping(&self) -> ColIndexMapping {
        self.output2internal_col_mapping()
            .composite(&self.internal2input_col_mapping())
    }

    pub fn i2o_col_mapping(&self) -> ColIndexMapping {
        self.input2internal_col_mapping()
            .composite(&self.internal2output_col_mapping())
    }

    pub fn internal_column_num(&self) -> usize {
        self.window_start_col_idx() + 2
    }

    fn output2internal_col_mapping(&self) -> ColIndexMapping {
        self.internal2output_col_mapping().inverse()
    }

    fn internal2output_col_mapping(&self) -> ColIndexMapping {
        ColIndexMapping::with_remaining_columns(
            &self.core.output_indices,
            self.internal_column_num(),
        )
    }

    fn input2internal_col_mapping(&self) -> ColIndexMapping {
        ColIndexMapping::identity_or_none(self.window_start_col_idx(), self.internal_column_num())
    }

    fn internal2input_col_mapping(&self) -> ColIndexMapping {
        ColIndexMapping::identity_or_none(self.internal_column_num(), self.window_start_col_idx())
    }

    fn clone_with_output_indices(&self, output_indices: Vec<usize>) -> Self {
        Self::new(
            self.input(),
            self.core.time_col.clone(),
            self.core.window_slide,
            self.core.window_size,
            Some(output_indices),
        )
    }

    pub fn fmt_with_name(&self, f: &mut fmt::Formatter<'_>, name: &str) -> fmt::Result {
        self.core.fmt_with_name(f, name)
    }

    pub fn fmt_fields_with_builder(&self, builder: &mut fmt::DebugStruct<'_, '_>) {
        self.core.fmt_fields_with_builder(builder)
    }

    /// Map the order of the input to use the updated indices
    pub fn get_out_column_index_order(&self) -> Order {
        self.i2o_col_mapping()
            .rewrite_provided_order(self.input().order())
    }

    /// Get output indices
    pub fn output_indices(&self) -> &Vec<usize> {
        &self.core.output_indices
    }

    fn derive_window_start_and_end_exprs(&self) -> Result<(Vec<ExprImpl>, Vec<ExprImpl>)> {
        let generic::HopWindow::<PlanRef> {
            window_size,
            window_slide,
            time_col,
            ..
        } = &self.core;
        let units = window_size
            .exact_div(window_slide)
            .and_then(|x| NonZeroUsize::new(usize::try_from(x).ok()?))
            .ok_or_else(|| ExprError::InvalidParam {
                name: "window",
                reason: format!(
                    "window_size {} cannot be divided by window_slide {}",
                    window_size, window_slide
                ),
            })?
            .get();
        let window_size_expr = Literal::new(Some((*window_size).into()), DataType::Interval).into();
        let window_slide_expr: ExprImpl =
            Literal::new(Some((*window_slide).into()), DataType::Interval).into();
        let window_size_sub_slide = FunctionCall::new(
            ExprType::Subtract,
            vec![window_size_expr, window_slide_expr.clone()],
        )?
        .into();

        let time_col_shifted = FunctionCall::new(
            ExprType::Subtract,
            vec![
                ExprImpl::InputRef(Box::new(time_col.clone())),
                window_size_sub_slide,
            ],
        )?
        .into();

        let hop_start: ExprImpl = FunctionCall::new(
            ExprType::TumbleStart,
            vec![time_col_shifted, window_slide_expr],
        )?
        .into();

        let mut window_start_exprs = Vec::with_capacity(units);
        let mut window_end_exprs = Vec::with_capacity(units);
        for i in 0..units {
            {
                let window_start_offset =
                    window_slide
                        .checked_mul_int(i)
                        .ok_or_else(|| ExprError::InvalidParam {
                            name: "window",
                            reason: format!(
                                "window_slide {} cannot be multiplied by {}",
                                window_slide, i
                            ),
                        })?;
                let window_start_offset_expr =
                    Literal::new(Some(window_start_offset.into()), DataType::Interval).into();
                let window_start_expr = FunctionCall::new(
                    ExprType::Add,
                    vec![hop_start.clone(), window_start_offset_expr],
                )?
                .into();
                window_start_exprs.push(window_start_expr);
            }
            {
                let window_end_offset =
                    window_slide.checked_mul_int(i + units).ok_or_else(|| {
                        ExprError::InvalidParam {
                            name: "window",
                            reason: format!(
                                "window_slide {} cannot be multiplied by {}",
                                window_slide,
                                i + units
                            ),
                        }
                    })?;
                let window_end_offset_expr =
                    Literal::new(Some(window_end_offset.into()), DataType::Interval).into();
                let window_end_expr = FunctionCall::new(
                    ExprType::Add,
                    vec![hop_start.clone(), window_end_offset_expr],
                )?
                .into();
                window_end_exprs.push(window_end_expr);
            }
        }
        assert_eq!(window_start_exprs.len(), window_end_exprs.len());
        Ok((window_start_exprs, window_end_exprs))
    }
}

impl PlanTreeNodeUnary for LogicalHopWindow {
    fn input(&self) -> PlanRef {
        self.core.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(
            input,
            self.core.time_col.clone(),
            self.core.window_slide,
            self.core.window_size,
            Some(self.core.output_indices.clone()),
        )
    }

    #[must_use]
    fn rewrite_with_input(
        &self,
        input: PlanRef,
        input_col_change: ColIndexMapping,
    ) -> (Self, ColIndexMapping) {
        let mut time_col = self.core.time_col.clone();
        time_col.index = input_col_change.map(time_col.index);
        let mut columns_to_be_kept = Vec::new();
        let new_output_indices = self
            .core
            .output_indices
            .iter()
            .enumerate()
            .filter_map(|(i, &idx)| match input_col_change.try_map(idx) {
                Some(new_idx) => {
                    columns_to_be_kept.push(i);
                    Some(new_idx)
                }
                None => {
                    if idx == self.window_start_col_idx() {
                        columns_to_be_kept.push(i);
                        Some(input.schema().len())
                    } else if idx == self.window_end_col_idx() {
                        columns_to_be_kept.push(i);
                        Some(input.schema().len() + 1)
                    } else {
                        None
                    }
                }
            })
            .collect_vec();
        let new_hop = Self::new(
            input,
            time_col,
            self.core.window_slide,
            self.core.window_size,
            Some(new_output_indices),
        );
        (
            new_hop,
            ColIndexMapping::with_remaining_columns(
                &columns_to_be_kept,
                self.core.output_indices.len(),
            ),
        )
    }
}

impl_plan_tree_node_for_unary! {LogicalHopWindow}

impl fmt::Display for LogicalHopWindow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt_with_name(f, "LogicalHopWindow")
    }
}

impl ColPrunable for LogicalHopWindow {
    fn prune_col(&self, required_cols: &[usize], ctx: &mut ColumnPruningContext) -> PlanRef {
        let o2i = self.o2i_col_mapping();
        let input_required_cols = {
            let mut tmp = FixedBitSet::with_capacity(self.schema().len());
            tmp.extend(required_cols.iter().copied());
            tmp = o2i.rewrite_bitset(&tmp);
            // LogicalHopWindow should keep all required cols from upstream,
            // as well as its own time_col.
            tmp.put(self.core.time_col.index());
            tmp.ones().collect_vec()
        };
        let input = self.input().prune_col(&input_required_cols, ctx);
        let input_change = ColIndexMapping::with_remaining_columns(
            &input_required_cols,
            self.input().schema().len(),
        );
        let (new_hop, _) = self.rewrite_with_input(input, input_change.clone());
        let output_cols = {
            // output cols = { cols required by upstream from input node } ∪ { additional window
            // cols }
            #[derive(Copy, Clone, Debug)]
            enum IndexType {
                Input(usize),
                WindowStart,
                WindowEnd,
            }
            let output2internal = self.output2internal_col_mapping();
            // map the indices from output to input
            let input_required_cols = required_cols
                .iter()
                .filter_map(|&idx| {
                    if let Some(idx) = o2i.try_map(idx) {
                        Some(IndexType::Input(idx))
                    } else if let Some(idx) = output2internal.try_map(idx) {
                        if idx == self.window_start_col_idx() {
                            Some(IndexType::WindowStart)
                        } else if idx == self.window_end_col_idx() {
                            Some(IndexType::WindowEnd)
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                })
                .collect_vec();
            // this mapping will only keeps required columns
            input_required_cols
                .iter()
                .filter_map(|&idx| match idx {
                    IndexType::Input(x) => input_change.try_map(x),
                    IndexType::WindowStart => Some(new_hop.window_start_col_idx()),
                    IndexType::WindowEnd => Some(new_hop.window_end_col_idx()),
                })
                .collect_vec()
        };
        new_hop.clone_with_output_indices(output_cols).into()
    }
}

impl ExprRewritable for LogicalHopWindow {}

impl PredicatePushdown for LogicalHopWindow {
    /// Keep predicate on time window parameters (`window_start`, `window_end`),
    /// the rest may be pushed-down.
    fn predicate_pushdown(
        &self,
        predicate: Condition,
        ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        let mut window_columns = FixedBitSet::with_capacity(self.schema().len());

        let window_start_idx = self.window_start_col_idx();
        let window_end_idx = self.window_end_col_idx();
        for (i, v) in self.output_indices().iter().enumerate() {
            if *v == window_start_idx || *v == window_end_idx {
                window_columns.insert(i);
            }
        }
        let (time_window_pred, pushed_predicate) = predicate.split_disjoint(&window_columns);
        let mut mapping = self.o2i_col_mapping();
        let pushed_predicate = pushed_predicate.rewrite_expr(&mut mapping);
        gen_filter_and_pushdown(self, time_window_pred, pushed_predicate, ctx)
    }
}

impl ToBatch for LogicalHopWindow {
    fn to_batch(&self) -> Result<PlanRef> {
        let new_input = self.input().to_batch()?;
        let new_logical = self.clone_with_input(new_input);
        let (window_start_exprs, window_end_exprs) =
            new_logical.derive_window_start_and_end_exprs()?;
        Ok(BatchHopWindow::new(new_logical, window_start_exprs, window_end_exprs).into())
    }
}

impl ToStream for LogicalHopWindow {
    fn to_stream(&self, ctx: &mut ToStreamContext) -> Result<PlanRef> {
        let new_input = self.input().to_stream(ctx)?;
        let new_logical = self.clone_with_input(new_input);
        Ok(StreamHopWindow::new(new_logical).into())
    }

    fn logical_rewrite_for_stream(
        &self,
        ctx: &mut RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        let (input, input_col_change) = self.input().logical_rewrite_for_stream(ctx)?;
        let (hop, out_col_change) = self.rewrite_with_input(input, input_col_change);
        let (input, time_col, window_slide, window_size, mut output_indices) = hop.into_parts();
        if !output_indices.contains(&input.schema().len())
            && !output_indices.contains(&(input.schema().len() + 1))
        // When both `window_start` and `window_end` are not in `output_indices`,
        // we add `window_start` to ensure we can derive pk.
        {
            output_indices.push(input.schema().len());
        }
        let i2o = self.i2o_col_mapping();
        output_indices.extend(
            input
                .logical_pk()
                .iter()
                .cloned()
                .filter(|i| i2o.try_map(*i).is_none()),
        );
        let new_hop = Self::new(
            input,
            time_col,
            window_slide,
            window_size,
            Some(output_indices),
        );
        Ok((new_hop.into(), out_col_change))
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;

    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::DataType;

    use super::*;
    use crate::expr::InputRef;
    use crate::optimizer::optimizer_context::OptimizerContext;
    use crate::optimizer::plan_node::LogicalValues;
    use crate::optimizer::property::FunctionalDependency;
    use crate::Explain;
    #[tokio::test]
    /// Pruning
    /// ```text
    /// HopWindow(time_col: $0 slide: 1 day size: 3 days)
    ///   TableScan(date, v1, v2)
    /// ```
    /// with required columns [4, 2, 3] will result in
    /// ```text
    ///   HopWindow(time_col: $0 slide: 1 day size: 3 days output_indices: [3, 1, 2])
    ///     TableScan(date, v3)
    /// ```
    async fn test_prune_hop_window_with_order_required() {
        let ctx = OptimizerContext::mock().await;
        let fields: Vec<Field> = vec![
            Field::with_name(DataType::Date, "date"),
            Field::with_name(DataType::Int32, "v1"),
            Field::with_name(DataType::Int32, "v2"),
        ];
        let values = LogicalValues::new(
            vec![],
            Schema {
                fields: fields.clone(),
            },
            ctx,
        );
        let hop_window: PlanRef = LogicalHopWindow::new(
            values.into(),
            InputRef::new(0, DataType::Date),
            IntervalUnit::new(0, 1, 0),
            IntervalUnit::new(0, 3, 0),
            None,
        )
        .into();
        // Perform the prune
        let required_cols = vec![4, 2, 3];
        let plan = hop_window.prune_col(
            &required_cols,
            &mut ColumnPruningContext::new(hop_window.clone()),
        );
        println!(
            "{}\n{}",
            hop_window.explain_to_string().unwrap(),
            plan.explain_to_string().unwrap()
        );
        // Check the result
        let hop_window = plan.as_logical_hop_window().unwrap();
        assert_eq!(hop_window.core.output_indices, vec![3, 1, 2]);
        assert_eq!(hop_window.schema().fields().len(), 3);

        let values = hop_window.input();
        let values = values.as_logical_values().unwrap();
        assert_eq!(values.schema().fields().len(), 2);
        assert_eq!(values.schema().fields()[0], fields[0]);
        assert_eq!(values.schema().fields()[1], fields[2]);

        let required_cols = (0..plan.schema().len()).collect_vec();
        let plan2 = plan.prune_col(&required_cols, &mut ColumnPruningContext::new(plan.clone()));
        assert_eq!(plan2.schema(), plan.schema());
    }

    #[tokio::test]
    async fn fd_derivation_hop_window() {
        // input: [date, v1, v2]
        // FD: { date, v1 } --> { v2 }
        // output: [date, v1, v2, window_start, window_end],
        // FD: { date, v1 } --> { v2 }
        //     window_start --> window_end
        //     window_end --> window_start
        let ctx = OptimizerContext::mock().await;
        let fields: Vec<Field> = vec![
            Field::with_name(DataType::Date, "date"),
            Field::with_name(DataType::Int32, "v1"),
            Field::with_name(DataType::Int32, "v2"),
        ];
        let mut values = LogicalValues::new(vec![], Schema { fields }, ctx);
        // 0, 1 --> 2
        values
            .base
            .functional_dependency
            .add_functional_dependency_by_column_indices(&[0, 1], &[2]);
        let hop_window: PlanRef = LogicalHopWindow::new(
            values.into(),
            InputRef::new(0, DataType::Date),
            IntervalUnit::new(0, 1, 0),
            IntervalUnit::new(0, 3, 0),
            None,
        )
        .into();
        let fd_set: HashSet<_> = hop_window
            .functional_dependency()
            .as_dependencies()
            .iter()
            .cloned()
            .collect();
        let expected_fd_set: HashSet<_> = [
            FunctionalDependency::with_indices(5, &[0, 1], &[2]),
            FunctionalDependency::with_indices(5, &[3], &[4]),
            FunctionalDependency::with_indices(5, &[4], &[3]),
        ]
        .into_iter()
        .collect();
        assert_eq!(fd_set, expected_fd_set);
    }
}
