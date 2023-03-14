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

use itertools::Itertools;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::Result;
use risingwave_common::types::{DataType, IntervalUnit, IntervalUnitDisplay};
use risingwave_common::util::column_index_mapping::ColIndexMapping;
use risingwave_expr::ExprError;

use super::super::utils::IndicesDisplay;
use super::{GenericPlanNode, GenericPlanRef};
use crate::expr::{ExprImpl, ExprType, FunctionCall, InputRef, InputRefDisplay, Literal};
use crate::optimizer::optimizer_context::OptimizerContextRef;

/// [`HopWindow`] implements Hop Table Function.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct HopWindow<PlanRef> {
    pub input: PlanRef,
    pub time_col: InputRef,
    pub window_slide: IntervalUnit,
    pub window_size: IntervalUnit,
    pub window_offset: IntervalUnit,
    /// Provides mapping from input schema, window_start, window_end to output schema.
    /// For example, if we had:
    /// input schema: | 0: trip_time | 1: trip_name |
    /// window_start: 2
    /// window_end: 3
    /// output schema: | trip_name | window_start |
    /// Then, output_indices: [1, 2]
    pub output_indices: Vec<usize>,
}

impl<PlanRef: GenericPlanRef> GenericPlanNode for HopWindow<PlanRef> {
    fn schema(&self) -> Schema {
        let output_type = DataType::window_of(&self.time_col.data_type).unwrap();
        let original_schema: Schema = self
            .input
            .schema()
            .clone()
            .into_fields()
            .into_iter()
            .chain([
                Field::with_name(output_type.clone(), "window_start"),
                Field::with_name(output_type, "window_end"),
            ])
            .collect();
        self.output_indices
            .iter()
            .map(|&idx| original_schema[idx].clone())
            .collect()
    }

    fn logical_pk(&self) -> Option<Vec<usize>> {
        let window_start_index = self
            .output_indices
            .iter()
            .position(|&idx| idx == self.input.schema().len());
        let window_end_index = self
            .output_indices
            .iter()
            .position(|&idx| idx == self.input.schema().len() + 1);
        if window_start_index.is_none() && window_end_index.is_none() {
            None
        } else {
            let mut pk = self
                .input
                .logical_pk()
                .iter()
                .filter_map(|&pk_idx| self.output_indices.iter().position(|&idx| idx == pk_idx))
                .collect_vec();
            if let Some(start_idx) = window_start_index {
                pk.push(start_idx);
            };
            if let Some(end_idx) = window_end_index {
                pk.push(end_idx);
            };
            Some(pk)
        }
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.input.ctx()
    }
}

impl<PlanRef: GenericPlanRef> HopWindow<PlanRef> {
    pub fn into_parts(
        self,
    ) -> (
        PlanRef,
        InputRef,
        IntervalUnit,
        IntervalUnit,
        IntervalUnit,
        Vec<usize>,
    ) {
        (
            self.input,
            self.time_col,
            self.window_slide,
            self.window_size,
            self.window_offset,
            self.output_indices,
        )
    }

    pub fn internal_window_start_col_idx(&self) -> usize {
        self.input.schema().len()
    }

    pub fn internal_window_end_col_idx(&self) -> usize {
        self.internal_window_start_col_idx() + 1
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
        self.internal_window_start_col_idx() + 2
    }

    pub fn output2internal_col_mapping(&self) -> ColIndexMapping {
        self.internal2output_col_mapping().inverse()
    }

    pub fn internal2output_col_mapping(&self) -> ColIndexMapping {
        ColIndexMapping::with_remaining_columns(&self.output_indices, self.internal_column_num())
    }

    pub fn input2internal_col_mapping(&self) -> ColIndexMapping {
        ColIndexMapping::identity_or_none(
            self.internal_window_start_col_idx(),
            self.internal_column_num(),
        )
    }

    pub fn internal2input_col_mapping(&self) -> ColIndexMapping {
        ColIndexMapping::identity_or_none(
            self.internal_column_num(),
            self.internal_window_start_col_idx(),
        )
    }

    pub fn derive_window_start_and_end_exprs(&self) -> Result<(Vec<ExprImpl>, Vec<ExprImpl>)> {
        let Self {
            window_size,
            window_slide,
            window_offset,
            time_col,
            ..
        } = &self;
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
        let window_size_expr: ExprImpl =
            Literal::new(Some((*window_size).into()), DataType::Interval).into();
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

        let offset_modules_window_size = {
            const DAY_MS: i64 = 86400000;
            const MONTH_MS: i64 = 30 * DAY_MS;
            let mut remaining_ms = (window_offset.get_months() as i64 * MONTH_MS
                + window_offset.get_days() as i64 * DAY_MS
                + window_offset.get_ms())
                % (window_size.get_months() as i64 * MONTH_MS
                    + window_size.get_days() as i64 * DAY_MS
                    + window_size.get_ms());
            let months = remaining_ms / MONTH_MS;
            remaining_ms -= months * MONTH_MS;
            let days = remaining_ms / DAY_MS;
            remaining_ms -= days * DAY_MS;
            IntervalUnit::new(months as i32, days as i32, remaining_ms)
        };

        let offset_modules_window_size: ExprImpl = Literal::new(
            Some((offset_modules_window_size).into()),
            DataType::Interval,
        )
        .into();

        let hop_start_with_offset: ExprImpl =
            FunctionCall::new(ExprType::Add, vec![hop_start, offset_modules_window_size])?.into();

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
                    vec![hop_start_with_offset.clone(), window_start_offset_expr],
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
                    vec![hop_start_with_offset.clone(), window_end_offset_expr],
                )?
                .into();
                window_end_exprs.push(window_end_expr);
            }
        }
        assert_eq!(window_start_exprs.len(), window_end_exprs.len());
        Ok((window_start_exprs, window_end_exprs))
    }

    pub fn fmt_fields_with_builder(&self, builder: &mut fmt::DebugStruct<'_, '_>) {
        let output_type = DataType::window_of(&self.time_col.data_type).unwrap();
        builder.field(
            "time_col",
            &InputRefDisplay {
                input_ref: &self.time_col,
                input_schema: self.input.schema(),
            },
        );

        builder.field(
            "slide",
            &IntervalUnitDisplay {
                core: &self.window_slide,
            },
        );

        builder.field(
            "size",
            &IntervalUnitDisplay {
                core: &self.window_size,
            },
        );

        if self
            .output_indices
            .iter()
            .copied()
            // Behavior is the same as `LogicalHopWindow::internal_column_num`
            .eq(0..(self.input.schema().len() + 2))
        {
            builder.field("output", &format_args!("all"));
        } else {
            let original_schema: Schema = self
                .input
                .schema()
                .clone()
                .into_fields()
                .into_iter()
                .chain([
                    Field::with_name(output_type.clone(), "window_start"),
                    Field::with_name(output_type, "window_end"),
                ])
                .collect();
            builder.field(
                "output",
                &IndicesDisplay {
                    indices: &self.output_indices,
                    input_schema: &original_schema,
                },
            );
        }
    }

    pub fn fmt_with_name(&self, f: &mut fmt::Formatter<'_>, name: &str) -> fmt::Result {
        let mut builder = f.debug_struct(name);
        self.fmt_fields_with_builder(&mut builder);
        builder.finish()
    }
}
