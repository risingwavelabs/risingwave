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

use itertools::Itertools;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::types::{DataType, IntervalUnit};

use super::super::utils::IndicesDisplay;
use super::{GenericPlanNode, GenericPlanRef};
use crate::expr::{InputRef, InputRefDisplay};
use crate::optimizer::optimizer_context::OptimizerContextRef;

/// [`HopWindow`] implements Hop Table Function.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct HopWindow<PlanRef> {
    pub input: PlanRef,
    pub time_col: InputRef,
    pub window_slide: IntervalUnit,
    pub window_size: IntervalUnit,
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
    pub fn into_parts(self) -> (PlanRef, InputRef, IntervalUnit, IntervalUnit, Vec<usize>) {
        (
            self.input,
            self.time_col,
            self.window_slide,
            self.window_size,
            self.output_indices,
        )
    }

    pub fn fmt_with_name(&self, f: &mut fmt::Formatter<'_>, name: &str) -> fmt::Result {
        let output_type = DataType::window_of(&self.time_col.data_type).unwrap();
        write!(
            f,
            "{} {{ time_col: {}, slide: {}, size: {}, output: {} }}",
            name,
            InputRefDisplay {
                input_ref: &self.time_col,
                input_schema: self.input.schema()
            },
            self.window_slide,
            self.window_size,
            if self
                .output_indices
                .iter()
                .copied()
                // Behavior is the same as `LogicalHopWindow::internal_column_num`
                .eq(0..(self.input.schema().len() + 2))
            {
                "all".to_string()
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
                format!(
                    "{:?}",
                    &IndicesDisplay {
                        indices: &self.output_indices,
                        input_schema: &original_schema,
                    }
                )
            },
        )
    }
}
