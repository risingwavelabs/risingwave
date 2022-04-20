// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt;

use fixedbitset::FixedBitSet;
use risingwave_common::catalog::Field;
use risingwave_common::types::{DataType, IntervalUnit};

use super::{
    ColPrunable, PlanBase, PlanRef, PlanTreeNodeUnary, StreamHopWindow, ToBatch, ToStream,
};
use crate::expr::InputRef;
use crate::utils::ColIndexMapping;

/// `LogicalHopWindow` implements Hop Table Function.
#[derive(Debug, Clone)]
pub struct LogicalHopWindow {
    pub base: PlanBase,
    input: PlanRef,
    pub(super) time_col: InputRef,
    pub(super) window_slide: IntervalUnit,
    pub(super) window_size: IntervalUnit,
}

impl LogicalHopWindow {
    fn new(
        input: PlanRef,
        time_col: InputRef,
        window_slide: IntervalUnit,
        window_size: IntervalUnit,
    ) -> Self {
        let ctx = input.ctx();
        let schema = input
            .schema()
            .clone()
            .into_fields()
            .into_iter()
            .chain([
                Field::with_name(DataType::Timestamp, "window_start"),
                Field::with_name(DataType::Timestamp, "window_end"),
            ])
            .collect();
        let pk_indices = input.pk_indices().to_vec();
        let base = PlanBase::new_logical(ctx, schema, pk_indices);
        LogicalHopWindow {
            base,
            input,
            time_col,
            window_slide,
            window_size,
        }
    }

    /// the function will check if the cond is bool expression
    pub fn create(
        input: PlanRef,
        time_col: InputRef,
        window_slide: IntervalUnit,
        window_size: IntervalUnit,
    ) -> PlanRef {
        Self::new(input, time_col, window_slide, window_size).into()
    }
}

impl PlanTreeNodeUnary for LogicalHopWindow {
    fn input(&self) -> PlanRef {
        self.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(
            input,
            self.time_col.clone(),
            self.window_slide,
            self.window_size,
        )
    }

    #[must_use]
    fn rewrite_with_input(
        &self,
        input: PlanRef,
        input_col_change: ColIndexMapping,
    ) -> (Self, ColIndexMapping) {
        let mut time_col = self.time_col.clone();
        time_col.index = input_col_change.map(time_col.index);
        let new_hop = Self::new(input.clone(), time_col, self.window_slide, self.window_size);

        let (mut mapping, new_input_col_num) = input_col_change.into_parts();
        assert_eq!(new_input_col_num, input.schema().len());
        assert_eq!(new_input_col_num + 2, new_hop.schema().len());
        mapping.push(Some(new_input_col_num));
        mapping.push(Some(new_input_col_num + 1));

        (new_hop, ColIndexMapping::new(mapping))
    }
}

impl_plan_tree_node_for_unary! {LogicalHopWindow}

impl fmt::Display for LogicalHopWindow {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "LogicalHopWindow {{ time_col: {}, slide: {}, size: {} }}",
            self.time_col, self.window_slide, self.window_size,
        )
    }
}

impl ColPrunable for LogicalHopWindow {
    fn prune_col(&self, _required_cols: &FixedBitSet) -> PlanRef {
        unimplemented!("LogicalHopWindow::prune_col")
    }
}

impl ToBatch for LogicalHopWindow {
    fn to_batch(&self) -> PlanRef {
        unimplemented!("LogicalHopWindow::ToBatch")
    }
}

impl ToStream for LogicalHopWindow {
    fn to_stream(&self) -> PlanRef {
        let new_input = self.input().to_stream();
        let new_logical = self.clone_with_input(new_input);
        StreamHopWindow::new(new_logical).into()
    }

    fn logical_rewrite_for_stream(&self) -> (PlanRef, ColIndexMapping) {
        let (input, input_col_change) = self.input.logical_rewrite_for_stream();
        let (hop, out_col_change) = self.rewrite_with_input(input, input_col_change);
        (hop.into(), out_col_change)
    }
}
