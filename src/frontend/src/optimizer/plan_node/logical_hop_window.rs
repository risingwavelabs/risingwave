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
use risingwave_common::types::IntervalUnit;

use super::{ColPrunable, PlanBase, PlanNode, PlanRef, PlanTreeNodeUnary, ToBatch, ToStream, StreamHopWindow};
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
        let schema = input.schema().clone();
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
        Self::new(input, self.time_col, self.window_slide, self.window_size)
    }

    #[must_use]
    fn rewrite_with_input(
        &self,
        input: PlanRef,
        input_col_change: ColIndexMapping,
    ) -> (Self, ColIndexMapping) {
        (
            Self::new(input, self.time_col, self.window_slide, self.window_size),
            input_col_change,
        )
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
    fn prune_col(&self, required_cols: &FixedBitSet) -> PlanRef {
        self.must_contain_columns(required_cols);

        let new_input = self.input.prune_col(required_cols);
        self.clone_with_input(new_input).into()
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
}
