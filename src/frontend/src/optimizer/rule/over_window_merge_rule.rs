// Copyright 2024 RisingWave Labs
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

use super::{BoxedRule, Rule};
use crate::optimizer::plan_node::{LogicalOverWindow, PlanTreeNodeUnary};
use crate::PlanRef;

/// Merge chaining `LogicalOverWindow`s with same `PARTITION BY` and `ORDER BY`.
/// Should be applied after `OverWindowSplitRule`.
pub struct OverWindowMergeRule;

impl OverWindowMergeRule {
    pub fn create() -> BoxedRule {
        Box::new(OverWindowMergeRule)
    }
}

impl Rule for OverWindowMergeRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let over_window = plan.as_logical_over_window()?;
        let mut window_functions_rev = over_window
            .window_functions()
            .iter()
            .rev()
            .cloned()
            .collect::<Vec<_>>();
        let partition_key = over_window.partition_key_indices();
        let order_key = over_window.order_key();

        let mut curr = plan.clone();
        let mut curr_input = over_window.input();
        while let Some(input_over_window) = curr_input.as_logical_over_window() {
            if input_over_window.partition_key_indices() != partition_key
                || input_over_window.order_key() != order_key
            {
                // cannot merge `OverWindow`s with different partition key or order key
                break;
            }
            window_functions_rev.extend(input_over_window.window_functions().iter().rev().cloned());
            curr = curr_input.clone();
            curr_input = input_over_window.input();
        }

        if curr.as_logical_over_window().unwrap() == over_window {
            // unchanged
            return None;
        }

        let window_functions = window_functions_rev.into_iter().rev().collect::<Vec<_>>();
        Some(LogicalOverWindow::new(window_functions, curr_input).into())
    }
}
