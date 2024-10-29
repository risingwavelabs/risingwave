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

use std::collections::HashMap;

use itertools::Itertools;

use super::Rule;
use crate::PlanRef;

pub struct OverWindowSplitRule;

impl OverWindowSplitRule {
    pub fn create() -> Box<dyn Rule> {
        Box::new(OverWindowSplitRule)
    }
}

impl Rule for OverWindowSplitRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let over_window = plan.as_logical_over_window()?;
        let mut rank_func_seq = 0;
        let groups: HashMap<_, _> = over_window
            .window_functions()
            .iter()
            .enumerate()
            .map(|(idx, func)| {
                let func_seq = if func.kind.is_numbering() {
                    rank_func_seq += 1;
                    rank_func_seq
                } else {
                    0
                };
                ((&func.order_by, &func.partition_by, func_seq), idx)
            })
            .into_group_map();
        Some(over_window.split_with_rule(groups.into_values().sorted().collect()))
    }
}
