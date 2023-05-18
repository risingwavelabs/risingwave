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

use std::collections::HashMap;

use itertools::Itertools;

use super::Rule;
use crate::PlanRef;

pub struct OverWindowSplitDiffWindow;

impl OverWindowSplitDiffWindow {
    pub fn create() -> Box<dyn Rule> {
        Box::new(OverWindowSplitDiffWindow)
    }
}

impl Rule for OverWindowSplitDiffWindow {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let over_window = plan.as_logical_over_window()?;
        let mut hash_map = HashMap::new();
        for (idx, (order_by, partition_by)) in over_window
            .window_functions()
            .iter()
            .map(|window| (&window.order_by, &window.partition_by))
            .unique()
            .enumerate()
        {
            let _ = hash_map.insert((order_by.clone(), partition_by.clone()), idx)?;
        }
        let group_rule = over_window
            .window_functions()
            .iter()
            .map(|window| {
                *hash_map
                    .get(&(window.order_by.clone(), window.partition_by.clone()))
                    .unwrap()
            })
            .collect_vec();
        Some(over_window.split_with_rule(group_rule))
    }
}
