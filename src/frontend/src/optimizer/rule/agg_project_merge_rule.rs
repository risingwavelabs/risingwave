// Copyright 2025 RisingWave Labs
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

use itertools::Itertools;

use super::prelude::{PlanRef, *};
use crate::optimizer::plan_node::*;
use crate::utils::IndexSet;

/// Merge [`LogicalAgg`] <- [`LogicalProject`] to [`LogicalAgg`].
pub struct AggProjectMergeRule {}
impl Rule<Logical> for AggProjectMergeRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let agg = plan.as_logical_agg()?;
        let agg = agg.core().clone();
        assert!(agg.grouping_sets.is_empty());
        let old_input = agg.input.clone();
        let proj = old_input.as_logical_project()?;
        // only apply when the input proj is all input-ref
        if !proj.is_all_inputref() {
            return None;
        }
        let proj_o2i = proj.o2i_col_mapping();

        // modify group key according to projection
        let new_agg_group_keys_in_vec = agg
            .group_key
            .indices()
            .map(|x| proj_o2i.map(x))
            .collect_vec();
        let new_agg_group_keys = IndexSet::from_iter(new_agg_group_keys_in_vec.clone());

        let mut agg = agg;
        agg.input = proj.input();
        // modify agg calls according to projection
        agg.agg_calls
            .iter_mut()
            .for_each(|x| x.rewrite_input_index(proj_o2i.clone()));
        agg.group_key = new_agg_group_keys.clone();
        agg.input = proj.input();

        if new_agg_group_keys.to_vec() != new_agg_group_keys_in_vec {
            // Need a project
            let new_agg_group_keys_cardinality = new_agg_group_keys.len();
            let out_col_idx = new_agg_group_keys_in_vec
                .into_iter()
                .map(|x| new_agg_group_keys.indices().position(|y| y == x).unwrap())
                .chain(
                    new_agg_group_keys_cardinality
                        ..new_agg_group_keys_cardinality + agg.agg_calls.len(),
                );
            Some(LogicalProject::with_out_col_idx(agg.into(), out_col_idx).into())
        } else {
            Some(agg.into())
        }
    }
}

impl AggProjectMergeRule {
    pub fn create() -> BoxedRule {
        Box::new(AggProjectMergeRule {})
    }
}
