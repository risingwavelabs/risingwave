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

use super::super::plan_node::*;
use super::{BoxedRule, Rule};
use crate::optimizer::plan_node::generic::Agg;

/// Merge [`LogicalAgg`] <- [`LogicalProject`] to [`LogicalAgg`].
pub struct AggProjectMergeRule {}
impl Rule for AggProjectMergeRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let agg = plan.as_logical_agg()?;
        let (mut agg_calls, agg_group_keys, input) = agg.clone().decompose();
        let proj = input.as_logical_project()?;

        // only apply when the input proj is all input-ref
        if !proj.is_all_inputref() {
            return None;
        }

        let proj_o2i = proj.o2i_col_mapping();
        let new_input = proj.input();

        // modify agg calls according to projection
        agg_calls
            .iter_mut()
            .for_each(|x| x.rewrite_input_index(proj_o2i.clone()));

        // modify group key according to projection
        let new_agg_group_keys_in_vec: Vec<usize> =
            agg_group_keys.ones().map(|x| proj_o2i.map(x)).collect_vec();

        let new_agg_group_keys = FixedBitSet::from_iter(new_agg_group_keys_in_vec.iter().cloned());

        if new_agg_group_keys.ones().collect_vec() != new_agg_group_keys_in_vec {
            // Need a project
            let new_agg_group_keys_cardinality = new_agg_group_keys.count_ones(..);
            let out_col_idx = new_agg_group_keys_in_vec
                .into_iter()
                .map(|x| new_agg_group_keys.ones().position(|y| y == x).unwrap())
                .chain(
                    new_agg_group_keys_cardinality
                        ..new_agg_group_keys_cardinality + agg_calls.len(),
                );
            Some(
                LogicalProject::with_out_col_idx(
                    Agg::new(agg_calls, new_agg_group_keys.clone(), new_input).into(),
                    out_col_idx,
                )
                .into(),
            )
        } else {
            Some(Agg::new(agg_calls, new_agg_group_keys, new_input).into())
        }
    }
}

impl AggProjectMergeRule {
    pub fn create() -> BoxedRule {
        Box::new(AggProjectMergeRule {})
    }
}
