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

use fixedbitset::FixedBitSet;

use super::super::plan_node::*;
use super::{BoxedRule, Rule};
use crate::expr::InputRef;
use crate::optimizer::property::WithSchema;
use crate::utils::Substitute;

/// Pushes a [`LogicalFilter`] past a [`LogicalAgg`].
pub struct FilterAggRule {}
impl Rule for FilterAggRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let filter = plan.as_logical_filter()?;
        let input = filter.input();
        let agg = input.as_logical_agg()?;
        let num_group_keys = agg.group_keys().len();
        let num_agg_calls = agg.agg_calls().len();
        assert!(num_group_keys + num_agg_calls == agg.schema().len());

        // If the filter references agg_calls, we can not push it.
        let (agg_call_pred, pushed_predicate) = filter.predicate().clone().split_disjoint(
            &FixedBitSet::from_iter(num_group_keys..num_group_keys + num_agg_calls),
            num_group_keys + num_agg_calls,
        );
        if pushed_predicate.always_true() {
            return None;
        }

        // convert the predicate to one that references the child of the agg
        let mut subst = Substitute {
            mapping: agg
                .group_keys()
                .iter()
                .enumerate()
                .map(|(i, group_key)| {
                    InputRef::new(*group_key, agg.schema().fields()[i].data_type()).into()
                })
                .collect(),
        };
        let pushed_predicate = pushed_predicate.rewrite_expr(&mut subst);

        let input = agg.input();
        let pushed_filter = LogicalFilter::new(input, pushed_predicate);
        let new_agg = agg.clone_with_input(pushed_filter.into()).into();
        if agg_call_pred.always_true() {
            Some(new_agg)
        } else {
            Some(LogicalFilter::new(new_agg, agg_call_pred).into())
        }
    }
}

impl FilterAggRule {
    pub fn create() -> BoxedRule {
        Box::new(FilterAggRule {})
    }
}
