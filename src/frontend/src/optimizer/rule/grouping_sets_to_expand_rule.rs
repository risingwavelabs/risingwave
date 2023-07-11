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
use risingwave_common::util::column_index_mapping::ColIndexMapping;

use super::super::plan_node::*;
use super::{BoxedRule, Rule};
use crate::expr::{Expr, InputRef};
use crate::optimizer::plan_node::generic::{Agg, GenericPlanNode, GenericPlanRef};
pub struct GroupingSetsToExpandRule {}

impl GroupingSetsToExpandRule {
    pub fn create() -> BoxedRule {
        Box::new(Self {})
    }
}

impl Rule for GroupingSetsToExpandRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let agg: &LogicalAgg = plan.as_logical_agg()?;
        let (agg_calls, mut group_keys, grouping_sets, input) = agg.clone().decompose();

        let original_group_keys_num = group_keys.len();
        let input_schema_len = input.schema().len();

        if grouping_sets.is_empty() {
            return None;
        }

        // TODO: support GROUPING expression.
        // TODO: optimize the case existing only one set.

        let column_subset = grouping_sets
            .iter()
            .map(|set| set.indices().collect_vec())
            .collect_vec();
        let expand = LogicalExpand::create(input, column_subset);
        // Add the expand flag.
        group_keys.extend(std::iter::once(expand.schema().len() - 1));

        let mut input_col_change =
            ColIndexMapping::with_shift_offset(input_schema_len, input_schema_len as isize);

        // Shift agg_call to the original input columns
        let new_agg_calls = agg_calls
            .iter()
            .cloned()
            .map(|mut agg_call| {
                agg_call.inputs.iter_mut().for_each(|i| {
                    *i = InputRef::new(input_col_change.map(i.index()), i.return_type())
                });
                agg_call.order_by.iter_mut().for_each(|o| {
                    o.column_index = input_col_change.map(o.column_index);
                });
                agg_call.filter = agg_call.filter.rewrite_expr(&mut input_col_change);
                agg_call
            })
            .collect();

        let new_agg = Agg::new(new_agg_calls, group_keys, expand);

        let mut output_fields = FixedBitSet::with_capacity(new_agg.schema().len());
        output_fields.toggle(original_group_keys_num);
        output_fields.toggle_range(..);
        let project = LogicalProject::with_out_fields(new_agg.into(), &output_fields);

        Some(project.into())
    }
}
