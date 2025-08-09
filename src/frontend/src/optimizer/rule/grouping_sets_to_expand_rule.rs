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

use fixedbitset::FixedBitSet;
use itertools::Itertools;
use risingwave_common::types::DataType;
use risingwave_common::util::column_index_mapping::ColIndexMapping;
use risingwave_expr::aggregate::{AggType, PbAggKind};

use super::prelude::{PlanRef, *};
use crate::expr::{Expr, ExprImpl, ExprType, FunctionCall, InputRef};
use crate::optimizer::plan_node::generic::{Agg, GenericPlanNode, GenericPlanRef};
use crate::optimizer::plan_node::*;
pub struct GroupingSetsToExpandRule {}

impl GroupingSetsToExpandRule {
    pub fn create() -> BoxedRule {
        Box::new(Self {})
    }

    /// TODO: Remove this method when we support column pruning for `Expand`.
    fn prune_column_for_agg(agg: &LogicalAgg) -> LogicalAgg {
        let group_key_required_cols = agg.group_key().to_bitset();
        let agg_call_required_cols = {
            let input_cnt = agg.input().schema().len();
            let mut tmp = FixedBitSet::with_capacity(input_cnt);

            agg.agg_calls().iter().for_each(|agg_call| {
                tmp.extend(agg_call.inputs.iter().map(|x| x.index()));
                tmp.extend(agg_call.order_by.iter().map(|x| x.column_index));
                // collect columns used in aggregate filter expressions
                for i in &agg_call.filter.conjunctions {
                    tmp.union_with(&i.collect_input_refs(input_cnt));
                }
            });
            tmp
        };

        let input_required_cols = {
            let mut tmp = FixedBitSet::with_capacity(agg.input().schema().len());
            tmp.union_with(&group_key_required_cols);
            tmp.union_with(&agg_call_required_cols);
            tmp.ones().collect_vec()
        };
        let input_col_change = ColIndexMapping::with_remaining_columns(
            &input_required_cols,
            agg.input().schema().len(),
        );
        let input =
            LogicalProject::with_out_col_idx(agg.input(), input_required_cols.iter().cloned())
                .into();

        let (new_agg, output_col_change) =
            agg.rewrite_with_input_agg(input, agg.agg_calls(), input_col_change);
        assert!(output_col_change.is_identity());
        new_agg
    }
}

impl Rule<Logical> for GroupingSetsToExpandRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let agg: &LogicalAgg = plan.as_logical_agg()?;
        if agg.grouping_sets().is_empty() {
            return None;
        }
        let agg = Self::prune_column_for_agg(agg);
        let (old_agg_calls, old_group_keys, grouping_sets, input, enable_two_phase) =
            agg.decompose();

        let old_input_schema_len = input.schema().len();
        let flag_col_idx = old_group_keys.len();

        let column_subset = grouping_sets
            .iter()
            .map(|set| set.indices().collect_vec())
            .collect_vec();

        let expand = LogicalExpand::create(input, column_subset.clone());
        let new_group_keys = {
            let mut k = old_group_keys.clone();
            // Add the expand flag.
            k.extend(std::iter::once(expand.schema().len() - 1));
            k
        };

        // Map from old input ref to expanded input (`LogicalExpand` prepends the same number of fields
        // as expanded ones with NULLs before the real input fields).
        let mut input_col_change =
            ColIndexMapping::with_shift_offset(old_input_schema_len, old_input_schema_len as isize);

        // Grouping agg calls need to be transformed into a project expression, and other agg calls
        // need to shift their `input_ref`.
        let mut project_agg_call_exprs = vec![];
        let mut new_agg_calls = vec![];
        for agg_call in old_agg_calls {
            // Deal with grouping agg call for grouping sets.
            if matches!(agg_call.agg_type, AggType::Builtin(PbAggKind::Grouping)) {
                let mut grouping_values = vec![];
                let args = agg_call
                    .inputs
                    .iter()
                    .map(|input_ref| input_ref.index)
                    .collect_vec();
                for subset in &column_subset {
                    let mut value = 0;
                    for arg in &args {
                        value <<= 1;
                        if !subset.contains(arg) {
                            value += 1;
                        }
                    }
                    grouping_values.push(value);
                }

                let mut case_inputs = vec![];
                for (i, grouping_value) in grouping_values.into_iter().enumerate() {
                    let condition = ExprImpl::FunctionCall(
                        FunctionCall::new_unchecked(
                            ExprType::Equal,
                            vec![
                                ExprImpl::literal_bigint(i as i64),
                                ExprImpl::InputRef(
                                    InputRef::new(flag_col_idx, DataType::Int64).into(),
                                ),
                            ],
                            DataType::Boolean,
                        )
                        .into(),
                    );
                    let value = ExprImpl::literal_int(grouping_value);
                    case_inputs.push(condition);
                    case_inputs.push(value);
                }

                let case_expr = ExprImpl::FunctionCall(
                    FunctionCall::new_unchecked(ExprType::Case, case_inputs, DataType::Int32)
                        .into(),
                );
                project_agg_call_exprs.push(case_expr);
            } else {
                let mut new_agg_call = agg_call;
                // Shift agg_call to the original input columns
                new_agg_call.inputs.iter_mut().for_each(|i| {
                    let new_i = input_col_change.map(i.index());
                    assert_eq!(expand.schema()[new_i].data_type(), i.return_type());
                    *i = InputRef::new(new_i, i.return_type());
                });
                new_agg_call.order_by.iter_mut().for_each(|o| {
                    o.column_index = input_col_change.map(o.column_index);
                });
                new_agg_call.filter = new_agg_call.filter.rewrite_expr(&mut input_col_change);
                project_agg_call_exprs.push(ExprImpl::InputRef(
                    InputRef::new(
                        new_group_keys.len() + new_agg_calls.len(),
                        new_agg_call.return_type.clone(),
                    )
                    .into(),
                ));
                new_agg_calls.push(new_agg_call);
            }
        }

        let new_agg =
            Agg::new(new_agg_calls, new_group_keys, expand).with_enable_two_phase(enable_two_phase);
        let project_exprs = (0..old_group_keys.len())
            .map(|i| ExprImpl::InputRef(InputRef::new(i, new_agg.schema()[i].data_type()).into()))
            .chain(project_agg_call_exprs)
            .collect();

        let project = LogicalProject::new(new_agg.into(), project_exprs);

        Some(project.into())
    }
}
