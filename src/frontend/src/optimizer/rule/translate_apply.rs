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

use itertools::Itertools;
use risingwave_common::types::DataType;

use super::{BoxedRule, Rule};
use crate::expr::{ExprImpl, ExprType, FunctionCall, InputRef};
use crate::optimizer::plan_node::{LogicalAgg, LogicalApply};
use crate::optimizer::PlanRef;

/// General Unnesting based on the paper Unnesting Arbitrary Queries:
/// Translate the apply into a canonical form.
///
/// Before:
///
///   `LogicalApply`
///    /            \
///  LHS           RHS
///
/// After:
///
///    `LogicalJoin`
///    /            \
///  LHS       `LogicalApply`
///             /           \
///          Domain         RHS
pub struct TranslateApplyRule {}
impl Rule for TranslateApplyRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let apply: &LogicalApply = plan.as_logical_apply()?;

        let (
            apply_left,
            _apply_right,
            _apply_on,
            _apply_join_type,
            _correlated_id,
            correlated_indices,
            max_one_row,
        ) = apply.clone().decompose();

        if max_one_row {
            return None;
        }

        let apply_left_len = apply_left.schema().len();

        // Calculate the domain.
        let domain: PlanRef = LogicalAgg::new(
            vec![],
            correlated_indices.clone().into_iter().collect_vec(),
            apply_left,
        )
        .into();

        let eq_predicates = correlated_indices
            .into_iter()
            .enumerate()
            .map(|(i, correlated_index)| {
                let data_type = domain.schema().fields()[i].data_type.clone();
                let left = InputRef::new(correlated_index, data_type.clone());
                let right = InputRef::new(i + apply_left_len, data_type);
                // use null-safe equal
                FunctionCall::new_unchecked(
                    ExprType::IsNotDistinctFrom,
                    vec![left.into(), right.into()],
                    DataType::Boolean,
                )
                .into()
            })
            .collect::<Vec<ExprImpl>>();

        let new_node = apply.clone().translate_apply(domain, eq_predicates);
        Some(new_node)
    }
}

impl TranslateApplyRule {
    pub fn create() -> BoxedRule {
        Box::new(TranslateApplyRule {})
    }
}
