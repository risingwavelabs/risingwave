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
use risingwave_common::types::DataType;
use risingwave_pb::plan_common::JoinType;

use super::{BoxedRule, Rule};
use crate::expr::{ExprImpl, ExprType, FunctionCall, InputRef};
use crate::optimizer::plan_node::generic::{Agg, GenericPlanRef};
use crate::optimizer::plan_node::{
    LogicalApply, LogicalJoin, LogicalProject, LogicalScan, LogicalShare,
    PlanTreeNodeBinary, PlanTreeNodeUnary,
};
use crate::optimizer::PlanRef;
use crate::utils::{ColIndexMapping, Condition};

/// General Unnesting based on the paper Unnesting Arbitrary Queries:
/// Translate the apply into a canonical form.
///
/// Before:
///
/// ```text
///     LogicalApply
///    /            \
///  LHS           RHS
/// ```
///
/// After:
///
/// ```text
///      LogicalJoin
///    /            \
///  LHS        LogicalApply
///             /           \
///          Domain         RHS
/// ```
pub struct TranslateApplyRule {
    enable_share_plan: bool,
}

impl Rule for TranslateApplyRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let apply: &LogicalApply = plan.as_logical_apply()?;
        let mut left: PlanRef = apply.left();
        let right: PlanRef = apply.right();
        let apply_left_len = left.schema().len();
        let correlated_indices = apply.correlated_indices();

        let mut index_mapping =
            ColIndexMapping::with_target_size(vec![None; apply_left_len], apply_left_len);
        let mut data_types = HashMap::new();
        let mut index = 0;

        // First try to rewrite the left side of the apply.
        // TODO: remove the rewrite and always use the general way to calculate the domain
        //      after we support DAG.
        let domain: PlanRef = if let Some(rewritten_left) = Self::rewrite(
            &left,
            correlated_indices.clone(),
            0,
            &mut index_mapping,
            &mut data_types,
            &mut index,
        ) {
            // This `LogicalProject` is used to make sure that after `LogicalApply`'s left was
            // rewritten, the new index of `correlated_index` is always at its position in
            // `correlated_indices`.
            let exprs = correlated_indices
                .clone()
                .into_iter()
                .enumerate()
                .map(|(i, correlated_index)| {
                    let index = index_mapping.map(correlated_index);
                    let data_type = rewritten_left.schema().fields()[index].data_type.clone();
                    index_mapping.put(correlated_index, Some(i));
                    InputRef::new(index, data_type).into()
                })
                .collect();
            let project = LogicalProject::create(rewritten_left, exprs);
            let distinct = Agg::new(vec![], (0..project.schema().len()).collect_vec(), project);
            distinct.into()
        } else {
            // The left side of the apply is not SPJ. We need to use the general way to calculate
            // the domain. Distinct + Project + The Left of Apply

            // Use Share
            left = if self.enable_share_plan {
                let logical_share = LogicalShare::new(left);
                logical_share.into()
            } else {
                left
            };

            let distinct = Agg::new(
                vec![],
                correlated_indices.clone().into_iter().collect_vec(),
                left.clone(),
            );
            distinct.into()
        };

        let eq_predicates = correlated_indices
            .into_iter()
            .enumerate()
            .map(|(i, correlated_index)| {
                let shifted_index = i + apply_left_len;
                let data_type = domain.schema().fields()[i].data_type.clone();
                let left = InputRef::new(correlated_index, data_type.clone());
                let right = InputRef::new(shifted_index, data_type);
                // use null-safe equal
                FunctionCall::new_unchecked(
                    ExprType::IsNotDistinctFrom,
                    vec![left.into(), right.into()],
                    DataType::Boolean,
                )
                .into()
            })
            .collect::<Vec<ExprImpl>>();

        let new_apply = apply.clone_with_left_right(left, right);
        let new_node = new_apply.translate_apply(domain, eq_predicates);
        Some(new_node)
    }
}

impl TranslateApplyRule {
    pub fn create(enable_share_plan: bool) -> BoxedRule {
        Box::new(TranslateApplyRule { enable_share_plan })
    }

    /// Rewrite `LogicalApply`'s left according to `correlated_indices`.
    ///
    /// Assumption: only `LogicalJoin`, `LogicalScan`, `LogicalProject` and `LogicalFilter` are in
    /// the left.
    fn rewrite(
        plan: &PlanRef,
        correlated_indices: Vec<usize>,
        offset: usize,
        index_mapping: &mut ColIndexMapping,
        data_types: &mut HashMap<usize, DataType>,
        index: &mut usize,
    ) -> Option<PlanRef> {
        if let Some(join) = plan.as_logical_join() {
            Self::rewrite_join(
                join,
                correlated_indices,
                offset,
                index_mapping,
                data_types,
                index,
            )
        } else if let Some(scan) = plan.as_logical_scan() {
            Self::rewrite_scan(
                scan,
                correlated_indices,
                offset,
                index_mapping,
                data_types,
                index,
            )
        } else if let Some(filter) = plan.as_logical_filter() {
            Self::rewrite(
                &filter.input(),
                correlated_indices,
                offset,
                index_mapping,
                data_types,
                index,
            )
        } else {
            // TODO: better to return an error.
            None
        }
    }

    fn rewrite_join(
        join: &LogicalJoin,
        required_col_idx: Vec<usize>,
        mut offset: usize,
        index_mapping: &mut ColIndexMapping,
        data_types: &mut HashMap<usize, DataType>,
        index: &mut usize,
    ) -> Option<PlanRef> {
        // Only accept join which doesn't generate null columns.
        if !matches!(
            join.join_type(),
            JoinType::Inner
                | JoinType::LeftSemi
                | JoinType::RightSemi
                | JoinType::LeftAnti
                | JoinType::RightAnti
        ) {
            return None;
        }

        // TODO: Do we need to take the `on` into account?
        let left_len = join.left().schema().len();
        let (left_idxs, right_idxs): (Vec<_>, Vec<_>) = required_col_idx
            .into_iter()
            .partition(|idx| *idx < left_len);
        let mut rewrite =
            |plan: PlanRef, mut indices: Vec<usize>, is_right: bool| -> Option<PlanRef> {
                if is_right {
                    indices.iter_mut().for_each(|index| *index -= left_len);
                    offset += left_len;
                }
                if let Some(join) = plan.as_logical_join() {
                    Self::rewrite_join(join, indices, offset, index_mapping, data_types, index)
                } else if let Some(scan) = plan.as_logical_scan() {
                    Self::rewrite_scan(scan, indices, offset, index_mapping, data_types, index)
                } else {
                    None
                }
            };
        match (left_idxs.is_empty(), right_idxs.is_empty()) {
            (true, false) => rewrite(join.right(), right_idxs, true),
            (false, true) => rewrite(join.left(), left_idxs, false),
            (false, false) => {
                let left = rewrite(join.left(), left_idxs, false)?;
                let right = rewrite(join.right(), right_idxs, true)?;
                let new_join =
                    LogicalJoin::new(left, right, join.join_type(), Condition::true_cond());
                Some(new_join.into())
            }
            _ => None,
        }
    }

    fn rewrite_scan(
        scan: &LogicalScan,
        required_col_idx: Vec<usize>,
        offset: usize,
        index_mapping: &mut ColIndexMapping,
        data_types: &mut HashMap<usize, DataType>,
        index: &mut usize,
    ) -> Option<PlanRef> {
        for i in &required_col_idx {
            let correlated_index = *i + offset;
            index_mapping.put(correlated_index, Some(*index));
            data_types.insert(
                correlated_index,
                scan.schema().fields()[*i].data_type.clone(),
            );
            *index += 1;
        }

        Some(scan.clone_with_output_indices(required_col_idx).into())
    }
}
