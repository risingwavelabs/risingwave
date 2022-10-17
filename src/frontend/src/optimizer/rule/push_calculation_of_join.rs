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
use itertools::Itertools;
use risingwave_pb::expr::expr_node::Type;

use super::BoxedRule;
use crate::expr::{align_types, Expr, ExprImpl, ExprRewriter, FunctionCall, InputRef};
use crate::optimizer::plan_node::{LogicalJoin, LogicalProject};
use crate::optimizer::rule::Rule;
use crate::optimizer::PlanRef;
use crate::utils::{ColIndexMapping, Condition};

pub struct PushCalculationOfJoinRule {}

impl Rule for PushCalculationOfJoinRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let join: &LogicalJoin = plan.as_logical_join()?;
        let (mut left, mut right, mut on, join_type, mut output_indices) = join.clone().decompose();
        let left_col_num = left.schema().len();
        let right_col_num = right.schema().len();

        let exprs = on.conjunctions;
        let (left_exprs, right_exprs, indices_and_ty_of_func_calls) =
            Self::find_comparison_exprs(left_col_num, right_col_num, &exprs);

        // used to shift indices of input_refs pointing the right side of `join` with
        // `left_exprs.len`.
        let mut col_index_mapping = {
            let map = (0..left_col_num)
                .chain((left_col_num..left_col_num + right_col_num).map(|i| i + left_exprs.len()))
                .map(Some)
                .collect_vec();
            ColIndexMapping::new(map)
        };
        let (mut exprs, new_output_indices) =
            Self::remap_exprs_and_output_indices(exprs, output_indices, &mut col_index_mapping);
        output_indices = new_output_indices;

        // ```ignore
        // the internal table of join has has the following schema:
        // original left's columns | left_exprs | original right's columns | right_exprs
        //```
        // `left_index` and `right_index` will scan through `left_exprs` and `right_exprs`
        // respectively.
        let mut left_index = left_col_num;
        let mut right_index = left_col_num + left_exprs.len() + right_col_num;
        // replace chosen function calls.
        for (((index_of_func_call, ty), left_expr), right_expr) in indices_and_ty_of_func_calls
            .into_iter()
            .zip_eq(&left_exprs)
            .zip_eq(&right_exprs)
        {
            let left_input = InputRef::new(left_index, left_expr.return_type());
            let right_input = InputRef::new(right_index, right_expr.return_type());
            exprs[index_of_func_call] =
                FunctionCall::new(ty, vec![left_input.into(), right_input.into()])
                    .unwrap()
                    .into();
            left_index += 1;
            right_index += 1;
        }
        on = Condition {
            conjunctions: exprs,
        };

        // add project to do the calculation.
        let new_input = |input: PlanRef, appended_exprs: Vec<ExprImpl>| {
            let mut exprs = input
                .schema()
                .data_types()
                .into_iter()
                .enumerate()
                .map(|(i, data_type)| InputRef::new(i, data_type).into())
                .collect_vec();
            exprs.extend(appended_exprs);
            LogicalProject::create(input, exprs)
        };
        if !left_exprs.is_empty() {
            // avoid unnecessary `project`s.
            left = new_input(left, left_exprs);
            right = new_input(right, right_exprs);
        }

        Some(LogicalJoin::with_output_indices(left, right, join_type, on, output_indices).into())
    }
}

impl PushCalculationOfJoinRule {
    /// find the comparison exprs and return their inputs, types and indices.
    fn find_comparison_exprs(
        left_col_num: usize,
        right_col_num: usize,
        exprs: &[ExprImpl],
    ) -> (Vec<ExprImpl>, Vec<ExprImpl>, Vec<(usize, Type)>) {
        let left_bit_map = FixedBitSet::from_iter(0..left_col_num);
        let right_bit_map = FixedBitSet::from_iter(left_col_num..left_col_num + right_col_num);

        let mut left_exprs = vec![];
        let mut right_exprs = vec![];
        // indices and return types of function calls whose's inputs will be calculated in
        // `project`s
        let mut indices_and_ty_of_func_calls = vec![];
        let is_comparison_type = |ty| {
            matches!(
                ty,
                Type::LessThan
                    | Type::LessThanOrEqual
                    | Type::Equal
                    | Type::GreaterThan
                    | Type::GreaterThanOrEqual
            )
        };
        for (index, expr) in exprs.iter().enumerate() {
            let ExprImpl::FunctionCall(func) = expr else {continue};
            if !is_comparison_type(func.get_expr_type()) {
                continue;
            }
            let (ty, left, right) = func.clone().decompose_as_binary();
            // we just cast the return types of inputs of equal conditions for `HashJoin`.
            // non-equal conditions don't need unnecessary explicit cast.
            let left_input_bits = left.collect_input_refs(left_col_num + right_col_num);
            let right_input_bits = right.collect_input_refs(left_col_num + right_col_num);
            let (mut left, mut right) = if left_input_bits.is_subset(&left_bit_map)
                && right_input_bits.is_subset(&right_bit_map)
            {
                (left, right)
            } else if left_input_bits.is_subset(&right_bit_map)
                && right_input_bits.is_subset(&left_bit_map)
            {
                (right, left)
            } else {
                continue;
            };
            // when both `left` and `right` are `input_ref`, and they have the same return type
            // there is no need to calculate them in project.
            if left.as_input_ref().is_some()
                && right.as_input_ref().is_some()
                && left.return_type() == right.return_type()
            {
                continue;
            }
            // align return types to avoid error when executing join.
            align_types([&mut left, &mut right].into_iter()).unwrap();
            left_exprs.push(left);
            {
                let mut shift_with_offset = ColIndexMapping::with_shift_offset(
                    left_col_num + right_col_num,
                    -(left_col_num as isize),
                );
                let right = shift_with_offset.rewrite_expr(right);
                right_exprs.push(right);
            }
            indices_and_ty_of_func_calls.push((index, ty));
        }
        (left_exprs, right_exprs, indices_and_ty_of_func_calls)
    }

    /// use `col_index_mapping` to remap `exprs` and `output_indices`.
    fn remap_exprs_and_output_indices(
        exprs: Vec<ExprImpl>,
        output_indices: Vec<usize>,
        col_index_mapping: &mut ColIndexMapping,
    ) -> (Vec<ExprImpl>, Vec<usize>) {
        let exprs: Vec<ExprImpl> = exprs
            .into_iter()
            .map(|expr| col_index_mapping.rewrite_expr(expr))
            .collect();
        let output_indices = output_indices
            .into_iter()
            .map(|i| col_index_mapping.map(i))
            .collect();
        (exprs, output_indices)
    }

    pub fn create() -> BoxedRule {
        Box::new(PushCalculationOfJoinRule {})
    }
}
