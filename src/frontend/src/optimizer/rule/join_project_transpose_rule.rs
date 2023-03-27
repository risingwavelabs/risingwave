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

use itertools::Itertools;
use risingwave_common::util::column_index_mapping::ColIndexMapping;
use risingwave_pb::plan_common::JoinType;

use super::Rule;
use crate::expr::{ExprRewriter, InputRef};
use crate::optimizer::plan_node::{LogicalJoin, LogicalProject};
use crate::utils::IndexRewriter;

/// Before this rule:
/// `join(project_a(t1),project_b(t2))`
///
/// After this rule:
/// `new_project(new_join(t1,t2))`
/// `new_join` is a full out join.
/// `new_project` is a projection combine `project_a` and `project_b` and it only output the
/// columns that are output in the original `join`.
pub struct JoinProjectTransposeRule {}

impl Rule for JoinProjectTransposeRule {
    fn apply(&self, plan: crate::PlanRef) -> Option<crate::PlanRef> {
        let join = plan.as_logical_join()?;

        let (left, right, on, join_type, _) = join.clone().decompose();

        let (left_input_index_on_condition, right_input_index_on_condition) = {
            let input_refs = on.collect_input_refs(left.schema().len() + right.schema().len());
            let index_group = input_refs.ones().group_by(|i| *i < left.schema().len());
            let left_index = index_group
                .into_iter()
                .next()
                .map_or(vec![], |group| group.1.collect_vec());
            let right_index = index_group.into_iter().next().map_or(vec![], |group| {
                group.1.map(|i| i - left.schema().len()).collect_vec()
            });
            (left_index, right_index)
        };

        let full_output_len = left.schema().len() + right.schema().len();
        let right_output_len = right.schema().len();
        let left_output_len = left.schema().len();
        let mut full_proj_exprs = Vec::with_capacity(full_output_len);

        let mut old_i2new_i = ColIndexMapping::empty(0, 0);

        let mut has_new_left: bool = false;
        let mut has_new_right: bool = false;

        // prepare for pull up left child.
        let new_left = if let Some(project) = left.as_logical_project() && left_input_index_on_condition.iter().all(|index| project.exprs()[*index].as_input_ref().is_some()) && join_type != JoinType::RightAnti && join_type != JoinType::RightSemi && join_type != JoinType::RightOuter {
            let (exprs,child) = project.clone().decompose();

            old_i2new_i = old_i2new_i.union(&join.i2l_col_mapping_ignore_join_type().composite(&project.o2i_col_mapping()));

            full_proj_exprs.extend(exprs);

            has_new_left = true;

            child
        } else {
            old_i2new_i = old_i2new_i.union(&join.i2l_col_mapping_ignore_join_type());

            for i in 0..left_output_len {
                full_proj_exprs.push(InputRef{index:i,data_type:left.schema().data_types()[i].clone()}.into());
            }

            left
        };

        // prepare for pull up right child.
        let new_right = if let Some(project) = right.as_logical_project() && right_input_index_on_condition.iter().all(|index| project.exprs()[*index].as_input_ref().is_some()) && join_type != JoinType::LeftAnti && join_type != JoinType::LeftSemi && join_type != JoinType::LeftOuter{
            let (exprs,child) = project.clone().decompose();

            old_i2new_i = old_i2new_i.union(&join.i2r_col_mapping_ignore_join_type().composite(&project.o2i_col_mapping()).clone_with_offset(new_left.schema().len()));

            let mut index_writer = IndexRewriter::new(ColIndexMapping::identity(child.schema().len()).clone_with_offset(new_left.schema().len()));
            full_proj_exprs.extend(exprs.into_iter().map(|expr| index_writer.rewrite_expr(expr)));

            has_new_right = true;

            child
        } else {
            old_i2new_i = old_i2new_i.union(&join.i2r_col_mapping_ignore_join_type().clone_with_offset(new_left.schema().len()));

            for i in 0..right_output_len {
                full_proj_exprs.push(InputRef{index:i+new_left.schema().len(),data_type:right.schema().data_types()[i].clone()}.into());
            }

            right
        };

        // No project will be pulled up
        if !has_new_left && !has_new_right {
            return None;
        }

        let new_cond = on.rewrite_expr(&mut IndexRewriter::new(old_i2new_i));
        let new_join = LogicalJoin::new(new_left, new_right, join_type, new_cond);

        // remain only the columns that are output in the original join
        let new_proj_exprs = join
            .output_indices()
            .iter()
            .map(|i| full_proj_exprs[*i].clone())
            .collect_vec();
        let new_project = LogicalProject::new(new_join.into(), new_proj_exprs);

        Some(new_project.into())
    }
}

impl JoinProjectTransposeRule {
    pub fn create() -> Box<dyn Rule> {
        Box::new(JoinProjectTransposeRule {})
    }
}
