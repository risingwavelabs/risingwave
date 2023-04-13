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

use risingwave_common::util::column_index_mapping::ColIndexMapping;
use risingwave_pb::plan_common::JoinType;

use super::{BoxedRule, Rule};
use crate::optimizer::plan_node::{LogicalHopWindow, LogicalJoin};
use crate::utils::IndexRewriter;

pub struct PullUpHopRule {}

impl Rule for PullUpHopRule {
    fn apply(&self, plan: crate::PlanRef) -> Option<crate::PlanRef> {
        let join = plan.as_logical_join()?;

        let (left, right, on, join_type, mut output_index) = join.clone().decompose();

        let (left_input_index_on_condition, right_input_index_on_condition) =
            join.input_idx_on_condition();

        let (left_output_pos, right_output_pos) = {
            let mut left_output_pos = vec![];
            let mut right_output_pos = vec![];
            output_index.iter_mut().enumerate().for_each(|(pos, idx)| {
                if *idx < left.schema().len() {
                    left_output_pos.push(pos);
                } else {
                    right_output_pos.push(pos);
                    // make right output index start from 0. We can identify left and right output
                    // index by the output_pos.
                    *idx -= left.schema().len();
                }
            });
            (left_output_pos, right_output_pos)
        };

        let mut old_i2new_i = ColIndexMapping::empty(0, 0);

        let mut pull_up_left = false;
        let mut pull_up_right = false;

        let (new_left,
            left_time_col,
            left_window_slide,
            left_window_size,
            left_window_offset,
        ) =  if let Some(hop) = left.as_logical_hop_window() && left_input_index_on_condition.iter().all(|&index| hop.output_window_start_col_idx().map_or(true, |v|index!=v) && hop.output_window_end_col_idx().map_or(true, |v|index!=v)) && join_type != JoinType::RightAnti && join_type != JoinType::RightSemi && join_type != JoinType::RightOuter && join_type != JoinType::FullOuter {
            let (input,
                time_col,
                window_slide,
                window_size,
                window_offset,
                _) = hop.clone().into_parts();

            old_i2new_i = old_i2new_i.union(&join.i2l_col_mapping_ignore_join_type().composite(&hop.o2i_col_mapping()));
            left_output_pos.iter().for_each(|&pos| {
                output_index[pos] = hop.output2internal_col_mapping().map(output_index[pos]);
            });
            pull_up_left = true;
            (input,Some(time_col),Some(window_slide),Some(window_size),Some(window_offset))
        } else {
            old_i2new_i = old_i2new_i.union(&join.i2l_col_mapping_ignore_join_type());

            (left,None,None,None,None)
        };

        let (new_right,
            right_time_col,
            right_window_slide,
            right_window_size,
            right_window_offset
        ) = if let Some(hop) = right.as_logical_hop_window() && right_input_index_on_condition.iter().all(|&index| hop.output_window_start_col_idx().map_or(true, |v|index!=v) && hop.output_window_end_col_idx().map_or(true, |v|index!=v)) && join_type != JoinType::LeftAnti && join_type != JoinType::LeftSemi && join_type != JoinType::LeftOuter && join_type != JoinType::FullOuter {
            let (input,
                time_col,
                window_slide,
                window_size,
                window_offset,
                _) = hop.clone().into_parts();

            old_i2new_i = old_i2new_i.union(&join.i2r_col_mapping_ignore_join_type().composite(&hop.o2i_col_mapping()).clone_with_offset(new_left.schema().len()));

            right_output_pos.iter().for_each(|&pos| {
                output_index[pos] = hop.output2internal_col_mapping().map(output_index[pos]);
            });
            pull_up_right = true;
            (input,Some(time_col),Some(window_slide),Some(window_size),Some(window_offset))
        } else {
            old_i2new_i = old_i2new_i.union(&join.i2r_col_mapping_ignore_join_type().clone_with_offset(new_left.schema().len()));

            (right,None,None,None,None)
        };

        if !pull_up_left && !pull_up_right {
            return None;
        }

        let new_output_index = {
            let new_right_output_len =
                if join_type == JoinType::LeftSemi || join_type == JoinType::LeftAnti {
                    0
                } else {
                    new_right.schema().len()
                };
            let new_left_output_len =
                if join_type == JoinType::RightSemi || join_type == JoinType::RightAnti {
                    0
                } else {
                    new_left.schema().len()
                };

            // The left output index can separate into two parts:
            // `left_other_column | left_window_start | letf_window_end`
            // The right output index can separate into two parts:
            // `right_other_column | right_window_start | right_window_end`
            //
            // If we pull up left, the column index will be changed to:
            // `left_other_column | right_column | left_window_start | letf_window_end`,
            // we need to update the index of left window start and left window end.
            //
            // If we pull up right and left, the column index will be changed to:
            // `left_other_column | right_other_column | left_window_start | letf_window_end |
            // right_window_tart | right_window_end |`, we need to update the index of
            // left window start and left window end and right window start and right window end.
            if pull_up_left {
                left_output_pos.iter().for_each(|&pos| {
                    if output_index[pos] >= new_left_output_len {
                        output_index[pos] += new_right_output_len;
                    }
                });
            }
            if pull_up_right && pull_up_left {
                right_output_pos.iter().for_each(|&pos| {
                    if output_index[pos] < new_right_output_len {
                        output_index[pos] += new_left_output_len;
                    } else {
                        output_index[pos] +=
                            new_left_output_len + LogicalHopWindow::ADDITION_COLUMN_LEN;
                    }
                });
            } else {
                right_output_pos.iter().for_each(|&pos| {
                    output_index[pos] += new_left_output_len;
                });
            }
            output_index
        };
        let new_left_len = new_left.schema().len();
        let new_cond = on.rewrite_expr(&mut IndexRewriter::new(old_i2new_i));
        let new_join = LogicalJoin::new(new_left, new_right, join_type, new_cond);

        let new_hop = if pull_up_left && pull_up_right {
            let left_hop = LogicalHopWindow::create(
                new_join.into(),
                left_time_col.unwrap(),
                left_window_slide.unwrap(),
                left_window_size.unwrap(),
                left_window_offset.unwrap(),
            );
            LogicalHopWindow::create(
                left_hop,
                right_time_col
                    .unwrap()
                    .clone_with_offset(new_left_len as isize),
                right_window_slide.unwrap(),
                right_window_size.unwrap(),
                right_window_offset.unwrap(),
            )
        } else if pull_up_left {
            LogicalHopWindow::create(
                new_join.into(),
                left_time_col.unwrap(),
                left_window_slide.unwrap(),
                left_window_size.unwrap(),
                left_window_offset.unwrap(),
            )
        } else {
            LogicalHopWindow::create(
                new_join.into(),
                right_time_col
                    .unwrap()
                    .clone_with_offset(new_left_len as isize),
                right_window_slide.unwrap(),
                right_window_size.unwrap(),
                right_window_offset.unwrap(),
            )
        };

        Some(
            new_hop
                .as_logical_hop_window()
                .unwrap()
                .clone_with_output_indices(new_output_index)
                .into(),
        )
    }
}

impl PullUpHopRule {
    pub fn create() -> BoxedRule {
        Box::new(PullUpHopRule {})
    }
}
