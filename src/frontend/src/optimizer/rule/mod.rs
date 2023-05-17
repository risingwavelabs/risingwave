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

//! Define all [`Rule`]

use super::PlanRef;

/// A one-to-one transform for the [`PlanNode`](super::plan_node::PlanNode), every [`Rule`] should
/// downcast and check if the node matches the rule.
pub trait Rule: Send + Sync + Description {
    /// return err(()) if not match
    fn apply(&self, plan: PlanRef) -> Option<PlanRef>;
}

pub trait Description {
    fn description(&self) -> &str;
}

pub(super) type BoxedRule = Box<dyn Rule>;

mod project_join_merge_rule;
pub use project_join_merge_rule::*;
mod project_eliminate_rule;
pub use project_eliminate_rule::*;
mod project_merge_rule;
pub use project_merge_rule::*;
mod pull_up_correlated_predicate_rule;
pub use pull_up_correlated_predicate_rule::*;
mod index_delta_join_rule;
pub use index_delta_join_rule::*;
mod left_deep_tree_join_ordering_rule;
pub use left_deep_tree_join_ordering_rule::*;
mod apply_agg_transpose_rule;
pub use apply_agg_transpose_rule::*;
mod apply_filter_transpose_rule;
pub use apply_filter_transpose_rule::*;
mod apply_project_transpose_rule;
pub use apply_project_transpose_rule::*;
mod apply_eliminate_rule;
pub use apply_eliminate_rule::*;
mod translate_apply_rule;
pub use translate_apply_rule::*;
mod merge_multijoin_rule;
pub use merge_multijoin_rule::*;
mod max_one_row_eliminate_rule;
pub use max_one_row_eliminate_rule::*;
mod apply_join_transpose_rule;
pub use apply_join_transpose_rule::*;
mod apply_to_join_rule;
pub use apply_to_join_rule::*;
mod distinct_agg_rule;
pub use distinct_agg_rule::*;
mod index_selection_rule;
pub use index_selection_rule::*;
mod push_calculation_of_join_rule;
pub use push_calculation_of_join_rule::*;
mod join_commute_rule;
mod over_agg_to_topn_rule;
pub use join_commute_rule::*;
pub use over_agg_to_topn_rule::*;
mod union_to_distinct_rule;
pub use union_to_distinct_rule::*;
mod agg_project_merge_rule;
pub use agg_project_merge_rule::*;
mod union_merge_rule;
pub use union_merge_rule::*;
mod dag_to_tree_rule;
pub use dag_to_tree_rule::*;
mod apply_share_eliminate_rule;
pub use apply_share_eliminate_rule::*;
mod top_n_on_index_rule;
pub use top_n_on_index_rule::*;
mod stream;
pub use stream::bushy_tree_join_ordering_rule::*;
pub use stream::filter_with_now_to_join_rule::*;
pub use stream::stream_project_merge_rule::*;
mod trivial_project_to_values_rule;
pub use trivial_project_to_values_rule::*;
mod union_input_values_merge_rule;
pub use union_input_values_merge_rule::*;
mod rewrite_like_expr_rule;
pub use rewrite_like_expr_rule::*;
mod min_max_on_index_rule;
pub use min_max_on_index_rule::*;
mod always_false_filter_rule;
pub use always_false_filter_rule::*;
mod join_project_transpose_rule;
pub use join_project_transpose_rule::*;
mod limit_push_down_rule;
pub use limit_push_down_rule::*;
mod pull_up_hop_rule;
pub use pull_up_hop_rule::*;
mod apply_offset_rewriter;
use apply_offset_rewriter::ApplyOffsetRewriter;
mod intersect_to_semi_join_rule;
pub use intersect_to_semi_join_rule::*;
mod except_to_anti_join_rule;
pub use except_to_anti_join_rule::*;
mod intersect_merge_rule;
pub use intersect_merge_rule::*;
mod except_merge_rule;
pub use except_merge_rule::*;
mod apply_dedup_transpose_rule;
pub use apply_dedup_transpose_rule::*;

#[macro_export]
macro_rules! for_all_rules {
    ($macro:ident) => {
        $macro! {
              { ApplyAggTransposeRule }
            , { ApplyFilterTransposeRule }
            , { ApplyProjectTransposeRule }
            , { ApplyEliminateRule }
            , { ApplyJoinTransposeRule }
            , { ApplyShareEliminateRule }
            , { ApplyToJoinRule }
            , { MaxOneRowEliminateRule }
            , { DistinctAggRule }
            , { IndexDeltaJoinRule }
            , { MergeMultiJoinRule }
            , { ProjectEliminateRule }
            , { ProjectJoinMergeRule }
            , { ProjectMergeRule }
            , { PullUpCorrelatedPredicateRule }
            , { LeftDeepTreeJoinOrderingRule }
            , { TranslateApplyRule }
            , { PushCalculationOfJoinRule }
            , { IndexSelectionRule }
            , { OverWindowToTopNRule }
            , { JoinCommuteRule }
            , { UnionToDistinctRule }
            , { AggProjectMergeRule }
            , { UnionMergeRule }
            , { DagToTreeRule }
            , { FilterWithNowToJoinRule }
            , { TopNOnIndexRule }
            , { TrivialProjectToValuesRule }
            , { UnionInputValuesMergeRule }
            , { RewriteLikeExprRule }
            , { MinMaxOnIndexRule }
            , { AlwaysFalseFilterRule }
            , { BushyTreeJoinOrderingRule }
            , { StreamProjectMergeRule }
            , { JoinProjectTransposeRule }
            , { LimitPushDownRule }
            , { PullUpHopRule }
            , { IntersectToSemiJoinRule }
            , { ExceptToAntiJoinRule }
            , { IntersectMergeRule }
            , { ExceptMergeRule }
            , { ApplyDedupTransposeRule }
        }
    };
}

macro_rules! impl_description {
    ($( { $name:ident }),*) => {
        paste::paste!{
            $(impl Description for [<$name>] {
                fn description(&self) -> &str {
                    stringify!([<$name>])
                }
            })*
        }
    }
}

for_all_rules! {impl_description}
