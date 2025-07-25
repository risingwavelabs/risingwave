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

//! Define all [`Rule`]

use std::convert::Infallible;
use std::ops::FromResidual;

use thiserror_ext::AsReport;

use super::PlanRef;
use crate::error::RwError;

/// Result when applying a [`Rule`] to a [`PlanNode`](super::plan_node::PlanNode).
pub enum ApplyResult<T> {
    /// Successfully applied the rule and returned a new plan.
    Ok(T),
    /// The current rule is not applicable to the input.
    /// The optimizer may try another rule.
    NotApplicable,
    /// An unrecoverable error occurred while applying the rule.
    /// The optimizer should stop applying other rules and report the error to the user.
    Err(RwError),
}

impl<T> ApplyResult<T> {
    /// Unwrap the result, panicking if it's not `Ok`.
    pub fn unwrap(self) -> T {
        match self {
            ApplyResult::Ok(plan) => plan,
            ApplyResult::NotApplicable => panic!("unwrap ApplyResult::NotApplicable"),
            ApplyResult::Err(e) => panic!("unwrap ApplyResult::Err, error: {:?}", e.as_report()),
        }
    }
}

/// Allow calling `?` on an `Option` in a function returning `ApplyResult`.
impl<T> FromResidual<Option<Infallible>> for ApplyResult<T> {
    fn from_residual(residual: Option<Infallible>) -> Self {
        match residual {
            Some(i) => match i {},
            None => Self::NotApplicable,
        }
    }
}

/// Allow calling `?` on a `Result` in a function returning `ApplyResult`.
impl<T, E> FromResidual<Result<Infallible, E>> for ApplyResult<T>
where
    E: Into<RwError>,
{
    fn from_residual(residual: Result<Infallible, E>) -> Self {
        match residual {
            Ok(i) => match i {},
            Err(e) => Self::Err(e.into()),
        }
    }
}

/// An one-to-one transform for the [`PlanNode`](super::plan_node::PlanNode).
///
/// It's a convenient trait to implement [`FallibleRule`], thus made available only within this module.
trait InfallibleRule<C: ConventionMarker>: Send + Sync + Description {
    /// Apply the rule to the plan node.
    ///
    /// - Returns `Some` if the apply is successful.
    /// - Returns `None` if it's not applicable. The optimizer may try other rules.
    fn apply(&self, plan: PlanRef<C>) -> Option<PlanRef<C>>;
}

/// An one-to-one transform for the [`PlanNode`](super::plan_node::PlanNode) that may return an
/// unrecoverable error that stops further optimization.
///
/// An [`InfallibleRule`] is always a [`FallibleRule`].
pub trait FallibleRule<C: ConventionMarker>: Send + Sync + Description {
    /// Apply the rule to the plan node, which may return an unrecoverable error.
    ///
    /// - Returns `ApplyResult::Ok` if the apply is successful.
    /// - Returns `ApplyResult::NotApplicable` if it's not applicable. The optimizer may try other rules.
    /// - Returns `ApplyResult::Err` if an unrecoverable error occurred. The optimizer should stop applying
    ///   other rules and report the error to the user.
    fn apply(&self, plan: PlanRef<C>) -> ApplyResult<PlanRef<C>>;
}

impl<C: ConventionMarker, R> FallibleRule<C> for R
where
    R: InfallibleRule<C>,
{
    fn apply(&self, plan: PlanRef<C>) -> ApplyResult<PlanRef<C>> {
        match InfallibleRule::apply(self, plan) {
            Some(plan) => ApplyResult::Ok(plan),
            None => ApplyResult::NotApplicable,
        }
    }
}

pub trait Description {
    fn description(&self) -> &str;
}

pub(super) type BoxedRule<C> = Box<dyn FallibleRule<C>>;

mod stream;
pub use stream::add_logstore_rule::*;
pub use stream::index_delta_join_rule::*;
pub use stream::separate_consecutive_join::*;
pub use stream::stream_project_merge_rule::*;

mod batch;
pub use batch::batch_iceberg_count_star::*;
pub use batch::batch_iceberg_predicate_pushdown::*;
pub use batch::batch_project_merge_rule::*;
pub use batch::batch_push_limit_to_scan_rule::*;
mod logical;
pub use logical::*;

use crate::optimizer::plan_node::ConventionMarker;

#[macro_export]
macro_rules! for_all_rules {
    ($macro:ident) => {
        $macro! {
              { ApplyAggTransposeRule }
            , { ApplyFilterTransposeRule }
            , { ApplyProjectTransposeRule }
            , { ApplyProjectSetTransposeRule }
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
            , { OverWindowToAggAndJoinRule }
            , { OverWindowSplitRule }
            , { OverWindowMergeRule }
            , { JoinCommuteRule }
            , { UnionToDistinctRule }
            , { AggProjectMergeRule }
            , { UnionMergeRule }
            , { DagToTreeRule }
            , { SplitNowAndRule }
            , { SplitNowOrRule }
            , { FilterWithNowToJoinRule }
            , { GenerateSeriesWithNowRule }
            , { TopNOnIndexRule }
            , { TrivialProjectToValuesRule }
            , { UnionInputValuesMergeRule }
            , { RewriteLikeExprRule }
            , { MinMaxOnIndexRule }
            , { AlwaysFalseFilterRule }
            , { BushyTreeJoinOrderingRule }
            , { StreamProjectMergeRule }
            , { SeparateConsecutiveJoinRule }
            , { LogicalFilterExpressionSimplifyRule }
            , { JoinProjectTransposeRule }
            , { LimitPushDownRule }
            , { PullUpHopRule }
            , { IntersectToSemiJoinRule }
            , { ExceptToAntiJoinRule }
            , { IntersectMergeRule }
            , { ExceptMergeRule }
            , { ApplyUnionTransposeRule }
            , { ApplyDedupTransposeRule }
            , { ProjectJoinSeparateRule }
            , { GroupingSetsToExpandRule }
            , { CrossJoinEliminateRule }
            , { ApplyTopNTransposeRule }
            , { TableFunctionToProjectSetRule }
            , { TableFunctionToFileScanRule }
            , { TableFunctionToPostgresQueryRule }
            , { TableFunctionToMySqlQueryRule }
            , { TableFunctionToInternalBackfillProgressRule }
            , { TableFunctionToInternalSourceBackfillProgressRule }
            , { ApplyLimitTransposeRule }
            , { CommonSubExprExtractRule }
            , { BatchProjectMergeRule }
            , { ApplyOverWindowTransposeRule }
            , { ApplyExpandTransposeRule }
            , { ExpandToProjectRule }
            , { AggGroupBySimplifyRule }
            , { ApplyHopWindowTransposeRule }
            , { AggCallMergeRule }
            , { ValuesExtractProjectRule }
            , { BatchPushLimitToScanRule }
            , { BatchIcebergPredicatePushDownRule }
            , { BatchIcebergCountStar }
            , { PullUpCorrelatedPredicateAggRule }
            , { SourceToKafkaScanRule }
            , { SourceToIcebergScanRule }
            , { AddLogstoreRule }
            , { EmptyAggRemoveRule }
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
