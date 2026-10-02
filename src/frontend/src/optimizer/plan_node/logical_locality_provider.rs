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

use itertools::Itertools;

use super::generic::GenericPlanRef;
use super::stream::StreamPlanNodeMetadata;
use super::utils::impl_distill_by_unit;
use super::{
    BatchPlanRef, ColPrunable, ExprRewritable, Logical, LogicalPlanRef as PlanRef, LogicalProject,
    PlanBase, PlanTreeNodeUnary, PredicatePushdown, StreamExchange, StreamLocalityProvider,
    StreamPlanRef, ToBatch, ToStream, generic,
};
use crate::error::Result;
use crate::expr::{ExprRewriter, ExprVisitor};
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::{
    ColumnPruningContext, PredicatePushdownContext, RewriteStreamContext, ToStreamContext,
};
use crate::optimizer::property::RequiredDist;
use crate::utils::{ColIndexMapping, Condition};

/// `LogicalLocalityProvider` requires that the operator above it gets its input replayed with
/// locality on `locality_columns` during backfilling, so that it reads its state sequentially. It
/// reserves the locality columns in the stream key, while the operator converts it through
/// [`LocalityInput`], which builds a `StreamLocalityProvider` only if the input does not already
/// replay rows in the order the operator needs. The provider buffers input data into a state table
/// with the locality columns as primary key prefix.
///
/// The `LocalityProvider` has 2 states:
/// - One is used to buffer data during backfilling and provide data locality.
/// - The other one is a progress table like normal backfill operator to track the backfilling progress of itself.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalLocalityProvider {
    pub base: PlanBase<Logical>,
    core: generic::LocalityProvider<PlanRef>,
}

impl LogicalLocalityProvider {
    pub fn new(input: PlanRef, locality_columns: Vec<usize>) -> Self {
        assert!(!locality_columns.is_empty());
        let core = generic::LocalityProvider::new(input, locality_columns);
        let base = PlanBase::new_logical_with_core(&core);
        LogicalLocalityProvider { base, core }
    }

    pub fn create(input: PlanRef, locality_columns: Vec<usize>) -> PlanRef {
        LogicalLocalityProvider::new(input, locality_columns).into()
    }

    pub fn locality_columns(&self) -> &[usize] {
        &self.core.locality_columns
    }
}

impl PlanTreeNodeUnary<Logical> for LogicalLocalityProvider {
    fn input(&self) -> PlanRef {
        self.core.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(input, self.locality_columns().to_vec())
    }

    fn rewrite_with_input(
        &self,
        input: PlanRef,
        input_col_change: ColIndexMapping,
    ) -> (Self, ColIndexMapping) {
        let locality_columns = self
            .locality_columns()
            .iter()
            .map(|&i| input_col_change.map(i))
            .collect();

        (Self::new(input, locality_columns), input_col_change)
    }
}

impl_plan_tree_node_for_unary! { Logical, LogicalLocalityProvider}
impl_distill_by_unit!(LogicalLocalityProvider, core, "LogicalLocalityProvider");

impl ColPrunable for LogicalLocalityProvider {
    fn prune_col(&self, required_cols: &[usize], ctx: &mut ColumnPruningContext) -> PlanRef {
        // No pruning.
        let input_required_cols = (0..self.input().schema().len()).collect_vec();
        LogicalProject::with_out_col_idx(
            self.clone_with_input(self.input().prune_col(&input_required_cols, ctx))
                .into(),
            required_cols.iter().cloned(),
        )
        .into()
    }
}

impl PredicatePushdown for LogicalLocalityProvider {
    fn predicate_pushdown(
        &self,
        predicate: Condition,
        ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        let new_input = self.input().predicate_pushdown(predicate, ctx);
        let new_provider = self.clone_with_input(new_input);
        new_provider.into()
    }
}

impl ToBatch for LogicalLocalityProvider {
    fn to_batch(&self) -> Result<BatchPlanRef> {
        // LocalityProvider is a streaming-only operator
        Err(crate::error::ErrorCode::NotSupported(
            "LocalityProvider in batch mode".to_owned(),
            "LocalityProvider is only supported in streaming mode for backfilling".to_owned(),
        )
        .into())
    }
}

impl ToStream for LogicalLocalityProvider {
    fn to_stream(&self, _ctx: &mut ToStreamContext) -> Result<StreamPlanRef> {
        unreachable!(
            "a locality provider is converted by the operator it feeds via `LocalityInput`"
        )
    }

    fn logical_rewrite_for_stream(
        &self,
        ctx: &mut RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        let (input, input_col_change) = self.input().logical_rewrite_for_stream(ctx)?;
        let (locality_provider, out_col_change) = self.rewrite_with_input(input, input_col_change);
        Ok((locality_provider.into(), out_col_change))
    }
}

/// The stream input of an operator that requires locality on its input during backfilling, before
/// the operator decides the layout of its state.
///
/// If the input is a [`LogicalLocalityProvider`], only the input of the provider is converted, so
/// that the operator can decide its state layout from it, e.g. by its watermark columns.
/// [`Self::into_stream_with_layout`] then builds the provider laid out like the state, unless the
/// input already replays rows in the order the state needs.
pub enum LocalityInput {
    Stream(StreamPlanRef),
    Provider(LogicalLocalityProvider, StreamPlanRef),
}

impl LocalityInput {
    /// Converts `input`, satisfying `required_dist` unless it is a provider: the operator enforces
    /// its distribution on the provider it lays out with [`Self::into_stream_with_layout`].
    pub fn new(
        input: &PlanRef,
        required_dist: &RequiredDist,
        ctx: &mut ToStreamContext,
    ) -> Result<Self> {
        Ok(match input.as_logical_locality_provider() {
            Some(provider) => Self::Provider(provider.clone(), provider.input().to_stream(ctx)?),
            None => Self::Stream(input.to_stream_with_dist_required(required_dist, ctx)?),
        })
    }

    /// The stream input, which is the input of the provider if there is one.
    pub fn stream(&self) -> &StreamPlanRef {
        match self {
            Self::Stream(input) | Self::Provider(_, input) => input,
        }
    }

    /// Whether [`Self::into_stream_with_layout`] builds a provider for `locality_columns_ordered`.
    pub fn needs_provider(&self, locality_columns_ordered: &[usize]) -> bool {
        matches!(self, Self::Provider(_, input)
            if !order_starts_with(input.replay_order(), locality_columns_ordered))
    }

    /// Lays out the input like the state of the operator, replaying its rows in the order of
    /// `locality_columns_ordered`.
    pub fn into_stream_with_layout(
        self,
        locality_columns_ordered: &[usize],
    ) -> Result<StreamPlanRef> {
        let needs_provider = self.needs_provider(locality_columns_ordered);
        let (provider, input) = match self {
            Self::Provider(provider, input) if needs_provider => (provider, input),
            Self::Stream(input) | Self::Provider(_, input) => return Ok(input),
        };
        // Use `shard_by_exact_key` instead of `shard_by_key`, because locality provider will change the `stream_key` to include locality columns.
        // If we use `shard_by_key`, it is possible that the locality columns are (`a`, `b`), but input stream key is only (`a`).
        // In this case, `shard_by_key` will only shuffle by `a`. once `b` is changed, we will meet  U- and U+ should have same stream key error.
        // Though we can let locality provider stream key to include its distribution columns only to fix the error,
        // using `shard_by_exact_key` for another reason that it can provide better locality by `shard_by_key`.
        // For example, if locality columns are (`a`, `b`), and we use `shard_by_key` with key (`b`),
        // then all data with same `a` but different b will be shuffled to different nodes, which hurts locality.
        let required_dist =
            RequiredDist::shard_by_exact_key(input.schema().len(), provider.locality_columns());
        let input = required_dist.streaming_enforce_if_not_satisfies(input)?;
        let input = if input.as_stream_exchange().is_none() {
            // Force a no shuffle exchange to ensure locality provider is in its own fragment.
            // This is important to ensure the backfill ordering can recognize and build
            // the dependency graph among different backfill-needed fragments.
            StreamExchange::new_no_shuffle(input).into()
        } else {
            input
        };
        let stream_core =
            generic::LocalityProvider::new(input, provider.locality_columns().to_vec());
        Ok(StreamLocalityProvider::new(stream_core, locality_columns_ordered).into())
    }
}

impl ExprRewritable<Logical> for LogicalLocalityProvider {
    fn has_rewritable_expr(&self) -> bool {
        false
    }

    fn rewrite_exprs(&self, _r: &mut dyn ExprRewriter) -> PlanRef {
        self.clone().into()
    }
}

impl ExprVisitable for LogicalLocalityProvider {
    fn visit_exprs(&self, _v: &mut dyn ExprVisitor) {
        // No expressions to visit
    }
}

/// Whether rows sorted by `order` are also sorted by `prefix`. A repeated column adds nothing to an
/// order.
fn order_starts_with(order: &[usize], prefix: &[usize]) -> bool {
    let mut order = order.iter().unique();
    prefix.iter().unique().all(|col| order.next() == Some(col))
}
