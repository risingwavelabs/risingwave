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

use std::collections::HashMap;

use itertools::Itertools;
use pretty_xmlish::XmlNode;
use risingwave_common::catalog::Field;
use risingwave_common::hash::VirtualNode;
use risingwave_common::types::DataType;
use risingwave_common::util::sort_util::OrderType;
use risingwave_pb::stream_plan::stream_node::PbNodeBody;
use risingwave_pb::stream_plan::{LocalityProviderNode, StreamScanType};

use super::stream::prelude::*;
use super::utils::{Distill, TableCatalogBuilder, childless_record};
use super::{
    ExprRewritable, PlanNodeId, PlanTreeNodeUnary, StreamNode, StreamPlanRef as PlanRef, generic,
};
use crate::TableCatalog;
use crate::expr::{ExprRewriter, ExprVisitor};
use crate::optimizer::plan_node::PlanBase;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_rewriter::PlanRewriter;
use crate::optimizer::property::{Distribution, ReplayOrder};
use crate::stream_fragmenter::BuildFragmentGraphState;

/// `StreamLocalityProvider` buffers its input during backfill and then replays each vnode in the
/// order of its locality columns, so that the operator it feeds accesses its state in that order.
/// It sits on the input of the operator, in the operator's fragment, so it replays each vnode of
/// the operator in the order of the operator's state.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamLocalityProvider {
    pub base: PlanBase<Stream>,
    core: generic::LocalityProvider<PlanRef>,
}

impl StreamLocalityProvider {
    pub fn new(core: generic::LocalityProvider<PlanRef>) -> Self {
        let input = core.input.clone();
        let replay_order = ReplayOrder::new(
            (core.locality_columns.iter())
                .chain(input.expect_stream_key())
                .copied()
                .collect(),
        );
        // LocalityProvider maintains the append-only behavior if input is append-only
        let base = PlanBase::new_stream_with_core(
            &core,
            input.distribution().clone(),
            input.stream_kind(),
            input.emit_on_window_close(),
            input.watermark_columns().clone(),
            input.columns_monotonicity().clone(),
        )
        .with_replay_order(replay_order);
        StreamLocalityProvider { base, core }
    }

    /// Puts a provider on each input of the stateful operators of `plan` that needs one, in the
    /// fragment of the operator. Runs after the rules that rewrite the structure of the stream plan,
    /// which then never see a provider. Returns the plan and the number of providers.
    pub fn place(plan: PlanRef) -> (PlanRef, usize) {
        let mut placer = Placer::default();
        let plan = plan.rewrite_with(&mut placer);
        (plan, placer.count)
    }

    /// Whether [`Self::place`] puts a provider on `input` of an operator that accesses its state in
    /// the order of `locality_columns`.
    pub fn needed(input: &PlanRef, locality_columns: &[usize]) -> bool {
        needs_provider(input, locality_columns, &mut HashMap::new())
    }

    pub fn locality_columns(&self) -> &[usize] {
        &self.core.locality_columns
    }
}

impl PlanTreeNodeUnary<Stream> for StreamLocalityProvider {
    fn input(&self) -> PlanRef {
        self.core.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        let mut core = self.core.clone();
        core.input = input;
        Self::new(core)
    }
}

impl_plan_tree_node_for_unary! { Stream, StreamLocalityProvider }

impl Distill for StreamLocalityProvider {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let vec = self.core.fields_pretty();
        childless_record("StreamLocalityProvider", vec)
    }
}

impl StreamNode for StreamLocalityProvider {
    fn to_stream_prost_body(&self, state: &mut BuildFragmentGraphState) -> PbNodeBody {
        let state_table = self.build_state_catalog(state);
        let progress_table = self.build_progress_catalog(state);

        let locality_provider_node = LocalityProviderNode {
            locality_columns: self.locality_columns().iter().map(|&i| i as u32).collect(),
            // State table for buffering input data
            state_table: Some(state_table.to_prost()),
            // Progress table for tracking backfill progress
            progress_table: Some(progress_table.to_prost()),
            rate_limit: self.base.ctx().overwrite_options().backfill_rate_limit,
        };

        PbNodeBody::LocalityProvider(Box::new(locality_provider_node))
    }
}

impl ExprRewritable<Stream> for StreamLocalityProvider {
    fn has_rewritable_expr(&self) -> bool {
        false
    }

    fn rewrite_exprs(&self, _r: &mut dyn ExprRewriter) -> PlanRef {
        self.clone().into()
    }
}

impl ExprVisitable for StreamLocalityProvider {
    fn visit_exprs(&self, _v: &mut dyn ExprVisitor) {
        // No expressions to visit
    }
}

impl StreamLocalityProvider {
    /// Build the state table catalog for buffering input data
    /// Schema: same as input schema (locality handled by primary key ordering)
    /// Key: `locality_columns` (vnode handled internally by `StateTable`)
    fn build_state_catalog(&self, state: &mut BuildFragmentGraphState) -> TableCatalog {
        let mut catalog_builder = TableCatalogBuilder::default();
        let input = self.input();
        let input_schema = input.schema();

        // Add all input columns in original order
        for field in &input_schema.fields {
            catalog_builder.add_column(field);
        }

        // Set locality columns as primary key.
        for locality_col_idx in self.locality_columns() {
            catalog_builder.add_order_column(*locality_col_idx, OrderType::ascending());
        }
        // add streaming key of the input as the rest of the primary key
        for &key_col_idx in input.expect_stream_key() {
            catalog_builder.add_order_column(key_col_idx, OrderType::ascending());
        }

        catalog_builder.set_value_indices((0..input_schema.len()).collect());

        catalog_builder
            .build(
                self.input().distribution().dist_column_indices().to_vec(),
                0,
            )
            .with_id(state.gen_table_id_wrapped())
    }

    /// Build the progress table catalog for tracking backfill progress
    /// Schema: | vnode | pk(locality columns + input stream keys) | `backfill_finished` | `row_count` |
    /// Key: | vnode | pk(locality columns + input stream keys) |
    fn build_progress_catalog(&self, state: &mut BuildFragmentGraphState) -> TableCatalog {
        let mut catalog_builder = TableCatalogBuilder::default();
        let input = self.input();
        let input_schema = input.schema();

        // Add vnode column as primary key
        catalog_builder.add_column(&Field::with_name(VirtualNode::RW_TYPE, "vnode"));
        catalog_builder.add_order_column(0, OrderType::ascending());

        // Add locality columns as part of primary key
        for &locality_col_idx in self.locality_columns() {
            let field = &input_schema.fields[locality_col_idx];
            catalog_builder.add_column(field);
        }

        // Add stream key columns as part of primary key (excluding those already added as locality columns)
        for &key_col_idx in input.expect_stream_key() {
            let field = &input_schema.fields[key_col_idx];
            catalog_builder.add_column(field);
        }

        // Add backfill_finished column
        catalog_builder.add_column(&Field::with_name(DataType::Boolean, "backfill_finished"));

        // Add row_count column
        catalog_builder.add_column(&Field::with_name(DataType::Int64, "row_count"));

        // Set vnode column index and distribution key
        catalog_builder.set_vnode_col_idx(0);
        catalog_builder.set_dist_key_in_pk(vec![0]);

        let num_of_columns = catalog_builder.columns().len();
        catalog_builder.set_value_indices((0..num_of_columns).collect_vec());

        catalog_builder
            .build(vec![0], 1)
            .with_id(state.gen_table_id_wrapped())
    }
}

/// The order in which a stateful operator accesses its state, as columns of each input it lays out
/// like its state.
fn state_orders(plan: &PlanRef) -> Vec<Option<Vec<usize>>> {
    if let Some(agg) = plan.as_stream_hash_agg() {
        vec![agg.ordered_group_key()]
    } else if let Some(top_n) = plan.as_stream_group_top_n() {
        vec![
            top_n
                .vnode_col_idx()
                .is_none()
                .then(|| top_n.group_key().to_vec()),
        ]
    } else if let Some(dedup) = plan.as_stream_dedup() {
        vec![Some(dedup.dedup_cols().to_vec())]
    } else if let Some(over_window) = plan.as_stream_over_window() {
        vec![Some(over_window.partition_key_indices())]
    } else if let Some(join) = plan.as_stream_hash_join() {
        let predicate = join.eq_join_predicate();
        vec![
            Some(predicate.left_eq_indexes()),
            Some(predicate.right_eq_indexes()),
        ]
    } else if let Some(join) = plan.as_stream_as_of_join() {
        let predicate = join.eq_join_predicate();
        vec![
            Some(predicate.left_eq_indexes()),
            Some(predicate.right_eq_indexes()),
        ]
    } else if let Some(join) = plan.as_stream_temporal_join()
        && !join.is_nested_loop()
    {
        // The lookups read the table in the order of the predicate.
        vec![Some(join.eq_join_predicate().left_eq_indexes()), None]
    } else {
        vec![]
    }
}

/// Whether `input` of an operator that accesses its state in the order of `locality_columns` needs a
/// provider: it carries backfilled rows, is hash distributed and doesn't replay its rows in that
/// order already.
fn needs_provider(
    input: &PlanRef,
    locality_columns: &[usize],
    carries_backfill_memo: &mut HashMap<PlanNodeId, bool>,
) -> bool {
    !locality_columns.is_empty()
        && !input.replay_order().starts_with(locality_columns)
        && matches!(
            input.distribution(),
            Distribution::HashShard(_) | Distribution::UpstreamHashShard(..)
        )
        && carries_backfill(input, carries_backfill_memo)
}

/// Whether `plan` carries rows replayed by backfill. A provider has nothing to replay otherwise,
/// e.g. on a source.
fn carries_backfill(plan: &PlanRef, memo: &mut HashMap<PlanNodeId, bool>) -> bool {
    if let Some(&carries_backfill) = memo.get(&plan.id()) {
        return carries_backfill;
    }
    let carries_backfill = if let Some(scan) = plan.as_stream_table_scan() {
        scan.stream_scan_type() != StreamScanType::UpstreamOnly
    } else {
        plan.as_stream_source_scan().is_some()
            || plan.as_stream_locality_provider().is_some()
            || plan
                .inputs()
                .iter()
                .any(|input| carries_backfill(input, memo))
    };
    memo.insert(plan.id(), carries_backfill);
    carries_backfill
}

#[derive(Default)]
struct Placer {
    count: usize,
    carries_backfill: HashMap<PlanNodeId, bool>,
}

impl PlanRewriter<Stream> for Placer {
    fn rewrite_with_inputs(&mut self, plan: &PlanRef, inputs: Vec<PlanRef>) -> PlanRef {
        let state_orders = state_orders(plan);
        let inputs = inputs
            .into_iter()
            .enumerate()
            .map(|(i, input)| match state_orders.get(i) {
                Some(Some(locality_columns))
                    if needs_provider(&input, locality_columns, &mut self.carries_backfill) =>
                {
                    self.count += 1;
                    StreamLocalityProvider::new(generic::LocalityProvider::new(
                        input,
                        locality_columns.clone(),
                    ))
                    .into()
                }
                _ => input,
            })
            .collect_vec();
        plan.clone_root_with_inputs(&inputs)
    }
}
