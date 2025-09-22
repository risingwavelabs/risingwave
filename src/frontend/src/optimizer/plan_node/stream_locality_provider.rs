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
use risingwave_common::catalog::Field;
use risingwave_common::hash::VirtualNode;
use risingwave_common::types::DataType;
use risingwave_common::util::sort_util::OrderType;
use risingwave_pb::stream_plan::LocalityProviderNode;
use risingwave_pb::stream_plan::stream_node::PbNodeBody;

use super::stream::prelude::*;
use super::utils::{TableCatalogBuilder, impl_distill_by_unit};
use super::{ExprRewritable, PlanTreeNodeUnary, StreamNode, StreamPlanRef as PlanRef, generic};
use crate::TableCatalog;
use crate::catalog::TableId;
use crate::expr::{ExprRewriter, ExprVisitor};
use crate::optimizer::plan_node::PlanBase;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::property::Distribution;
use crate::stream_fragmenter::BuildFragmentGraphState;

/// `StreamLocalityProvider` implements [`super::LogicalLocalityProvider`]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamLocalityProvider {
    pub base: PlanBase<Stream>,
    core: generic::LocalityProvider<PlanRef>,
}

impl StreamLocalityProvider {
    pub fn new(core: generic::LocalityProvider<PlanRef>) -> Self {
        let input = core.input.clone();

        let dist = match input.distribution() {
            Distribution::HashShard(keys) => {
                // If the input is hash-distributed, we make it a UpstreamHashShard distribution
                // just like a normal table scan. It is used to ensure locality provider is in its own fragment
                Distribution::UpstreamHashShard(keys.clone(), TableId::placeholder())
            }
            Distribution::UpstreamHashShard(keys, table_id) => {
                Distribution::UpstreamHashShard(keys.clone(), *table_id)
            }
            _ => {
                panic!("LocalityProvider input must be hash-distributed");
            }
        };

        // LocalityProvider maintains the append-only behavior if input is append-only
        let base = PlanBase::new_stream_with_core(
            &core,
            dist,
            input.stream_kind(),
            input.emit_on_window_close(),
            input.watermark_columns().clone(),
            input.columns_monotonicity().clone(),
        );
        StreamLocalityProvider { base, core }
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
impl_distill_by_unit!(StreamLocalityProvider, core, "StreamLocalityProvider");

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
        };

        PbNodeBody::LocalityProvider(locality_provider_node)
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
    /// Key: locality_columns (vnode handled internally by StateTable)
    fn build_state_catalog(&self, state: &mut BuildFragmentGraphState) -> TableCatalog {
        let mut catalog_builder = TableCatalogBuilder::default();
        let input = self.input();
        let input_schema = input.schema();

        // Add all input columns in original order
        for field in &input_schema.fields {
            catalog_builder.add_column(field);
        }

        // Set locality columns as primary key (vnode will be handled internally)
        for locality_col_idx in self.locality_columns() {
            catalog_builder.add_order_column(*locality_col_idx, OrderType::ascending());
        }
        // add streaming key of the input as the rest of the primary key
        if let Some(stream_key) = input.stream_key() {
            for &key_col_idx in stream_key {
                catalog_builder.add_order_column(key_col_idx, OrderType::ascending());
            }
        }

        catalog_builder.set_value_indices((0..input_schema.len()).into_iter().collect());

        catalog_builder
            .build(
                self.input().distribution().dist_column_indices().to_vec(),
                0,
            )
            .with_id(state.gen_table_id_wrapped())
    }

    /// Build the progress table catalog for tracking backfill progress
    /// Schema: | vnode | pk(locality columns + input stream keys) | backfill_finished | row_count |
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
        if let Some(stream_key) = input.stream_key() {
            for &key_col_idx in stream_key {
                let field = &input_schema.fields[key_col_idx];
                catalog_builder.add_column(field);
            }
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
