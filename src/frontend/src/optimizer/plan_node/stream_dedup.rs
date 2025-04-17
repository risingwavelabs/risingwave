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
use risingwave_common::util::sort_util::OrderType;
use risingwave_pb::stream_plan::DedupNode;
use risingwave_pb::stream_plan::stream_node::PbNodeBody;

use super::generic::GenericPlanNode;
use super::stream::prelude::*;
use super::utils::{TableCatalogBuilder, impl_distill_by_unit};
use super::{ExprRewritable, PlanBase, PlanTreeNodeUnary, StreamNode, generic};
use crate::TableCatalog;
use crate::optimizer::plan_node::PlanRef;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::stream_fragmenter::BuildFragmentGraphState;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamDedup {
    pub base: PlanBase<Stream>,
    core: generic::Dedup<PlanRef>,
}

impl StreamDedup {
    pub fn new(core: generic::Dedup<PlanRef>) -> Self {
        let input = core.input.clone();
        // A dedup operator must be append-only.
        assert!(input.append_only());

        let base = PlanBase::new_stream_with_core(
            &core,
            input.distribution().clone(),
            true,
            input.emit_on_window_close(),
            input.watermark_columns().clone(),
            input.columns_monotonicity().clone(),
        );
        StreamDedup { base, core }
    }

    pub fn infer_internal_table_catalog(&self) -> TableCatalog {
        let schema = self.core.schema();
        let mut builder = TableCatalogBuilder::default();

        schema.fields().iter().for_each(|field| {
            builder.add_column(field);
        });

        self.core.dedup_cols.iter().for_each(|idx| {
            builder.add_order_column(*idx, OrderType::ascending());
        });

        let read_prefix_len_hint = builder.get_current_pk_len();

        builder.build(
            self.base.distribution().dist_column_indices().to_vec(),
            read_prefix_len_hint,
        )
    }
}

// assert!(self.base.append_only());
impl_distill_by_unit!(StreamDedup, core, "StreamAppendOnlyDedup");

impl PlanTreeNodeUnary for StreamDedup {
    fn input(&self) -> PlanRef {
        self.core.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        let mut core = self.core.clone();
        core.input = input;
        Self::new(core)
    }
}

impl_plan_tree_node_for_unary! { StreamDedup }

impl StreamNode for StreamDedup {
    fn to_stream_prost_body(&self, state: &mut BuildFragmentGraphState) -> PbNodeBody {
        let table_catalog = self
            .infer_internal_table_catalog()
            .with_id(state.gen_table_id_wrapped());
        PbNodeBody::AppendOnlyDedup(Box::new(DedupNode {
            state_table: Some(table_catalog.to_internal_table_prost()),
            dedup_column_indices: self
                .core
                .dedup_cols
                .iter()
                .map(|idx| *idx as _)
                .collect_vec(),
        }))
    }
}

impl ExprRewritable for StreamDedup {}

impl ExprVisitable for StreamDedup {}
