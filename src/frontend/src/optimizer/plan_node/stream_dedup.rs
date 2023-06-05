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
use risingwave_common::util::sort_util::OrderType;
use risingwave_pb::stream_plan::stream_node::PbNodeBody;
use risingwave_pb::stream_plan::DedupNode;

use super::generic::{self, GenericPlanNode, GenericPlanRef};
use super::utils::{TableCatalogBuilder, impl_distill_by_unit};
use super::{ExprRewritable, PlanBase, PlanTreeNodeUnary, StreamNode};
use crate::optimizer::plan_node::stream::StreamPlanRef;
use crate::optimizer::plan_node::PlanRef;
use crate::stream_fragmenter::BuildFragmentGraphState;
use crate::TableCatalog;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamDedup {
    pub base: PlanBase,
    logical: generic::Dedup<PlanRef>,
}

impl StreamDedup {
    pub fn new(logical: generic::Dedup<PlanRef>) -> Self {
        let input = logical.input.clone();
        // A dedup operator must be append-only.
        assert!(input.append_only());

        let base = PlanBase::new_stream_with_logical(
            &logical,
            input.distribution().clone(),
            true,
            input.emit_on_window_close(),
            input.watermark_columns().clone(),
        );
        StreamDedup { base, logical }
    }

    pub fn infer_internal_table_catalog(&self) -> TableCatalog {
        let schema = self.logical.schema();
        let mut builder =
            TableCatalogBuilder::new(self.base.ctx().with_options().internal_table_subset());

        schema.fields().iter().for_each(|field| {
            builder.add_column(field);
        });

        self.logical.dedup_cols.iter().for_each(|idx| {
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
impl_distill_by_unit!(StreamDedup, logical, "StreamAppendOnlyDedup");

impl PlanTreeNodeUnary for StreamDedup {
    fn input(&self) -> PlanRef {
        self.logical.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        let mut logical = self.logical.clone();
        logical.input = input;
        Self::new(logical)
    }
}

impl_plan_tree_node_for_unary! { StreamDedup }

impl StreamNode for StreamDedup {
    fn to_stream_prost_body(&self, state: &mut BuildFragmentGraphState) -> PbNodeBody {
        let table_catalog = self
            .infer_internal_table_catalog()
            .with_id(state.gen_table_id_wrapped());
        PbNodeBody::AppendOnlyDedup(DedupNode {
            state_table: Some(table_catalog.to_internal_table_prost()),
            dedup_column_indices: self
                .logical
                .dedup_cols
                .iter()
                .map(|idx| *idx as _)
                .collect_vec(),
        })
    }
}

impl ExprRewritable for StreamDedup {}
