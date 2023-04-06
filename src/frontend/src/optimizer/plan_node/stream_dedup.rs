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

use std::fmt;

use itertools::Itertools;
use risingwave_common::util::sort_util::OrderType;
use risingwave_pb::stream_plan::stream_node::PbNodeBody;
use risingwave_pb::stream_plan::DedupNode;

use super::generic::GenericPlanRef;
use super::utils::TableCatalogBuilder;
use super::{ExprRewritable, LogicalDedup, PlanBase, PlanTreeNodeUnary, StreamNode};
use crate::optimizer::plan_node::stream::StreamPlanRef;
use crate::optimizer::plan_node::PlanRef;
use crate::stream_fragmenter::BuildFragmentGraphState;
use crate::TableCatalog;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamDedup {
    pub base: PlanBase,
    logical: LogicalDedup,
}

impl StreamDedup {
    pub fn new(logical: LogicalDedup) -> Self {
        let input = logical.input();
        // A dedup operator must be append-only.
        assert!(input.append_only());

        let base = PlanBase::new_stream(
            input.ctx(),
            input.schema().clone(),
            input.logical_pk().to_vec(),
            input.functional_dependency().clone(),
            input.distribution().clone(),
            true,
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

        self.logical.dedup_cols().iter().for_each(|idx| {
            builder.add_order_column(*idx, OrderType::ascending());
        });

        let read_prefix_len_hint = builder.get_current_pk_len();

        builder.build(
            self.logical.distribution().dist_column_indices().to_vec(),
            read_prefix_len_hint,
        )
    }
}

impl fmt::Display for StreamDedup {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        assert!(self.base.append_only());
        self.logical.fmt_with_name(f, "StreamAppendOnlyDedup")
    }
}

impl PlanTreeNodeUnary for StreamDedup {
    fn input(&self) -> PlanRef {
        self.logical.input()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(self.logical.clone_with_input(input))
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
                .dedup_cols()
                .iter()
                .map(|idx| *idx as _)
                .collect_vec(),
        })
    }
}

impl ExprRewritable for StreamDedup {}
