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

use std::collections::HashSet;

use fixedbitset::FixedBitSet;
use pretty_xmlish::{Pretty, XmlNode};
use risingwave_common::catalog::FieldDisplay;
use risingwave_common::util::sort_util::OrderType;
use risingwave_pb::stream_plan::stream_node::PbNodeBody;

use super::utils::{childless_record, Distill, TableCatalogBuilder};
use super::{ExprRewritable, PlanBase, PlanRef, PlanTreeNodeUnary, StreamNode};
use crate::stream_fragmenter::BuildFragmentGraphState;
use crate::{Explain, TableCatalog};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamEowcSort {
    pub base: PlanBase,

    input: PlanRef,
    sort_column_index: usize,
}

impl Distill for StreamEowcSort {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let fields = vec![(
            "sort_column",
            Pretty::display(&FieldDisplay(&self.input.schema()[self.sort_column_index])),
        )];
        childless_record("StreamEowcSort", fields)
    }
}

impl StreamEowcSort {
    pub fn new(input: PlanRef, sort_column_index: usize) -> Self {
        assert!(input.watermark_columns().contains(sort_column_index));

        let schema = input.schema().clone();
        let stream_key = input.stream_key().map(|v| v.to_vec());
        let fd_set = input.functional_dependency().clone();
        let dist = input.distribution().clone();
        let mut watermark_columns = FixedBitSet::with_capacity(input.schema().len());
        watermark_columns.insert(sort_column_index);
        let base = PlanBase::new_stream(
            input.ctx(),
            schema,
            stream_key,
            fd_set,
            dist,
            true,
            true,
            watermark_columns,
        );
        Self {
            base,
            input,
            sort_column_index,
        }
    }

    fn infer_state_table(&self) -> TableCatalog {
        // The sort state table has the same schema as the input.

        let in_fields = self.input.schema().fields();
        let mut tbl_builder =
            TableCatalogBuilder::new(self.ctx().with_options().internal_table_subset());
        for field in in_fields {
            tbl_builder.add_column(field);
        }

        let mut order_cols = HashSet::new();
        tbl_builder.add_order_column(self.sort_column_index, OrderType::ascending());
        order_cols.insert(self.sort_column_index);

        let dist_key = self.base.dist.dist_column_indices().to_vec();
        for idx in &dist_key {
            if !order_cols.contains(idx) {
                tbl_builder.add_order_column(*idx, OrderType::ascending());
                order_cols.insert(*idx);
            }
        }

        for idx in self.input.expect_stream_key() {
            if !order_cols.contains(idx) {
                tbl_builder.add_order_column(*idx, OrderType::ascending());
                order_cols.insert(*idx);
            }
        }

        let read_prefix_len_hint = 0;
        tbl_builder.build(dist_key, read_prefix_len_hint)
    }
}

impl PlanTreeNodeUnary for StreamEowcSort {
    fn input(&self) -> PlanRef {
        self.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(input, self.sort_column_index)
    }
}

impl_plan_tree_node_for_unary! { StreamEowcSort }

impl StreamNode for StreamEowcSort {
    fn to_stream_prost_body(&self, state: &mut BuildFragmentGraphState) -> PbNodeBody {
        use risingwave_pb::stream_plan::*;
        PbNodeBody::Sort(SortNode {
            state_table: Some(
                self.infer_state_table()
                    .with_id(state.gen_table_id_wrapped())
                    .to_internal_table_prost(),
            ),
            sort_column_index: self.sort_column_index as _,
        })
    }
}

impl ExprRewritable for StreamEowcSort {}
