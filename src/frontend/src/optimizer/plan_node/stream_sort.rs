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
use std::fmt;

use fixedbitset::FixedBitSet;
use risingwave_common::util::sort_util::OrderType;
use risingwave_pb::stream_plan::stream_node::PbNodeBody;

use super::utils::TableCatalogBuilder;
use super::{ExprRewritable, PlanBase, PlanRef, PlanTreeNodeUnary, StreamNode};
use crate::stream_fragmenter::BuildFragmentGraphState;
use crate::TableCatalog;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamSort {
    pub base: PlanBase,

    input: PlanRef,
    sort_column_index: usize,
}

impl fmt::Display for StreamSort {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamSort")
            .field("sort_column_index", &self.sort_column_index)
            .finish()
    }
}

impl StreamSort {
    pub fn new(input: PlanRef, sort_column_index: usize) -> Self {
        assert!(input.watermark_columns().contains(sort_column_index));

        let schema = input.schema().clone();
        let logical_pk = input.logical_pk().to_vec();
        let fd_set = input.functional_dependency().clone();
        let dist = input.distribution().clone();
        let mut watermark_columns = FixedBitSet::with_capacity(input.schema().len());
        watermark_columns.insert(sort_column_index);
        let base = PlanBase::new_stream(
            input.ctx(),
            schema,
            logical_pk,
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
        for idx in self.input.logical_pk() {
            if !order_cols.contains(idx) {
                tbl_builder.add_order_column(*idx, OrderType::ascending());
                order_cols.insert(*idx);
            }
        }

        let in_dist_key = self.input.distribution().dist_column_indices();
        let read_prefix_len_hint = 0;
        tbl_builder.build(in_dist_key.to_vec(), read_prefix_len_hint)
    }
}

impl PlanTreeNodeUnary for StreamSort {
    fn input(&self) -> PlanRef {
        self.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(input, self.sort_column_index)
    }
}

impl_plan_tree_node_for_unary! { StreamSort }

impl StreamNode for StreamSort {
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

impl ExprRewritable for StreamSort {}
