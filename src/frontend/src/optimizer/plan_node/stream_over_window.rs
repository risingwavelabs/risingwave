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
use risingwave_common::util::sort_util::{ColumnOrder, OrderType};
use risingwave_pb::stream_plan::stream_node::PbNodeBody;

use super::generic::{GenericPlanNode, PlanWindowFunction};
use super::utils::{impl_distill_by_unit, TableCatalogBuilder};
use super::{generic, ExprRewritable, PlanBase, PlanRef, PlanTreeNodeUnary, StreamNode};
use crate::stream_fragmenter::BuildFragmentGraphState;
use crate::TableCatalog;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamOverWindow {
    pub base: PlanBase,
    logical: generic::OverWindow<PlanRef>,
}

impl StreamOverWindow {
    pub fn new(logical: generic::OverWindow<PlanRef>) -> Self {
        assert!(logical.funcs_have_same_partition_and_order());

        let input = &logical.input;
        let watermark_columns = FixedBitSet::with_capacity(logical.output_len());

        let base = PlanBase::new_stream_with_logical(
            &logical,
            input.distribution().clone(),
            false, // general over window cannot be append-only
            false,
            watermark_columns,
        );
        StreamOverWindow { base, logical }
    }

    fn infer_state_table(&self) -> TableCatalog {
        let mut tbl_builder =
            TableCatalogBuilder::new(self.ctx().with_options().internal_table_subset());

        let out_schema = self.logical.schema();
        for field in out_schema.fields() {
            tbl_builder.add_column(field);
        }

        let mut order_cols = HashSet::new();
        for idx in self.logical.partition_key_indices() {
            if order_cols.insert(idx) {
                tbl_builder.add_order_column(idx, OrderType::ascending());
            }
        }
        let read_prefix_len_hint = tbl_builder.get_current_pk_len();
        for o in self.logical.order_key() {
            if order_cols.insert(o.column_index) {
                tbl_builder.add_order_column(o.column_index, o.order_type);
            }
        }
        for &idx in self.logical.input.expect_stream_key() {
            if order_cols.insert(idx) {
                tbl_builder.add_order_column(idx, OrderType::ascending());
            }
        }

        let in_dist_key = self.logical.input.distribution().dist_column_indices();
        tbl_builder.build(in_dist_key.to_vec(), read_prefix_len_hint)
    }
}

impl_distill_by_unit!(StreamOverWindow, logical, "StreamOverWindow");

impl PlanTreeNodeUnary for StreamOverWindow {
    fn input(&self) -> PlanRef {
        self.logical.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        let mut logical = self.logical.clone();
        logical.input = input;
        Self::new(logical)
    }
}
impl_plan_tree_node_for_unary! { StreamOverWindow }

impl StreamNode for StreamOverWindow {
    fn to_stream_prost_body(&self, state: &mut BuildFragmentGraphState) -> PbNodeBody {
        use risingwave_pb::stream_plan::*;

        let calls = self
            .logical
            .window_functions()
            .iter()
            .map(PlanWindowFunction::to_protobuf)
            .collect();
        let partition_by = self
            .logical
            .partition_key_indices()
            .into_iter()
            .map(|idx| idx as _)
            .collect();
        let order_by = self
            .logical
            .order_key()
            .iter()
            .map(ColumnOrder::to_protobuf)
            .collect();
        let state_table = self
            .infer_state_table()
            .with_id(state.gen_table_id_wrapped())
            .to_internal_table_prost();
        let cache_policy = self
            .base
            .ctx
            .session_ctx()
            .config()
            .get_streaming_over_window_cache_policy();

        PbNodeBody::OverWindow(OverWindowNode {
            calls,
            partition_by,
            order_by,
            state_table: Some(state_table),
            cache_policy: cache_policy.to_protobuf() as _,
        })
    }
}

impl ExprRewritable for StreamOverWindow {}
