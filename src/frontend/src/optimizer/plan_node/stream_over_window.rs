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

    fn window_functions(&self) -> &[PlanWindowFunction] {
        &self.logical.window_functions
    }

    fn partition_key_indices(&self) -> Vec<usize> {
        self.window_functions()[0]
            .partition_by
            .iter()
            .map(|i| i.index())
            .collect()
    }

    fn order_key(&self) -> &[ColumnOrder] {
        &self.window_functions()[0].order_by
    }

    fn infer_state_table(&self) -> TableCatalog {
        let mut tbl_builder =
            TableCatalogBuilder::new(self.ctx().with_options().internal_table_subset());

        let out_schema = self.logical.schema();
        for field in out_schema.fields() {
            tbl_builder.add_column(field);
        }

        let mut order_cols = HashSet::new();
        for idx in self.partition_key_indices() {
            if !order_cols.contains(&idx) {
                tbl_builder.add_order_column(idx, OrderType::ascending());
                order_cols.insert(idx);
            }
        }
        let read_prefix_len_hint = tbl_builder.get_current_pk_len();
        for o in self.order_key() {
            if !order_cols.contains(&o.column_index) {
                tbl_builder.add_order_column(o.column_index, o.order_type);
                order_cols.insert(o.column_index);
            }
        }
        for idx in self.logical.input.logical_pk() {
            if !order_cols.contains(idx) {
                tbl_builder.add_order_column(*idx, OrderType::ascending());
                order_cols.insert(*idx);
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
            .window_functions()
            .iter()
            .map(PlanWindowFunction::to_protobuf)
            .collect();
        let partition_by = self.window_functions()[0]
            .partition_by
            .iter()
            .map(|i| i.index() as _)
            .collect();
        let order_by = self.window_functions()[0]
            .order_by
            .iter()
            .map(|o| o.to_protobuf())
            .collect();
        let state_table = self
            .infer_state_table()
            .with_id(state.gen_table_id_wrapped())
            .to_internal_table_prost();

        PbNodeBody::OverWindow(OverWindowNode {
            calls,
            partition_by,
            order_by,
            state_table: Some(state_table),
        })
    }
}

impl ExprRewritable for StreamOverWindow {}
