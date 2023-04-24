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

use fixedbitset::FixedBitSet;
use risingwave_common::util::sort_util::OrderType;
use risingwave_pb::stream_plan::stream_node::PbNodeBody;

use super::{generic, ExprRewritable, PlanBase, PlanRef, PlanTreeNodeUnary, StreamNode};
use crate::stream_fragmenter::BuildFragmentGraphState;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamEowcOverWindow {
    pub base: PlanBase,
    logical: generic::OverWindow<PlanRef>,
}

impl StreamEowcOverWindow {
    pub fn new(logical: generic::OverWindow<PlanRef>) -> Self {
        assert!(logical.funcs_have_same_partition_and_order());

        let input = &logical.input;
        assert!(input.append_only());

        // Should order by a single watermark column.
        let order_key = &logical.window_functions[0].order_by;
        assert_eq!(order_key.len(), 1);
        assert_eq!(order_key[0].order_type, OrderType::ascending());
        let order_key_idx = order_key[0].column_index;
        let input_watermark_cols = logical.input.watermark_columns();
        assert!(input_watermark_cols.contains(order_key_idx));

        // `EowcOverWindowExecutor` only maintains watermark on the order key column.
        let mut watermark_columns = FixedBitSet::with_capacity(logical.output_len());
        watermark_columns.insert(order_key_idx);

        let base = PlanBase::new_stream_with_logical(
            &logical,
            input.distribution().clone(),
            true,
            watermark_columns,
        );
        StreamEowcOverWindow { base, logical }
    }
}

impl fmt::Display for StreamEowcOverWindow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.logical.fmt_with_name(f, "StreamEowcOverWindow")
    }
}

impl PlanTreeNodeUnary for StreamEowcOverWindow {
    fn input(&self) -> PlanRef {
        self.logical.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        let mut logical = self.logical.clone();
        logical.input = input;
        Self::new(logical)
    }
}
impl_plan_tree_node_for_unary! { StreamEowcOverWindow }

impl StreamNode for StreamEowcOverWindow {
    fn to_stream_prost_body(&self, _state: &mut BuildFragmentGraphState) -> PbNodeBody {
        todo!() // TODO()
    }
}
impl ExprRewritable for StreamEowcOverWindow {}
