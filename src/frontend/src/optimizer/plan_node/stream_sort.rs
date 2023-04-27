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
use risingwave_pb::stream_plan::stream_node::PbNodeBody;

use super::generic::Sort;
use super::{ExprRewritable, PlanBase, PlanRef, PlanTreeNodeUnary, StreamNode};
use crate::stream_fragmenter::BuildFragmentGraphState;
use crate::TableCatalog;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamSort {
    pub base: PlanBase,
    logical: Sort<PlanRef>,
}

impl fmt::Display for StreamSort {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.logical.fmt_with_name(f, "StreamSort")
    }
}

impl StreamSort {
    pub fn new(logical: Sort<PlanRef>) -> Self {
        assert!(logical
            .input
            .watermark_columns()
            .contains(logical.sort_column_index));

        let dist = logical.input.distribution().clone();
        let mut watermark_columns = FixedBitSet::with_capacity(logical.input.schema().len());
        watermark_columns.insert(logical.sort_column_index);
        let base = PlanBase::new_stream_with_logical(&logical, dist, true, watermark_columns);
        Self { base, logical }
    }

    fn infer_state_table(&self) -> TableCatalog {
        todo!()
    }
}

impl PlanTreeNodeUnary for StreamSort {
    fn input(&self) -> PlanRef {
        self.logical.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        let mut logical = self.logical.clone();
        logical.input = input;
        Self::new(logical)
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
            sort_column_index: self.logical.sort_column_index as _,
        })
    }
}

impl ExprRewritable for StreamSort {}
