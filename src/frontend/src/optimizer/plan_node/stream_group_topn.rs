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
use itertools::Itertools;
use risingwave_common::catalog::FieldDisplay;
use risingwave_pb::stream_plan::stream_node::PbNodeBody;

use super::generic::Limit;
use super::{generic, ExprRewritable, PlanBase, PlanTreeNodeUnary, StreamNode};
use crate::optimizer::property::{Order, OrderDisplay};
use crate::stream_fragmenter::BuildFragmentGraphState;
use crate::PlanRef;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamGroupTopN {
    pub base: PlanBase,
    logical: generic::TopN<PlanRef>,
    /// an optional column index which is the vnode of each row computed by the input's consistent
    /// hash distribution
    vnode_col_idx: Option<usize>,
}

impl StreamGroupTopN {
    pub fn new(logical: generic::TopN<PlanRef>, vnode_col_idx: Option<usize>) -> Self {
        assert!(!logical.group_key.is_empty());
        assert!(logical.limit_attr.limit() > 0);
        let input = &logical.input;
        let schema = input.schema().clone();

        let watermark_columns = if input.append_only() {
            input.watermark_columns().clone()
        } else {
            let mut watermark_columns = FixedBitSet::with_capacity(schema.len());
            for &idx in &logical.group_key {
                if input.watermark_columns().contains(idx) {
                    watermark_columns.insert(idx);
                }
            }
            watermark_columns
        };

        // We can use the group key as the stream key when limit is 1, since there is only one
        // record for each value of group key.
        let logical_pk = if logical.limit == 1 {
            logical.group_key.clone()
        } else {
            input.logical_pk().to_vec()
        };

        let base = PlanBase::new_stream(
            input.ctx(),
            schema,
            logical_pk,
            input.functional_dependency().clone(),
            input.distribution().clone(),
            false,
            watermark_columns,
        );
        StreamGroupTopN {
            base,
            logical,
            vnode_col_idx,
        }
    }

    pub fn limit_attr(&self) -> Limit {
        self.logical.limit_attr
    }

    pub fn offset(&self) -> u64 {
        self.logical.offset
    }

    pub fn topn_order(&self) -> &Order {
        &self.logical.order
    }

    pub fn group_key(&self) -> &[usize] {
        &self.logical.group_key
    }
}

impl StreamNode for StreamGroupTopN {
    fn to_stream_prost_body(&self, state: &mut BuildFragmentGraphState) -> PbNodeBody {
        use risingwave_pb::stream_plan::*;

        let input = self.input();
        let table = self
            .logical
            .infer_internal_table_catalog(
                input.schema(),
                input.ctx(),
                input.logical_pk(),
                self.vnode_col_idx,
            )
            .with_id(state.gen_table_id_wrapped());
        assert!(!self.group_key().is_empty());
        let group_topn_node = GroupTopNNode {
            limit: self.limit_attr().limit(),
            offset: self.offset(),
            with_ties: self.limit_attr().with_ties(),
            group_key: self.group_key().iter().map(|idx| *idx as u32).collect(),
            table: Some(table.to_internal_table_prost()),
            order_by: self.topn_order().to_protobuf(),
        };
        if self.input().append_only() {
            PbNodeBody::AppendOnlyGroupTopN(group_topn_node)
        } else {
            PbNodeBody::GroupTopN(group_topn_node)
        }
    }
}

impl fmt::Display for StreamGroupTopN {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut builder = f.debug_struct(if self.input().append_only() {
            "StreamAppendOnlyGroupTopN"
        } else {
            "StreamGroupTopN"
        });
        let input = self.input();
        let input_schema = input.schema();
        builder.field(
            "order",
            &format!(
                "{}",
                OrderDisplay {
                    order: self.topn_order(),
                    input_schema
                }
            ),
        );
        builder
            .field("limit", &self.limit_attr().limit())
            .field("offset", &self.offset())
            .field("group_key", &self.group_key());
        if self.limit_attr().with_ties() {
            builder.field("with_ties", &true);
        }
        let watermark_columns = &self.base.watermark_columns;
        if self.base.watermark_columns.count_ones(..) > 0 {
            let schema = self.schema();
            builder.field(
                "output_watermarks",
                &watermark_columns
                    .ones()
                    .map(|idx| FieldDisplay(schema.fields.get(idx).unwrap()))
                    .collect_vec(),
            );
        };

        builder.finish()
    }
}

impl_plan_tree_node_for_unary! { StreamGroupTopN }

impl PlanTreeNodeUnary for StreamGroupTopN {
    fn input(&self) -> PlanRef {
        self.logical.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        let mut logical = self.logical.clone();
        logical.input = input;
        Self::new(logical, self.vnode_col_idx)
    }
}

impl ExprRewritable for StreamGroupTopN {}
