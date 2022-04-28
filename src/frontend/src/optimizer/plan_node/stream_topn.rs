// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt;

use risingwave_pb::stream_plan::stream_node::Node as ProstStreamNode;

use super::{LogicalTopN, PlanBase, PlanRef, PlanTreeNodeUnary, ToStreamProst};
use crate::optimizer::property::{Distribution, FieldOrder, Order};

/// `StreamTopN` implements [`super::LogicalTopN`] to find the top N elements with a heap
#[derive(Debug, Clone)]
pub struct StreamTopN {
    pub base: PlanBase,
    logical: LogicalTopN,
}

impl StreamTopN {
    pub fn new(logical: LogicalTopN) -> Self {
        let ctx = logical.base.ctx.clone();
        let dist = match logical.input().distribution() {
            Distribution::Any => Distribution::Any,
            Distribution::Single => Distribution::Single,
            _ => panic!(),
        };

        // TODO: This is ported from the legacy Java code. Refactor to use input's PK as TopN's PK
        let (pk_indices, extended_order) =
            Self::derive_pk_and_order(logical.input().pk_indices(), logical.topn_order());
        let logical = LogicalTopN::new(
            logical.input(),
            logical.limit(),
            logical.offset(),
            extended_order,
        );
        let base = PlanBase::new_stream(ctx, logical.schema().clone(), pk_indices, dist, false);
        StreamTopN { base, logical }
    }

    fn derive_pk_and_order(input_pk: &[usize], order: &Order) -> (Vec<usize>, Order) {
        let mut output_pk = vec![];
        let mut extended_order = vec![];
        for field_order in &order.field_order {
            output_pk.push(field_order.index);
            extended_order.push(field_order.clone());
        }
        for col_index in input_pk {
            if !output_pk.contains(col_index) {
                output_pk.push(*col_index);
                extended_order.push(FieldOrder::ascending(*col_index));
            }
        }
        (output_pk, Order::new(extended_order))
    }
}

impl fmt::Display for StreamTopN {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "StreamTopN {{ order: {}, limit: {}, offset: {} }}",
            self.logical.topn_order(),
            self.logical.limit(),
            self.logical.offset(),
        )
    }
}

impl PlanTreeNodeUnary for StreamTopN {
    fn input(&self) -> PlanRef {
        self.logical.input()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(self.logical.clone_with_input(input))
    }
}

impl_plan_tree_node_for_unary! { StreamTopN }

impl ToStreamProst for StreamTopN {
    fn to_stream_prost_body(&self) -> ProstStreamNode {
        use risingwave_pb::stream_plan::*;
        let order_types = self
            .logical
            .topn_order()
            .field_order
            .iter()
            .map(|f| f.direct.to_protobuf() as i32)
            .collect();
        ProstStreamNode::TopNNode(TopNNode {
            order_types,
            limit: self.logical.limit() as u64,
            offset: self.logical.offset() as u64,
            distribution_keys: vec![], // TODO: seems unnecessary
        })
    }
}
