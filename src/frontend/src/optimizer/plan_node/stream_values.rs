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
use risingwave_pb::stream_plan::stream_node::NodeBody as ProstStreamNode;
use risingwave_pb::stream_plan::values_node::ExprTuple;
use risingwave_pb::stream_plan::ValuesNode;

use super::{ExprRewritable, LogicalValues, PlanBase, StreamNode};
use crate::expr::{Expr, ExprImpl};
use crate::optimizer::property::Distribution;
use crate::stream_fragmenter::BuildFragmentGraphState;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamValues {
    pub base: PlanBase,
    logical: LogicalValues,
}

impl_plan_tree_node_for_leaf! { StreamValues }

impl StreamValues {
    pub fn new(logical: LogicalValues) -> Self {
        Self::with_dist(logical, Distribution::Single)
    }

    pub fn with_dist(logical: LogicalValues, dist: Distribution) -> Self {
        let ctx = logical.ctx();
        let mut watermark_columns = FixedBitSet::with_capacity(logical.schema().len());
        (0..(logical.schema().len()-1)).into_iter().for_each(|i| watermark_columns.set(i, true));
        let base = PlanBase::new_stream(
            ctx,
            logical.schema().clone(),
            logical.logical_pk().to_vec(),
            logical.functional_dependency().clone(),
            dist,
            false,
            watermark_columns,
        );
        Self { base, logical }
    }

    pub fn logical(&self) -> &LogicalValues {
        &self.logical
    }

    fn row_to_protobuf(&self, row: &[ExprImpl]) -> ExprTuple {
        let cells = row.iter().map(|x| x.to_expr_proto()).collect();
        ExprTuple { cells }
    }
}

impl fmt::Display for StreamValues {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamValues")
            .field("rows", &self.logical.rows())
            .field("schema", &self.logical.schema())
            .finish()
    }
}

impl StreamNode for StreamValues {
    fn to_stream_prost_body(&self, _state: &mut BuildFragmentGraphState) -> ProstStreamNode {
        ProstStreamNode::Values(ValuesNode {
            tuples: self
                .logical
                .rows()
                .iter()
                .map(|row| self.row_to_protobuf(row))
                .collect(),
            fields: self
                .logical
                .schema()
                .fields()
                .iter()
                .map(|f| f.to_prost())
                .collect(),
        })
    }
}

impl ExprRewritable for StreamValues {}
