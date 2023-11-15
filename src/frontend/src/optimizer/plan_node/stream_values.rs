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

use fixedbitset::FixedBitSet;
use pretty_xmlish::XmlNode;
use risingwave_pb::stream_plan::stream_node::NodeBody as ProstStreamNode;
use risingwave_pb::stream_plan::values_node::ExprTuple;
use risingwave_pb::stream_plan::ValuesNode;

use super::stream::prelude::*;
use super::utils::{childless_record, Distill};
use super::{ExprRewritable, LogicalValues, PlanBase, StreamNode};
use crate::expr::{Expr, ExprImpl};
use crate::optimizer::property::Distribution;
use crate::stream_fragmenter::BuildFragmentGraphState;

/// `StreamValues` implements `LogicalValues.to_stream()`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamValues {
    pub base: PlanBase<Stream>,
    logical: LogicalValues,
}

impl_plan_tree_node_for_leaf! { StreamValues }

impl StreamValues {
    /// `StreamValues` should enforce `Distribution::Single`
    pub fn new(logical: LogicalValues) -> Self {
        let ctx = logical.ctx();
        let base = PlanBase::new_stream(
            ctx,
            logical.schema().clone(),
            logical.stream_key().map(|v| v.to_vec()),
            logical.functional_dependency().clone(),
            Distribution::Single,
            true,
            false,
            FixedBitSet::with_capacity(logical.schema().len()),
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

impl Distill for StreamValues {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let data = self.logical.rows_pretty();
        childless_record("StreamValues", vec![("rows", data)])
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

impl ExprRewritable for StreamValues {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn crate::expr::ExprRewriter) -> crate::PlanRef {
        Self::new(
            self.logical
                .rewrite_exprs(r)
                .as_logical_values()
                .unwrap()
                .clone(),
        )
        .into()
    }
}
