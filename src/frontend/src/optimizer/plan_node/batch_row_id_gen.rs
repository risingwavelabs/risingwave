// Copyright 2024 RisingWave Labs
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

use pretty_xmlish::{Pretty, XmlNode};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::types::DataType;
use risingwave_common::util::row_id;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::batch_plan::BatchRowIdGenNode;

use super::batch::prelude::*;
use super::utils::{childless_record, Distill};
use super::{generic, PlanBase, PlanRef, PlanTreeNodeUnary, ToDistributedBatch};
use crate::error::Result;
use crate::expr::ExprVisitor;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::{ToBatchPb, ToLocalBatch};
use crate::optimizer::property::{Distribution, Order};

/// `BatchRowIdGen` generates row IDs for each row in the input.
/// The row ID is guranteed to be unique within a executor instead of globally.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BatchRowIdGen {
    pub base: PlanBase<Batch>,
    input: PlanRef,
    row_id_index: usize,
}

impl BatchRowIdGen {
    pub fn new(input: PlanRef) -> Self {
        let ctx = input.ctx();
        let mut fields = input.schema().fields().to_vec();
        // add row_id field
        // Consider using a Serial type instead of Int64
        fields.push(Field::unnamed(DataType::Int64));
        let row_id_index = fields.len() - 1;
        let schema = Schema::new(fields);
        let dist = input.distribution().clone();
        let base = PlanBase::new_batch(ctx, schema, dist, Order::default());
        Self {
            base,
            input,
            row_id_index,
        }
    }
}

impl PlanTreeNodeUnary for BatchRowIdGen {
    fn input(&self) -> PlanRef {
        self.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(input)
    }
}

impl_plan_tree_node_for_unary! { BatchRowIdGen }

impl ToBatchPb for BatchRowIdGen {
    fn to_batch_prost_body(&self) -> NodeBody {
        let pb_node = BatchRowIdGenNode {
            input: Some(input),
            row_id_index: self.row_id_index as u32,
        };
        Ok(NodeBody::BatchRowIdGen(pb_node))
    }
}

impl ToLocalBatch for BatchRowIdGen {
    fn to_local(&self) -> Result<PlanRef> {
        let new_input = self.input.to_local()?;
        Ok(self.clone_with_input(new_input).into())
    }
}

impl ToDistributedBatch for BatchRowIdGen {
    fn to_distributed(&self) -> Result<PlanRef> {
        let new_input = self.input().to_distributed()?;
        Ok(self.clone_with_input(new_input).into())
    }
}

impl ExprVisitable for BatchRowIdGen {}

impl Distill for BatchRowIdGen {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let fields = vec![("row_id_index", Pretty::debug(&self.row_id_index))];
        childless_record("BatchRowIdGen", fields)
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::optimizer::plan_node::expr_visitable::ExprVisitor;
//     use crate::optimizer::property::Distribution;
//     use crate::test_common::assert_eq_xml;

//     #[test]
//     fn test_row_id_gen() {
