// Copyright 2025 RisingWave Labs
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
use risingwave_pb::batch_plan::TableFunctionNode;
use risingwave_pb::batch_plan::plan_node::NodeBody;

use super::batch::prelude::*;
use super::utils::{Distill, childless_record};
use super::{ExprRewritable, PlanBase, PlanRef, PlanTreeNodeLeaf, ToBatchPb, ToDistributedBatch};
use crate::error::Result;
use crate::expr::{ExprRewriter, ExprVisitor};
use crate::optimizer::plan_node::ToLocalBatch;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::logical_table_function::LogicalTableFunction;
use crate::optimizer::property::{Distribution, Order};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BatchTableFunction {
    pub base: PlanBase<Batch>,
    logical: LogicalTableFunction,
}

impl PlanTreeNodeLeaf for BatchTableFunction {}
impl_plan_tree_node_for_leaf!(BatchTableFunction);

impl BatchTableFunction {
    pub fn new(logical: LogicalTableFunction) -> Self {
        Self::with_dist(logical, Distribution::Single)
    }

    pub fn with_dist(logical: LogicalTableFunction, dist: Distribution) -> Self {
        let ctx = logical.base.ctx().clone();
        let base = PlanBase::new_batch(ctx, logical.schema().clone(), dist, Order::any());
        BatchTableFunction { base, logical }
    }

    #[must_use]
    pub fn logical(&self) -> &LogicalTableFunction {
        &self.logical
    }
}

impl Distill for BatchTableFunction {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let data = Pretty::debug(&self.logical.table_function);
        childless_record("BatchTableFunction", vec![("table_function", data)])
    }
}

impl ToDistributedBatch for BatchTableFunction {
    fn to_distributed(&self) -> Result<PlanRef> {
        Ok(Self::with_dist(self.logical().clone(), Distribution::Single).into())
    }
}

impl ToBatchPb for BatchTableFunction {
    fn to_batch_prost_body(&self) -> NodeBody {
        NodeBody::TableFunction(TableFunctionNode {
            table_function: Some(self.logical.table_function.to_protobuf()),
        })
    }
}

impl ToLocalBatch for BatchTableFunction {
    fn to_local(&self) -> Result<PlanRef> {
        Ok(Self::with_dist(self.logical().clone(), Distribution::Single).into())
    }
}

impl ExprRewritable for BatchTableFunction {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef {
        Self::new(
            self.logical
                .rewrite_exprs(r)
                .as_logical_table_function()
                .unwrap()
                .clone(),
        )
        .into()
    }
}

impl ExprVisitable for BatchTableFunction {
    fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        self.logical
            .table_function
            .args
            .iter()
            .for_each(|e| v.visit_expr(e));
    }
}
