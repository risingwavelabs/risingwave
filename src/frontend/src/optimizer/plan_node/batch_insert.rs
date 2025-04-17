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

use pretty_xmlish::XmlNode;
use risingwave_pb::batch_plan::InsertNode;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::plan_common::{DefaultColumns, IndexAndExpr};

use super::batch::prelude::*;
use super::utils::{Distill, childless_record};
use super::{ExprRewritable, PlanRef, PlanTreeNodeUnary, ToBatchPb, ToDistributedBatch, generic};
use crate::error::Result;
use crate::expr::Expr;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::{PlanBase, ToLocalBatch, utils};
use crate::optimizer::plan_visitor::DistributedDmlVisitor;
use crate::optimizer::property::{Distribution, Order, RequiredDist};

/// `BatchInsert` implements [`super::LogicalInsert`]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BatchInsert {
    pub base: PlanBase<Batch>,
    pub core: generic::Insert<PlanRef>,
}

impl BatchInsert {
    pub fn new(core: generic::Insert<PlanRef>) -> Self {
        let base: PlanBase<Batch> =
            PlanBase::new_batch_with_core(&core, core.input.distribution().clone(), Order::any());

        BatchInsert { base, core }
    }
}

impl Distill for BatchInsert {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let vec = self
            .core
            .fields_pretty(self.base.ctx().is_explain_verbose());
        childless_record("BatchInsert", vec)
    }
}

impl PlanTreeNodeUnary for BatchInsert {
    fn input(&self) -> PlanRef {
        self.core.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        let mut core = self.core.clone();
        core.input = input;
        Self::new(core)
    }
}

impl_plan_tree_node_for_unary! { BatchInsert }

impl ToDistributedBatch for BatchInsert {
    fn to_distributed(&self) -> Result<PlanRef> {
        if DistributedDmlVisitor::dml_should_run_in_distributed(self.input()) {
            // Add an hash shuffle between the insert and its input.
            let new_input = RequiredDist::PhysicalDist(Distribution::HashShard(
                (0..self.input().schema().len()).collect(),
            ))
            .enforce_if_not_satisfies(self.input().to_distributed()?, &Order::any())?;
            let new_insert: PlanRef = self.clone_with_input(new_input).into();
            if self.core.returning {
                Ok(new_insert)
            } else {
                utils::sum_affected_row(new_insert)
            }
        } else {
            let new_input = RequiredDist::single()
                .enforce_if_not_satisfies(self.input().to_distributed()?, &Order::any())?;
            Ok(self.clone_with_input(new_input).into())
        }
    }
}

impl ToBatchPb for BatchInsert {
    fn to_batch_prost_body(&self) -> NodeBody {
        let column_indices = self.core.column_indices.iter().map(|&i| i as u32).collect();

        let default_columns = &self.core.default_columns;
        let has_default_columns = !default_columns.is_empty();
        let default_columns = DefaultColumns {
            default_columns: default_columns
                .iter()
                .map(|(i, expr)| IndexAndExpr {
                    index: *i as u32,
                    expr: Some(expr.to_expr_proto()),
                })
                .collect(),
        };
        NodeBody::Insert(InsertNode {
            table_id: self.core.table_id.table_id(),
            table_version_id: self.core.table_version_id,
            column_indices,
            default_columns: if has_default_columns {
                Some(default_columns)
            } else {
                None
            },
            row_id_index: self.core.row_id_index.map(|index| index as _),
            returning: self.core.returning,
            session_id: self.base.ctx().session_ctx().session_id().0 as u32,
        })
    }
}

impl ToLocalBatch for BatchInsert {
    fn to_local(&self) -> Result<PlanRef> {
        let new_input = RequiredDist::single()
            .enforce_if_not_satisfies(self.input().to_local()?, &Order::any())?;
        Ok(self.clone_with_input(new_input).into())
    }
}

impl ExprRewritable for BatchInsert {}

impl ExprVisitable for BatchInsert {}
