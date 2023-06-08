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

use risingwave_common::error::Result;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::batch_plan::InsertNode;
use risingwave_pb::plan_common::{DefaultColumns, IndexAndExpr};

use super::{
    generic, ExprRewritable, PlanRef, PlanTreeNodeUnary, ToBatchPb,
    ToDistributedBatch,
};
use crate::expr::Expr;
use crate::optimizer::plan_node::{PlanBase, ToLocalBatch};
use crate::optimizer::property::{Distribution, Order, RequiredDist};

/// `BatchInsert` implements [`LogicalInsert`]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BatchInsert {
    pub base: PlanBase,
    pub logical: generic::Insert<PlanRef>,
}

impl BatchInsert {
    pub fn new(logical: generic::Insert<PlanRef>) -> Self {
        // TODO: derive from input
        let base = PlanBase::new_batch(
            logical.ctx(),
            logical.schema().clone(),
            Distribution::Single,
            Order::any(),
        );
        BatchInsert { base, logical }
    }
}

impl fmt::Display for BatchInsert {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.logical.fmt_with_name(f, "BatchInsert")
    }
}
// impl_distill_by_unit!(BatchInsert, logical, "BatchInsert");

impl PlanTreeNodeUnary for BatchInsert {
    fn input(&self) -> PlanRef {
        self.logical.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        let mut logical = self.logical.clone();
        logical.input = input;
        Self::new(logical)
    }
}

impl_plan_tree_node_for_unary! { BatchInsert }

impl ToDistributedBatch for BatchInsert {
    fn to_distributed(&self) -> Result<PlanRef> {
        let new_input = RequiredDist::single()
            .enforce_if_not_satisfies(self.input().to_distributed()?, &Order::any())?;
        Ok(self.clone_with_input(new_input).into())
    }
}

impl ToBatchPb for BatchInsert {
    fn to_batch_prost_body(&self) -> NodeBody {
        let column_indices = self
            .logical
            .column_indices
            .iter()
            .map(|&i| i as u32)
            .collect();

        let default_columns = &self.logical.default_columns;
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
            table_id: self.logical.table_id.table_id(),
            table_version_id: self.logical.table_version_id,
            column_indices,
            default_columns: if has_default_columns {
                Some(default_columns)
            } else {
                None
            },
            row_id_index: self.logical.row_id_index.map(|index| index as _),
            returning: self.logical.returning,
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
