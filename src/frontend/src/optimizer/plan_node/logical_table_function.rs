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

use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::DataType;

use super::{
    ColPrunable, ExprRewritable, LogicalFilter, PlanBase, PlanRef, PredicatePushdown, ToBatch,
    ToStream,
};
use crate::expr::{Expr, ExprRewriter, TableFunction};
use crate::optimizer::optimizer_context::OptimizerContextRef;
use crate::optimizer::plan_node::{
    BatchTableFunction, ColumnPruningContext, PredicatePushdownContext, RewriteStreamContext,
    ToStreamContext,
};
use crate::optimizer::property::FunctionalDependencySet;
use crate::utils::{ColIndexMapping, Condition};

/// `LogicalGenerateSeries` implements Hop Table Function.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalTableFunction {
    pub base: PlanBase,
    pub table_function: TableFunction,
}

impl LogicalTableFunction {
    /// Create a [`LogicalTableFunction`] node. Used internally by optimizer.
    pub fn new(table_function: TableFunction, ctx: OptimizerContextRef) -> Self {
        let schema = if let DataType::Struct(s) = table_function.return_type() {
            Schema::from(&*s)
        } else {
            Schema {
                fields: vec![Field::with_name(
                    table_function.return_type(),
                    table_function.name(),
                )],
            }
        };
        let functional_dependency = FunctionalDependencySet::new(schema.len());
        let base = PlanBase::new_logical(ctx, schema, vec![], functional_dependency);
        Self {
            base,
            table_function,
        }
    }
}

impl_plan_tree_node_for_leaf! { LogicalTableFunction }

impl fmt::Display for LogicalTableFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LogicalTableFunction {{ {:?} }}", self.table_function)
    }
}

// the leaf node don't need colprunable
impl ColPrunable for LogicalTableFunction {
    fn prune_col(&self, required_cols: &[usize], _ctx: &mut ColumnPruningContext) -> PlanRef {
        let _ = required_cols;
        self.clone().into()
    }
}

impl ExprRewritable for LogicalTableFunction {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef {
        let mut new = self.clone();
        new.table_function.args = new
            .table_function
            .args
            .into_iter()
            .map(|e| r.rewrite_expr(e))
            .collect();
        new.base = self.base.clone_with_new_plan_id();
        new.into()
    }
}

impl PredicatePushdown for LogicalTableFunction {
    fn predicate_pushdown(
        &self,
        predicate: Condition,
        _ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        LogicalFilter::create(self.clone().into(), predicate)
    }
}

impl ToBatch for LogicalTableFunction {
    fn to_batch(&self) -> Result<PlanRef> {
        Ok(BatchTableFunction::new(self.clone()).into())
    }
}

impl ToStream for LogicalTableFunction {
    fn to_stream(&self, _ctx: &mut ToStreamContext) -> Result<PlanRef> {
        Err(
            ErrorCode::NotImplemented("LogicalTableFunction::to_stream".to_string(), None.into())
                .into(),
        )
    }

    fn logical_rewrite_for_stream(
        &self,
        _ctx: &mut RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        Err(ErrorCode::NotImplemented(
            "LogicalTableFunction::logical_rewrite_for_stream".to_string(),
            None.into(),
        )
        .into())
    }
}
