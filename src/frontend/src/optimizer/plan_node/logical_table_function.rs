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
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::types::DataType;

use super::utils::{Distill, childless_record};
use super::{
    ColPrunable, ExprRewritable, Logical, LogicalFilter, LogicalProject, PlanBase, PlanRef,
    PredicatePushdown, ToBatch, ToStream,
};
use crate::error::Result;
use crate::expr::{Expr, ExprRewriter, ExprVisitor, TableFunction};
use crate::optimizer::optimizer_context::OptimizerContextRef;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::{
    ColumnPruningContext, PredicatePushdownContext, RewriteStreamContext, ToStreamContext,
};
use crate::optimizer::property::FunctionalDependencySet;
use crate::utils::{ColIndexMapping, Condition};

/// `LogicalTableFunction` is a scalar/table function used as a relation (in the `FROM` clause).
///
/// If the function returns a struct, it will be flattened into multiple columns.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalTableFunction {
    pub base: PlanBase<Logical>,
    pub table_function: TableFunction,
    pub with_ordinality: bool,
}

impl LogicalTableFunction {
    /// Create a [`LogicalTableFunction`] node. Used internally by optimizer.
    pub fn new(
        table_function: TableFunction,
        with_ordinality: bool,
        ctx: OptimizerContextRef,
    ) -> Self {
        let mut schema = if let DataType::Struct(s) = table_function.return_type() {
            // If the function returns a struct, it will be flattened into multiple columns.
            Schema::from(&s)
        } else {
            Schema {
                fields: vec![Field::with_name(
                    table_function.return_type(),
                    table_function.name(),
                )],
            }
        };
        if with_ordinality {
            schema
                .fields
                .push(Field::with_name(DataType::Int64, "ordinality"));
        }
        let functional_dependency = FunctionalDependencySet::new(schema.len());
        let base = PlanBase::new_logical(ctx, schema, None, functional_dependency);
        Self {
            base,
            table_function,
            with_ordinality,
        }
    }

    pub fn table_function(&self) -> &TableFunction {
        &self.table_function
    }
}

impl_plan_tree_node_for_leaf! { LogicalTableFunction }

impl Distill for LogicalTableFunction {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let data = Pretty::debug(&self.table_function);
        childless_record("LogicalTableFunction", vec![("table_function", data)])
    }
}

impl ColPrunable for LogicalTableFunction {
    fn prune_col(&self, required_cols: &[usize], _ctx: &mut ColumnPruningContext) -> PlanRef {
        // No pruning.
        LogicalProject::with_out_col_idx(self.clone().into(), required_cols.iter().copied()).into()
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

impl ExprVisitable for LogicalTableFunction {
    fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        self.table_function
            .args
            .iter()
            .for_each(|e| v.visit_expr(e));
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
        unreachable!("TableFunction should be converted to ProjectSet")
    }
}

impl ToStream for LogicalTableFunction {
    fn to_stream(&self, _ctx: &mut ToStreamContext) -> Result<PlanRef> {
        unreachable!("TableFunction should be converted to ProjectSet")
    }

    fn logical_rewrite_for_stream(
        &self,
        _ctx: &mut RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        unreachable!("TableFunction should be converted to ProjectSet")
    }
}
