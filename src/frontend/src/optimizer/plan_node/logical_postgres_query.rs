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
use risingwave_common::bail;
use risingwave_common::catalog::Schema;

use super::generic::GenericPlanRef;
use super::utils::{Distill, childless_record};
use super::{
    BatchPostgresQuery, ColPrunable, ExprRewritable, Logical, LogicalProject, PlanBase, PlanRef,
    PredicatePushdown, ToBatch, ToStream, generic,
};
use crate::OptimizerContextRef;
use crate::error::Result;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::utils::column_names_pretty;
use crate::optimizer::plan_node::{
    ColumnPruningContext, LogicalFilter, PredicatePushdownContext, RewriteStreamContext,
    ToStreamContext,
};
use crate::utils::{ColIndexMapping, Condition};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalPostgresQuery {
    pub base: PlanBase<Logical>,
    pub core: generic::PostgresQuery,
}

impl LogicalPostgresQuery {
    pub fn new(
        ctx: OptimizerContextRef,
        schema: Schema,
        hostname: String,
        port: String,
        username: String,
        password: String,
        database: String,
        query: String,
    ) -> Self {
        let core = generic::PostgresQuery {
            schema,
            hostname,
            port,
            username,
            password,
            database,
            query,
            ctx,
        };

        let base = PlanBase::new_logical_with_core(&core);

        LogicalPostgresQuery { base, core }
    }
}

impl_plan_tree_node_for_leaf! {LogicalPostgresQuery}
impl Distill for LogicalPostgresQuery {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let fields = vec![("columns", column_names_pretty(self.schema()))];
        childless_record("LogicalPostgresQuery", fields)
    }
}

impl ColPrunable for LogicalPostgresQuery {
    fn prune_col(&self, required_cols: &[usize], _ctx: &mut ColumnPruningContext) -> PlanRef {
        LogicalProject::with_out_col_idx(self.clone().into(), required_cols.iter().cloned()).into()
    }
}

impl ExprRewritable for LogicalPostgresQuery {}

impl ExprVisitable for LogicalPostgresQuery {}

impl PredicatePushdown for LogicalPostgresQuery {
    fn predicate_pushdown(
        &self,
        predicate: Condition,
        _ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        // No pushdown.
        LogicalFilter::create(self.clone().into(), predicate)
    }
}

impl ToBatch for LogicalPostgresQuery {
    fn to_batch(&self) -> Result<PlanRef> {
        Ok(BatchPostgresQuery::new(self.core.clone()).into())
    }
}

impl ToStream for LogicalPostgresQuery {
    fn to_stream(&self, _ctx: &mut ToStreamContext) -> Result<PlanRef> {
        bail!("postgres_query function is not supported in streaming mode")
    }

    fn logical_rewrite_for_stream(
        &self,
        _ctx: &mut RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        bail!("postgres_query function is not supported in streaming mode")
    }
}
