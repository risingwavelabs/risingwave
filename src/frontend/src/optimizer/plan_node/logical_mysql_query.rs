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
    BatchMySqlQuery, ColPrunable, ExprRewritable, Logical, LogicalProject, PlanBase, PlanRef,
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
pub struct LogicalMySqlQuery {
    pub base: PlanBase<Logical>,
    pub core: generic::MySqlQuery,
}

impl LogicalMySqlQuery {
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
        let core = generic::MySqlQuery {
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

        LogicalMySqlQuery { base, core }
    }
}

impl_plan_tree_node_for_leaf! {LogicalMySqlQuery}
impl Distill for LogicalMySqlQuery {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let fields = vec![("columns", column_names_pretty(self.schema()))];
        childless_record("LogicalMySqlQuery", fields)
    }
}

impl ColPrunable for LogicalMySqlQuery {
    fn prune_col(&self, required_cols: &[usize], _ctx: &mut ColumnPruningContext) -> PlanRef {
        LogicalProject::with_out_col_idx(self.clone().into(), required_cols.iter().cloned()).into()
    }
}

impl ExprRewritable for LogicalMySqlQuery {}

impl ExprVisitable for LogicalMySqlQuery {}

impl PredicatePushdown for LogicalMySqlQuery {
    fn predicate_pushdown(
        &self,
        predicate: Condition,
        _ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        // No pushdown.
        LogicalFilter::create(self.clone().into(), predicate)
    }
}

impl ToBatch for LogicalMySqlQuery {
    fn to_batch(&self) -> Result<PlanRef> {
        Ok(BatchMySqlQuery::new(self.core.clone()).into())
    }
}

impl ToStream for LogicalMySqlQuery {
    fn to_stream(&self, _ctx: &mut ToStreamContext) -> Result<PlanRef> {
        bail!("mysql_query function is not supported in streaming mode")
    }

    fn logical_rewrite_for_stream(
        &self,
        _ctx: &mut RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        bail!("mysql_query function is not supported in streaming mode")
    }
}
