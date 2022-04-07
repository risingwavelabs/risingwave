// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::rc::Rc;

use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::DataType;

use crate::binder::{
    BoundBaseTable, BoundJoin, BoundSource, BoundWindowTableFunction, Relation,
    WindowTableFunctionKind,
};
use crate::expr::{ExprImpl, ExprType, FunctionCall, InputRef};
use crate::optimizer::plan_node::{
    LogicalJoin, LogicalProject, LogicalScan, LogicalSource, PlanRef,
};
use crate::planner::Planner;

impl Planner {
    pub(super) fn plan_relation(&mut self, relation: Relation) -> Result<PlanRef> {
        match relation {
            Relation::BaseTable(t) => self.plan_base_table(*t),
            // TODO: order is ignored in the subquery
            Relation::Subquery(q) => Ok(self.plan_query(q.query)?.as_subplan()),
            Relation::Join(join) => self.plan_join(*join),
            Relation::WindowTableFunction(tf) => self.plan_window_table_function(*tf),
            Relation::Source(s) => self.plan_source(*s),
        }
    }

    pub(super) fn plan_base_table(&mut self, base_table: BoundBaseTable) -> Result<PlanRef> {
        LogicalScan::create(
            base_table.name,
            Rc::new(base_table.table_catalog.table_desc()),
            self.ctx(),
        )
    }

    pub(super) fn plan_source(&mut self, source: BoundSource) -> Result<PlanRef> {
        Ok(LogicalSource::new(Rc::new(source.catalog), self.ctx()).into())
    }

    pub(super) fn plan_join(&mut self, join: BoundJoin) -> Result<PlanRef> {
        let left = self.plan_relation(join.left)?;
        let right = self.plan_relation(join.right)?;
        let join_type = join.join_type;
        let on_clause = join.cond;
        Ok(LogicalJoin::create(left, right, join_type, on_clause))
    }

    pub(super) fn plan_window_table_function(
        &mut self,
        table_function: BoundWindowTableFunction,
    ) -> Result<PlanRef> {
        use WindowTableFunctionKind::*;
        match table_function.kind {
            Tumble => self.plan_tumble_window(
                table_function.input,
                table_function.time_col,
                table_function.args,
            ),
            Hop => Err(ErrorCode::NotImplemented(
                "HOP window function is not implemented yet".to_string(),
                1191.into(),
            )
            .into()),
        }
    }

    fn plan_tumble_window(
        &mut self,
        input: Relation,
        time_col: InputRef,
        args: Vec<ExprImpl>,
    ) -> Result<PlanRef> {
        let mut args = args.into_iter();

        let cols = match &input {
            Relation::Source(s) => s.catalog.columns.to_vec(),
            Relation::BaseTable(t) => t.table_catalog.columns().to_vec(),
            _ => return Err(ErrorCode::BindError("the ".to_string()).into()),
        };

        match (args.next(), args.next()) {
            (Some(window_size @ ExprImpl::Literal(_)), None) => {
                let mut exprs = Vec::with_capacity(cols.len() + 2);
                let mut expr_aliases = Vec::with_capacity(cols.len() + 2);
                for (idx, col) in cols.iter().enumerate() {
                    exprs.push(InputRef::new(idx, col.data_type().clone()).into());
                    expr_aliases.push(None);
                }
                let window_start =
                    ExprImpl::FunctionCall(Box::new(FunctionCall::new_with_return_type(
                        ExprType::TumbleStart,
                        vec![ExprImpl::InputRef(Box::new(time_col)), window_size.clone()],
                        DataType::Timestamp,
                    )));
                // TODO: `window_end` may be optimized to avoid double calculation of
                // `tumble_start`, or we can depends on common expression
                // optimization.
                let window_end =
                    ExprImpl::FunctionCall(Box::new(FunctionCall::new_with_return_type(
                        ExprType::Add,
                        vec![window_start.clone(), window_size],
                        DataType::Timestamp,
                    )));
                exprs.push(window_start);
                exprs.push(window_end);
                expr_aliases.push(Some("window_start".to_string()));
                expr_aliases.push(Some("window_end".to_string()));
                let base = self.plan_relation(input)?;
                let project = LogicalProject::create(base, exprs, expr_aliases);
                Ok(project)
            }
            _ => Err(ErrorCode::BindError(
                "Invalid arguments for TUMBLE window function".to_string(),
            )
            .into()),
        }
    }
}
