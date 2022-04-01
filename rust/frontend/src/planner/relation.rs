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

use crate::binder::{
    BoundBaseTable, BoundJoin, BoundWindowTableFunction, Relation, WindowTableFunctionKind,
};
use crate::expr::{ExprImpl, ExprType, FunctionCall, InputRef};
use crate::optimizer::plan_node::{LogicalJoin, LogicalProject, LogicalScan, PlanRef};
use crate::planner::Planner;

impl Planner {
    pub(super) fn plan_relation(&mut self, relation: Relation) -> Result<PlanRef> {
        match relation {
            Relation::BaseTable(t) => self.plan_base_table(*t),
            // TODO: order is ignored in the subquery
            Relation::Subquery(q) => Ok(self.plan_query(q.query)?.as_subplan()),
            Relation::Join(join) => self.plan_join(*join),
            Relation::WindowTableFunction(tf) => self.plan_window_table_function(*tf),
        }
    }

    pub(super) fn plan_base_table(&mut self, base_table: BoundBaseTable) -> Result<PlanRef> {
        LogicalScan::create(base_table.name, Rc::new(base_table.table_desc), self.ctx())
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
            Hop => Err(ErrorCode::NotImplementedError(
                "HOP window function is not implemented yet".to_string(),
            )
            .into()),
        }
    }

    fn plan_tumble_window(
        &mut self,
        input: BoundBaseTable,
        time_col: InputRef,
        args: Vec<ExprImpl>,
    ) -> Result<PlanRef> {
        let mut args = args.into_iter();
        match (args.next(), args.next()) {
            (Some(window_size @ ExprImpl::Literal(_)), None) => {
                let cols = &input.table_desc.columns;
                let mut exprs = Vec::with_capacity(cols.len() + 2);
                let mut expr_aliases = Vec::with_capacity(cols.len() + 2);
                for (idx, col) in cols.iter().enumerate() {
                    exprs.push(ExprImpl::InputRef(Box::new(InputRef::new(
                        idx,
                        col.data_type.clone(),
                    ))));
                    expr_aliases.push(None);
                }
                let time_col_data_type = time_col.data_type();
                let window_start =
                    ExprImpl::FunctionCall(Box::new(FunctionCall::new_with_return_type(
                        ExprType::TumbleStart,
                        vec![ExprImpl::InputRef(Box::new(time_col)), window_size.clone()],
                        time_col_data_type.clone(),
                    )));
                // TODO: `window_end` may be optimized to avoid double calculation of
                // `tumble_start`, or we can depends on common expression
                // optimization.
                let window_end =
                    ExprImpl::FunctionCall(Box::new(FunctionCall::new_with_return_type(
                        ExprType::Add,
                        vec![window_start.clone(), window_size],
                        time_col_data_type,
                    )));
                exprs.push(window_start);
                exprs.push(window_end);
                // TODO: check if the names `window_[start|end]` is valid.
                expr_aliases.push(Some("window_start".to_string()));
                expr_aliases.push(Some("window_end".to_string()));
                let base = self.plan_base_table(input)?;
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
