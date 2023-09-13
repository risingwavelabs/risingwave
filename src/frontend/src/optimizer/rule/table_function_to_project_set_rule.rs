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

use itertools::Itertools;
use risingwave_common::catalog::Schema;
use risingwave_common::types::DataType;

use super::{BoxedRule, Rule};
use crate::expr::{Expr, ExprImpl, ExprType, FunctionCall, InputRef};
use crate::optimizer::plan_node::{
    LogicalProject, LogicalProjectSet, LogicalTableFunction, LogicalValues,
};
use crate::optimizer::PlanRef;

/// Transform a table function into a project set
///
/// Before:
///
/// ```text
///             LogicalTableFunction
/// ```
///
/// After:
///
///
/// ```text
///             LogicalProject (type alignment)
///                   |
///            LogicalProjectSet
///                   |
///             LogicalValues
/// ```
pub struct TableFunctionToProjectSetRule {}
impl Rule for TableFunctionToProjectSetRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let logical_table_function: &LogicalTableFunction = plan.as_logical_table_function()?;
        let table_function =
            ExprImpl::TableFunction(logical_table_function.table_function().clone().into());
        let table_function_return_type = table_function.return_type();
        let logical_values = LogicalValues::create(
            vec![vec![]],
            Schema::new(vec![]),
            logical_table_function.base.ctx.clone(),
        );
        let logical_project_set = LogicalProjectSet::create(logical_values, vec![table_function]);
        // We need a project to align schema type because `LogicalProjectSet` has a hidden column
        // `projected_row_id` and table function could return multiple columns, while project set
        // return only one column with struct type.
        let table_function_col_idx = 1;
        if let DataType::Struct(st) = table_function_return_type.clone() {
            let exprs = st
                .types()
                .enumerate()
                .map(|(i, data_type)| {
                    let field_access = FunctionCall::new_unchecked(
                        ExprType::Field,
                        vec![
                            ExprImpl::InputRef(
                                InputRef::new(
                                    table_function_col_idx,
                                    table_function_return_type.clone(),
                                )
                                .into(),
                            ),
                            ExprImpl::literal_int(i as i32),
                        ],
                        data_type.clone(),
                    );
                    ExprImpl::FunctionCall(field_access.into())
                })
                .collect_vec();
            let logical_project = LogicalProject::new(logical_project_set, exprs);
            Some(logical_project.into())
        } else {
            let logical_project = LogicalProject::with_out_col_idx(
                logical_project_set,
                std::iter::once(table_function_col_idx),
            );
            Some(logical_project.into())
        }
    }
}

impl TableFunctionToProjectSetRule {
    pub fn create() -> BoxedRule {
        Box::new(TableFunctionToProjectSetRule {})
    }
}
