// Copyright 2026 RisingWave Labs
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
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::types::{DataType, ScalarImpl};

use super::prelude::{PlanRef, *};
use crate::expr::{Expr, ExprImpl, MSSQL_QUERY_ARG_COUNT, TableFunctionType};
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::{LogicalMssqlQuery, LogicalTableFunction, MssqlConnection};

/// Optimizer rule that rewrites a `LogicalTableFunction` of type
/// `MSSQL_QUERY` into a `LogicalMssqlQuery` plan node. Registered in the
/// `TABLE_FUNCTION_TO_MSSQL_QUERY` stage of the logical optimization
/// pipeline (after the equivalent `postgres_query` and `mysql_query`
/// rules). Validates that exactly 8 arguments were supplied; the 2-arg
/// source-reference form is handled earlier by the binder, not here.
pub struct TableFunctionToMssqlQueryRule {}

fn fold_const_varchar(expr: &ExprImpl) -> Option<String> {
    let value = expr.try_fold_const()?.ok()??;
    match value {
        ScalarImpl::Utf8(value) => Some(value.to_string()),
        _ => None,
    }
}

fn fold_const_bool(expr: &ExprImpl) -> Option<bool> {
    let value = expr.try_fold_const()?.ok()??;
    match value {
        ScalarImpl::Bool(value) => Some(value),
        _ => None,
    }
}

impl Rule<Logical> for TableFunctionToMssqlQueryRule {
    /// Apply the rewrite: if the plan is a `LogicalTableFunction` of
    /// type `MSSQL_QUERY`, build a `LogicalMssqlQuery` from its 8 inline
    /// arguments. Returns `None` for any other plan.
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let logical_table_function: &LogicalTableFunction = plan.as_logical_table_function()?;
        if logical_table_function.table_function().function_type != TableFunctionType::MssqlQuery {
            return None;
        }
        // The binder rejects this form with a user-facing error. Keep this
        // check fallible as defense against malformed internally-built plans.
        if logical_table_function.with_ordinality() {
            return None;
        }
        let table_function_return_type = logical_table_function.table_function().return_type();

        if let DataType::Struct(st) = table_function_return_type {
            let fields = st
                .iter()
                .map(|(name, data_type)| Field::with_name(data_type.clone(), name.to_owned()))
                .collect_vec();

            let schema = Schema::new(fields);

            let args = &logical_table_function.table_function().args;
            if args.len() != MSSQL_QUERY_ARG_COUNT {
                return None;
            }

            // `TableFunction::new_mssql_query` validates all arguments as
            // constants and normalizes the two TLS flags to boolean literals.
            // Re-check the invariant here instead of panicking if an internal
            // caller constructs a malformed table function.
            let hostname = fold_const_varchar(&args[0])?;
            let port = fold_const_varchar(&args[1])?;
            let username = fold_const_varchar(&args[2])?;
            let password = fold_const_varchar(&args[3])?;
            let database = fold_const_varchar(&args[4])?;
            let query = fold_const_varchar(&args[5])?;
            let encrypt = fold_const_bool(&args[6])?;
            let trust_cert = fold_const_bool(&args[7])?;

            Some(
                LogicalMssqlQuery::new(
                    logical_table_function.ctx(),
                    schema,
                    MssqlConnection {
                        hostname,
                        port,
                        username,
                        password,
                        database,
                        encrypt,
                        trust_cert,
                    },
                    query,
                )
                .into(),
            )
        } else {
            unreachable!("TableFunction return type should be struct")
        }
    }
}

impl TableFunctionToMssqlQueryRule {
    /// Construct a boxed instance for registration in the
    /// `TABLE_FUNCTION_TO_MSSQL_QUERY` stage.
    pub fn create() -> BoxedRule {
        Box::new(TableFunctionToMssqlQueryRule {})
    }
}
