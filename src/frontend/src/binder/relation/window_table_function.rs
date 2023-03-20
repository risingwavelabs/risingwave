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

use std::str::FromStr;

use itertools::Itertools;
use risingwave_common::catalog::Field;
use risingwave_common::error::ErrorCode;
use risingwave_common::types::DataType;
use risingwave_sqlparser::ast::{FunctionArg, TableAlias};

use super::{Binder, Relation, Result};
use crate::binder::statement::RewriteExprsRecursive;
use crate::expr::{ExprImpl, InputRef};

#[derive(Copy, Clone, Debug)]
pub enum WindowTableFunctionKind {
    Tumble,
    Hop,
}

impl FromStr for WindowTableFunctionKind {
    type Err = ();

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        if s.eq_ignore_ascii_case("tumble") {
            Ok(WindowTableFunctionKind::Tumble)
        } else if s.eq_ignore_ascii_case("hop") {
            Ok(WindowTableFunctionKind::Hop)
        } else {
            Err(())
        }
    }
}

#[derive(Debug, Clone)]
pub struct BoundWindowTableFunction {
    pub(crate) input: Relation,
    pub(crate) kind: WindowTableFunctionKind,
    pub(crate) time_col: InputRef,
    pub(crate) args: Vec<ExprImpl>,
}

impl RewriteExprsRecursive for BoundWindowTableFunction {
    fn rewrite_exprs_recursive(&mut self, rewriter: &mut impl crate::expr::ExprRewriter) {
        self.input.rewrite_exprs_recursive(rewriter);
        let new_agrs = std::mem::take(&mut self.args)
            .into_iter()
            .map(|expr| rewriter.rewrite_expr(expr))
            .collect::<Vec<_>>();
        self.args = new_agrs;
    }
}

const ERROR_1ST_ARG: &str = "The 1st arg of window table function should be a table name (incl. source, CTE, view) but not complex structure (subquery, join, another table function). Consider using an intermediate CTE or view as workaround.";
const ERROR_2ND_ARG_EXPR: &str = "The 2st arg of window table function should be a column name but not complex expression. Consider using an intermediate CTE or view as workaround.";
const ERROR_2ND_ARG_TYPE: &str = "The 2st arg of window table function should be a column of type timestamp with time zone, timestamp or date.";

impl Binder {
    pub(super) fn bind_window_table_function(
        &mut self,
        alias: Option<TableAlias>,
        kind: WindowTableFunctionKind,
        args: Vec<FunctionArg>,
    ) -> Result<BoundWindowTableFunction> {
        let mut args = args.into_iter();

        self.push_context();

        let (base, table_name) = self.bind_relation_by_function_arg(args.next(), ERROR_1ST_ARG)?;

        let time_col = self.bind_column_by_function_args(args.next(), ERROR_2ND_ARG_EXPR)?;

        let Some(output_type) = DataType::window_of(&time_col.data_type) else {
            return Err(ErrorCode::BindError(ERROR_2ND_ARG_TYPE.to_string()).into());
        };

        let base_columns = std::mem::take(&mut self.context.columns);

        self.pop_context()?;

        let columns = base_columns
            .into_iter()
            .map(|c| {
                if c.field.name == "window_start" || c.field.name == "window_end" {
                    Err(ErrorCode::BindError(
                        "column names `window_start` and `window_end` are not allowed in window table function's input."
                        .into())
                    .into())
                } else {
                    Ok((c.is_hidden, c.field))
                }
            })
            .chain(
                [
                    Ok((false, Field::with_name(output_type.clone(), "window_start"))),
                    Ok((false, Field::with_name(output_type, "window_end"))),
                ]
                .into_iter(),
            ).collect::<Result<Vec<_>>>()?;

        let (_, table_name) = Self::resolve_schema_qualified_name(&self.db_name, table_name)?;
        self.bind_table_to_context(columns, table_name, alias)?;

        // Other arguments are validated in `plan_window_table_function`
        let exprs: Vec<_> = args
            .map(|arg| self.bind_function_arg(arg))
            .flatten_ok()
            .try_collect()?;
        Ok(BoundWindowTableFunction {
            input: base,
            time_col: *time_col,
            kind,
            args: exprs,
        })
    }
}
