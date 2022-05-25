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

use std::str::FromStr;

use itertools::Itertools;
use risingwave_common::catalog::Field;
use risingwave_common::error::{ErrorCode, RwError};
use risingwave_common::types::DataType;
use risingwave_sqlparser::ast::{Expr, FunctionArg, FunctionArgExpr, ObjectName, TableAlias};

use super::{Binder, Relation, Result};
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

impl Binder {
    pub(super) fn bind_window_table_function(
        &mut self,
        alias: Option<TableAlias>,
        kind: WindowTableFunctionKind,
        args: Vec<FunctionArg>,
    ) -> Result<BoundWindowTableFunction> {
        let mut args = args.into_iter();

        self.push_context();

        let Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))) = args.next() else {
            return Err(ErrorCode::BindError(
                "the 1st arg of window table function should be table".to_string(),
            )
            .into());
        };
        let table_name = match expr {
            Expr::Identifier(ident) => Ok::<_, RwError>(ObjectName(vec![ident])),
            Expr::CompoundIdentifier(idents) => Ok(ObjectName(idents)),
            _ => Err(ErrorCode::BindError(
                "the 1st arg of window table function should be table".to_string(),
            )
            .into()),
        }?;
        let (schema_name, table_name) = Self::resolve_table_name(table_name)?;

        let base = self.bind_table_or_source(&schema_name, &table_name, None)?;

        let Some(time_col_arg) = args.next() else {
            return Err(ErrorCode::BindError(
                "the 2st arg of window table function should be time_col".to_string(),
            )
            .into());
        };
        let Some(ExprImpl::InputRef(time_col)) = self.bind_function_arg(time_col_arg)?.into_iter().next() else {
            return Err(ErrorCode::BindError(
                "the 2st arg of window table function should be time_col".to_string(),
            )
            .into());
        };

        self.pop_context();

        let table_catalog =
            self.catalog
                .get_table_by_name(&self.db_name, &schema_name, &table_name)?;

        let columns = table_catalog.columns().to_vec();
        if columns.iter().any(|col| {
            col.name().eq_ignore_ascii_case("window_start")
                || col.name().eq_ignore_ascii_case("window_end")
        }) {
            return Err(ErrorCode::BindError(
                "column names `window_start` and `window_end` are not allowed in window table function's input."
                .into())
            .into());
        }

        let columns = columns
            .iter()
            .map(|c| (c.is_hidden, (&c.column_desc).into()))
            .chain(
                [
                    (
                        false,
                        Field {
                            data_type: DataType::Timestamp,
                            name: "window_start".to_string(),
                            sub_fields: vec![],
                            type_name: "".to_string(),
                        },
                    ),
                    (
                        false,
                        Field {
                            data_type: DataType::Timestamp,
                            name: "window_end".to_string(),
                            sub_fields: vec![],
                            type_name: "".to_string(),
                        },
                    ),
                ]
                .into_iter(),
            );

        self.bind_context(columns, table_name.clone(), alias)?;

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
