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
use risingwave_common::catalog::{
    Field, Schema, PG_CATALOG_SCHEMA_NAME, RW_INTERNAL_TABLE_FUNCTION_NAME,
};
use risingwave_common::types::DataType;
use risingwave_sqlparser::ast::{Function, FunctionArg, ObjectName, TableAlias};

use super::watermark::is_watermark_func;
use super::{Binder, Relation, Result, WindowTableFunctionKind};
use crate::binder::bind_context::Clause;
use crate::catalog::system_catalog::pg_catalog::{
    PG_GET_KEYWORDS_FUNC_NAME, PG_KEYWORDS_TABLE_NAME,
};
use crate::expr::Expr;

impl Binder {
    /// Binds a table function AST, which is a function call in a relation position.
    ///
    /// Besides [`TableFunction`] expr, it can also be other things like window table functions, or
    /// scalar functions.
    pub(super) fn bind_table_function(
        &mut self,
        name: ObjectName,
        alias: Option<TableAlias>,
        args: Vec<FunctionArg>,
    ) -> Result<Relation> {
        let func_name = &name.0[0].real_value();
        // internal/system table functions
        {
            if func_name.eq_ignore_ascii_case(RW_INTERNAL_TABLE_FUNCTION_NAME) {
                return self.bind_internal_table(args, alias);
            }
            if func_name.eq_ignore_ascii_case(PG_GET_KEYWORDS_FUNC_NAME)
                || name.real_value().eq_ignore_ascii_case(
                    format!("{}.{}", PG_CATALOG_SCHEMA_NAME, PG_GET_KEYWORDS_FUNC_NAME).as_str(),
                )
            {
                return self.bind_relation_by_name_inner(
                    Some(PG_CATALOG_SCHEMA_NAME),
                    PG_KEYWORDS_TABLE_NAME,
                    alias,
                    false,
                );
            }
        }
        // window table functions (tumble/hop)
        if let Ok(kind) = WindowTableFunctionKind::from_str(func_name) {
            return Ok(Relation::WindowTableFunction(Box::new(
                self.bind_window_table_function(alias, kind, args)?,
            )));
        }
        // watermark
        if is_watermark_func(func_name) {
            return Ok(Relation::Watermark(Box::new(
                self.bind_watermark(alias, args)?,
            )));
        };

        let mut clause = Some(Clause::From);
        std::mem::swap(&mut self.context.clause, &mut clause);
        let func = self.bind_function(Function {
            name,
            args,
            over: None,
            distinct: false,
            order_by: vec![],
            filter: None,
            within_group: None,
        })?;
        self.context.clause = clause;

        let columns = if let DataType::Struct(s) = func.return_type() {
            // If the table function returns a struct, it's fields can be accessed just
            // like a table's columns.
            let schema = Schema::from(&s);
            schema.fields.into_iter().map(|f| (false, f)).collect_vec()
        } else {
            // If there is an table alias, we should use the alias as the table function's
            // column name. If column aliases are also provided, they
            // are handled in bind_table_to_context.
            //
            // Note: named return value should take precedence over table alias.
            // But we don't support it yet.
            // e.g.,
            // ```
            // > create function foo(ret out int) language sql as 'select 1';
            // > select t.ret from foo() as t;
            // ```
            let col_name = if let Some(alias) = &alias {
                alias.name.real_value()
            } else {
                func_name.clone()
            };
            vec![(false, Field::with_name(func.return_type(), col_name))]
        };

        self.bind_table_to_context(columns, func_name.clone(), alias)?;

        Ok(Relation::TableFunction(func))
    }
}
