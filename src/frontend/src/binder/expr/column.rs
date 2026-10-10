// Copyright 2022 RisingWave Labs
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

use risingwave_common::types::DataType;
use risingwave_sqlparser::ast::Ident;

use crate::binder::{Binder, Clause};
use crate::error::{ErrorCode, Result};
use crate::expr::{CorrelatedInputRef, ExprImpl, ExprType, FunctionCall, InputRef, Literal};

impl Binder {
    pub fn bind_column(&mut self, idents: &[Ident]) -> Result<ExprImpl> {
        // TODO: check quote style of `ident`.
        let (schema_name, table_name, column_name) = match idents {
            [column] => (None, None, column.real_value()),
            [table, column] => (None, Some(table.real_value()), column.real_value()),
            [schema, table, column] => (
                Some(schema.real_value()),
                Some(table.real_value()),
                column.real_value(),
            ),
            _ => {
                return Err(
                    ErrorCode::InternalError(format!("Too many idents: {:?}", idents)).into(),
                );
            }
        };

        // If we find `sql_udf_arguments` in the current context, it means we're binding an inline SQL UDF
        // (without a layer of subquery). This only happens when the function body is a trivial `SELECT`
        // statement without any `FROM` clause etc. In this case, the column must be a UDF parameter.
        if self.is_binding_inline_sql_udf() {
            return self.bind_sql_udf_parameter(&column_name);
        }

        match self
            .context
            .get_column_binding_indices(&schema_name, &table_name, &column_name)
        {
            Ok(mut indices) => {
                match indices.len() {
                    0 => unreachable!(),
                    1 => {
                        let index = indices[0];
                        let column = &self.context.columns[index];
                        return Ok(
                            InputRef::new(column.index, column.field.data_type.clone()).into()
                        );
                    }
                    _ => {
                        indices.sort(); // make sure we have a consistent result
                        let inputs = indices
                            .iter()
                            .map(|index| {
                                let column = &self.context.columns[*index];
                                InputRef::new(column.index, column.field.data_type.clone()).into()
                            })
                            .collect::<Vec<_>>();
                        return Ok(FunctionCall::new(ExprType::Coalesce, inputs)?.into());
                    }
                }
            }
            Err(e) => {
                // If a column is referenced using three-level qualification and the table has an alias,
                // prompt the user to use the table alias instead.
                if let ErrorCode::ItemNotFound(_) = e {
                    if let (Some(schema), Some(table)) = (&schema_name, &table_name)
                        && let Some(index) =
                            self.context.get_table_alias(schema, table, &column_name)?
                    {
                        let column = &self.context.columns[index];
                        return Err(ErrorCode::InvalidReference(format!(
                            "missing FROM-clause entry for table \"{}\"\n\
                            HINT:  Perhaps you meant to reference the table alias \"{}\".",
                            table, column.table_name
                        ))
                        .into());
                    };
                } else {
                    // If the error message is not that the column is not found, throw the error
                    return Err(e.into());
                }
            }
        }

        // Try to find a correlated column in the enclosing scopes, starting from the innermost one.
        let mut err = ErrorCode::ItemNotFound(format!("Invalid column: {}", column_name));

        for scope in self.correlation_scopes() {
            let mut found = None;
            for (context, depth) in scope {
                if matches!(context.clause, Some(Clause::Insert)) {
                    continue;
                }
                match context.get_column_binding_indices(&schema_name, &table_name, &column_name) {
                    Ok(indices) => {
                        // All `FROM` items of a query level share one namespace, even when the
                        // binder keeps the left inputs of a lateral factor in separate contexts.
                        if found.is_some() {
                            return Err(match &table_name {
                                Some(table_name) => ErrorCode::InvalidReference(format!(
                                    "table reference \"{}\" is ambiguous",
                                    table_name
                                )),
                                None => ErrorCode::InvalidReference(format!(
                                    "column reference \"{}\" is ambiguous",
                                    column_name
                                )),
                            }
                            .into());
                        }
                        found = Some((context, indices, depth));
                    }
                    Err(e @ ErrorCode::ItemNotFound(_)) => err = e,
                    // An ambiguous name must not fall through to an outer scope.
                    Err(e) => return Err(e.into()),
                }
            }

            if let Some((context, mut indices, depth)) = found {
                indices.sort(); // make sure we have a consistent result
                let mut inputs = indices
                    .iter()
                    .map(|index| {
                        let column = &context.columns[*index];
                        CorrelatedInputRef::new(column.index, column.field.data_type.clone(), depth)
                            .into()
                    })
                    .collect::<Vec<ExprImpl>>();
                return if inputs.len() == 1 {
                    Ok(inputs.pop().unwrap())
                } else {
                    // An unqualified reference to a column merged by `NATURAL`/`USING` join.
                    Ok(FunctionCall::new(ExprType::Coalesce, inputs)?.into())
                };
            }
        }

        // `CTID` is a system column in postgres.
        // https://www.postgresql.org/docs/current/ddl-system-columns.html
        //
        // We return an empty string here to support some tools such as DataGrip.
        //
        // FIXME: The type of `CTID` should be `tid`.
        // FIXME: The `CTID` column should be unique, so literal may break something.
        // FIXME: At least we should add a notice here.
        if let ErrorCode::ItemNotFound(_) = err
            && column_name == "ctid"
        {
            return Ok(Literal::new(Some("".into()), DataType::Varchar).into());
        }

        // Failed to resolve the column in current context. Now check if it's a sql udf parameter.
        if let ErrorCode::ItemNotFound(_) = err
            && self.is_binding_subquery_sql_udf()
        {
            return self.bind_sql_udf_parameter(&column_name);
        }

        Err(err.into())
    }

    /// Return visible outer column contexts grouped by name-resolution scope, innermost first.
    /// Each context is paired with the semantic correlation depth at which it is owned.
    ///
    /// An upper query context and its visible lateral contexts are the `FROM` items of one query
    /// level, so they form one scope: a name found in more than one of them is ambiguous rather
    /// than resolved to whichever context comes first.
    ///
    /// A non-empty lateral context represents the left input of a potential `Apply`, so it adds a
    /// depth boundary. Empty contexts are parser/binder isolation frames and do not. An upper
    /// query or table-function context always contributes at least one boundary, even if its local
    /// `FROM` context is empty.
    fn correlation_scopes(&self) -> Vec<Vec<(&crate::binder::BindContext, usize)>> {
        let mut scopes = vec![];
        let mut depth = 1;

        let mut scope = vec![];
        for lateral_context in self.lateral_contexts.iter().rev() {
            if lateral_context.is_visible {
                scope.push((&lateral_context.context, depth));
            }
            if !lateral_context.context.columns.is_empty() {
                depth += 1;
            }
        }
        scopes.push(scope);

        for (context, lateral_contexts) in self.visible_upper_subquery_contexts_rev() {
            let entry_depth = depth;
            let mut scope = vec![(context, depth)];
            if !context.columns.is_empty() {
                depth += 1;
            }

            for lateral_context in lateral_contexts.iter().rev() {
                if lateral_context.is_visible {
                    scope.push((&lateral_context.context, depth));
                }
                if !lateral_context.context.columns.is_empty() {
                    depth += 1;
                }
            }

            depth = depth.max(entry_depth + 1);
            scopes.push(scope);
        }

        scopes
    }
}
