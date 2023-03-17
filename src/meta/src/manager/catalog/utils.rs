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
use risingwave_sqlparser::ast::{
    Array, CreateSink, CreateSinkStatement, CreateSourceStatement, Distinct, Expr, Function,
    FunctionArg, FunctionArgExpr, Ident, ObjectName, Query, SelectItem, SetExpr, Statement,
    TableAlias, TableFactor, TableWithJoins,
};
use risingwave_sqlparser::parser::Parser;

use crate::{MetaError, MetaResult};

/// `alter_relation_rename` renames a relation to a new name in its `Create` statement, and returns
/// the updated definition raw sql. Note that the `definition` must be a `Create` statement and the
/// `new_name` must be a valid identifier, it should be validated before calling this function. To
/// update all relations that depend on the renamed one, use `alter_relation_rename_refs`.
pub fn alter_relation_rename(definition: &str, new_name: &str) -> String {
    // This happens when we try to rename a table that's created by `CREATE TABLE AS`. Remove it
    // when we support `SHOW CREATE TABLE` for `CREATE TABLE AS`.
    if definition == "" {
        tracing::warn!("found empty definition when renaming relation, ignored.");
        return "".into();
    }
    let ast = Parser::parse_sql(definition).expect("failed to parse relation definition");
    let mut stmt = ast
        .into_iter()
        .exactly_one()
        .expect("should contains only one statement");

    match &mut stmt {
        Statement::CreateTable { name, .. }
        | Statement::CreateView { name, .. }
        | Statement::CreateIndex { name, .. }
        | Statement::CreateSource {
            stmt: CreateSourceStatement {
                source_name: name, ..
            },
        }
        | Statement::CreateSink {
            stmt: CreateSinkStatement {
                sink_name: name, ..
            },
        } => replace_table_name(name, new_name),
        _ => unreachable!(),
    };

    stmt.to_string()
}

/// `alter_relation_rename_refs` updates all references of renamed-relation in the definition of
/// target relation's `Create` statement. It will returns an ambiguous error if the new name is used
/// as alias by the target relation.
/// TODO: We can recursively change the original alias so that we don't have to return ambiguous
/// error in the future.
pub fn alter_relation_rename_refs(
    relation: &str,
    definition: &str,
    from: &str,
    to: &str,
) -> MetaResult<String> {
    let ast = Parser::parse_sql(definition).expect("failed to parse relation definition");
    let mut stmt = ast
        .into_iter()
        .exactly_one()
        .expect("should contains only one statement");

    match &mut stmt {
        Statement::CreateTable {
            query: Some(query), ..
        }
        | Statement::CreateView { query, .. }
        | Statement::Query(query) // Used by view, actually we store a query as the definition of view.
        | Statement::CreateSink {
            stmt:
                CreateSinkStatement {
                    sink_from: CreateSink::AsQuery(query),
                    ..
                },
        } => {
            QueryRewriter::rewrite_query(query, relation, from, to)?;
        }
        Statement::CreateIndex { table_name, .. }
        | Statement::CreateSink {
            stmt:
                CreateSinkStatement {
                    sink_from: CreateSink::From(table_name),
                    ..
                },
        } => replace_table_name(table_name, to),
        _ => unreachable!(),
    };
    Ok(stmt.to_string())
}

/// Returns an ambiguous error if the new name is used as alias by the target relation.
fn ambiguous_error(relation: &str, from: &str, to: &str) -> MetaError {
    MetaError::invalid_parameter(format!(
        "Can't rename \'{from}\' to \'{to}\', which is ambiguous and used by relation \'{relation}\'"
    ))
}

/// Replace the last ident in the `table_name` with the given name, the object name is ensured to be
/// non-empty. e.g. `schema.table` or `database.schema.table`.
fn replace_table_name(table_name: &mut ObjectName, to: &str) {
    let idx = table_name.0.len() - 1;
    table_name.0[idx] = Ident::new_unchecked(to);
}

/// `QueryRewriter` is a visitor that updates all references of relation named `from` to `to` in the
/// given query, which is the part of create statement of `relation`. It will returns an ambiguous
/// error if the new name is used as alias by the target relation.
struct QueryRewriter<'a> {
    relation: &'a str,
    from: &'a str,
    to: &'a str,
}

impl QueryRewriter<'_> {
    fn rewrite_query(query: &mut Query, relation: &str, from: &str, to: &str) -> MetaResult<()> {
        let rewriter = QueryRewriter { relation, from, to };
        rewriter.visit_query(query)
    }

    /// Visit the query and update all references of relation named `from` to `to`.
    fn visit_query(&self, query: &mut Query) -> MetaResult<()> {
        if let Some(with) = &mut query.with {
            for cte_table in &mut with.cte_tables {
                if cte_table.alias.name.real_value() == self.to {
                    return Err(ambiguous_error(self.relation, self.from, self.to));
                }
                self.visit_query(&mut cte_table.query)?;
            }
        }
        self.visit_set_expr(&mut query.body)?;
        for expr in &mut query.order_by {
            self.visit_expr(&mut expr.expr)?;
        }

        Ok(())
    }

    /// Check if the alias is the same as the new name, if so, return an ambiguous error.
    fn check_alias(&self, alias: &Option<TableAlias>) -> MetaResult<()> {
        if let Some(alias) = alias {
            if alias.name.real_value() == self.to {
                return Err(ambiguous_error(self.relation, self.from, self.to));
            }
        }
        Ok(())
    }

    /// Visit table with joins and update all references of relation named `from` to `to`.
    fn visit_table_with_joins(&self, table_with_joins: &mut TableWithJoins) -> MetaResult<()> {
        match &mut table_with_joins.relation {
            TableFactor::Table { name, alias, .. } => {
                self.check_alias(alias)?;
                self.may_rewrite_idents(&mut name.0)?;
            }
            TableFactor::Derived {
                subquery, alias, ..
            } => {
                self.check_alias(alias)?;
                self.visit_query(subquery)?;
            }
            TableFactor::TableFunction { alias, args, .. } => {
                self.check_alias(alias)?;
                for arg in args {
                    self.visit_function_args(arg)?;
                }
            }
            TableFactor::NestedJoin(table_with_joins) => {
                self.visit_table_with_joins(table_with_joins)?
            }
        }

        Ok(())
    }

    /// Visit query body expression and update all references.
    fn visit_set_expr(&self, set_expr: &mut SetExpr) -> MetaResult<()> {
        match set_expr {
            SetExpr::Select(select) => {
                if let Distinct::DistinctOn(exprs) = &mut select.distinct {
                    for expr in exprs {
                        self.visit_expr(expr)?;
                    }
                }
                for select_item in &mut select.projection {
                    self.visit_select_item(select_item)?;
                }
                for from_item in &mut select.from {
                    self.visit_table_with_joins(from_item)?;
                }
                if let Some(where_clause) = &mut select.selection {
                    self.visit_expr(where_clause)?;
                }
                for expr in &mut select.group_by {
                    self.visit_expr(expr)?;
                }
                if let Some(having) = &mut select.having {
                    self.visit_expr(having)?;
                }
            }
            SetExpr::Query(query) => self.visit_query(query)?,
            SetExpr::SetOperation { left, right, .. } => {
                self.visit_set_expr(left)?;
                self.visit_set_expr(right)?;
            }
            SetExpr::Values(_) => {}
        }

        Ok(())
    }

    /// Rewrite idents like `schema.table`, `table` if the last ident is the same as `from`.
    fn may_rewrite_idents(&self, idents: &mut [Ident]) -> MetaResult<()> {
        let idx = idents.len() - 1;
        if idents[idx].real_value() == self.from {
            idents[idx] = Ident::new_unchecked(self.to.to_string());
        }

        Ok(())
    }

    /// Rewrite idents like `schema.table.column`, `table.column` if the last ident is the same as
    /// `from`.
    fn may_rewrite_idents_with_column(&self, idents: &mut [Ident]) -> MetaResult<()> {
        if idents.len() >= 2 && let idx = idents.len() -2 && idents[idx].real_value() == self.from {
            idents[idx] = Ident::new_unchecked(self.to.to_string());
        }

        Ok(())
    }

    /// Visit function arguments and update all references.
    fn visit_function_args(&self, function_args: &mut FunctionArg) -> MetaResult<()> {
        match function_args {
            FunctionArg::Unnamed(arg) | FunctionArg::Named { arg, .. } => match arg {
                FunctionArgExpr::Expr(expr) | FunctionArgExpr::ExprQualifiedWildcard(expr, _) => {
                    self.visit_expr(expr)
                }
                FunctionArgExpr::QualifiedWildcard(obj_name) => {
                    self.may_rewrite_idents(&mut obj_name.0)
                }
                FunctionArgExpr::Wildcard => Ok(()),
            },
        }
    }

    /// Visit function and update all references.
    fn visit_function(&self, function: &mut Function) -> MetaResult<()> {
        for arg in &mut function.args {
            self.visit_function_args(arg)?;
        }

        Ok(())
    }

    /// Visit expression and update all references.
    fn visit_expr(&self, expr: &mut Expr) -> MetaResult<()> {
        match expr {
            Expr::CompoundIdentifier(idents) => self.may_rewrite_idents_with_column(idents),
            Expr::FieldIdentifier(expr, ..) => self.visit_expr(expr),

            Expr::IsNull(expr)
            | Expr::IsNotNull(expr)
            | Expr::IsTrue(expr)
            | Expr::IsNotTrue(expr)
            | Expr::IsFalse(expr)
            | Expr::IsNotFalse(expr)
            | Expr::InList { expr, .. }
            | Expr::SomeOp(expr)
            | Expr::AllOp(expr)
            | Expr::UnaryOp { expr, .. }
            | Expr::Cast { expr, .. }
            | Expr::TryCast { expr, .. }
            | Expr::AtTimeZone {
                timestamp: expr, ..
            }
            | Expr::Extract { expr, .. }
            | Expr::Substring { expr, .. }
            | Expr::Overlay { expr, .. }
            | Expr::Trim { expr, .. }
            | Expr::Nested(expr)
            | Expr::ArrayIndex { obj: expr, .. } => self.visit_expr(expr),

            Expr::InSubquery { expr, subquery, .. } => {
                self.visit_expr(expr)?;
                self.visit_query(subquery)
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                self.visit_expr(expr)?;
                self.visit_expr(low)?;
                self.visit_expr(high)
            }

            Expr::IsDistinctFrom(expr1, expr2)
            | Expr::IsNotDistinctFrom(expr1, expr2)
            | Expr::BinaryOp {
                left: expr1,
                right: expr2,
                ..
            } => {
                self.visit_expr(expr1)?;
                self.visit_expr(expr2)
            }
            Expr::Function(function) => self.visit_function(function),
            Expr::Exists(query) | Expr::Subquery(query) => self.visit_query(query),

            Expr::GroupingSets(exprs_vec) | Expr::Cube(exprs_vec) | Expr::Rollup(exprs_vec) => {
                for exprs in exprs_vec {
                    for expr in exprs {
                        self.visit_expr(expr)?;
                    }
                }
                Ok(())
            }

            Expr::Row(exprs) | Expr::Array(Array { elem: exprs, .. }) => {
                for expr in exprs {
                    self.visit_expr(expr)?;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    /// Visit select item and update all references.
    fn visit_select_item(&self, select_item: &mut SelectItem) -> MetaResult<()> {
        match select_item {
            SelectItem::UnnamedExpr(expr)
            | SelectItem::ExprQualifiedWildcard(expr, _)
            | SelectItem::ExprWithAlias { expr, .. } => self.visit_expr(expr),
            SelectItem::QualifiedWildcard(obj_name) => self.may_rewrite_idents(&mut obj_name.0),
            SelectItem::Wildcard => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::manager::catalog::utils::{
        alter_relation_rename, alter_relation_rename_refs, ambiguous_error,
    };

    #[test]
    fn test_alter_table_rename() {
        let definition = "CREATE TABLE foo (a int, b int)";
        let new_name = "bar";
        let expected = "CREATE TABLE bar (a INT, b INT)";
        let actual = alter_relation_rename(definition, new_name);
        assert_eq!(expected, actual);
    }

    #[test]
    fn test_rename_index_refs() {
        let definition = "CREATE INDEX idx1 ON foo(v1 DESC, v2)";
        let from = "foo";
        let to = "bar";
        let expected = "CREATE INDEX idx1 ON bar(v1 DESC, v2)";
        let actual = alter_relation_rename_refs("idx1", definition, from, to).unwrap();
        assert_eq!(expected, actual);
    }

    #[test]
    fn test_rename_sink_refs() {
        let definition =
            "CREATE SINK sink_t FROM foo WITH (connector = 'kafka', format = 'append_only')";
        let from = "foo";
        let to = "bar";
        let expected =
            "CREATE SINK sink_t FROM bar WITH (connector = 'kafka', format = 'append_only')";
        let actual = alter_relation_rename_refs("sink_t", definition, from, to).unwrap();
        assert_eq!(expected, actual);
    }

    #[test]
    fn test_rename_with_alias_refs() {
        let definition =
            "CREATE MATERIALIZED VIEW mv1 AS SELECT foo.v1 AS m1v, foo.v2 AS m2v FROM foo";
        let from = "foo";
        let to = "bar";
        let expected =
            "CREATE MATERIALIZED VIEW mv1 AS SELECT bar.v1 AS m1v, bar.v2 AS m2v FROM bar";
        let actual = alter_relation_rename_refs("mv1", definition, from, to).unwrap();
        assert_eq!(expected, actual);

        let definition = "CREATE MATERIALIZED VIEW mv1 AS SELECT foo.v1 AS m1v, (foo.v2).v3 AS m2v FROM foo WHERE foo.v1 = 1 AND (foo.v2).v3 IS TRUE";
        let expected = "CREATE MATERIALIZED VIEW mv1 AS SELECT bar.v1 AS m1v, (bar.v2).v3 AS m2v FROM bar WHERE bar.v1 = 1 AND (bar.v2).v3 IS TRUE";
        let actual = alter_relation_rename_refs("mv1", definition, from, to).unwrap();
        assert_eq!(expected, actual);

        let definition = "CREATE MATERIALIZED VIEW mv1 AS SELECT bar.v1 AS m1v, (bar.v2).v3 AS m2v FROM foo as bar WHERE bar.v1 = 1";
        let expected = ambiguous_error("mv1", "foo", "bar").to_string();
        let actual = alter_relation_rename_refs("mv1", definition, from, to)
            .err()
            .unwrap()
            .to_string();
        assert_eq!(expected, actual);
    }
}
