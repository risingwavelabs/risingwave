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

/// This module contains internal utilities used for testing the library.
/// While technically public, the library's users are not supposed to rely
/// on this module, as it will change without notice.
// Integration tests (i.e. everything under `tests/`) import this
// via `tests/test_utils/executor`.

#[cfg(not(feature = "std"))]
use alloc::{
    boxed::Box,
    string::{String, ToString},
    vec,
    vec::Vec,
};
use core::fmt::Debug;

use crate::ast::*;
use crate::parser::{Parser, ParserError};
use crate::tokenizer::Tokenizer;

pub fn run_parser_method<F, T: Debug + PartialEq>(sql: &str, f: F) -> T
where
    F: Fn(&mut Parser) -> T,
{
    let mut tokenizer = Tokenizer::new(sql);
    let tokens = tokenizer.tokenize().unwrap();
    f(&mut Parser::new(tokens))
}

pub fn parse_sql_statements(sql: &str) -> Result<Vec<Statement>, ParserError> {
    Parser::parse_sql(sql)
    // To fail the `ensure_multiple_dialects_are_tested` test:
    // Parser::parse_sql(&**self.dialects.first().unwrap(), sql)
}

/// Ensures that `sql` parses as a single statement and returns it.
/// If non-empty `canonical` SQL representation is provided,
/// additionally asserts that parsing `sql` results in the same parse
/// tree as parsing `canonical`, and that serializing it back to string
/// results in the `canonical` representation.
#[track_caller]
pub fn one_statement_parses_to(sql: &str, canonical: &str) -> Statement {
    let mut statements = parse_sql_statements(sql).unwrap();
    assert_eq!(statements.len(), 1);

    if !canonical.is_empty() && sql != canonical {
        assert_eq!(parse_sql_statements(canonical).unwrap(), statements);
    }

    let only_statement = statements.pop().unwrap();
    if !canonical.is_empty() {
        assert_eq!(canonical, only_statement.to_string())
    }
    only_statement
}

/// Ensures that `sql` parses as a single [Statement], and is not modified
/// after a serialization round-trip.
#[track_caller]
pub fn verified_stmt(query: &str) -> Statement {
    one_statement_parses_to(query, query)
}

/// Ensures that `sql` parses as a single [Query], and is not modified
/// after a serialization round-trip.
#[track_caller]
pub fn verified_query(sql: &str) -> Query {
    match verified_stmt(sql) {
        Statement::Query(query) => *query,
        _ => panic!("Expected Query"),
    }
}

#[track_caller]
pub fn query(sql: &str, canonical: &str) -> Query {
    match one_statement_parses_to(sql, canonical) {
        Statement::Query(query) => *query,
        _ => panic!("Expected Query"),
    }
}

/// Ensures that `sql` parses as a single [Select], and is not modified
/// after a serialization round-trip.
pub fn verified_only_select(query: &str) -> Select {
    match verified_query(query).body {
        SetExpr::Select(s) => *s,
        _ => panic!("Expected SetExpr::Select"),
    }
}

/// Ensures that `sql` parses as an expression, and is not modified
/// after a serialization round-trip.
pub fn verified_expr(sql: &str) -> Expr {
    let ast = run_parser_method(sql, |parser| parser.parse_expr()).unwrap();
    assert_eq!(sql, &ast.to_string(), "round-tripping without changes");
    ast
}

pub fn only<T>(v: impl IntoIterator<Item = T>) -> T {
    let mut iter = v.into_iter();
    if let (Some(item), None) = (iter.next(), iter.next()) {
        item
    } else {
        panic!("only called on collection without exactly one item")
    }
}

pub fn expr_from_projection(item: &SelectItem) -> &Expr {
    match item {
        SelectItem::UnnamedExpr(expr) => expr,
        _ => panic!("Expected UnnamedExpr"),
    }
}

pub fn number(n: &'static str) -> Value {
    Value::Number(n.parse().unwrap(), false)
}

pub fn table_alias(name: impl Into<String>) -> Option<TableAlias> {
    Some(TableAlias {
        name: Ident::new(name),
        columns: vec![],
    })
}

pub fn table(name: impl Into<String>) -> TableFactor {
    TableFactor::Table {
        name: ObjectName(vec![Ident::new(name.into())]),
        alias: None,
        args: vec![],
    }
}

pub fn join(relation: TableFactor) -> Join {
    Join {
        relation,
        join_operator: JoinOperator::Inner(JoinConstraint::Natural),
    }
}
