// Copyright 2025 RisingWave Labs
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

use std::sync::Arc;

use pgwire::pg_response::StatementType;
use risingwave_sqlparser::ast::{DataType, Expr, Ident, Statement};
use risingwave_sqlparser::parser::Parser;
use thiserror_ext::AsReport;

use super::HandlerArgs;
use crate::error::{ErrorCode, Result};
use crate::handler::RwPgResponse;
use crate::session::PreparedStatementEntry;

/// Handles simple-query-mode `PREPARE name [ ( data_type [, ...] ) ] AS statement`.
///
/// The statement is stored as its rendered SQL text (rather than the parsed AST), keyed by
/// name in the current session. `EXECUTE` later substitutes `$n` parameters into this text
/// and re-parses/re-dispatches it.
pub fn handle_prepare(
    handler_args: HandlerArgs,
    name: Ident,
    data_types: Vec<DataType>,
    statement: Box<Statement>,
) -> Result<RwPgResponse> {
    let entry = PreparedStatementEntry {
        data_types,
        statement_sql: statement.to_string(),
    };
    handler_args
        .session
        .create_simple_prepared_statement(name.real_value(), entry)
        .map_err(ErrorCode::ProtocolError)?;

    Ok(RwPgResponse::builder(StatementType::PREPARE).into())
}

/// Handles simple-query-mode `DEALLOCATE [ PREPARE ] { name | ALL }`.
pub fn handle_deallocate(
    handler_args: HandlerArgs,
    name: Option<Ident>,
    _prepare: bool,
) -> Result<RwPgResponse> {
    let name = name.map(|n| n.real_value());
    handler_args
        .session
        .deallocate_simple_prepared_statement(name.as_deref());

    Ok(RwPgResponse::builder(StatementType::DEALLOCATE).into())
}

/// Handles simple-query-mode `EXECUTE name [ ( parameter [, ...] ) ]`.
///
/// Looks up the statement text stored by a previous `PREPARE`, substitutes the `$n`
/// placeholders with the textual SQL rendering of the given parameter expressions, re-parses
/// the result, and dispatches it exactly as if the user had typed it directly.
///
/// Note: substitution is purely textual and skips the contents of single-quoted string
/// literals; it does not perform PostgreSQL-style type coercion of parameters against the
/// declared `data_types`.
pub async fn handle_execute(
    handler_args: HandlerArgs,
    name: Ident,
    parameters: Vec<Expr>,
) -> Result<RwPgResponse> {
    let name = name.real_value();
    let entry = handler_args
        .session
        .get_simple_prepared_statement(&name)
        .ok_or_else(|| {
            ErrorCode::ProtocolError(format!("prepared statement \"{name}\" does not exist"))
        })?;

    let params: Vec<String> = parameters.iter().map(|e| e.to_string()).collect();
    let substituted_sql = substitute_parameters(&entry.statement_sql, &params)?;

    let mut parsed = Parser::parse_sql(&substituted_sql).map_err(|e| {
        ErrorCode::InternalError(format!(
            "failed to re-parse prepared statement \"{name}\" after substituting parameters: {}",
            e.as_report()
        ))
    })?;
    if parsed.len() != 1 {
        return Err(ErrorCode::InternalError(format!(
            "prepared statement \"{name}\" did not resolve to exactly one statement"
        ))
        .into());
    }
    let stmt = parsed.remove(0);

    // Use `dispatch_statement` rather than `handle` here: an implicit transaction was already
    // begun for the outer `EXECUTE` statement by `handle`, and beginning a second one on the
    // same session would panic.
    Box::pin(super::dispatch_statement(
        handler_args.session.clone(),
        stmt,
        Arc::from(substituted_sql.as_str()),
        vec![],
    ))
    .await
}

/// Replaces `$n` placeholders in `sql` with `params[n - 1]`, skipping the contents of
/// single-quoted string literals (with `''` treated as an escaped quote, matching SQL syntax).
fn substitute_parameters(sql: &str, params: &[String]) -> Result<String> {
    let mut out = String::with_capacity(sql.len());
    let mut chars = sql.chars().peekable();
    let mut in_string = false;

    while let Some(c) = chars.next() {
        if in_string {
            out.push(c);
            if c == '\'' {
                in_string = false;
            }
            continue;
        }
        if c == '\'' {
            in_string = true;
            out.push(c);
            continue;
        }
        if c == '$' && chars.peek().is_some_and(|c| c.is_ascii_digit()) {
            let mut digits = String::new();
            while let Some(d) = chars.peek()
                && d.is_ascii_digit()
            {
                digits.push(*d);
                chars.next();
            }
            let idx: usize = digits.parse().unwrap();
            let value = params.get(idx.wrapping_sub(1)).ok_or_else(|| {
                ErrorCode::ProtocolError(format!(
                    "no parameter was provided for placeholder ${digits}"
                ))
            })?;
            out.push_str(value);
        } else {
            out.push(c);
        }
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_substitute_parameters_basic() {
        let sql = "SELECT * FROM t WHERE a = $1 AND b = $2";
        let params = vec!["1".to_owned(), "'foo'".to_owned()];
        assert_eq!(
            substitute_parameters(sql, &params).unwrap(),
            "SELECT * FROM t WHERE a = 1 AND b = 'foo'"
        );
    }

    #[test]
    fn test_substitute_parameters_double_digit_index() {
        let sql = "SELECT $1, $10";
        let params: Vec<String> = (1..=10).map(|i| i.to_string()).collect();
        assert_eq!(substitute_parameters(sql, &params).unwrap(), "SELECT 1, 10");
    }

    #[test]
    fn test_substitute_parameters_skips_string_literals() {
        let sql = "SELECT '$1' , $1";
        let params = vec!["42".to_owned()];
        assert_eq!(
            substitute_parameters(sql, &params).unwrap(),
            "SELECT '$1' , 42"
        );
    }

    #[test]
    fn test_substitute_parameters_missing_param_errors() {
        let sql = "SELECT $1, $2";
        let params = vec!["1".to_owned()];
        assert!(substitute_parameters(sql, &params).is_err());
    }
}
