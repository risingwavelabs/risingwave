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

//! Provides E2E Test runner functionality.

use std::collections::HashSet;
use std::path::Path;

use anyhow::anyhow;
#[cfg(madsim)]
use rand_chacha::ChaChaRng;
use risingwave_sqlparser::ast::{
    Cte, Expr, FunctionArgExpr, Join, Query, Select, SetExpr, Statement, TableFactor,
    TableWithJoins, With,
};

use crate::parse_sql;
use crate::utils::{create_file, read_file_contents, write_to_file};

type Result<A> = anyhow::Result<A>;

/// Shrinks a given failing query file.
/// The shrunk query will be written to [`{outdir}/{filename}.reduced.sql`].
pub fn shrink(input_file_path: &str, outdir: &str) -> Result<()> {
    let file_stem = Path::new(input_file_path)
        .file_stem()
        .ok_or_else(|| anyhow!("Failed to stem input file path: {input_file_path}"))?;
    let output_file_path = format!("{outdir}/{}.reduced.sql", file_stem.to_string_lossy());

    let file_contents = read_file_contents(input_file_path)?;
    let sql_statements = parse_sql(file_contents);

    // Session variable before the failing query.
    let session_variable = sql_statements
        .get(sql_statements.len() - 2)
        .filter(|statement| matches!(statement, Statement::SetVariable { .. }));

    let failing_query = sql_statements
        .last()
        .ok_or_else(|| anyhow!("Could not get last sql statement"))?;

    let mut ddl_references = HashSet::new();
    match failing_query {
        Statement::Query(query) | Statement::CreateView { query, .. } => {
            find_ddl_references(query.as_ref(), &mut ddl_references);
            Ok(())
        }
        _ => Err(anyhow!("Last statement was not a query, can't shrink")),
    }?;

    let mut ddl = sql_statements
        .iter()
        .filter(|s| {
            matches!(*s,
            Statement::CreateView { name, .. } | Statement::CreateTable { name, .. }
                if ddl_references.contains(&name.real_value()))
        })
        .collect();

    let mut dml = sql_statements
        .iter()
        .filter(|s| {
            matches!(*s,
            Statement::Insert { table_name, .. }
                if ddl_references.contains(&table_name.real_value()))
        })
        .collect();

    let mut reduced_statements = vec![];
    reduced_statements.append(&mut ddl);
    reduced_statements.append(&mut dml);
    if let Some(session_variable) = session_variable {
        reduced_statements.push(session_variable);
    }
    reduced_statements.push(failing_query);

    let sql = reduced_statements
        .iter()
        .map(|s| format!("{s};\n"))
        .collect::<String>();

    let mut file = create_file(output_file_path).unwrap();
    write_to_file(&mut file, sql)
}

fn find_ddl_references(query: &Query, ddl_references: &mut HashSet<String>) {
    let Query { with, body, .. } = query;
    if let Some(With { cte_tables, .. }) = with {
        for Cte { query, .. } in cte_tables {
            find_ddl_references(query, ddl_references)
        }
        find_ddl_references_in_set_expr(body, ddl_references);
    }
}

fn find_ddl_references_in_set_expr(set_expr: &SetExpr, ddl_references: &mut HashSet<String>) {
    match set_expr {
        SetExpr::Select(box Select { from, .. }) => {
            for table_with_joins in from {
                find_ddl_references_in_table_with_joins(table_with_joins, ddl_references);
            }
        }
        SetExpr::Query(q) => find_ddl_references(q, ddl_references),
        SetExpr::SetOperation { left, right, .. } => {
            find_ddl_references_in_set_expr(left, ddl_references);
            find_ddl_references_in_set_expr(right, ddl_references);
        }
        SetExpr::Values(_) => {}
    }
}

fn find_ddl_references_in_table_with_joins(
    TableWithJoins { relation, joins }: &TableWithJoins,
    ddl_references: &mut HashSet<String>,
) {
    find_ddl_references_in_table_factor(relation, ddl_references);
    for Join { relation, .. } in joins {
        find_ddl_references_in_table_factor(relation, ddl_references);
    }
}

fn find_ddl_references_in_table_factor(
    table_factor: &TableFactor,
    ddl_references: &mut HashSet<String>,
) {
    match table_factor {
        TableFactor::Table { name, .. } => {
            ddl_references.insert(name.real_value());
        }
        TableFactor::Derived { subquery, .. } => find_ddl_references(subquery, ddl_references),
        TableFactor::TableFunction { name, args, .. } => {
            let name = name.real_value();
            if (name == "hop" || name == "tumble") && args.len() > 2 {
                let table_name = &args[0];
                if let FunctionArgExpr::Expr(Expr::Identifier(table_name)) = table_name.get_expr() {
                    ddl_references.insert(table_name.to_string());
                }
            }
        }
        TableFactor::NestedJoin(table_with_joins) => {
            find_ddl_references_in_table_with_joins(table_with_joins, ddl_references);
        }
    }
}
