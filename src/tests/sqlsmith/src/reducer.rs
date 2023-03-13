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
use itertools::Itertools;
use regex::Regex;

type Result<A> = anyhow::Result<A>;

/// Shrinks a given failing query file.
/// The shrunk query will be written to [`{outdir}/{filename}.reduced.sql`].
pub fn shrink_file(input_file_path: &str, outdir: &str) -> Result<()> {
    // read failed sql
    let file_stem = Path::new(input_file_path)
        .file_stem()
        .ok_or_else(|| anyhow!("Failed to stem input file path: {input_file_path}"))?;
    let output_file_path = format!("{outdir}/{}.reduced.sql", file_stem.to_string_lossy());
    let file_contents = read_file_contents(input_file_path)?;

    // reduce failed sql
    let reduced_sql = shrink(&file_contents)?;

    // write reduced sql
    let mut file = create_file(output_file_path).unwrap();
    write_to_file(&mut file, reduced_sql)
}

fn shrink(sql: &str) -> Result<String> {
    let sql_statements = parse_sql(sql);

    // Session variable before the failing query.
    let session_variable = sql_statements
        .get(sql_statements.len() - 2)
        .filter(|statement| matches!(statement, Statement::SetVariable { .. }));

    let failing_query = sql_statements
        .last()
        .ok_or_else(|| anyhow!("Could not get last sql statement"))?;

    let ddl_references = find_ddl_references(&sql_statements);

    tracing::info!("[DDL REFERENCES]: {}", ddl_references.iter().join(", "));

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

    Ok(sql)

}

pub(crate) fn find_ddl_references(sql_statements: &Vec<Statement>) -> HashSet<String> {
    let mut ddl_references = HashSet::new();
    let mut sql_statements = sql_statements.iter().rev();
    let failing = sql_statements.next().unwrap();
    match failing {
        Statement::Query(query) | Statement::CreateView { query, .. } => {
            find_ddl_references_for_query(query.as_ref(), &mut ddl_references);
        },
        _ => {}
    };
    for sql_statement in sql_statements {
         match sql_statement {
            Statement::Query(query) => {
                find_ddl_references_for_query(query.as_ref(), &mut ddl_references);
            },
            Statement::CreateView { query, name, .. } if ddl_references.contains(&name.real_value()) => {
                find_ddl_references_for_query(query.as_ref(), &mut ddl_references);
            }
            _ => {}
        };
    }
    ddl_references
}

pub(crate) fn find_ddl_references_for_query(query: &Query, ddl_references: &mut HashSet<String>) {
    let Query { with, body, .. } = query;
    if let Some(With { cte_tables, .. }) = with {
        for Cte { query, .. } in cte_tables {
            find_ddl_references_for_query(&query, ddl_references)
        }
    }
    find_ddl_references_for_query_in_set_expr(body, ddl_references);
}

fn find_ddl_references_for_query_in_set_expr(set_expr: &SetExpr, ddl_references: &mut HashSet<String>) {
    match set_expr {
        SetExpr::Select(box Select { from, .. }) => {
            for table_with_joins in from {
                find_ddl_references_for_query_in_table_with_joins(table_with_joins, ddl_references);
            }
        }
        SetExpr::Query(q) => find_ddl_references_for_query(q, ddl_references),
        SetExpr::SetOperation { left, right, .. } => {
            find_ddl_references_for_query_in_set_expr(left, ddl_references);
            find_ddl_references_for_query_in_set_expr(right, ddl_references);
        }
        SetExpr::Values(_) => {}
    }
}

fn find_ddl_references_for_query_in_table_with_joins(
    TableWithJoins { relation, joins }: &TableWithJoins,
    ddl_references: &mut HashSet<String>,
) {
    find_ddl_references_for_query_in_table_factor(relation, ddl_references);
    for Join { relation, .. } in joins {
        find_ddl_references_for_query_in_table_factor(&relation, ddl_references);
    }
}

fn find_ddl_references_for_query_in_table_factor(
    table_factor: &TableFactor,
    ddl_references: &mut HashSet<String>,
) {
    match table_factor {
        TableFactor::Table { name, .. } => {
            ddl_references.insert(name.real_value());
        }
        TableFactor::Derived { subquery, .. } => find_ddl_references_for_query(subquery, ddl_references),
        TableFactor::TableFunction { name, args, .. } => {
            let name = name.real_value();
            // https://docs.rs/regex/latest/regex/#grouping-and-flags
            let regex = Regex::new(r"(?i)(tumble|hop)").unwrap();
            if regex.is_match(&name) && args.len() >= 3 {
                let table_name = &args[0];
                if let FunctionArgExpr::Expr(Expr::Identifier(table_name)) = table_name.get_expr() {
                    ddl_references.insert(table_name.to_string().to_lowercase());
                }
            }
        }
        TableFactor::NestedJoin(table_with_joins) => {
            find_ddl_references_for_query_in_table_with_joins(table_with_joins, ddl_references);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DDL_AND_DML: &str = "
CREATE TABLE T1 (V1 INT, V2 INT, V3 INT);
CREATE TABLE T2 (V1 INT, V2 INT, V3 INT);
CREATE TABLE T3 (V1 timestamp, V2 INT, V3 INT);
CREATE MATERIALIZED VIEW M1 AS SELECT * FROM T1;
CREATE MATERIALIZED VIEW M2 AS SELECT * FROM T2 LEFT JOIN T3 ON T2.V1 = T3.V2;
CREATE MATERIALIZED VIEW M3 AS SELECT * FROM T1 LEFT JOIN T2;
CREATE MATERIALIZED VIEW M4 AS SELECT * FROM M3;
INSERT INTO T1 VALUES(0, 0, 1);
INSERT INTO T1 VALUES(0, 0, 2);
INSERT INTO T2 VALUES(0, 0, 3);
INSERT INTO T2 VALUES(0, 0, 4);
INSERT INTO T3 VALUES (TIMESTAMP '00:00:00', 0, 5);
INSERT INTO T3 VALUES (TIMESTAMP '00:00:00', 0, 6);
SET RW_TWO_PHASE_AGG=TRUE;
    ";

    fn sql_to_query(sql: &str) -> Box<Query> {
        let sql_statement = parse_sql(sql).into_iter().next().unwrap();
        match sql_statement {
            Statement::Query(query) | Statement::CreateView { query, .. } => query,
            _ => panic!("Last statement was not a query, can't shrink"),
        }
    }

    #[test]
    fn test_find_ddl_references_for_query_simple() {
        let sql = "SELECT * FROM T1;";
        let query = sql_to_query(sql);
        let mut ddl_references = HashSet::new();
        find_ddl_references_for_query(&query, &mut ddl_references);
        println!("{:#?}", ddl_references);
        assert!(ddl_references.contains("t1"));
    }

    #[test]
    fn test_find_ddl_references_for_tumble() {
        let sql = "SELECT * FROM TUMBLE(T3, V1, INTERVAL '3' DAY);";
        let query = sql_to_query(sql);
        let mut ddl_references = HashSet::new();
        find_ddl_references_for_query(&query, &mut ddl_references);
        println!("{:#?}", ddl_references);
        assert!(ddl_references.contains("t3"));
    }

    #[test]
    fn test_find_ddl_references_for_query_with_cte() {
        let sql = "WITH WITH0 AS (SELECT * FROM M3) SELECT * FROM WITH0";
        let sql_statements = DDL_AND_DML.to_owned() + sql;
        let sql_statements = parse_sql(sql_statements);
        let ddl_references = find_ddl_references(&sql_statements);
        assert!(ddl_references.contains("m3"));
        assert!(ddl_references.contains("t1"));
        assert!(ddl_references.contains("t2"));

        assert!(!ddl_references.contains("m4"));
        assert!(!ddl_references.contains("t3"));
        assert!(!ddl_references.contains("m1"));
        assert!(!ddl_references.contains("m2"));
    }

    #[test]
    fn test_find_ddl_references_for_query_with_mv_on_mv() {
        let sql = "WITH WITH0 AS (SELECT * FROM M4) SELECT * FROM WITH0";
        let sql_statements = DDL_AND_DML.to_owned() + sql;
        let sql_statements = parse_sql(sql_statements);
        let ddl_references = find_ddl_references(&sql_statements);
        assert!(ddl_references.contains("m4"));
        assert!(ddl_references.contains("m3"));
        assert!(ddl_references.contains("t1"));
        assert!(ddl_references.contains("t2"));

        assert!(!ddl_references.contains("t3"));
        assert!(!ddl_references.contains("m1"));
        assert!(!ddl_references.contains("m2"));
    }

    #[test]
    fn test_find_ddl_references_for_query_joins() {
        let sql = "SELECT * FROM (T1 JOIN T2 ON T1.V1 = T2.V2) JOIN T3 ON T2.V1 = T3.V2";
        let sql_statements = DDL_AND_DML.to_owned() + sql;
        let sql_statements = parse_sql(sql_statements);
        let ddl_references = find_ddl_references(&sql_statements);
        assert!(ddl_references.contains("t1"));
        assert!(ddl_references.contains("t2"));
        assert!(ddl_references.contains("t3"));

        assert!(!ddl_references.contains("m1"));
        assert!(!ddl_references.contains("m2"));
        assert!(!ddl_references.contains("m3"));
        assert!(!ddl_references.contains("m4"));
    }

    #[test]
    fn test_shrink_values() {
        let query = "SELECT 1;";
        let sql = DDL_AND_DML.to_owned() + query;
        let expected = format!("\
SET RW_TWO_PHASE_AGG = true;
{query}
");
        assert_eq!(expected, shrink(&sql).unwrap());
    }

    #[test]
    fn test_shrink_simple_table() {
        let query = "SELECT * FROM t1;";
        let sql = DDL_AND_DML.to_owned() + query;
        let expected = format!("\
CREATE TABLE T1 (V1 INT, V2 INT, V3 INT);
INSERT INTO T1 VALUES (0, 0, 1);
INSERT INTO T1 VALUES (0, 0, 2);
SET RW_TWO_PHASE_AGG = true;
{query}
");
        assert_eq!(expected, shrink(&sql).unwrap());
    }

    #[test]
    fn test_shrink_simple_table_with_alias() {
        let query = "SELECT * FROM t1 AS s1;";
        let sql = DDL_AND_DML.to_owned() + query;
        let expected = format!("\
CREATE TABLE T1 (V1 INT, V2 INT, V3 INT);
INSERT INTO T1 VALUES (0, 0, 1);
INSERT INTO T1 VALUES (0, 0, 2);
SET RW_TWO_PHASE_AGG = true;
{query}
");
        assert_eq!(expected, shrink(&sql).unwrap());
    }

    #[test]
    fn test_shrink_join() {
        let query = "SELECT * FROM (T1 JOIN T2 ON T1.V1 = T2.V2) JOIN T3 ON T2.V1 = T3.V2;";
        let sql = DDL_AND_DML.to_owned() + query;
        let expected = format!("\
CREATE TABLE T1 (V1 INT, V2 INT, V3 INT);
CREATE TABLE T2 (V1 INT, V2 INT, V3 INT);
CREATE TABLE T3 (V1 TIMESTAMP, V2 INT, V3 INT);
INSERT INTO T1 VALUES (0, 0, 1);
INSERT INTO T1 VALUES (0, 0, 2);
INSERT INTO T2 VALUES (0, 0, 3);
INSERT INTO T2 VALUES (0, 0, 4);
INSERT INTO T3 VALUES (TIMESTAMP '00:00:00', 0, 5);
INSERT INTO T3 VALUES (TIMESTAMP '00:00:00', 0, 6);
SET RW_TWO_PHASE_AGG = true;
{query}
");
        assert_eq!(expected, shrink(&sql).unwrap());
    }

    #[test]
    fn test_shrink_tumble() {
        let query = "SELECT * FROM TUMBLE(T3, V1, INTERVAL '3' DAY);";
        let sql = DDL_AND_DML.to_owned() + query;
        let expected = format!("\
CREATE TABLE T3 (V1 TIMESTAMP, V2 INT, V3 INT);
INSERT INTO T3 VALUES (TIMESTAMP '00:00:00', 0, 5);
INSERT INTO T3 VALUES (TIMESTAMP '00:00:00', 0, 6);
SET RW_TWO_PHASE_AGG = true;
{query}
");
        assert_eq!(expected, shrink(&sql).unwrap());
    }

    #[test]
    fn test_shrink_subquery() {

    }

    #[test]
    fn test_shrink_mview() {

    }
}