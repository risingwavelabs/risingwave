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

//! SQL reducer that applies a sequence of reduction passes to simplify a failing SQL query
//! while preserving its failure behavior.
//!
//! The reducer works in fixed-point fashion: each transformation pass is applied iteratively
//! until no further simplification is possible. Only transformations that preserve failure
//! behavior (as checked by the `Checker`) are accepted.
//!
//! This is used for SQL test case minimization, debugging, or fuzzing feedback reduction.

use anyhow::{Result, anyhow};

use crate::parse_sql;
use crate::sqlreduce::checker::Checker;
use crate::sqlreduce::passes::pullup::{
    ArrayPullup, BinaryOperatorPullup, CasePullup, RowPullup, SetOperationPullup,
};
use crate::sqlreduce::passes::remove::{
    FromRemove, GroupByRemove, HavingRemove, OrderByRemove, SelectItemRemove, WhereRemove,
};
use crate::sqlreduce::passes::replace::{NullReplace, ScalarReplace};
use crate::sqlreduce::passes::{Strategy, Transform};

pub struct Reducer<'a> {
    transforms: Vec<Box<dyn Transform>>,
    checker: Checker<'a>,
    strategy: Strategy,
}

impl<'a> Reducer<'a> {
    pub fn new(checker: Checker<'a>, strategy: Strategy) -> Self {
        let transforms: Vec<Box<dyn Transform>> = vec![
            Box::new(ScalarReplace),
            Box::new(NullReplace),
            Box::new(GroupByRemove),
            Box::new(OrderByRemove),
            Box::new(WhereRemove),
            Box::new(FromRemove),
            Box::new(SelectItemRemove),
            Box::new(BinaryOperatorPullup),
            Box::new(CasePullup),
            Box::new(RowPullup),
            Box::new(ArrayPullup),
            Box::new(SetOperationPullup),
            Box::new(HavingRemove),
        ];
        Self {
            transforms,
            checker,
            strategy,
        }
    }

    /// Perform reduction on a SQL input containing multiple statements,
    /// where only the **last** statement is considered the failing one.
    ///
    /// The reducer:
    /// 1. Executes all preceding statements using the checker client.
    /// 2. Verifies that the last statement indeed fails (self-check).
    /// 3. Applies transformation passes to simplify the failing query
    ///    while preserving the failure behavior.
    /// 4. Returns the reduced failing SQL query as a string.
    ///
    /// # Arguments
    /// - `sql`: A SQL script with multiple statements (e.g., setup + failing query).
    ///
    /// # Returns
    /// - A simplified version of the last statement that still fails in the same way.
    /// - The preceding statements are also returned as a string.
    ///
    /// # Errors
    /// - Returns an error if SQL parsing fails or if no statements are found.
    /// - Panics if the checker fails to validate failure preservation on the original failing query.
    pub async fn reduce(&mut self, sql: &str) -> Result<String> {
        tracing::info!("Preparing schema...");
        self.checker.prepare_schema().await;

        tracing::info!("Starting reduction...");
        let sql_statements = parse_sql(sql);

        let (failing_query, proceeding_stmts) = sql_statements
            .split_last()
            .ok_or_else(|| anyhow!("No SQL statements found"))?;

        for s in proceeding_stmts {
            tracing::info!("Executing preceding statement: {}", s);
            self.checker.client.simple_query(&s.to_string()).await?;
        }

        if !self
            .checker
            .is_failure_preserved(&failing_query.to_string(), &failing_query.to_string())
            .await
        {
            tracing::error!("Checker failed: failing query does not fail on itself");
            panic!("There is a bug in the checker!")
        }

        tracing::info!("Beginning fixed-point reduction...");
        let reduced_sql = self
            .reduce_until_fixed_point(&failing_query.to_string())
            .await;

        tracing::info!("Reduction complete.");

        let mut reduced_sqls = String::new();
        for s in proceeding_stmts {
            reduced_sqls.push_str(&s.to_string());
            reduced_sqls.push_str(";\n");
        }
        reduced_sqls.push_str(&reduced_sql);

        // Drop the schema after the reduction is complete.
        self.checker.drop_schema().await;

        Ok(reduced_sqls)
    }

    /// Apply all transformations in a fixed-point loop until no further reduction is possible.
    ///
    /// For each transformation:
    /// - Iterate over all applicable reduction points.
    /// - If a smaller version of the query is found and passes the failure check,
    ///   accept it and continue from that point.
    ///
    /// The process continues until a global fixed point is reached (i.e., no transformation
    /// makes progress on any part of the SQL).
    ///
    /// # Arguments
    /// - `sql`: The SQL string (usually the failing query) to reduce.
    ///
    /// # Returns
    /// - A reduced SQL string (still failing) that is minimized w.r.t the current passes.
    async fn reduce_until_fixed_point(&self, sql: &str) -> String {
        let mut global_fixed_point = false;
        let mut ast = parse_sql(sql)[0].clone();
        let mut iteration = 0;

        while !global_fixed_point {
            iteration += 1;
            tracing::info!("Global iteration {} starting", iteration);
            global_fixed_point = true;
            for trans in &self.transforms {
                let mut local_fixed_point = false;
                let mut idx = 0;
                let mut sql_len = ast.to_string().len();
                tracing::info!("Applying transform: {}", trans.name());

                while !local_fixed_point {
                    local_fixed_point = true;
                    tracing::info!("  Transform iteration starting at index {}", idx);

                    let items = trans.transform(ast.clone(), idx, self.strategy.clone());

                    for (new_ast, i) in items {
                        let ast_sql = ast.to_string();
                        let new_ast_sql = new_ast.to_string();
                        tracing::info!("  SQL changes from \n{} \n to \n{}", ast_sql, new_ast_sql);
                        if new_ast_sql.len() < sql_len {
                            tracing::info!(
                                "  Candidate reduction found: len {} → {}",
                                sql_len,
                                new_ast_sql.len()
                            );
                            if self
                                .checker
                                .is_failure_preserved(&ast_sql, &new_ast_sql)
                                .await
                            {
                                tracing::info!(
                                    "    Valid reduction applied at index {} ({} → {})",
                                    i,
                                    sql_len,
                                    new_ast_sql.len()
                                );
                                ast = new_ast;
                                idx = i;
                                local_fixed_point = false;
                                global_fixed_point = false;
                                sql_len = new_ast_sql.len();
                                break;
                            } else {
                                tracing::info!("    Reduction not valid; failure not preserved.");
                            }
                        }
                    }
                }
            }
            tracing::info!("Global iteration {} complete", iteration);
        }

        ast.to_string()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use thiserror_ext::AsReport;
    use tokio_postgres::{Client, NoTls};

    use super::*;

    async fn setup_client() -> Client {
        let (client, connection) = tokio_postgres::Config::new()
            .host("localhost")
            .port(4566)
            .dbname("dev")
            .user("root")
            .password("")
            .connect_timeout(Duration::from_secs(5))
            .connect(NoTls)
            .await
            .unwrap_or_else(|e| panic!("Failed to connect to database: {}", e.as_report()));

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!(error = %e.as_report(), "Postgres connection error");
            }
        });

        client
    }

    fn normalize_sql(sql: &str) -> String {
        parse_sql(sql)
            .into_iter()
            .map(|stmt| stmt.to_string())
            .collect::<Vec<_>>()
            .join("\n")
    }

    #[tokio::test]
    async fn test_reducer_pr21930() {
        let client = setup_client().await;

        let preceding_sql = "
CREATE TABLE alltypes3 (
    c1 boolean,
    c2 smallint,
    c3 integer,
    c4 bigint,
    c5 real,
    c6 double precision,
    c7 numeric,
    c8 date,
    c9 varchar,
    c10 time without time zone,
    c11 timestamp without time zone,
    c13 interval,
    c14 struct < a integer >,
    c15 integer [],
    c16 varchar [],
    WATERMARK FOR c11 AS c11 - INTERVAL '5 seconds',
    PRIMARY KEY (c4)
) APPEND ONLY;
        ";

        let preceding_stmts = parse_sql(preceding_sql);
        let checker = Checker::new(&client, preceding_stmts);

        let failing_sql = "
CREATE MATERIALIZED VIEW m1 AS SELECT t_0.c11 AS col_0, t_0.c11 AS col_1 FROM alltypes3 AS t_0 WHERE t_0.c1 GROUP BY t_0.c11, t_0.c1, t_0.c14, t_0.c4, t_0.c15 HAVING t_0.c1 EMIT ON WINDOW CLOSE;";
        let expected_sql = format!(
            "{}\n{}",
            preceding_sql,
            "CREATE MATERIALIZED VIEW m1 AS SELECT t_0.c11 AS col_0 FROM alltypes3 AS t_0 GROUP BY t_0.c11, t_0.c4 EMIT ON WINDOW CLOSE;"
        );

        let sql = format!("{}\n{}", preceding_sql, failing_sql);
        let mut reducer = Reducer::new(checker, Strategy::Single);
        let reduced_sql = reducer.reduce(&sql).await.unwrap();
        assert_eq!(normalize_sql(&reduced_sql), normalize_sql(&expected_sql));
    }
}
