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

//! SQL reduction framework for `RisingWave`.
//!
//! This module provides two approaches for reducing SQL queries:
//! 1. `PassBased` reduction: Traditional approach using multiple reduction passes
//! 2. `PathBased` reduction: Advanced approach using systematic path enumeration
//!
//! The `PathBased` approach uses path enumeration and rules-based transformations
//! to systematically reduce SQL queries with better coverage and control.

use std::collections::HashSet;

use anyhow::{Result, anyhow};

use crate::parse_sql;
use crate::sqlreduce::checker::Checker;
// Keep pass-based approach for backward compatibility
use crate::sqlreduce::passes::pullup::{
    ArrayPullup, BinaryOperatorPullup, CasePullup, RowPullup, SetOperationPullup,
};
use crate::sqlreduce::passes::remove::{
    FromRemove, GroupByRemove, HavingRemove, OrderByRemove, SelectItemRemove, WhereRemove,
};
use crate::sqlreduce::passes::replace::{NullReplace, ScalarReplace};
use crate::sqlreduce::passes::{Strategy, Transform};
use crate::sqlreduce::path::{
    ast_node_to_statement, enumerate_reduction_paths, statement_to_ast_node,
};
use crate::sqlreduce::rules::{
    ReductionRules, apply_reduction_operation, generate_reduction_candidates,
};

/// Reduction mode determines which approach to use.
#[derive(Debug, Clone)]
pub enum ReductionMode {
    /// Use the new path-based reduction approach (default)
    PathBased,
    /// Use the traditional pass-based reduction approach
    PassBased,
}

pub struct Reducer {
    // PassBased mode fields
    transforms: Vec<Box<dyn Transform>>,
    strategy: Strategy,

    // New path-based fields
    rules: ReductionRules,
    mode: ReductionMode,

    // Shared checker
    checker: Checker,
}

impl Reducer {
    pub fn new(checker: Checker, strategy: Strategy) -> Self {
        Self::new_with_mode(checker, strategy, ReductionMode::PathBased)
    }

    pub fn new_with_mode(checker: Checker, strategy: Strategy, mode: ReductionMode) -> Self {
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
            strategy,
            rules: ReductionRules::default(),
            mode,
            checker,
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
    /// - `sql`: A SQL string with multiple statements (e.g., setup + failing query).
    ///
    /// # Returns
    /// - A simplified version of the last statement that still fails in the same way.
    /// - The preceding statements are also returned as a string.
    ///
    /// # Errors
    /// - Returns an error if SQL parsing fails or if no statements are found.
    /// - Panics if the checker fails to validate failure preservation on the original failing query.
    pub async fn reduce(&mut self, sql: &str) -> Result<String> {
        tracing::info!("Preparing schema");
        self.checker.prepare_schema().await;

        tracing::info!("Starting reduction");
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

        tracing::info!("Beginning fixed-point reduction");
        let reduced_sql = match self.mode {
            ReductionMode::PathBased => self.reduce_path_based(&failing_query.to_string()).await,
            ReductionMode::PassBased => {
                self.reduce_until_fixed_point(&failing_query.to_string())
                    .await
            }
        };

        tracing::info!("Reduction complete");

        let mut reduced_sqls = String::new();
        for s in proceeding_stmts {
            reduced_sqls.push_str(&s.to_string());
            reduced_sqls.push_str(";\n");
        }
        reduced_sqls.push_str(&reduced_sql);
        reduced_sqls.push_str(";\n");

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
    async fn reduce_until_fixed_point(&mut self, sql: &str) -> String {
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
                    tracing::debug!("Transform iteration starting at index {}", idx);

                    let items = trans.transform(ast.clone(), idx, self.strategy.clone());

                    for (new_ast, i) in items {
                        let ast_sql = ast.to_string();
                        let new_ast_sql = new_ast.to_string();
                        tracing::debug!("SQL changes from\n{}\nto\n{}", ast_sql, new_ast_sql);
                        if new_ast_sql.len() < sql_len {
                            tracing::debug!(
                                "Candidate reduction found: len {} → {}",
                                sql_len,
                                new_ast_sql.len()
                            );
                            if self
                                .checker
                                .is_failure_preserved(&ast_sql, &new_ast_sql)
                                .await
                            {
                                tracing::info!(
                                    "Valid reduction applied at index {} ({} → {})",
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
                                tracing::debug!("Reduction not valid; failure not preserved");
                            }
                        }
                    }
                }
            }
            tracing::info!("Global iteration {} complete", iteration);
        }

        ast.to_string()
    }

    /// Path-based reduction approach using systematic AST traversal.
    ///
    /// This method:
    /// 1. Enumerates all reduction paths in the AST
    /// 2. Generates reduction candidates based on rules
    /// 3. Applies candidates in fixed-point fashion until no more reductions are possible
    /// 4. Uses a seen-query cache to avoid redundant checks
    async fn reduce_path_based(&mut self, sql: &str) -> String {
        let sql_statements = parse_sql(sql);
        let mut ast_node = statement_to_ast_node(&sql_statements[0]);
        let mut seen_queries = HashSet::new();
        let mut iteration = 0;
        let mut sql_len = sql.len();
        let mut candidate_index = 0;

        // Track the original query
        seen_queries.insert(sql.to_owned());

        tracing::info!(
            "Starting path-based reduction with initial SQL length: {}",
            sql_len
        );

        loop {
            iteration += 1;
            tracing::info!("Path-based iteration {} starting", iteration);
            let mut found_reduction = false;

            // Enumerate all paths in the current AST
            let paths = enumerate_reduction_paths(&ast_node, vec![]);
            tracing::debug!("Found {} reduction paths in AST", paths.len());

            // Generate reduction candidates
            let candidates = generate_reduction_candidates(&ast_node, &self.rules, &paths);
            tracing::debug!("Generated {} reduction candidates", candidates.len());

            // Try applying each candidate
            for (i, candidate) in candidates.iter().enumerate() {
                candidate_index += 1;
                tracing::debug!(
                    "Trying candidate {} of {} (global #{}): {:?}",
                    i + 1,
                    candidates.len(),
                    candidate_index,
                    candidate
                );

                let Some(new_ast) = apply_reduction_operation(&ast_node, candidate) else {
                    tracing::debug!("Failed to apply reduction operation");
                    continue;
                };

                let Some(new_stmt) = ast_node_to_statement(&new_ast) else {
                    tracing::debug!("Failed to convert reduced AST back to statement");
                    continue;
                };

                let new_sql = new_stmt.to_string();
                let new_len = new_sql.len();

                tracing::debug!(
                    "Generated candidate SQL with length: {} (reduction: {})",
                    new_len,
                    sql_len as i32 - new_len as i32
                );

                // Only consider if it's actually smaller and we haven't seen it
                if new_len >= sql_len {
                    tracing::debug!(
                        "Candidate not smaller ({} >= {}), skipping",
                        new_len,
                        sql_len
                    );
                    continue;
                }

                if seen_queries.contains(&new_sql) {
                    tracing::debug!("Candidate already seen, skipping");
                    continue;
                }

                tracing::debug!(
                    "SQL changes from:\n{}\nto:\n{}",
                    ast_node_to_statement(&ast_node)
                        .map(|s| s.to_string())
                        .unwrap_or_else(|| "<failed to convert AST to statement>".to_owned()),
                    new_sql
                );

                seen_queries.insert(new_sql.clone());

                // Check if the failure is preserved
                tracing::debug!("Checking if failure is preserved");
                if !self.checker.is_failure_preserved(sql, &new_sql).await {
                    tracing::debug!("Reduction not valid; failure not preserved");
                    continue;
                }

                tracing::info!("✓ Valid reduction found! SQL len {} → {}", sql_len, new_len);
                tracing::info!("Applying candidate and continuing to next iteration");
                ast_node = new_ast;
                sql_len = new_len;
                found_reduction = true;
                break;
            }

            if !found_reduction {
                tracing::info!(
                    "Path-based iteration {} complete: no valid reductions found",
                    iteration
                );
                tracing::info!(
                    "Path-based reduction finished after {} iterations",
                    iteration
                );
                tracing::info!(
                    "Final SQL length: {} (reduced by {} characters)",
                    sql_len,
                    sql.len() as i32 - sql_len as i32
                );
                break;
            } else {
                tracing::debug!(
                    "Path-based iteration {} complete: found valid reduction, continuing",
                    iteration
                );
            }
        }

        let final_sql = ast_node_to_statement(&ast_node)
            .map(|s| s.to_string())
            .unwrap_or_else(|| sql.to_owned());

        tracing::info!(
            "Path-based reduction complete. Processed {} total candidates across {} iterations",
            candidate_index,
            iteration
        );

        final_sql
    }
}
