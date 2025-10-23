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
//! This module provides path-based SQL query reduction using systematic
//! path enumeration and rules-based transformations to systematically
//! reduce SQL queries with better coverage and control.

use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;

use anyhow::{Result, anyhow};

use crate::parse_sql;
use crate::sqlreduce::checker::Checker;
use crate::sqlreduce::path::{
    AstNode, ast_node_to_statement, enumerate_reduction_paths, statement_to_ast_node,
};
use crate::sqlreduce::rules::{
    ReductionCandidate, ReductionRules, apply_reduction_operation, generate_reduction_candidates,
};

type BatchReductionResult = Option<(AstNode, usize, String, usize)>;

type BatchReductionFuture<'a> = Pin<Box<dyn Future<Output = BatchReductionResult> + 'a>>;

pub struct Reducer {
    rules: ReductionRules,
    checker: Checker,
}

impl Reducer {
    pub fn new(checker: Checker) -> Self {
        Self {
            rules: ReductionRules::default(),
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

        tracing::info!("Beginning path-based reduction");
        let reduced_sql = self.reduce_path_based(&failing_query.to_string()).await;

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

    /// Path-based reduction approach using systematic AST traversal with batching optimization.
    ///
    /// This method:
    /// 1. Enumerates all reduction paths in the AST
    /// 2. Generates reduction candidates based on rules
    /// 3. Applies candidates in batches (with binary search fallback) to reduce validation overhead
    /// 4. Uses a seen-query cache to avoid redundant checks
    ///
    /// Optimization: Instead of validating after each single change, we try applying
    /// multiple changes at once, and use binary search to find the largest valid batch.
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

            // Try applying candidates in batches with binary search fallback
            let mut candidate_offset = 0;
            while candidate_offset < candidates.len() {
                // Start with an adaptive batch size (increase as we succeed)
                let initial_batch_size = if found_reduction { 16 } else { 8 };
                let remaining = candidates.len() - candidate_offset;
                let max_batch = remaining.min(32); // Cap at 32 to avoid too large batches

                tracing::debug!(
                    "Attempting batch reduction starting at candidate {} (remaining: {})",
                    candidate_offset,
                    remaining
                );

                // Try to apply a batch of candidates
                match self
                    .try_batch_reduction(
                        &ast_node,
                        &candidates[candidate_offset..],
                        initial_batch_size.min(max_batch),
                        sql,
                        &mut seen_queries,
                        &mut candidate_index,
                    )
                    .await
                {
                    Some((new_ast, batch_size, _new_sql, new_len)) => {
                        tracing::info!(
                            "✓ Valid batch reduction found! Applied {} candidates, SQL len {} → {}",
                            batch_size,
                            sql_len,
                            new_len
                        );
                        ast_node = new_ast;
                        sql_len = new_len;
                        found_reduction = true;
                        candidate_offset += batch_size;
                    }
                    None => {
                        // This batch didn't work, skip to next candidate
                        candidate_offset += 1;
                    }
                }
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

    /// Try to apply a batch of candidates with binary search fallback.
    ///
    /// Returns `Some((new_ast, batch_size, new_sql, new_len))` if successful, `None` otherwise.
    fn try_batch_reduction<'a>(
        &'a mut self,
        ast_node: &'a AstNode,
        candidates: &'a [ReductionCandidate],
        batch_size: usize,
        original_sql: &'a str,
        seen_queries: &'a mut HashSet<String>,
        candidate_index: &'a mut usize,
    ) -> BatchReductionFuture<'a> {
        Box::pin(async move {
            if candidates.is_empty() || batch_size == 0 {
                return None;
            }

            let actual_batch_size = batch_size.min(candidates.len());
            tracing::debug!(
                "Trying batch of {} candidates (priorities: {})",
                actual_batch_size,
                candidates[..actual_batch_size]
                    .iter()
                    .map(|c| c.operation.priority())
                    .collect::<Vec<_>>()
                    .iter()
                    .map(|p| p.to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            );

            // Try applying all candidates in the batch
            let mut current_ast = ast_node.clone();
            let mut applied_count = 0;

            for candidate in &candidates[..actual_batch_size] {
                *candidate_index += 1;

                if let Some(new_ast) = apply_reduction_operation(&current_ast, candidate) {
                    current_ast = new_ast;
                    applied_count += 1;
                    tracing::debug!(
                        "Applied candidate #{} (priority: {}, op: {:?})",
                        *candidate_index,
                        candidate.operation.priority(),
                        candidate.operation
                    );
                } else {
                    tracing::debug!(
                        "Skipping candidate #{} (priority: {}, failed to apply): {:?}",
                        *candidate_index,
                        candidate.operation.priority(),
                        candidate
                    );
                }
            }

            if applied_count == 0 {
                tracing::debug!("No candidates in batch could be applied");
                return None;
            }

            // Convert to SQL
            let Some(new_stmt) = ast_node_to_statement(&current_ast) else {
                tracing::debug!("Failed to convert batch-reduced AST to statement");
                return None;
            };

            let new_sql = new_stmt.to_string();
            let new_len = new_sql.len();

            // Check if smaller and not seen
            let orig_stmt = ast_node_to_statement(ast_node)?;
            let orig_len = orig_stmt.to_string().len();
            if new_len >= orig_len {
                tracing::debug!("Batch result not smaller, skipping");
                return None;
            }

            if seen_queries.contains(&new_sql) {
                tracing::debug!("Batch result already seen, skipping");
                return None;
            }

            tracing::debug!(
                "Batch applied {} operations, validating result (len: {})",
                applied_count,
                new_len
            );

            // Validate the batch result
            if self
                .checker
                .is_failure_preserved(original_sql, &new_sql)
                .await
            {
                seen_queries.insert(new_sql.clone());
                return Some((current_ast, actual_batch_size, new_sql, new_len));
            }

            // Validation failed - try binary search if batch size > 1
            if actual_batch_size > 1 {
                tracing::debug!(
                    "Batch validation failed, trying binary search (left half: {})",
                    actual_batch_size / 2
                );

                // Try left half
                if let Some(result) = self
                    .try_batch_reduction(
                        ast_node,
                        candidates,
                        actual_batch_size / 2,
                        original_sql,
                        seen_queries,
                        candidate_index,
                    )
                    .await
                {
                    return Some(result);
                }

                // Try right half
                let right_start = actual_batch_size / 2;
                if right_start < actual_batch_size {
                    tracing::debug!(
                        "Left half failed, trying right half starting at candidate {}",
                        right_start
                    );

                    if let Some(result) = self
                        .try_batch_reduction(
                            ast_node,
                            &candidates[right_start..],
                            actual_batch_size - right_start,
                            original_sql,
                            seen_queries,
                            candidate_index,
                        )
                        .await
                    {
                        return Some(result);
                    }
                }
            }

            tracing::debug!("Batch and binary search all failed");
            None
        })
    }
}
