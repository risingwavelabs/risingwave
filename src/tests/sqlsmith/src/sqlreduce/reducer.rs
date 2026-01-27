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

use anyhow::{Result, anyhow};

use crate::parse_sql;
use crate::sqlreduce::checker::Checker;
use crate::sqlreduce::path::{
    ast_node_to_statement, enumerate_reduction_paths, statement_to_ast_node,
};
use crate::sqlreduce::rules::{
    ReductionRules, apply_reduction_operation, generate_reduction_candidates,
};

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

    /// Path-based reduction approach using systematic AST traversal.
    ///
    /// This method:
    /// 1. Enumerates all reduction paths in the AST
    /// 2. Generates reduction candidates based on rules
    /// 3. Applies candidates one-by-one, validating after each successful application
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

            // Try applying each candidate in order, with scoped multi-step removal for same-path operations
            let mut i = 0usize;
            while i < candidates.len() {
                let candidate = &candidates[i];
                candidate_index += 1;
                tracing::debug!(
                    "Trying candidate {} of {} (global #{}): {:?}",
                    i + 1,
                    candidates.len(),
                    candidate_index,
                    candidate
                );

                // Check if we can batch multiple operations of the same type at the same path
                let base_path = &candidate.path;
                let mut batch_applied = false;

                // Try batching based on operation type
                match &candidate.operation {
                    crate::sqlreduce::rules::ReductionOperation::RemoveListElement(_) => {
                        // Collect consecutive RemoveListElement operations on the same list path
                        let mut j = i;
                        let mut group_indices = Vec::new();
                        while j < candidates.len() {
                            if let crate::sqlreduce::rules::ReductionOperation::RemoveListElement(
                                _,
                            ) = &candidates[j].operation
                            {
                                if candidates[j].path == *base_path {
                                    group_indices.push(j);
                                    j += 1;
                                } else {
                                    break; // Different path
                                }
                            } else {
                                break; // Different operation type
                            }
                        }

                        // Use binary search to find the maximum batch size that works
                        if group_indices.len() > 1
                            && let Some((success_ast, success_sql, applied_count)) = self
                                .try_batch_with_binary_search(
                                    &ast_node,
                                    &candidates,
                                    &group_indices,
                                    sql,
                                    sql_len,
                                    &mut seen_queries,
                                    "List-batch",
                                    base_path,
                                )
                                .await
                        {
                            tracing::info!(
                                "✓ Valid list-batch reduction! Removed {} items, SQL len {} → {}",
                                applied_count,
                                sql_len,
                                success_sql.len()
                            );
                            ast_node = success_ast;
                            sql_len = success_sql.len();
                            found_reduction = true;
                            batch_applied = true;
                        }
                    }

                    crate::sqlreduce::rules::ReductionOperation::Remove(_) => {
                        // Collect consecutive Remove operations on the same node path
                        let mut j = i;
                        let mut group_indices = Vec::new();
                        while j < candidates.len() {
                            if let crate::sqlreduce::rules::ReductionOperation::Remove(_) =
                                &candidates[j].operation
                            {
                                if candidates[j].path == *base_path {
                                    group_indices.push(j);
                                    j += 1;
                                } else {
                                    break;
                                }
                            } else {
                                break;
                            }
                        }

                        // Use binary search to find the maximum batch size that works
                        if group_indices.len() > 1
                            && let Some((success_ast, success_sql, applied_count)) = self
                                .try_batch_with_binary_search(
                                    &ast_node,
                                    &candidates,
                                    &group_indices,
                                    sql,
                                    sql_len,
                                    &mut seen_queries,
                                    "Attr-batch",
                                    base_path,
                                )
                                .await
                        {
                            tracing::info!(
                                "✓ Valid attr-batch reduction! Removed {} attributes, SQL len {} → {}",
                                applied_count,
                                sql_len,
                                success_sql.len()
                            );
                            ast_node = success_ast;
                            sql_len = success_sql.len();
                            found_reduction = true;
                            batch_applied = true;
                        }
                    }

                    crate::sqlreduce::rules::ReductionOperation::Replace(_) => {
                        // Collect consecutive Replace operations on the same node path
                        let mut j = i;
                        let mut group_indices = Vec::new();
                        while j < candidates.len() {
                            if let crate::sqlreduce::rules::ReductionOperation::Replace(_) =
                                &candidates[j].operation
                            {
                                if candidates[j].path == *base_path {
                                    group_indices.push(j);
                                    j += 1;
                                } else {
                                    break;
                                }
                            } else {
                                break;
                            }
                        }

                        // Use binary search to find the maximum batch size that works
                        if group_indices.len() > 1
                            && let Some((success_ast, success_sql, applied_count)) = self
                                .try_batch_with_binary_search(
                                    &ast_node,
                                    &candidates,
                                    &group_indices,
                                    sql,
                                    sql_len,
                                    &mut seen_queries,
                                    "Replace-batch",
                                    base_path,
                                )
                                .await
                        {
                            tracing::info!(
                                "✓ Valid replace-batch reduction! Applied {} replacements, SQL len {} → {}",
                                applied_count,
                                sql_len,
                                success_sql.len()
                            );
                            ast_node = success_ast;
                            sql_len = success_sql.len();
                            found_reduction = true;
                            batch_applied = true;
                        }
                    }

                    crate::sqlreduce::rules::ReductionOperation::Pullup(_) => {
                        // Collect consecutive Pullup operations on the same node path
                        let mut j = i;
                        let mut group_indices = Vec::new();
                        while j < candidates.len() {
                            if let crate::sqlreduce::rules::ReductionOperation::Pullup(_) =
                                &candidates[j].operation
                            {
                                if candidates[j].path == *base_path {
                                    group_indices.push(j);
                                    j += 1;
                                } else {
                                    break;
                                }
                            } else {
                                break;
                            }
                        }

                        // Use binary search to find the maximum batch size that works
                        if group_indices.len() > 1
                            && let Some((success_ast, success_sql, applied_count)) = self
                                .try_batch_with_binary_search(
                                    &ast_node,
                                    &candidates,
                                    &group_indices,
                                    sql,
                                    sql_len,
                                    &mut seen_queries,
                                    "Pullup-batch",
                                    base_path,
                                )
                                .await
                        {
                            tracing::info!(
                                "✓ Valid pullup-batch reduction! Applied {} pullups, SQL len {} → {}",
                                applied_count,
                                sql_len,
                                success_sql.len()
                            );
                            ast_node = success_ast;
                            sql_len = success_sql.len();
                            found_reduction = true;
                            batch_applied = true;
                        }
                    }

                    _ => {
                        // TryNull and other operations: no batching
                    }
                }

                // If batch was applied successfully, restart iteration with new AST
                if batch_applied {
                    break;
                }

                // Fallback: try single candidate
                let Some(new_ast) = apply_reduction_operation(&ast_node, candidate) else {
                    tracing::debug!("Failed to apply reduction operation");
                    i += 1;
                    continue;
                };

                let Some(new_stmt) = ast_node_to_statement(&new_ast) else {
                    tracing::debug!("Failed to convert reduced AST back to statement");
                    i += 1;
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
                        "Candidate not smaller ({} >= {}), skipping. Generated SQL: {}",
                        new_len,
                        sql_len,
                        new_sql
                    );
                    i += 1;
                    continue;
                }

                if seen_queries.contains(&new_sql) {
                    tracing::debug!("Candidate already seen, skipping");
                    i += 1;
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
                    i += 1;
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

    /// Try to apply a batch of operations using binary search to find the maximum working batch size.
    ///
    /// Binary search strategy:
    /// - Start with the full batch size
    /// - If it works, return success immediately
    /// - If it fails, binary search for the largest working subset
    ///
    /// Returns: (AST, SQL, `applied_count`) if any batch succeeds, None otherwise
    #[allow(clippy::too_many_arguments)]
    async fn try_batch_with_binary_search(
        &mut self,
        ast_node: &crate::sqlreduce::path::AstNode,
        candidates: &[crate::sqlreduce::rules::ReductionCandidate],
        group_indices: &[usize],
        original_sql: &str,
        sql_len: usize,
        seen_queries: &mut HashSet<String>,
        batch_type: &str,
        base_path: &crate::sqlreduce::path::AstPath,
    ) -> Option<(crate::sqlreduce::path::AstNode, String, usize)> {
        let total = group_indices.len();

        tracing::debug!(
            "{}: Found {} candidates at same path, trying binary search",
            batch_type,
            total
        );

        // Binary search for the maximum working batch size
        let mut left = 2; // Minimum batch size
        let mut right = total;
        let mut best_result: Option<(crate::sqlreduce::path::AstNode, String, usize)> = None;

        while left <= right {
            let mid = (left + right) / 2;

            tracing::debug!(
                "{}: Trying batch size {} (range: {}-{})",
                batch_type,
                mid,
                left,
                right
            );

            // Try to apply this batch size
            let mut tmp_ast = ast_node.clone();
            let mut applied = 0usize;

            for &idx in &group_indices[..mid] {
                if let Some(next_ast) = apply_reduction_operation(&tmp_ast, &candidates[idx]) {
                    tmp_ast = next_ast;
                    applied += 1;
                } else {
                    break;
                }
            }

            if applied >= 2
                && let Some(new_stmt) = ast_node_to_statement(&tmp_ast)
            {
                let new_sql = new_stmt.to_string();
                let new_len = new_sql.len();

                if new_len < sql_len && !seen_queries.contains(&new_sql) {
                    // Check if the failure is preserved
                    if self
                        .checker
                        .is_failure_preserved(original_sql, &new_sql)
                        .await
                    {
                        tracing::debug!(
                            "{}: Batch size {} succeeded, trying larger",
                            batch_type,
                            mid
                        );
                        seen_queries.insert(new_sql.clone());
                        best_result = Some((tmp_ast, new_sql, applied));
                        left = mid + 1; // Try larger batch
                        continue;
                    }
                }
            }

            // If we reach here, this batch size didn't work
            tracing::debug!("{}: Batch size {} failed, trying smaller", batch_type, mid);
            right = mid - 1;
        }

        if let Some((_, ref sql, count)) = best_result {
            tracing::debug!(
                "{}: Binary search found optimal batch size {} at path {} (len: {})",
                batch_type,
                count,
                crate::sqlreduce::path::display_ast_path(base_path),
                sql.len()
            );
        } else {
            tracing::debug!(
                "{}: Binary search found no valid batch at path {}",
                batch_type,
                crate::sqlreduce::path::display_ast_path(base_path)
            );
        }

        best_result
    }
}
