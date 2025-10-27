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

            // Try applying each candidate in order, with scoped multi-step removal for lists
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

                // If this is a list element removal, try a small batch within the same list path
                if let crate::sqlreduce::rules::ReductionOperation::RemoveListElement(_) =
                    candidate.operation
                {
                    // Collect subsequent removals on the same path (already generated in reverse order)
                    let base_path = &candidate.path;
                    let mut j = i;
                    let mut group_indices = Vec::new();
                    while j < candidates.len() {
                        match &candidates[j].operation {
                            crate::sqlreduce::rules::ReductionOperation::RemoveListElement(_)
                                if candidates[j].path == *base_path =>
                            {
                                group_indices.push(j);
                                j += 1;
                            }
                            _ => break,
                        }
                    }
                    // If this is an attribute removal on the same node, try removing a few attributes together
                    if let crate::sqlreduce::rules::ReductionOperation::Remove(_) =
                        candidate.operation
                    {
                        let base_path = &candidate.path;
                        let mut j = i;
                        let mut group_indices = Vec::new();
                        while j < candidates.len() {
                            match &candidates[j].operation {
                                crate::sqlreduce::rules::ReductionOperation::Remove(_)
                                    if candidates[j].path == *base_path =>
                                {
                                    group_indices.push(j);
                                    j += 1;
                                }
                                _ => break,
                            }
                        }

                        // Try to remove up to 3 attributes at once (conservative)
                        let take_n = group_indices.len().min(3);
                        if take_n > 1 {
                            let mut tmp_ast = ast_node.clone();
                            let mut applied = 0usize;
                            for &idx in &group_indices[..take_n] {
                                if let Some(next_ast) =
                                    apply_reduction_operation(&tmp_ast, &candidates[idx])
                                {
                                    tmp_ast = next_ast;
                                    applied += 1;
                                }
                            }

                            if applied > 1
                                && let Some(new_stmt) = ast_node_to_statement(&tmp_ast)
                            {
                                let new_sql = new_stmt.to_string();
                                let new_len = new_sql.len();

                                if new_len < sql_len && !seen_queries.contains(&new_sql) {
                                    tracing::debug!(
                                        "Attr-batch: removed {} attributes at path {}, validating (len: {})",
                                        applied,
                                        crate::sqlreduce::path::display_ast_path(base_path),
                                        new_len
                                    );

                                    if self.checker.is_failure_preserved(sql, &new_sql).await {
                                        tracing::info!(
                                            "✓ Valid attr-batch reduction! Removed {} attributes, SQL len {} → {}",
                                            applied,
                                            sql_len,
                                            new_len
                                        );
                                        seen_queries.insert(new_sql.clone());
                                        ast_node = tmp_ast;
                                        sql_len = new_len;
                                        found_reduction = true;
                                        break;
                                    }
                                }
                            }
                        }
                    }

                    // If this is a Replace operation on the same node, try multiple replacements
                    if let crate::sqlreduce::rules::ReductionOperation::Replace(_) =
                        candidate.operation
                    {
                        let base_path = &candidate.path;
                        let mut j = i;
                        let mut group_indices = Vec::new();
                        while j < candidates.len() {
                            match &candidates[j].operation {
                                crate::sqlreduce::rules::ReductionOperation::Replace(_)
                                    if candidates[j].path == *base_path =>
                                {
                                    group_indices.push(j);
                                    j += 1;
                                }
                                _ => break,
                            }
                        }

                        // Try up to 2 replace operations together (very conservative)
                        let take_n = group_indices.len().min(2);
                        if take_n > 1 {
                            let mut tmp_ast = ast_node.clone();
                            let mut applied = 0usize;
                            for &idx in &group_indices[..take_n] {
                                if let Some(next_ast) =
                                    apply_reduction_operation(&tmp_ast, &candidates[idx])
                                {
                                    tmp_ast = next_ast;
                                    applied += 1;
                                }
                            }

                            if applied > 1
                                && let Some(new_stmt) = ast_node_to_statement(&tmp_ast)
                            {
                                let new_sql = new_stmt.to_string();
                                let new_len = new_sql.len();

                                if new_len < sql_len && !seen_queries.contains(&new_sql) {
                                    tracing::debug!(
                                        "Replace-batch: applied {} replacements at path {}, validating (len: {})",
                                        applied,
                                        crate::sqlreduce::path::display_ast_path(base_path),
                                        new_len
                                    );

                                    if self.checker.is_failure_preserved(sql, &new_sql).await {
                                        tracing::info!(
                                            "✓ Valid replace-batch reduction! Applied {} replacements, SQL len {} → {}",
                                            applied,
                                            sql_len,
                                            new_len
                                        );
                                        seen_queries.insert(new_sql.clone());
                                        ast_node = tmp_ast;
                                        sql_len = new_len;
                                        found_reduction = true;
                                        break;
                                    }
                                }
                            }
                        }
                    }

                    // If this is a Pullup operation on the same node, try multiple pullups
                    if let crate::sqlreduce::rules::ReductionOperation::Pullup(_) =
                        candidate.operation
                    {
                        let base_path = &candidate.path;
                        let mut j = i;
                        let mut group_indices = Vec::new();
                        while j < candidates.len() {
                            match &candidates[j].operation {
                                crate::sqlreduce::rules::ReductionOperation::Pullup(_)
                                    if candidates[j].path == *base_path =>
                                {
                                    group_indices.push(j);
                                    j += 1;
                                }
                                _ => break,
                            }
                        }

                        // Try up to 2 pullup operations together
                        let take_n = group_indices.len().min(2);
                        if take_n > 1 {
                            let mut tmp_ast = ast_node.clone();
                            let mut applied = 0usize;
                            for &idx in &group_indices[..take_n] {
                                if let Some(next_ast) =
                                    apply_reduction_operation(&tmp_ast, &candidates[idx])
                                {
                                    tmp_ast = next_ast;
                                    applied += 1;
                                }
                            }

                            if applied > 1
                                && let Some(new_stmt) = ast_node_to_statement(&tmp_ast)
                            {
                                let new_sql = new_stmt.to_string();
                                let new_len = new_sql.len();

                                if new_len < sql_len && !seen_queries.contains(&new_sql) {
                                    tracing::debug!(
                                        "Pullup-batch: applied {} pullups at path {}, validating (len: {})",
                                        applied,
                                        crate::sqlreduce::path::display_ast_path(base_path),
                                        new_len
                                    );

                                    if self.checker.is_failure_preserved(sql, &new_sql).await {
                                        tracing::info!(
                                            "✓ Valid pullup-batch reduction! Applied {} pullups, SQL len {} → {}",
                                            applied,
                                            sql_len,
                                            new_len
                                        );
                                        seen_queries.insert(new_sql.clone());
                                        ast_node = tmp_ast;
                                        sql_len = new_len;
                                        found_reduction = true;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    // Try to remove up to 8 elements from this list in one go
                    let take_n = group_indices.len().min(8);
                    if take_n > 1 {
                        let mut tmp_ast = ast_node.clone();
                        let mut applied = 0usize;
                        for &idx in &group_indices[..take_n] {
                            if let Some(next_ast) =
                                apply_reduction_operation(&tmp_ast, &candidates[idx])
                            {
                                tmp_ast = next_ast;
                                applied += 1;
                            }
                        }

                        if applied > 1
                            && let Some(new_stmt) = ast_node_to_statement(&tmp_ast)
                        {
                            let new_sql = new_stmt.to_string();
                            let new_len = new_sql.len();

                            // Only consider if it's actually smaller and we haven't seen it
                            if new_len < sql_len && !seen_queries.contains(&new_sql) {
                                tracing::debug!(
                                    "List-batch: applied {} removals at path {}, validating (len: {})",
                                    applied,
                                    crate::sqlreduce::path::display_ast_path(base_path),
                                    new_len
                                );

                                if self.checker.is_failure_preserved(sql, &new_sql).await {
                                    tracing::info!(
                                        "✓ Valid list-batch reduction! Removed {} items, SQL len {} → {}",
                                        applied,
                                        sql_len,
                                        new_len
                                    );
                                    seen_queries.insert(new_sql.clone());
                                    ast_node = tmp_ast;
                                    sql_len = new_len;
                                    found_reduction = true;
                                    break; // proceed to next outer iteration
                                }
                            }
                        }
                    }
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
                        "Candidate not smaller ({} >= {}), skipping",
                        new_len,
                        sql_len
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
}
