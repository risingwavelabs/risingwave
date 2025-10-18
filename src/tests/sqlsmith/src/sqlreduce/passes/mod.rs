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

use std::fmt;

use risingwave_sqlparser::ast::{Query, Statement};

pub mod pullup;
pub mod remove;
pub mod replace;

pub type Ast = Statement;

#[derive(Debug, Clone)]
pub enum Strategy {
    Single,             // one-at-a-time
    Aggressive,         // all combinations
    Consecutive(usize), // delete k consecutive elements
}

/// A transformation that can reduce parts of a SQL AST while preserving failure behavior.
///
/// A `Transform` operates by identifying **reduction points** in the AST—locations where
/// a simplification or mutation can be safely attempted—and then applying those changes.
///
/// ### Reduction Points
///
/// A **reduction point** is an index identifying a target element (e.g., a SELECT item,
/// a WHERE clause, or a binary operator) that can be removed, replaced, or mutated.
///
/// #### Example:
///
/// - For a `SELECT` list:
///   ```sql
///   SELECT a + b, c, d FROM t;
///             ^    ^
///             |    └── reduction point 1 (c)
///             └────── reduction point 0 (a + b)
///   ```
pub trait Transform: Send + Sync {
    fn name(&self) -> String;

    /// This function analyzes the given SQL AST and returns all the reduction points where
    /// the transformation might be applicable.
    ///
    /// # Arguments
    /// - `ast`: The SQL AST to analyze.
    ///
    /// # Returns
    /// - A list of reduction points where the transformation might be applicable.
    ///
    /// Implementors should return a list of all applicable reduction indices for their transform.
    fn get_reduction_points(&self, ast: Ast) -> Vec<usize>;

    /// Applies the transformation to the AST at the given reduction points.
    ///
    /// # Arguments
    /// - `ast`: The SQL AST to apply the transformation to.
    /// - `reduction_points`: The list of reduction points to apply the transformation to.
    ///
    /// # Returns
    /// - The modified AST.
    fn apply_on(&self, ast: Ast, reduction_points: &[usize]) -> Ast;

    /// Applies the transformation to the AST at the given reduction points.
    ///
    /// # Arguments
    /// - `ast`: The SQL AST to apply the transformation to.
    /// - `idx`: The index of the reduction point to apply the transformation to.
    /// - `strategy`: The strategy to use for applying the transformation.
    fn transform(&self, ast: Ast, idx: usize, strategy: Strategy) -> Vec<(Ast, usize)> {
        let reduction_points = self.get_reduction_points(ast.clone());

        match strategy {
            Strategy::Single => {
                let mut results = Vec::new();
                for (i, rp) in reduction_points.iter().enumerate() {
                    if i < idx {
                        continue;
                    }
                    let new_ast = self.apply_on(ast.clone(), &[*rp]);
                    results.push((new_ast, i));
                }
                results
            }
            Strategy::Aggressive => {
                let mut results = Vec::new();
                let new_ast = self.apply_on(ast.clone(), &reduction_points[idx..]);
                results.push((new_ast, idx));
                results
            }
            Strategy::Consecutive(k) => {
                let mut results = Vec::new();
                if reduction_points.len() >= k {
                    for i in 0..=reduction_points.len() - k {
                        let new_ast =
                            self.apply_on(ast.clone(), &reduction_points[i..i + k]);
                        results.push((new_ast, i));
                    }
                }
                results
            }
        }
    }
}

impl fmt::Display for dyn Transform {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

pub fn extract_query(stmt: &Statement) -> Option<&Query> {
    match stmt {
        Statement::Query(query) => Some(query),
        Statement::CreateView { query, .. } => Some(query),
        _ => None,
    }
}

pub fn extract_query_mut(stmt: &mut Statement) -> Option<&mut Query> {
    match stmt {
        Statement::Query(query) => Some(query),
        Statement::CreateView { query, .. } => Some(query),
        _ => None,
    }
}
