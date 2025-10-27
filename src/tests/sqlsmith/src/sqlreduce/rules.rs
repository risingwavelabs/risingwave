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

//! Rules-based SQL reduction system.
//!
//! This module defines reduction rules and operations for different AST node types,
//! providing configurable reduction behavior for comprehensive SQL simplification.

use std::collections::HashMap;

use risingwave_sqlparser::ast::*;

use crate::sqlreduce::path::{AstField, AstNode, AstPath, PathComponent};

/// Defines what actions can be performed on an AST node during reduction.
#[derive(Debug, Clone, Default)]
pub struct ReductionRule {
    /// Whether to try replacing this node with NULL/None
    pub try_null: bool,
    /// Attributes to descend into for further reduction
    pub descend: Vec<AstField>,
    /// Attributes that can be removed (set to None)
    pub remove: Vec<AstField>,
    /// Attributes whose children can be pulled up to replace this node
    pub pullup: Vec<AstField>,
    /// Attributes whose subtrees can replace this entire node
    pub replace: Vec<AstField>,
}

/// Repository of reduction rules for different AST node types.
/// Configures how different SQL constructs should be reduced.
pub struct ReductionRules {
    rules: HashMap<String, ReductionRule>,
}

impl Default for ReductionRules {
    fn default() -> Self {
        let mut rules = HashMap::new();

        // SelectStmt rules (most important for SQL reduction)
        // Dependency order (independent first, dependent later):
        // 1. Selection (WHERE) - independent, doesn't affect other clauses
        // 2. Having - semi-independent, requires GROUP BY but rarely referenced
        // 3. GroupBy - dependent, SELECT may reference GROUP BY columns
        // 4. Projection (SELECT list) - dependent, may reference GROUP BY
        // 5. From - fundamental, almost everything depends on it
        rules.insert(
            "Select".to_owned(),
            ReductionRule {
                try_null: false,
                descend: vec![
                    AstField::Projection,
                    AstField::From,
                    AstField::Selection,
                    AstField::GroupBy,
                    AstField::Having,
                ],
                remove: vec![
                    AstField::Selection,  // Try removing WHERE first (most independent)
                    AstField::Having,     // Then HAVING
                    AstField::Projection, // Then SELECT list
                    AstField::From,       // Then FROM
                    AstField::GroupBy,    // Then GROUP BY
                ],
                pullup: vec![],
                replace: vec![],
            },
        );

        // Query rules
        rules.insert(
            "Query".to_owned(),
            ReductionRule {
                try_null: false,
                descend: vec![AstField::Body, AstField::With, AstField::OrderBy],
                remove: vec![AstField::With, AstField::OrderBy],
                pullup: vec![],
                replace: vec![AstField::Body],
            },
        );

        // WITH clause rules
        rules.insert(
            "With".to_owned(),
            ReductionRule {
                try_null: true,
                descend: vec![AstField::CteTable],
                remove: vec![],
                pullup: vec![],
                replace: vec![],
            },
        );

        // CTE list rules
        rules.insert(
            "CteList".to_owned(),
            ReductionRule {
                try_null: false,
                descend: vec![], // Individual CTEs accessed via index
                remove: vec![],
                pullup: vec![],
                replace: vec![],
            },
        );

        // CTE rules
        rules.insert(
            "Cte".to_owned(),
            ReductionRule {
                try_null: false,
                descend: vec![AstField::CteInner],
                remove: vec![],
                pullup: vec![AstField::CteInner],
                replace: vec![],
            },
        );

        // Expression rules - focus on pullup for simplification
        rules.insert(
            "BinaryOp".to_owned(),
            ReductionRule {
                try_null: true,
                descend: vec![],
                remove: vec![],
                pullup: vec![AstField::Left, AstField::Right],
                replace: vec![],
            },
        );

        rules.insert(
            "Case".to_owned(),
            ReductionRule {
                try_null: true,
                descend: vec![],
                remove: vec![],
                pullup: vec![AstField::Operand, AstField::ElseResult],
                replace: vec![],
            },
        );

        // Function call rules
        rules.insert(
            "Function".to_owned(),
            ReductionRule {
                try_null: true,
                descend: vec![], // args is not modeled as an AstField in our enum
                remove: vec![],
                pullup: vec![],
                replace: vec![],
            },
        );

        // Subquery rules
        rules.insert(
            "Subquery".to_owned(),
            ReductionRule {
                try_null: true,
                descend: vec![],
                remove: vec![],
                pullup: vec![],
                replace: vec![AstField::Subquery],
            },
        );

        // Constant rules
        rules.insert(
            "Value".to_owned(),
            ReductionRule {
                try_null: true,
                descend: vec![],
                remove: vec![],
                pullup: vec![],
                replace: vec![],
            },
        );

        // List reduction rules for SQL collections
        rules.insert(
            "SelectItemList".to_owned(),
            ReductionRule {
                try_null: false,
                descend: vec![],
                remove: vec![],
                pullup: vec![],
                replace: vec![],
            },
        );

        rules.insert(
            "ExprList".to_owned(),
            ReductionRule {
                try_null: true,
                descend: vec![],
                remove: vec![],
                pullup: vec![],
                replace: vec![],
            },
        );

        rules.insert(
            "TableList".to_owned(),
            ReductionRule {
                try_null: false,
                descend: vec![],
                remove: vec![],
                pullup: vec![],
                replace: vec![],
            },
        );

        rules.insert(
            "OrderByList".to_owned(),
            ReductionRule {
                try_null: false,
                descend: vec![],
                remove: vec![],
                pullup: vec![],
                replace: vec![],
            },
        );

        // JOIN-related rules for better JOIN handling
        rules.insert(
            "TableWithJoins".to_owned(),
            ReductionRule {
                try_null: false,
                descend: vec![AstField::Relation, AstField::Joins],
                remove: vec![AstField::Joins],
                pullup: vec![AstField::Relation],
                replace: vec![AstField::Relation],
            },
        );

        rules.insert(
            "Join".to_owned(),
            ReductionRule {
                try_null: true,
                descend: vec![AstField::Relation],
                remove: vec![],
                pullup: vec![AstField::Relation],
                replace: vec![AstField::Relation],
            },
        );

        rules.insert(
            "TableFactor".to_owned(),
            ReductionRule {
                try_null: true, // Allow trying NULL for derived tables
                descend: vec![AstField::Subquery],
                remove: vec![AstField::Subquery], // Allow removing subquery entirely
                pullup: vec![AstField::Subquery],
                replace: vec![AstField::Subquery],
            },
        );

        rules.insert(
            "JoinList".to_owned(),
            ReductionRule {
                try_null: true, // Allow trying NULL for join lists
                descend: vec![],
                remove: vec![], // Remove individual joins through list operations
                pullup: vec![],
                replace: vec![],
            },
        );

        Self { rules }
    }
}

impl ReductionRules {
    /// Get the reduction rule for a specific node type.
    pub fn get_rule(&self, node_type: &str) -> Option<&ReductionRule> {
        self.rules.get(node_type)
    }

    /// Get the node type string for an AST node.
    pub fn get_node_type(node: &AstNode) -> String {
        match node {
            AstNode::Statement(_) => "Statement".to_owned(),
            AstNode::Query(_) => "Query".to_owned(),
            AstNode::Select(_) => "Select".to_owned(),
            AstNode::Expr(expr) => match expr {
                Expr::BinaryOp { .. } => "BinaryOp".to_owned(),
                Expr::Case { .. } => "Case".to_owned(),
                Expr::Function { .. } => "Function".to_owned(),
                Expr::Subquery(_) => "Subquery".to_owned(),
                Expr::Value(_) => "Value".to_owned(),
                _ => "Expr".to_owned(),
            },
            AstNode::SelectItem(_) => "SelectItem".to_owned(),
            AstNode::TableWithJoins(_) => "TableWithJoins".to_owned(),
            AstNode::Join(_) => "Join".to_owned(),
            AstNode::TableFactor(_) => "TableFactor".to_owned(),
            AstNode::OrderByExpr(_) => "OrderByExpr".to_owned(),
            AstNode::With(_) => "With".to_owned(),
            AstNode::Cte(_) => "Cte".to_owned(),
            AstNode::ExprList(_) => "ExprList".to_owned(),
            AstNode::SelectItemList(_) => "SelectItemList".to_owned(),
            AstNode::TableList(_) => "TableList".to_owned(),
            AstNode::JoinList(_) => "JoinList".to_owned(),
            AstNode::OrderByList(_) => "OrderByList".to_owned(),
            AstNode::CteList(_) => "CteList".to_owned(),
            AstNode::Option(_) => "Option".to_owned(),
        }
    }
}

/// Different types of reduction operations that can be applied.
#[derive(Debug, Clone)]
pub enum ReductionOperation {
    /// Replace node with NULL/None
    TryNull,
    /// Remove a specific attribute (set to None)
    Remove(AstField),
    /// Pull up a subnode to replace this node
    Pullup(AstField),
    /// Replace this node with a subtree
    Replace(AstField),
    /// Remove an element from a list/tuple
    RemoveListElement(usize),
}

impl ReductionOperation {
    /// Get the priority score for this operation (higher = tried first).
    /// Uses dependency-aware priority: independent components removed first,
    /// then dependent components to minimize validation failures.
    pub fn priority(&self) -> i32 {
        match self {
            // List element removal: Very safe, high success rate
            // Removing a single item rarely breaks the query structure
            ReductionOperation::RemoveListElement(_) => 100,

            // Remove operations: Priority based on SQL dependencies
            // Independent clauses get higher priority, dependent ones lower
            ReductionOperation::Remove(field) => {
                use AstField::*;
                match field {
                    // Tier 1: Fully independent clauses (no other clause depends on them)
                    // WHERE is independent - can be removed without affecting structure
                    Selection => 95,
                    // ORDER BY is independent - only affects result ordering
                    OrderBy => 94,
                    // LIMIT/OFFSET are independent
                    Limit | Offset => 93,
                    // WITH/CTE can be removed if not referenced
                    With => 92,

                    // Tier 2: Semi-independent clauses
                    // HAVING depends on aggregations, but often removable
                    Having => 85,

                    // Tier 3: Core dependent clauses (have circular dependencies)
                    // GROUP BY and Projection (SELECT list) depend on each other
                    // Removing these is riskier as they form the core query structure
                    GroupBy => 70,
                    Projection => 65,

                    // FROM is most fundamental - almost everything depends on it
                    From => 60,

                    // Other fields get default priority
                    _ => 80,
                }
            }

            // Replace with subtree: Medium risk, good success rate
            // Simplifies structure while preserving a valid subtree
            ReductionOperation::Replace(_) => 55,

            // Pullup: Medium-low risk, variable success rate
            // Depends heavily on context compatibility
            ReductionOperation::Pullup(_) => 40,

            // Try NULL: High risk, lower success rate
            // Often breaks type constraints or NOT NULL requirements
            ReductionOperation::TryNull => 20,
        }
    }
}

/// A reduction candidate: a path to a node and the operation to apply.
#[derive(Debug, Clone)]
pub struct ReductionCandidate {
    pub path: AstPath,
    pub operation: ReductionOperation,
}

/// Generate all possible reduction candidates for a given AST.
/// Systematically creates all viable reduction operations.
pub fn generate_reduction_candidates(
    root: &AstNode,
    rules: &ReductionRules,
    paths: &[AstPath],
) -> Vec<ReductionCandidate> {
    let mut candidates = Vec::new();

    tracing::debug!("Generating reduction candidates for {} paths", paths.len());

    for (path_idx, path) in paths.iter().enumerate() {
        if let Some(node) = crate::sqlreduce::path::get_node_at_path(root, path) {
            let node_type = ReductionRules::get_node_type(&node);
            let path_str = crate::sqlreduce::path::display_ast_path(path);

            tracing::debug!("Path {}: {} ({})", path_idx, path_str, node_type);

            // Handle list/tuple removals (most important for reduction)
            // Generate in reverse order so batch processing works correctly:
            // removing higher indices first doesn't affect lower indices
            match &node {
                AstNode::SelectItemList(items) if items.len() > 1 => {
                    tracing::debug!(
                        "Adding {} RemoveListElement candidates for SelectItemList (reverse order)",
                        items.len()
                    );
                    for i in (0..items.len()).rev() {
                        candidates.push(ReductionCandidate {
                            path: path.clone(),
                            operation: ReductionOperation::RemoveListElement(i),
                        });
                    }
                }
                AstNode::ExprList(exprs) if exprs.len() > 1 => {
                    tracing::debug!(
                        "Adding {} RemoveListElement candidates for ExprList (reverse order)",
                        exprs.len()
                    );
                    for i in (0..exprs.len()).rev() {
                        candidates.push(ReductionCandidate {
                            path: path.clone(),
                            operation: ReductionOperation::RemoveListElement(i),
                        });
                    }
                }
                AstNode::TableList(tables) if tables.len() > 1 => {
                    tracing::debug!(
                        "Adding {} RemoveListElement candidates for TableList (reverse order)",
                        tables.len()
                    );
                    for i in (0..tables.len()).rev() {
                        candidates.push(ReductionCandidate {
                            path: path.clone(),
                            operation: ReductionOperation::RemoveListElement(i),
                        });
                    }
                }
                AstNode::OrderByList(orders) if orders.len() > 1 => {
                    tracing::debug!(
                        "Adding {} RemoveListElement candidates for OrderByList (reverse order)",
                        orders.len()
                    );
                    for i in (0..orders.len()).rev() {
                        candidates.push(ReductionCandidate {
                            path: path.clone(),
                            operation: ReductionOperation::RemoveListElement(i),
                        });
                    }
                }
                _ => {}
            }

            // Apply rule-based reductions
            if let Some(rule) = rules.get_rule(&node_type) {
                let mut rule_candidates = 0;

                // Try null replacement
                if rule.try_null {
                    candidates.push(ReductionCandidate {
                        path: path.clone(),
                        operation: ReductionOperation::TryNull,
                    });
                    rule_candidates += 1;
                }

                // Try attribute removal
                for attr in &rule.remove {
                    candidates.push(ReductionCandidate {
                        path: path.clone(),
                        operation: ReductionOperation::Remove(attr.clone()),
                    });
                    rule_candidates += 1;
                }

                // Try pullup operations
                for attr in &rule.pullup {
                    candidates.push(ReductionCandidate {
                        path: path.clone(),
                        operation: ReductionOperation::Pullup(attr.clone()),
                    });
                    rule_candidates += 1;
                }

                // Try replace operations
                for attr in &rule.replace {
                    candidates.push(ReductionCandidate {
                        path: path.clone(),
                        operation: ReductionOperation::Replace(attr.clone()),
                    });
                    rule_candidates += 1;
                }

                if rule_candidates > 0 {
                    tracing::debug!(
                        "Added {} rule-based candidates for {}",
                        rule_candidates,
                        node_type
                    );
                }
            } else {
                tracing::debug!("No rules found for node type: {}", node_type);
            }
        }
    }

    tracing::debug!(
        "Generated {} total candidates from {} paths (before sorting)",
        candidates.len(),
        paths.len()
    );

    // Sort candidates by dependency-aware priority (higher priority first)
    // Independent clauses (WHERE, ORDER BY) tried first, then dependent ones (SELECT, GROUP BY)
    candidates.sort_by(|a, b| b.operation.priority().cmp(&a.operation.priority()));

    tracing::debug!(
        "Sorted candidates by dependency-aware priority - first 10: {:?}",
        candidates
            .iter()
            .take(10)
            .map(|c| {
                let op_desc = match &c.operation {
                    ReductionOperation::Remove(field) => format!("Remove({:?})", field),
                    op => format!("{:?}", op),
                };
                (c.operation.priority(), op_desc)
            })
            .collect::<Vec<_>>()
    );

    candidates
}

/// Apply a reduction operation to an AST node.
/// Returns the new AST root if the operation was successful.
pub fn apply_reduction_operation(
    root: &AstNode,
    candidate: &ReductionCandidate,
) -> Option<AstNode> {
    use crate::sqlreduce::path::{display_ast_path, get_node_at_path, set_node_at_path};

    tracing::debug!(
        "apply_reduction_operation: Trying to apply {:?} at path {}",
        candidate.operation,
        display_ast_path(&candidate.path)
    );

    let target_node = get_node_at_path(root, &candidate.path);
    if target_node.is_none() {
        tracing::debug!(
            "apply_reduction_operation: Failed to get node at path {}",
            display_ast_path(&candidate.path)
        );
        return None;
    }
    let target_node = target_node?;

    match &candidate.operation {
        ReductionOperation::TryNull => {
            // Replace with NULL expression
            let null_expr = AstNode::Expr(Expr::Value(Value::Null));
            set_node_at_path(root, &candidate.path, Some(null_expr))
        }

        ReductionOperation::Remove(field) => {
            // Remove an attribute (set to None)
            let attr_path = [
                candidate.path.clone(),
                vec![PathComponent::field(field.clone())],
            ]
            .concat();
            tracing::debug!(
                "apply_reduction_operation: Removing attribute '{}' at path {}",
                field.to_string(),
                display_ast_path(&attr_path)
            );

            let result = set_node_at_path(root, &attr_path, None);
            if result.is_none() {
                tracing::debug!(
                    "apply_reduction_operation: Failed to remove attribute '{}'",
                    field.to_string()
                );
            } else {
                tracing::debug!(
                    "apply_reduction_operation: Successfully removed attribute '{}'",
                    field.to_string()
                );
            }
            result
        }

        ReductionOperation::Pullup(field) => {
            // Pull up a subnode to replace the current node
            let attr_path = [
                candidate.path.clone(),
                vec![PathComponent::field(field.clone())],
            ]
            .concat();
            if let Some(subnode) = get_node_at_path(root, &attr_path) {
                set_node_at_path(root, &candidate.path, Some(subnode))
            } else {
                None
            }
        }

        ReductionOperation::Replace(field) => {
            // Replace current node with a subtree
            let attr_path = [
                candidate.path.clone(),
                vec![PathComponent::field(field.clone())],
            ]
            .concat();
            tracing::debug!(
                "apply_reduction_operation: Replacing with attribute '{}' from path {}",
                field.to_string(),
                display_ast_path(&attr_path)
            );

            if let Some(subtree) = get_node_at_path(root, &attr_path) {
                tracing::debug!("apply_reduction_operation: Found subtree for replacement");
                let result = set_node_at_path(root, &candidate.path, Some(subtree));
                if result.is_none() {
                    tracing::debug!("apply_reduction_operation: Failed to set replacement subtree");
                } else {
                    tracing::debug!(
                        "apply_reduction_operation: Successfully replaced with subtree"
                    );
                }
                result
            } else {
                tracing::debug!(
                    "apply_reduction_operation: No subtree found at path {}",
                    display_ast_path(&attr_path)
                );
                None
            }
        }

        ReductionOperation::RemoveListElement(index) => {
            // Remove an element from a list
            match target_node {
                AstNode::SelectItemList(mut items) => {
                    if *index < items.len() && items.len() > 1 {
                        items.remove(*index);
                        set_node_at_path(
                            root,
                            &candidate.path,
                            Some(AstNode::SelectItemList(items)),
                        )
                    } else {
                        None
                    }
                }
                AstNode::ExprList(mut exprs) => {
                    if *index < exprs.len() && exprs.len() > 1 {
                        exprs.remove(*index);
                        set_node_at_path(root, &candidate.path, Some(AstNode::ExprList(exprs)))
                    } else {
                        None
                    }
                }
                AstNode::TableList(mut tables) => {
                    if *index < tables.len() && tables.len() > 1 {
                        tables.remove(*index);
                        set_node_at_path(root, &candidate.path, Some(AstNode::TableList(tables)))
                    } else {
                        None
                    }
                }
                AstNode::OrderByList(mut orders) => {
                    if *index < orders.len() && orders.len() > 1 {
                        orders.remove(*index);
                        set_node_at_path(root, &candidate.path, Some(AstNode::OrderByList(orders)))
                    } else {
                        None
                    }
                }
                _ => None,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use risingwave_sqlparser::parser::Parser;

    use super::*;
    use crate::sqlreduce::path::{enumerate_reduction_paths, statement_to_ast_node};

    #[test]
    fn test_reduction_candidates() {
        let sql = "SELECT a, b, c FROM t1, t2;";
        let parsed = Parser::parse_sql(sql).expect("Failed to parse SQL");
        let stmt = &parsed[0];
        let ast_node = statement_to_ast_node(stmt);

        let paths = enumerate_reduction_paths(&ast_node, vec![]);
        let rules = ReductionRules::default();
        let candidates = generate_reduction_candidates(&ast_node, &rules, &paths);

        // Should generate multiple candidates for removing SELECT items, FROM tables, etc.
        assert!(!candidates.is_empty());
        println!(
            "Generated {} candidates for complex query",
            candidates.len()
        );
    }

    #[test]
    fn test_list_element_removal() {
        let sql = "SELECT a, b, c FROM t;";
        let parsed = Parser::parse_sql(sql).expect("Failed to parse SQL");
        let stmt = &parsed[0];
        let ast_node = statement_to_ast_node(stmt);

        let paths = enumerate_reduction_paths(&ast_node, vec![]);
        let rules = ReductionRules::default();
        let candidates = generate_reduction_candidates(&ast_node, &rules, &paths);

        // Find candidates that remove SELECT list elements
        let list_removals: Vec<_> = candidates
            .iter()
            .filter(|c| matches!(c.operation, ReductionOperation::RemoveListElement(_)))
            .collect();

        // Should find 3 list removal candidates (for a, b, c)
        assert!(list_removals.len() == 3);
        println!(
            "✓ Found {} list element removal candidates as expected",
            list_removals.len()
        );
    }
}
