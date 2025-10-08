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

use crate::sqlreduce::path::{AstNode, AstPath, PathComponent};

/// Defines what actions can be performed on an AST node during reduction.
#[derive(Debug, Clone, Default)]
pub struct ReductionRule {
    /// Whether to try replacing this node with NULL/None
    pub try_null: bool,
    /// Attributes to descend into for further reduction
    pub descend: Vec<String>,
    /// Attributes that can be removed (set to None)
    pub remove: Vec<String>,
    /// Attributes whose children can be pulled up to replace this node
    pub pullup: Vec<String>,
    /// Attributes whose subtrees can replace this entire node
    pub replace: Vec<String>,
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
        rules.insert(
            "Select".to_owned(),
            ReductionRule {
                try_null: false,
                descend: vec![
                    "projection".to_owned(),
                    "from".to_owned(),
                    "selection".to_owned(),
                    "group_by".to_owned(),
                    "having".to_owned(),
                ],
                remove: vec![
                    "selection".to_owned(),
                    "having".to_owned(),
                    "projection".to_owned(),
                    "from".to_owned(),
                    "group_by".to_owned(),
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
                descend: vec!["body".to_owned(), "with".to_owned(), "order_by".to_owned()],
                remove: vec!["with".to_owned(), "order_by".to_owned()],
                pullup: vec![],
                replace: vec!["body".to_owned()],
            },
        );

        // WITH clause rules
        rules.insert(
            "With".to_owned(),
            ReductionRule {
                try_null: true,
                descend: vec!["cte_tables".to_owned()],
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
                descend: vec!["cte_inner".to_owned()],
                remove: vec![],
                pullup: vec!["cte_inner".to_owned()],
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
                pullup: vec!["left".to_owned(), "right".to_owned()],
                replace: vec![],
            },
        );

        rules.insert(
            "Case".to_owned(),
            ReductionRule {
                try_null: true,
                descend: vec![],
                remove: vec![],
                pullup: vec!["operand".to_owned(), "else_result".to_owned()],
                replace: vec![],
            },
        );

        // Function call rules
        rules.insert(
            "Function".to_owned(),
            ReductionRule {
                try_null: true,
                descend: vec!["args".to_owned()],
                remove: vec!["args".to_owned()],
                pullup: vec!["args".to_owned()],
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
                replace: vec!["query".to_owned()],
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
                descend: vec!["relation".to_owned(), "joins".to_owned()],
                remove: vec!["joins".to_owned()],
                pullup: vec!["relation".to_owned()],
                replace: vec!["relation".to_owned()],
            },
        );

        rules.insert(
            "Join".to_owned(),
            ReductionRule {
                try_null: true,
                descend: vec!["relation".to_owned()],
                remove: vec![],
                pullup: vec!["relation".to_owned()],
                replace: vec!["relation".to_owned()],
            },
        );

        rules.insert(
            "TableFactor".to_owned(),
            ReductionRule {
                try_null: true, // Allow trying NULL for derived tables
                descend: vec!["subquery".to_owned()],
                remove: vec!["subquery".to_owned()], // Allow removing subquery entirely
                pullup: vec!["subquery".to_owned()],
                replace: vec!["subquery".to_owned()],
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
    Remove(String),
    /// Pull up a subnode to replace this node
    Pullup(String),
    /// Replace this node with a subtree
    Replace(String),
    /// Remove an element from a list/tuple
    RemoveListElement(usize),
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

            tracing::debug!("  Path {}: {} ({})", path_idx, path_str, node_type);

            // Handle list/tuple removals (most important for reduction)
            match &node {
                AstNode::SelectItemList(items) if items.len() > 1 => {
                    tracing::debug!(
                        "    Adding {} RemoveListElement candidates for SelectItemList",
                        items.len()
                    );
                    for i in 0..items.len() {
                        candidates.push(ReductionCandidate {
                            path: path.clone(),
                            operation: ReductionOperation::RemoveListElement(i),
                        });
                    }
                }
                AstNode::ExprList(exprs) if exprs.len() > 1 => {
                    tracing::debug!(
                        "    Adding {} RemoveListElement candidates for ExprList",
                        exprs.len()
                    );
                    for i in 0..exprs.len() {
                        candidates.push(ReductionCandidate {
                            path: path.clone(),
                            operation: ReductionOperation::RemoveListElement(i),
                        });
                    }
                }
                AstNode::TableList(tables) if tables.len() > 1 => {
                    tracing::debug!(
                        "    Adding {} RemoveListElement candidates for TableList",
                        tables.len()
                    );
                    for i in 0..tables.len() {
                        candidates.push(ReductionCandidate {
                            path: path.clone(),
                            operation: ReductionOperation::RemoveListElement(i),
                        });
                    }
                }
                AstNode::OrderByList(orders) if orders.len() > 1 => {
                    tracing::debug!(
                        "    Adding {} RemoveListElement candidates for OrderByList",
                        orders.len()
                    );
                    for i in 0..orders.len() {
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
                        "    Added {} rule-based candidates for {}",
                        rule_candidates,
                        node_type
                    );
                }
            } else {
                tracing::debug!("    No rules found for node type: {}", node_type);
            }
        }
    }

    tracing::debug!(
        "Generated {} total candidates from {} paths",
        candidates.len(),
        paths.len()
    );
    candidates
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
        assert!(candidates.len() > 0);
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

        ReductionOperation::Remove(attr) => {
            // Remove an attribute (set to None)
            let attr_path = [
                candidate.path.clone(),
                vec![PathComponent::Field(attr.clone())],
            ]
            .concat();
            tracing::debug!(
                "apply_reduction_operation: Removing attribute '{}' at path {}",
                attr,
                display_ast_path(&attr_path)
            );

            // Check if the attribute exists first
            if get_node_at_path(root, &attr_path).is_none() {
                tracing::debug!(
                    "apply_reduction_operation: Attribute '{}' does not exist at path {}",
                    attr,
                    display_ast_path(&attr_path)
                );
                return None;
            }

            let result = set_node_at_path(root, &attr_path, None);
            if result.is_none() {
                tracing::debug!(
                    "apply_reduction_operation: Failed to remove attribute '{}'",
                    attr
                );
            } else {
                tracing::debug!(
                    "apply_reduction_operation: Successfully removed attribute '{}'",
                    attr
                );
            }
            result
        }

        ReductionOperation::Pullup(attr) => {
            // Pull up a subnode to replace the current node
            let attr_path = [
                candidate.path.clone(),
                vec![PathComponent::Field(attr.clone())],
            ]
            .concat();
            if let Some(subnode) = get_node_at_path(root, &attr_path) {
                set_node_at_path(root, &candidate.path, Some(subnode))
            } else {
                None
            }
        }

        ReductionOperation::Replace(attr) => {
            // Replace current node with a subtree
            let attr_path = [
                candidate.path.clone(),
                vec![PathComponent::Field(attr.clone())],
            ]
            .concat();
            tracing::debug!(
                "apply_reduction_operation: Replacing with attribute '{}' from path {}",
                attr,
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
