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

//! Path-based AST navigation for SQL reduction.
//!
//! This module provides utilities for navigating and modifying SQL ASTs using
//! path-based addressing for precise AST manipulation.

use std::fmt;

use risingwave_sqlparser::ast::*;

/// Represents a path component in an AST navigation path.
/// Components that make up a path through the AST.
/// Enables precise navigation to any AST node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PathComponent {
    /// Field access by name (e.g., "selection", "projection")
    Field(String),
    /// Array/Vec index access
    Index(usize),
}

impl fmt::Display for PathComponent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PathComponent::Field(name) => write!(f, ".{}", name),
            PathComponent::Index(idx) => write!(f, "[{}]", idx),
        }
    }
}

/// A path through the AST for precise node identification.
/// This allows us to precisely identify and modify any node in the tree.
pub type AstPath = Vec<PathComponent>;

// Note: Display implementation moved to helper function to avoid orphan rule
pub fn display_ast_path(path: &AstPath) -> String {
    path.iter().map(|c| c.to_string()).collect::<String>()
}

/// Represents a node in the AST that can be navigated and modified.
/// This is a simplified representation focusing on the most commonly
/// reduced SQL constructs.
#[derive(Debug, Clone)]
pub enum AstNode {
    Statement(Statement),
    Query(Box<Query>),
    Select(Box<Select>),
    Expr(Expr),
    SelectItem(SelectItem),
    TableWithJoins(TableWithJoins),
    Join(Join),
    TableFactor(TableFactor),
    OrderByExpr(OrderByExpr),
    With(With),
    Cte(Cte),
    ExprList(Vec<Expr>),
    SelectItemList(Vec<SelectItem>),
    TableList(Vec<TableWithJoins>),
    JoinList(Vec<Join>),
    OrderByList(Vec<OrderByExpr>),
    CteList(Vec<Cte>),
    Option(Option<Box<AstNode>>),
}

impl AstNode {
    /// Navigate to a child node using a path component.
    pub fn get_child(&self, component: &PathComponent) -> Option<AstNode> {
        match (self, component) {
            // Statement navigation
            (AstNode::Statement(Statement::Query(query)), PathComponent::Field(field))
                if field == "query" =>
            {
                Some(AstNode::Query(query.clone()))
            }

            (
                AstNode::Statement(Statement::CreateView { query, .. }),
                PathComponent::Field(field),
            ) if field == "query" => Some(AstNode::Query(query.clone())),

            // More CreateView field access
            (
                AstNode::Statement(Statement::CreateView {
                    name: _,
                    columns: _,
                    ..
                }),
                PathComponent::Field(field),
            ) => match field.as_str() {
                "name" => None,    // ObjectName is complex, skip for now
                "columns" => None, // Column list is complex, skip for now
                _ => None,
            },

            // Query navigation - enhanced
            (AstNode::Query(query), PathComponent::Field(field)) => match field.as_str() {
                "body" => match &query.body {
                    SetExpr::Select(select) => Some(AstNode::Select(select.clone())),
                    SetExpr::Query(subquery) => Some(AstNode::Query(subquery.clone())),
                    SetExpr::SetOperation { left, .. } => {
                        // For SetOperation, recursively handle the left side
                        match left.as_ref() {
                            SetExpr::Select(select) => Some(AstNode::Select(select.clone())),
                            SetExpr::Query(subquery) => Some(AstNode::Query(subquery.clone())),
                            _ => None,
                        }
                    }
                    _ => None,
                },
                "with" => query.with.as_ref().map(|w| AstNode::With(w.clone())),
                "order_by" => {
                    if query.order_by.is_empty() {
                        None
                    } else {
                        Some(AstNode::OrderByList(query.order_by.clone()))
                    }
                }
                "limit" => query.limit.as_ref().map(|e| AstNode::Expr(e.clone())),
                "offset" => None, // offset is Option<String>, not navigable as expression
                _ => None,
            },

            // Select navigation - enhanced
            (AstNode::Select(select), PathComponent::Field(field)) => match field.as_str() {
                "projection" => {
                    if select.projection.is_empty() {
                        None
                    } else {
                        Some(AstNode::SelectItemList(select.projection.clone()))
                    }
                }
                "selection" => select.selection.as_ref().map(|e| AstNode::Expr(e.clone())),
                "from" => {
                    if select.from.is_empty() {
                        None
                    } else {
                        Some(AstNode::TableList(select.from.clone()))
                    }
                }
                "group_by" => {
                    if select.group_by.is_empty() {
                        None
                    } else {
                        Some(AstNode::ExprList(select.group_by.clone()))
                    }
                }
                "having" => select.having.as_ref().map(|e| AstNode::Expr(e.clone())),
                "distinct" => None, // Distinct is an enum, handle separately if needed
                _ => None,
            },

            // List navigation
            (AstNode::SelectItemList(items), PathComponent::Index(idx)) => items
                .get(*idx)
                .map(|item| AstNode::SelectItem(item.clone())),
            (AstNode::ExprList(exprs), PathComponent::Index(idx)) => {
                exprs.get(*idx).map(|expr| AstNode::Expr(expr.clone()))
            }
            (AstNode::TableList(tables), PathComponent::Index(idx)) => tables
                .get(*idx)
                .map(|table| AstNode::TableWithJoins(table.clone())),
            (AstNode::OrderByList(orders), PathComponent::Index(idx)) => orders
                .get(*idx)
                .map(|order| AstNode::OrderByExpr(order.clone())),

            // Expression navigation (for pullup operations) - enhanced
            (AstNode::Expr(expr), PathComponent::Field(field)) => match (expr, field.as_str()) {
                (Expr::BinaryOp { left, .. }, "left") => Some(AstNode::Expr(*left.clone())),
                (Expr::BinaryOp { right, .. }, "right") => Some(AstNode::Expr(*right.clone())),
                (Expr::Case { operand, .. }, "operand") => {
                    operand.as_ref().map(|e| AstNode::Expr(*e.clone()))
                }
                (Expr::Case { else_result, .. }, "else_result") => {
                    else_result.as_ref().map(|e| AstNode::Expr(*e.clone()))
                }
                // 关键：添加对EXISTS和Subquery的支持
                (Expr::Exists(subquery), "subquery") => Some(AstNode::Query(subquery.clone())),
                (Expr::Subquery(subquery), "subquery") => Some(AstNode::Query(subquery.clone())),
                (Expr::Function(_func), "name") => None, // ObjectName is complex
                (Expr::Nested(inner), "inner") => Some(AstNode::Expr(*inner.clone())),
                (Expr::UnaryOp { expr, .. }, "expr") => Some(AstNode::Expr(*expr.clone())),
                (Expr::Cast { expr, .. }, "expr") => Some(AstNode::Expr(*expr.clone())),
                (Expr::IsNull(expr), "expr") => Some(AstNode::Expr(*expr.clone())),
                (Expr::IsNotNull(expr), "expr") => Some(AstNode::Expr(*expr.clone())),
                (
                    Expr::Between {
                        expr,
                        low: _,
                        high: _,
                        ..
                    },
                    "expr",
                ) => Some(AstNode::Expr(*expr.clone())),
                (Expr::Between { low, .. }, "low") => Some(AstNode::Expr(*low.clone())),
                (Expr::Between { high, .. }, "high") => Some(AstNode::Expr(*high.clone())),
                _ => None,
            },

            // TableWithJoins navigation
            (AstNode::TableWithJoins(table_with_joins), PathComponent::Field(field)) => {
                match field.as_str() {
                    "relation" => Some(AstNode::TableFactor(table_with_joins.relation.clone())),
                    "joins" => {
                        if !table_with_joins.joins.is_empty() {
                            Some(AstNode::JoinList(table_with_joins.joins.clone()))
                        } else {
                            None
                        }
                    }
                    _ => None,
                }
            }

            // Join navigation
            (AstNode::Join(join), PathComponent::Field(field)) => match field.as_str() {
                "relation" => Some(AstNode::TableFactor(join.relation.clone())),
                "join_operator" => None, // JoinOperator is simple enum, skip navigation
                _ => None,
            },

            // TableFactor navigation
            (AstNode::TableFactor(table_factor), PathComponent::Field(field)) => {
                match (table_factor, field.as_str()) {
                    (TableFactor::Table { .. }, _) => None, // Table references are terminal
                    (TableFactor::Derived { subquery, .. }, "subquery") => {
                        Some(AstNode::Query(subquery.clone()))
                    }
                    (TableFactor::TableFunction { .. }, _) => None, // Function calls are complex
                    _ => None,
                }
            }

            // JoinList navigation (for Vec<Join>)
            (AstNode::JoinList(joins), PathComponent::Index(idx)) => {
                joins.get(*idx).map(|join| AstNode::Join(join.clone()))
            }

            // SelectItem navigation
            (AstNode::SelectItem(select_item), PathComponent::Field(field)) => {
                match (select_item, field.as_str()) {
                    (SelectItem::UnnamedExpr(expr), "expr") => Some(AstNode::Expr(expr.clone())),
                    (SelectItem::ExprWithAlias { expr, .. }, "expr") => {
                        Some(AstNode::Expr(expr.clone()))
                    }
                    (SelectItem::QualifiedWildcard(..), _) => None,
                    (SelectItem::Wildcard(..), _) => None,
                    _ => None,
                }
            }

            // OrderByExpr navigation
            (AstNode::OrderByExpr(order_by), PathComponent::Field(field)) => match field.as_str() {
                "expr" => Some(AstNode::Expr(order_by.expr.clone())),
                "asc" => None,         // Boolean, not navigable
                "nulls_first" => None, // Option<bool>, not navigable
                _ => None,
            },

            // With clause navigation
            (AstNode::With(with_clause), PathComponent::Field(field)) => match field.as_str() {
                "cte_tables" => {
                    if with_clause.cte_tables.is_empty() {
                        None
                    } else {
                        Some(AstNode::CteList(with_clause.cte_tables.clone()))
                    }
                }
                "recursive" => None, // Boolean, not navigable
                _ => None,
            },

            // CTE list navigation
            (AstNode::CteList(ctes), PathComponent::Index(idx)) => {
                if *idx < ctes.len() {
                    Some(AstNode::Cte(ctes[*idx].clone()))
                } else {
                    None
                }
            }

            // CTE navigation
            (AstNode::Cte(cte), PathComponent::Field(field)) => match field.as_str() {
                "alias" => None, // TableAlias is complex, skip for now
                "cte_inner" => match &cte.cte_inner {
                    CteInner::Query(query) => Some(AstNode::Query(query.clone())),
                    CteInner::ChangeLog(_) => None, // ObjectName is complex, skip for now
                },
                _ => None,
            },

            _ => {
                // Add debug logging for unmatched cases
                tracing::debug!(
                    "get_child: No match for {:?} with component {:?}",
                    std::mem::discriminant(self),
                    component
                );
                None
            }
        }
    }

    /// Set a child node using a path component.
    /// Returns a new `AstNode` with the modification applied.
    pub fn set_child(
        &self,
        component: &PathComponent,
        new_child: Option<AstNode>,
    ) -> Option<AstNode> {
        match (self, component) {
            // Statement field modifications
            (
                AstNode::Statement(Statement::CreateView {
                    name,
                    columns,
                    query: _,
                    ..
                }),
                PathComponent::Field(field),
            ) => match field.as_str() {
                "query" => {
                    if let Some(AstNode::Query(new_query)) = new_child {
                        let new_stmt = Statement::CreateView {
                            or_replace: false,
                            materialized: true,
                            if_not_exists: false,
                            name: name.clone(),
                            columns: columns.clone(),
                            query: new_query,
                            emit_mode: None,
                            with_options: vec![],
                        };
                        Some(AstNode::Statement(new_stmt))
                    } else {
                        None
                    }
                }
                _ => None,
            },

            // Query field modifications
            (AstNode::Query(query), PathComponent::Field(field)) => {
                let mut new_query = (**query).clone();
                match field.as_str() {
                    "body" => {
                        if let Some(AstNode::Select(select)) = new_child {
                            new_query.body = SetExpr::Select(select);
                            Some(AstNode::Query(Box::new(new_query)))
                        } else {
                            None
                        }
                    }
                    "order_by" => {
                        if let Some(AstNode::OrderByList(orders)) = new_child {
                            new_query.order_by = orders;
                        } else {
                            new_query.order_by = vec![];
                        }
                        Some(AstNode::Query(Box::new(new_query)))
                    }
                    "limit" => {
                        new_query.limit = new_child.and_then(|n| match n {
                            AstNode::Expr(e) => Some(e),
                            _ => None,
                        });
                        Some(AstNode::Query(Box::new(new_query)))
                    }
                    "with" => {
                        new_query.with = new_child.and_then(|n| match n {
                            AstNode::With(w) => Some(w),
                            _ => None,
                        });
                        Some(AstNode::Query(Box::new(new_query)))
                    }
                    _ => None,
                }
            }

            // Select field modifications
            (AstNode::Select(select), PathComponent::Field(field)) => {
                let mut new_select = (**select).clone();
                match field.as_str() {
                    "selection" => {
                        new_select.selection = new_child.and_then(|n| match n {
                            AstNode::Expr(e) => Some(e),
                            _ => None,
                        });
                    }
                    "having" => {
                        new_select.having = new_child.and_then(|n| match n {
                            AstNode::Expr(e) => Some(e),
                            _ => None,
                        });
                    }
                    "projection" => {
                        if let Some(AstNode::SelectItemList(items)) = new_child {
                            new_select.projection = items;
                        }
                    }
                    "from" => {
                        if let Some(AstNode::TableList(tables)) = new_child {
                            new_select.from = tables;
                        }
                    }
                    "group_by" => {
                        if let Some(AstNode::ExprList(exprs)) = new_child {
                            new_select.group_by = exprs;
                        }
                    }
                    _ => return None,
                }
                Some(AstNode::Select(Box::new(new_select)))
            }

            // List modifications
            (AstNode::SelectItemList(items), PathComponent::Index(idx)) => {
                let mut new_items = items.clone();
                if *idx < new_items.len() {
                    if let Some(AstNode::SelectItem(item)) = new_child {
                        new_items[*idx] = item;
                    } else {
                        new_items.remove(*idx);
                    }
                    Some(AstNode::SelectItemList(new_items))
                } else {
                    None
                }
            }

            (AstNode::ExprList(exprs), PathComponent::Index(idx)) => {
                let mut new_exprs = exprs.clone();
                if *idx < new_exprs.len() {
                    if let Some(AstNode::Expr(expr)) = new_child {
                        new_exprs[*idx] = expr;
                    } else {
                        new_exprs.remove(*idx);
                    }
                    Some(AstNode::ExprList(new_exprs))
                } else {
                    None
                }
            }

            (AstNode::TableList(tables), PathComponent::Index(idx)) => {
                let mut new_tables = tables.clone();
                if *idx < new_tables.len() {
                    if let Some(AstNode::TableWithJoins(table)) = new_child {
                        new_tables[*idx] = table;
                    } else {
                        new_tables.remove(*idx);
                    }
                    Some(AstNode::TableList(new_tables))
                } else {
                    None
                }
            }

            // TableWithJoins modifications
            (AstNode::TableWithJoins(table_with_joins), PathComponent::Field(field)) => {
                match field.as_str() {
                    "relation" => {
                        if let Some(AstNode::TableFactor(new_relation)) = new_child {
                            Some(AstNode::TableWithJoins(TableWithJoins {
                                relation: new_relation,
                                joins: table_with_joins.joins.clone(),
                            }))
                        } else {
                            None
                        }
                    }
                    "joins" => {
                        if let Some(AstNode::JoinList(new_joins)) = new_child {
                            Some(AstNode::TableWithJoins(TableWithJoins {
                                relation: table_with_joins.relation.clone(),
                                joins: new_joins,
                            }))
                        } else {
                            // Allow removing joins by setting to empty list
                            Some(AstNode::TableWithJoins(TableWithJoins {
                                relation: table_with_joins.relation.clone(),
                                joins: vec![],
                            }))
                        }
                    }
                    _ => None,
                }
            }

            // Join modifications
            (AstNode::Join(join), PathComponent::Field(field)) => match field.as_str() {
                "relation" => {
                    if let Some(AstNode::TableFactor(new_relation)) = new_child {
                        Some(AstNode::Join(Join {
                            relation: new_relation,
                            join_operator: join.join_operator.clone(),
                        }))
                    } else {
                        None
                    }
                }
                _ => None,
            },

            // TableFactor modifications
            (AstNode::TableFactor(table_factor), PathComponent::Field(field)) => {
                match (table_factor, field.as_str()) {
                    (TableFactor::Derived { lateral, alias, .. }, "subquery") => {
                        if let Some(AstNode::Query(new_subquery)) = new_child {
                            Some(AstNode::TableFactor(TableFactor::Derived {
                                lateral: *lateral,
                                subquery: new_subquery,
                                alias: alias.clone(),
                            }))
                        } else {
                            None
                        }
                    }
                    _ => None,
                }
            }

            // JoinList modifications
            (AstNode::JoinList(joins), PathComponent::Index(idx)) => {
                let mut new_joins = joins.clone();
                if *idx < new_joins.len() {
                    if let Some(AstNode::Join(new_join)) = new_child {
                        new_joins[*idx] = new_join;
                    } else {
                        new_joins.remove(*idx);
                    }
                    Some(AstNode::JoinList(new_joins))
                } else {
                    None
                }
            }

            (AstNode::OrderByList(orders), PathComponent::Index(idx)) => {
                let mut new_orders = orders.clone();
                if *idx < new_orders.len() {
                    if let Some(AstNode::OrderByExpr(order)) = new_child {
                        new_orders[*idx] = order;
                    } else {
                        new_orders.remove(*idx);
                    }
                    Some(AstNode::OrderByList(new_orders))
                } else {
                    None
                }
            }

            // Expression field modifications
            (AstNode::Expr(expr), PathComponent::Field(field)) => match (expr, field.as_str()) {
                (Expr::BinaryOp { left: _, op, right }, "left") => {
                    if let Some(AstNode::Expr(new_left)) = new_child {
                        Some(AstNode::Expr(Expr::BinaryOp {
                            left: Box::new(new_left),
                            op: op.clone(),
                            right: right.clone(),
                        }))
                    } else {
                        None
                    }
                }
                (Expr::BinaryOp { left, op, right: _ }, "right") => {
                    if let Some(AstNode::Expr(new_right)) = new_child {
                        Some(AstNode::Expr(Expr::BinaryOp {
                            left: left.clone(),
                            op: op.clone(),
                            right: Box::new(new_right),
                        }))
                    } else {
                        None
                    }
                }
                (Expr::Nested(_), "inner") => {
                    if let Some(AstNode::Expr(new_inner)) = new_child {
                        Some(AstNode::Expr(Expr::Nested(Box::new(new_inner))))
                    } else {
                        None
                    }
                }
                _ => None,
            },

            // SelectItem field modifications
            (AstNode::SelectItem(select_item), PathComponent::Field(field)) => {
                match (select_item, field.as_str()) {
                    (SelectItem::UnnamedExpr(_), "expr") => {
                        if let Some(AstNode::Expr(new_expr)) = new_child {
                            Some(AstNode::SelectItem(SelectItem::UnnamedExpr(new_expr)))
                        } else {
                            None
                        }
                    }
                    (SelectItem::ExprWithAlias { alias, .. }, "expr") => {
                        if let Some(AstNode::Expr(new_expr)) = new_child {
                            Some(AstNode::SelectItem(SelectItem::ExprWithAlias {
                                expr: new_expr,
                                alias: alias.clone(),
                            }))
                        } else {
                            None
                        }
                    }
                    _ => None,
                }
            }

            // OrderByExpr field modifications
            (AstNode::OrderByExpr(order_by), PathComponent::Field(field)) => match field.as_str() {
                "expr" => {
                    if let Some(AstNode::Expr(new_expr)) = new_child {
                        Some(AstNode::OrderByExpr(OrderByExpr {
                            expr: new_expr,
                            asc: order_by.asc,
                            nulls_first: order_by.nulls_first,
                        }))
                    } else {
                        None
                    }
                }
                _ => None,
            },

            // With clause modifications
            (AstNode::With(with_clause), PathComponent::Field(field)) => match field.as_str() {
                "cte_tables" => {
                    if let Some(AstNode::CteList(new_ctes)) = new_child {
                        let mut new_with = with_clause.clone();
                        new_with.cte_tables = new_ctes;
                        Some(AstNode::With(new_with))
                    } else {
                        None
                    }
                }
                _ => None,
            },

            // CTE list modifications
            (AstNode::CteList(ctes), PathComponent::Index(idx)) => {
                if let Some(AstNode::Cte(new_cte)) = new_child {
                    if *idx < ctes.len() {
                        let mut new_ctes = ctes.clone();
                        new_ctes[*idx] = new_cte;
                        Some(AstNode::CteList(new_ctes))
                    } else {
                        None
                    }
                } else {
                    None
                }
            }

            // CTE modifications
            (AstNode::Cte(cte), PathComponent::Field(field)) => match field.as_str() {
                "cte_inner" => {
                    if let Some(AstNode::Query(new_query)) = new_child {
                        let mut new_cte = cte.clone();
                        new_cte.cte_inner = CteInner::Query(new_query);
                        Some(AstNode::Cte(new_cte))
                    } else {
                        None
                    }
                }
                _ => None,
            },

            _ => {
                // Add debug logging for unmatched cases
                tracing::debug!(
                    "set_child: No match for {:?} with component {:?}",
                    std::mem::discriminant(self),
                    component
                );
                None
            }
        }
    }

    /// Convert back to a Statement if this is the root node.
    pub fn to_statement(&self) -> Option<Statement> {
        match self {
            AstNode::Statement(stmt) => Some(stmt.clone()),
            _ => None,
        }
    }
}

/// Navigate to a node in the AST using the given path.
/// Enables precise AST node retrieval.
pub fn get_node_at_path(root: &AstNode, path: &AstPath) -> Option<AstNode> {
    let mut current = root.clone();
    for component in path {
        current = current.get_child(component)?;
    }
    Some(current)
}

/// Set a node in the AST at the given path.
/// Enables precise AST node modification.
pub fn set_node_at_path(
    root: &AstNode,
    path: &AstPath,
    new_node: Option<AstNode>,
) -> Option<AstNode> {
    if path.is_empty() {
        return new_node;
    }

    let result = root.clone();
    let mut current_path = Vec::new();

    // Navigate to the parent of the target node
    for component in &path[..path.len() - 1] {
        current_path.push(component.clone());
    }

    // Get the parent node
    let parent = get_node_at_path(&result, &current_path)?;

    // Apply the modification to the parent
    let modified_parent = parent.set_child(&path[path.len() - 1], new_node)?;

    // Now we need to set this modified parent back in the tree
    if current_path.is_empty() {
        Some(modified_parent)
    } else {
        set_node_at_path(&result, &current_path, Some(modified_parent))
    }
}

/// Helper function to get a child node and recurse if it exists.
fn explore_child_path(
    node: &AstNode,
    field_name: &str,
    current_path: &AstPath,
    paths: &mut Vec<AstPath>,
) {
    let child_path = [
        current_path.clone(),
        vec![PathComponent::Field(field_name.to_owned())],
    ]
    .concat();
    let relative_path = vec![PathComponent::Field(field_name.to_owned())];

    if let Some(child_node) = get_node_at_path(node, &relative_path) {
        paths.extend(enumerate_reduction_paths(&child_node, child_path));
    }
}

/// Enumerate all interesting paths in the AST for reduction.
/// Systematically discovers all reducible AST locations.
pub fn enumerate_reduction_paths(node: &AstNode, current_path: AstPath) -> Vec<AstPath> {
    let mut paths = vec![current_path.clone()];

    tracing::debug!(
        "Enumerating paths for node {:?} at path {}",
        get_node_type_name(node),
        display_ast_path(&current_path)
    );

    match node {
        AstNode::Statement(Statement::Query(_)) => {
            explore_child_path(node, "query", &current_path, &mut paths);
        }

        AstNode::Statement(Statement::CreateView { .. }) => {
            explore_child_path(node, "query", &current_path, &mut paths);
        }

        AstNode::Query(_query) => {
            explore_child_path(node, "body", &current_path, &mut paths);
            explore_child_path(node, "with", &current_path, &mut paths);
            explore_child_path(node, "order_by", &current_path, &mut paths);
        }

        AstNode::Select(_) => {
            explore_child_path(node, "projection", &current_path, &mut paths);
            explore_child_path(node, "from", &current_path, &mut paths);
            explore_child_path(node, "group_by", &current_path, &mut paths);

            // For optional fields, just add the path if they exist
            let selection_path = [
                current_path.clone(),
                vec![PathComponent::Field("selection".to_owned())],
            ]
            .concat();
            if get_node_at_path(node, &vec![PathComponent::Field("selection".to_owned())]).is_some()
            {
                paths.push(selection_path);
            }

            let having_path = [
                current_path.clone(),
                vec![PathComponent::Field("having".to_owned())],
            ]
            .concat();
            if get_node_at_path(node, &vec![PathComponent::Field("having".to_owned())]).is_some() {
                paths.push(having_path);
            }
        }

        // For lists, enumerate individual elements
        AstNode::SelectItemList(items) => {
            for i in 0..items.len() {
                let item_path = [current_path.clone(), vec![PathComponent::Index(i)]].concat();
                paths.push(item_path);
            }
        }

        AstNode::ExprList(exprs) => {
            for i in 0..exprs.len() {
                let expr_path = [current_path.clone(), vec![PathComponent::Index(i)]].concat();
                paths.push(expr_path.clone());
                // Also descend into expressions for pullup opportunities
                if let Some(expr_node) = get_node_at_path(node, &expr_path) {
                    paths.extend(enumerate_reduction_paths(&expr_node, expr_path));
                }
            }
        }

        AstNode::TableList(tables) => {
            for i in 0..tables.len() {
                let table_path = [current_path.clone(), vec![PathComponent::Index(i)]].concat();
                paths.push(table_path.clone());
                // Recursively explore TableWithJoins
                if let Some(table_node) = node.get_child(&PathComponent::Index(i)) {
                    paths.extend(enumerate_reduction_paths(&table_node, table_path));
                }
            }
        }

        // TableWithJoins path enumeration - key for JOIN reduction
        AstNode::TableWithJoins(_) => {
            explore_child_path(node, "relation", &current_path, &mut paths);
            explore_child_path(node, "joins", &current_path, &mut paths);
        }

        // Join path enumeration
        AstNode::Join(_) => {
            explore_child_path(node, "relation", &current_path, &mut paths);
        }

        // TableFactor path enumeration
        AstNode::TableFactor(_) => {
            // For Derived tables (subqueries), explore the subquery
            explore_child_path(node, "subquery", &current_path, &mut paths);
        }

        // JoinList path enumeration
        AstNode::JoinList(joins) => {
            for i in 0..joins.len() {
                let join_path = [current_path.clone(), vec![PathComponent::Index(i)]].concat();
                paths.push(join_path.clone());
                // Recursively explore Join
                if let Some(join_node) = node.get_child(&PathComponent::Index(i)) {
                    paths.extend(enumerate_reduction_paths(&join_node, join_path));
                }
            }
        }

        AstNode::OrderByList(orders) => {
            for i in 0..orders.len() {
                let order_path = [current_path.clone(), vec![PathComponent::Index(i)]].concat();
                paths.push(order_path);
            }
        }

        // For expressions, look for pullup opportunities
        AstNode::Expr(expr) => match expr {
            Expr::BinaryOp { .. } => {
                let left_path = [
                    current_path.clone(),
                    vec![PathComponent::Field("left".to_owned())],
                ]
                .concat();
                let right_path = [
                    current_path.clone(),
                    vec![PathComponent::Field("right".to_owned())],
                ]
                .concat();
                paths.push(left_path);
                paths.push(right_path);
            }
            Expr::Case {
                operand,
                else_result,
                ..
            } => {
                if operand.is_some() {
                    let operand_path = [
                        current_path.clone(),
                        vec![PathComponent::Field("operand".to_owned())],
                    ]
                    .concat();
                    paths.push(operand_path);
                }
                if else_result.is_some() {
                    let else_path = [
                        current_path.clone(),
                        vec![PathComponent::Field("else_result".to_owned())],
                    ]
                    .concat();
                    paths.push(else_path);
                }
            }
            // 关键：添加对EXISTS和Subquery的路径枚举
            Expr::Exists(_) => {
                explore_child_path(node, "subquery", &current_path, &mut paths);
            }
            Expr::Subquery(_) => {
                explore_child_path(node, "subquery", &current_path, &mut paths);
            }
            _ => {}
        },

        // WITH clause enumeration
        AstNode::With(_) => {
            explore_child_path(node, "cte_tables", &current_path, &mut paths);
        }

        // CTE list enumeration
        AstNode::CteList(ctes) => {
            for i in 0..ctes.len() {
                let cte_path = [current_path.clone(), vec![PathComponent::Index(i)]].concat();
                paths.push(cte_path.clone());

                // Recursively enumerate paths within each CTE
                if let Some(cte_node) = get_node_at_path(node, &vec![PathComponent::Index(i)]) {
                    paths.extend(enumerate_reduction_paths(&cte_node, cte_path));
                }
            }
        }

        // CTE enumeration
        AstNode::Cte(_) => {
            explore_child_path(node, "cte_inner", &current_path, &mut paths);
        }

        _ => {}
    }

    paths
}

/// Convert a Statement to an `AstNode` for path-based operations.
pub fn statement_to_ast_node(stmt: &Statement) -> AstNode {
    AstNode::Statement(stmt.clone())
}

/// Extract a Statement from an `AstNode`.
pub fn ast_node_to_statement(node: &AstNode) -> Option<Statement> {
    match node {
        AstNode::Statement(stmt) => Some(stmt.clone()),
        AstNode::Query(query) => Some(Statement::Query(Box::new(query.as_ref().clone()))),
        _ => None,
    }
}

/// Get a human-readable name for an AST node type.
pub fn get_node_type_name(node: &AstNode) -> &'static str {
    match node {
        AstNode::Statement(_) => "Statement",
        AstNode::Query(_) => "Query",
        AstNode::Select(_) => "Select",
        AstNode::Expr(_) => "Expr",
        AstNode::SelectItem(_) => "SelectItem",
        AstNode::TableWithJoins(_) => "TableWithJoins",
        AstNode::Join(_) => "Join",
        AstNode::TableFactor(_) => "TableFactor",
        AstNode::OrderByExpr(_) => "OrderByExpr",
        AstNode::With(_) => "With",
        AstNode::Cte(_) => "Cte",
        AstNode::ExprList(_) => "ExprList",
        AstNode::SelectItemList(_) => "SelectItemList",
        AstNode::TableList(_) => "TableList",
        AstNode::JoinList(_) => "JoinList",
        AstNode::OrderByList(_) => "OrderByList",
        AstNode::CteList(_) => "CteList",
        AstNode::Option(_) => "Option",
    }
}

#[cfg(test)]
mod tests {
    use risingwave_sqlparser::parser::Parser;

    use super::*;

    #[test]
    fn test_path_enumeration() {
        let sql = "SELECT a FROM b;";
        let parsed = Parser::parse_sql(sql).expect("Failed to parse SQL");
        let stmt = &parsed[0];
        let ast_node = statement_to_ast_node(stmt);

        let paths = enumerate_reduction_paths(&ast_node, vec![]);

        // Should have at least a few paths for this simple query
        assert!(paths.len() >= 3);
        println!("Found {} paths for simple query", paths.len());
    }

    #[test]
    fn test_create_materialized_view() {
        let sql = "CREATE MATERIALIZED VIEW stream_query AS SELECT min((tumble_0.c5 + tumble_0.c5)) AS col_0, false AS col_1, TIME '07:30:48' AS col_2, tumble_0.c14 AS col_3 FROM tumble(alltypes1, alltypes1.c11, INTERVAL '10') AS tumble_0 WHERE tumble_0.c1 GROUP BY tumble_0.c10, tumble_0.c9, tumble_0.c13, tumble_0.c1, tumble_0.c16, tumble_0.c14, tumble_0.c5, tumble_0.c8;";
        let parsed = Parser::parse_sql(sql).expect("Failed to parse SQL");
        let stmt = &parsed[0];
        let ast_node = statement_to_ast_node(stmt);

        let paths = enumerate_reduction_paths(&ast_node, vec![]);

        println!("Found {} paths for CREATE MATERIALIZED VIEW", paths.len());
        for (i, path) in paths.iter().enumerate() {
            println!("Path {}: {}", i, display_ast_path(path));
        }

        // Should have paths for the SELECT statement inside the MV
        assert!(paths.len() > 0);

        // Should be able to get some node
        assert!(get_node_at_path(&ast_node, &paths[0]).is_some());
    }
}
