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

/// Represents all possible AST field names that can be navigated.
/// This provides compile-time safety for field access.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AstField {
    // Statement fields
    Query,
    Name,
    Columns,

    // Query fields
    Body,
    With,
    OrderBy,
    Limit,
    Offset,

    // Select fields
    Projection,
    Selection,
    From,
    GroupBy,
    Having,
    Distinct,

    // Expression fields
    Left,
    Right,
    Operand,
    ElseResult,
    Subquery,
    Inner,
    Expr,
    Low,
    High,

    // TableWithJoins fields
    Relation,
    Joins,

    // Join fields
    JoinOperator,

    // SelectItem fields
    Alias,

    // OrderByExpr fields
    Asc,
    NullsFirst,

    // With clause fields
    CteTable,
    Recursive,

    // CTE fields
    CteInner,

    // TableFactor fields
    Lateral,
}

impl fmt::Display for AstField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            AstField::Query => "query",
            AstField::Name => "name",
            AstField::Columns => "columns",
            AstField::Body => "body",
            AstField::With => "with",
            AstField::OrderBy => "order_by",
            AstField::Limit => "limit",
            AstField::Offset => "offset",
            AstField::Projection => "projection",
            AstField::Selection => "selection",
            AstField::From => "from",
            AstField::GroupBy => "group_by",
            AstField::Having => "having",
            AstField::Distinct => "distinct",
            AstField::Left => "left",
            AstField::Right => "right",
            AstField::Operand => "operand",
            AstField::ElseResult => "else_result",
            AstField::Subquery => "subquery",
            AstField::Inner => "inner",
            AstField::Expr => "expr",
            AstField::Low => "low",
            AstField::High => "high",
            AstField::Relation => "relation",
            AstField::Joins => "joins",
            AstField::JoinOperator => "join_operator",
            AstField::Alias => "alias",
            AstField::Asc => "asc",
            AstField::NullsFirst => "nulls_first",
            AstField::CteTable => "cte_tables",
            AstField::Recursive => "recursive",
            AstField::CteInner => "cte_inner",
            AstField::Lateral => "lateral",
        };
        write!(f, "{}", s)
    }
}

/// Represents a path component in an AST navigation path.
/// Components that make up a path through the AST.
/// Enables precise navigation to any AST node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PathComponent {
    /// Field access by AST field enum (type-safe)
    Field(AstField),
    /// Array/Vec index access
    Index(usize),
}

impl PathComponent {
    /// Create a Field `PathComponent` from `AstField` enum (preferred)
    pub fn field(field: AstField) -> Self {
        PathComponent::Field(field)
    }

    /// Get the field name as owned String for compatibility
    pub fn field_name(&self) -> Option<String> {
        match self {
            PathComponent::Field(field) => Some(field.to_string()),
            PathComponent::Index(_) => None,
        }
    }

    /// Get the `AstField` enum if this is a field
    pub fn as_ast_field(&self) -> Option<&AstField> {
        match self {
            PathComponent::Field(field) => Some(field),
            PathComponent::Index(_) => None,
        }
    }

    // Convenience constructors for common fields
    pub fn query() -> Self {
        Self::field(AstField::Query)
    }

    pub fn body() -> Self {
        Self::field(AstField::Body)
    }

    pub fn selection() -> Self {
        Self::field(AstField::Selection)
    }

    pub fn projection() -> Self {
        Self::field(AstField::Projection)
    }

    pub fn from_clause() -> Self {
        Self::field(AstField::From)
    }

    pub fn group_by() -> Self {
        Self::field(AstField::GroupBy)
    }

    pub fn having() -> Self {
        Self::field(AstField::Having)
    }

    pub fn with_clause() -> Self {
        Self::field(AstField::With)
    }

    pub fn order_by() -> Self {
        Self::field(AstField::OrderBy)
    }

    pub fn left() -> Self {
        Self::field(AstField::Left)
    }

    pub fn right() -> Self {
        Self::field(AstField::Right)
    }

    pub fn operand() -> Self {
        Self::field(AstField::Operand)
    }

    pub fn else_result() -> Self {
        Self::field(AstField::ElseResult)
    }

    pub fn relation() -> Self {
        Self::field(AstField::Relation)
    }

    pub fn joins() -> Self {
        Self::field(AstField::Joins)
    }

    pub fn subquery() -> Self {
        Self::field(AstField::Subquery)
    }

    pub fn cte_tables() -> Self {
        Self::field(AstField::CteTable)
    }

    pub fn cte_inner() -> Self {
        Self::field(AstField::CteInner)
    }
}

impl fmt::Display for PathComponent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PathComponent::Field(field) => write!(f, ".{}", field),
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
                if *field == AstField::Query =>
            {
                Some(AstNode::Query(query.clone()))
            }

            (
                AstNode::Statement(Statement::CreateView { query, .. }),
                PathComponent::Field(field),
            ) if *field == AstField::Query => Some(AstNode::Query(query.clone())),

            // More CreateView field access
            (
                AstNode::Statement(Statement::CreateView {
                    name: _,
                    columns: _,
                    ..
                }),
                PathComponent::Field(field),
            ) => match field {
                AstField::Name => None,    // ObjectName is complex, skip for now
                AstField::Columns => None, // Column list is complex, skip for now
                _ => None,
            },

            // Query navigation - enhanced
            (AstNode::Query(query), PathComponent::Field(field)) => match field {
                AstField::Body => match &query.body {
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
                AstField::With => query.with.as_ref().map(|w| AstNode::With(w.clone())),
                AstField::OrderBy => {
                    if query.order_by.is_empty() {
                        None
                    } else {
                        Some(AstNode::OrderByList(query.order_by.clone()))
                    }
                }
                AstField::Limit => query.limit.as_ref().map(|e| AstNode::Expr(e.clone())),
                AstField::Offset => None, // offset is Option<String>, not navigable as expression
                _ => None,
            },

            // Select navigation - enhanced
            (AstNode::Select(select), PathComponent::Field(field)) => match field {
                AstField::Projection => {
                    if select.projection.is_empty() {
                        None
                    } else {
                        Some(AstNode::SelectItemList(select.projection.clone()))
                    }
                }
                AstField::Selection => select.selection.as_ref().map(|e| AstNode::Expr(e.clone())),
                AstField::From => {
                    if select.from.is_empty() {
                        None
                    } else {
                        Some(AstNode::TableList(select.from.clone()))
                    }
                }
                AstField::GroupBy => {
                    if select.group_by.is_empty() {
                        None
                    } else {
                        Some(AstNode::ExprList(select.group_by.clone()))
                    }
                }
                AstField::Having => select.having.as_ref().map(|e| AstNode::Expr(e.clone())),
                AstField::Distinct => None, // Distinct is an enum, handle separately if needed
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
            (AstNode::Expr(expr), PathComponent::Field(field)) => match (expr, field) {
                (Expr::BinaryOp { left, .. }, AstField::Left) => Some(AstNode::Expr(*left.clone())),
                (Expr::BinaryOp { right, .. }, AstField::Right) => {
                    Some(AstNode::Expr(*right.clone()))
                }
                (Expr::Case { operand, .. }, AstField::Operand) => {
                    operand.as_ref().map(|e| AstNode::Expr(*e.clone()))
                }
                (Expr::Case { else_result, .. }, AstField::ElseResult) => {
                    else_result.as_ref().map(|e| AstNode::Expr(*e.clone()))
                }
                (Expr::Exists(subquery), AstField::Subquery) => {
                    Some(AstNode::Query(subquery.clone()))
                }
                (Expr::Subquery(subquery), AstField::Subquery) => {
                    Some(AstNode::Query(subquery.clone()))
                }
                (Expr::Function(_func), AstField::Name) => None, // ObjectName is complex
                (Expr::Nested(inner), AstField::Inner) => Some(AstNode::Expr(*inner.clone())),
                (Expr::UnaryOp { expr, .. }, AstField::Expr) => Some(AstNode::Expr(*expr.clone())),
                (Expr::Cast { expr, .. }, AstField::Expr) => Some(AstNode::Expr(*expr.clone())),
                (Expr::IsNull(expr), AstField::Expr) => Some(AstNode::Expr(*expr.clone())),
                (Expr::IsNotNull(expr), AstField::Expr) => Some(AstNode::Expr(*expr.clone())),
                (
                    Expr::Between {
                        expr,
                        low: _,
                        high: _,
                        ..
                    },
                    AstField::Expr,
                ) => Some(AstNode::Expr(*expr.clone())),
                (Expr::Between { low, .. }, AstField::Low) => Some(AstNode::Expr(*low.clone())),
                (Expr::Between { high, .. }, AstField::High) => Some(AstNode::Expr(*high.clone())),
                _ => None,
            },

            // TableWithJoins navigation
            (AstNode::TableWithJoins(table_with_joins), PathComponent::Field(field)) => match field
            {
                AstField::Relation => Some(AstNode::TableFactor(table_with_joins.relation.clone())),
                AstField::Joins => {
                    if !table_with_joins.joins.is_empty() {
                        Some(AstNode::JoinList(table_with_joins.joins.clone()))
                    } else {
                        None
                    }
                }
                _ => None,
            },

            // Join navigation
            (AstNode::Join(join), PathComponent::Field(field)) => match field {
                AstField::Relation => Some(AstNode::TableFactor(join.relation.clone())),
                AstField::JoinOperator => None, // JoinOperator is simple enum, skip navigation
                _ => None,
            },

            // TableFactor navigation
            (AstNode::TableFactor(table_factor), PathComponent::Field(field)) => {
                match (table_factor, field) {
                    (TableFactor::Table { .. }, _) => None, // Table references are terminal
                    (TableFactor::Derived { subquery, .. }, AstField::Subquery) => {
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
                match (select_item, field) {
                    (SelectItem::UnnamedExpr(expr), AstField::Expr) => {
                        Some(AstNode::Expr(expr.clone()))
                    }
                    (SelectItem::ExprWithAlias { expr, .. }, AstField::Expr) => {
                        Some(AstNode::Expr(expr.clone()))
                    }
                    (SelectItem::QualifiedWildcard(..), _) => None,
                    (SelectItem::Wildcard(..), _) => None,
                    _ => None,
                }
            }

            // OrderByExpr navigation
            (AstNode::OrderByExpr(order_by), PathComponent::Field(field)) => match field {
                AstField::Expr => Some(AstNode::Expr(order_by.expr.clone())),
                AstField::Asc => None,        // Boolean, not navigable
                AstField::NullsFirst => None, // Option<bool>, not navigable
                _ => None,
            },

            // With clause navigation
            (AstNode::With(with_clause), PathComponent::Field(field)) => match field {
                AstField::CteTable => {
                    if with_clause.cte_tables.is_empty() {
                        None
                    } else {
                        Some(AstNode::CteList(with_clause.cte_tables.clone()))
                    }
                }
                AstField::Recursive => None, // Boolean, not navigable
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
            (AstNode::Cte(cte), PathComponent::Field(field)) => match field {
                AstField::Alias => None, // TableAlias is complex, skip for now
                AstField::CteInner => match &cte.cte_inner {
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
            // Handle Statement::Query (root query statements)
            (AstNode::Statement(Statement::Query(_)), PathComponent::Field(field))
                if *field == AstField::Query =>
            {
                if let Some(AstNode::Query(new_query)) = new_child {
                    Some(AstNode::Statement(Statement::Query(new_query)))
                } else {
                    None
                }
            }

            (
                AstNode::Statement(Statement::CreateView {
                    name,
                    columns,
                    query: _,
                    or_replace,
                    materialized,
                    if_not_exists,
                    emit_mode,
                    with_options,
                }),
                PathComponent::Field(field),
            ) => match field {
                AstField::Query => {
                    if let Some(AstNode::Query(new_query)) = new_child {
                        let new_stmt = Statement::CreateView {
                            or_replace: *or_replace,
                            materialized: *materialized,
                            if_not_exists: *if_not_exists,
                            name: name.clone(),
                            columns: columns.clone(),
                            query: new_query,
                            emit_mode: emit_mode.clone(),
                            with_options: with_options.clone(),
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
                match field {
                    AstField::Body => {
                        if let Some(AstNode::Select(select)) = new_child {
                            new_query.body = SetExpr::Select(select);
                            Some(AstNode::Query(Box::new(new_query)))
                        } else {
                            None
                        }
                    }
                    AstField::OrderBy => {
                        if let Some(AstNode::OrderByList(orders)) = new_child {
                            new_query.order_by = orders;
                        } else {
                            new_query.order_by = vec![];
                        }
                        Some(AstNode::Query(Box::new(new_query)))
                    }
                    AstField::Limit => {
                        new_query.limit = new_child.and_then(|n| match n {
                            AstNode::Expr(e) => Some(e),
                            _ => None,
                        });
                        Some(AstNode::Query(Box::new(new_query)))
                    }
                    AstField::With => {
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
                match field {
                    AstField::Selection => {
                        new_select.selection = new_child.and_then(|n| match n {
                            AstNode::Expr(e) => Some(e),
                            _ => None,
                        });
                    }
                    AstField::Having => {
                        new_select.having = new_child.and_then(|n| match n {
                            AstNode::Expr(e) => Some(e),
                            _ => None,
                        });
                    }
                    AstField::Projection => {
                        if let Some(AstNode::SelectItemList(items)) = new_child {
                            new_select.projection = items;
                        }
                    }
                    AstField::From => {
                        if let Some(AstNode::TableList(tables)) = new_child {
                            new_select.from = tables;
                        }
                    }
                    AstField::GroupBy => {
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
                match field {
                    AstField::Relation => {
                        if let Some(AstNode::TableFactor(new_relation)) = new_child {
                            Some(AstNode::TableWithJoins(TableWithJoins {
                                relation: new_relation,
                                joins: table_with_joins.joins.clone(),
                            }))
                        } else {
                            None
                        }
                    }
                    AstField::Joins => {
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
            (AstNode::Join(join), PathComponent::Field(field)) => match field {
                AstField::Relation => {
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
                match (table_factor, field) {
                    (TableFactor::Derived { lateral, alias, .. }, AstField::Subquery) => {
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
            (AstNode::Expr(expr), PathComponent::Field(field)) => match (expr, field) {
                (Expr::BinaryOp { left: _, op, right }, AstField::Left) => {
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
                (Expr::BinaryOp { left, op, right: _ }, AstField::Right) => {
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
                (Expr::Nested(_), AstField::Inner) => {
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
                match (select_item, field) {
                    (SelectItem::UnnamedExpr(_), AstField::Expr) => {
                        if let Some(AstNode::Expr(new_expr)) = new_child {
                            Some(AstNode::SelectItem(SelectItem::UnnamedExpr(new_expr)))
                        } else {
                            None
                        }
                    }
                    (SelectItem::ExprWithAlias { alias, .. }, AstField::Expr) => {
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
            (AstNode::OrderByExpr(order_by), PathComponent::Field(field)) => match field {
                AstField::Expr => {
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
            (AstNode::With(with_clause), PathComponent::Field(field)) => match field {
                AstField::CteTable => {
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
            (AstNode::Cte(cte), PathComponent::Field(field)) => match field {
                AstField::CteInner => {
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
                let node_type = match self {
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
                };
                tracing::debug!(
                    "set_child: No match for {} ({:?}) with component {:?}",
                    node_type,
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
fn explore_child_field(
    node: &AstNode,
    field: AstField,
    current_path: &AstPath,
    paths: &mut Vec<AstPath>,
) {
    let field_component = PathComponent::field(field);
    let child_path = [current_path.clone(), vec![field_component.clone()]].concat();
    let relative_path = vec![field_component];

    if let Some(child_node) = get_node_at_path(node, &relative_path) {
        // Collect child paths but don't add them yet (for outer-first ordering)
        let child_paths = enumerate_reduction_paths(&child_node, child_path);
        paths.extend(child_paths);
    }
}

/// Calculate the depth of a path (number of components).
/// Used for outer-first ordering: shallower paths (outer queries) come first.
fn path_depth(path: &AstPath) -> usize {
    path.len()
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
            explore_child_field(node, AstField::Query, &current_path, &mut paths);
        }

        AstNode::Statement(Statement::CreateView { .. }) => {
            explore_child_field(node, AstField::Query, &current_path, &mut paths);
        }

        AstNode::Query(_query) => {
            explore_child_field(node, AstField::Body, &current_path, &mut paths);
            explore_child_field(node, AstField::With, &current_path, &mut paths);
            explore_child_field(node, AstField::OrderBy, &current_path, &mut paths);
        }

        AstNode::Select(_) => {
            explore_child_field(node, AstField::Projection, &current_path, &mut paths);
            explore_child_field(node, AstField::From, &current_path, &mut paths);
            explore_child_field(node, AstField::GroupBy, &current_path, &mut paths);

            // For optional fields, just add the path if they exist
            let selection_path = [current_path.clone(), vec![PathComponent::selection()]].concat();
            if get_node_at_path(node, &vec![PathComponent::selection()]).is_some() {
                paths.push(selection_path);
            }

            let having_path = [current_path.clone(), vec![PathComponent::having()]].concat();
            if get_node_at_path(node, &vec![PathComponent::having()]).is_some() {
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
                let relative_path = vec![PathComponent::Index(i)];
                if let Some(expr_node) = get_node_at_path(node, &relative_path) {
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
            explore_child_field(node, AstField::Relation, &current_path, &mut paths);
            explore_child_field(node, AstField::Joins, &current_path, &mut paths);
        }

        // Join path enumeration
        AstNode::Join(_) => {
            explore_child_field(node, AstField::Relation, &current_path, &mut paths);
        }

        // TableFactor path enumeration
        AstNode::TableFactor(_) => {
            // For Derived tables (subqueries), explore the subquery
            explore_child_field(node, AstField::Subquery, &current_path, &mut paths);
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
                let left_path = [current_path.clone(), vec![PathComponent::left()]].concat();
                let right_path = [current_path.clone(), vec![PathComponent::right()]].concat();
                paths.push(left_path);
                paths.push(right_path);
            }
            Expr::Case {
                operand,
                else_result,
                ..
            } => {
                if operand.is_some() {
                    let operand_path =
                        [current_path.clone(), vec![PathComponent::operand()]].concat();
                    paths.push(operand_path);
                }
                if else_result.is_some() {
                    let else_path =
                        [current_path.clone(), vec![PathComponent::else_result()]].concat();
                    paths.push(else_path);
                }
            }
            Expr::Exists(_) => {
                explore_child_field(node, AstField::Subquery, &current_path, &mut paths);
            }
            Expr::Subquery(_) => {
                explore_child_field(node, AstField::Subquery, &current_path, &mut paths);
            }
            _ => {}
        },

        // WITH clause enumeration
        AstNode::With(_) => {
            explore_child_field(node, AstField::CteTable, &current_path, &mut paths);
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
            explore_child_field(node, AstField::CteInner, &current_path, &mut paths);
        }

        _ => {}
    }

    // Sort paths by depth (outer-first): shallower paths come first
    // This ensures outer queries are reduced before inner subqueries
    // For example: SELECT (SELECT ...) will reduce the outer SELECT first
    paths.sort_by_key(path_depth);

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
        assert!(!paths.is_empty());

        // Should be able to get some node
        assert!(get_node_at_path(&ast_node, &paths[0]).is_some());
    }
}
