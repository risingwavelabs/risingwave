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

use std::cell::RefCell;
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::rc::Rc;

use either::Either;
use parse_display::Display;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::types::DataType;
use risingwave_sqlparser::ast::{TableAlias, WindowSpec};

use crate::binder::Relation;
use crate::error::{ErrorCode, Result};

type LiteResult<T> = std::result::Result<T, ErrorCode>;

use super::BoundSetExpr;
use super::statement::RewriteExprsRecursive;
use crate::binder::{BoundQuery, COLUMN_GROUP_PREFIX, ShareId};

#[derive(Debug, Clone)]
pub struct ColumnBinding {
    pub table_name: String,
    pub index: usize,
    pub is_hidden: bool,
    pub field: Field,
}

impl ColumnBinding {
    pub fn new(table_name: String, index: usize, is_hidden: bool, field: Field) -> Self {
        ColumnBinding {
            table_name,
            index,
            is_hidden,
            field,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Display)]
#[display(style = "TITLE CASE")]
pub enum Clause {
    Where,
    Values,
    GroupBy,
    JoinOn,
    Having,
    Filter,
    From,
    GeneratedColumn,
    Insert,
}

/// A `BindContext` that is only visible if the `LATERAL` keyword
/// is provided.
pub struct LateralBindContext {
    pub is_visible: bool,
    pub context: BindContext,
}

/// For recursive CTE, we may need to store it in `cte_to_relation` first,
/// and then bind it *step by step*.
///
/// note: the below sql example is to illustrate when we get the
/// corresponding binding state when handling a recursive CTE like this.
///
/// ```sql
/// WITH RECURSIVE t(n) AS (
/// # -------------^ => Init
///     VALUES (1)
///   UNION ALL
///     SELECT n + 1 FROM t WHERE n < 100
/// # --------------------^ => BaseResolved (after binding the base term, this relation will be bound to `Relation::BackCteRef`)
/// )
/// SELECT sum(n) FROM t;
/// # -----------------^ => Bound (we know exactly what the entire `RecursiveUnion` looks like, and this relation will be bound to `Relation::Share`)
/// ```
#[derive(Default, Debug, Clone)]
pub enum BindingCteState {
    /// We know nothing about the CTE before resolving the body.
    #[default]
    Init,
    /// We know the schema form after the base term resolved.
    BaseResolved {
        base: BoundSetExpr,
    },
    /// We get the whole bound result of the (recursive) CTE.
    Bound {
        query: Either<BoundQuery, RecursiveUnion>,
    },

    ChangeLog {
        table: Relation,
    },
}

/// the entire `RecursiveUnion` represents a *bound* recursive cte.
/// reference: <https://github.com/risingwavelabs/risingwave/pull/15522/files#r1524367781>
#[derive(Debug, Clone)]
pub struct RecursiveUnion {
    /// currently this *must* be true,
    /// otherwise binding will fail.
    #[allow(dead_code)]
    pub all: bool,
    /// lhs part of the `UNION ALL` operator
    pub base: Box<BoundSetExpr>,
    /// rhs part of the `UNION ALL` operator
    pub recursive: Box<BoundSetExpr>,
    /// the aligned schema for this union
    /// will be the *same* schema as recursive's
    /// this is just for a better readability
    pub schema: Schema,
}

impl RewriteExprsRecursive for RecursiveUnion {
    fn rewrite_exprs_recursive(&mut self, rewriter: &mut impl crate::expr::ExprRewriter) {
        // rewrite `base` and `recursive` separately
        self.base.rewrite_exprs_recursive(rewriter);
        self.recursive.rewrite_exprs_recursive(rewriter);
    }
}

#[derive(Clone, Debug)]
pub struct BindingCte {
    pub share_id: ShareId,
    pub state: BindingCteState,
    pub alias: TableAlias,
}

#[derive(Default, Debug, Clone)]
pub struct BindContext {
    // Columns of all tables.
    pub columns: Vec<ColumnBinding>,
    // Mapping column name to indices in `columns`.
    pub indices_of: HashMap<String, Vec<usize>>,
    // Mapping table name to [begin, end) of its columns.
    pub range_of: HashMap<String, (usize, usize)>,
    // `clause` identifies in what clause we are binding.
    pub clause: Option<Clause>,
    // The `BindContext`'s data on its column groups
    pub column_group_context: ColumnGroupContext,
    /// Map the cte's name to its binding state.
    /// The `ShareId` in `BindingCte` of the value is used to help the planner identify the share plan.
    pub cte_to_relation: HashMap<String, Rc<RefCell<BindingCte>>>,
    /// Current lambda functions's arguments
    pub lambda_args: Option<HashMap<String, (usize, DataType)>>,
    /// Whether the security invoker is set, currently only used for views.
    pub disable_security_invoker: bool,
    /// Named window definitions from the `WINDOW` clause
    pub named_windows: HashMap<String, WindowSpec>,
}

/// Holds the context for the `BindContext`'s `ColumnGroup`s.
#[derive(Default, Debug, Clone)]
pub struct ColumnGroupContext {
    // Maps naturally-joined/USING columns to their column group id
    pub mapping: HashMap<usize, u32>,
    // Maps column group ids to their column group data
    // We use a BTreeMap to ensure that iteration over the groups is ordered.
    pub groups: BTreeMap<u32, ColumnGroup>,

    next_group_id: u32,
}

/// When binding a natural join or a join with USING, a `ColumnGroup` contains the columns with the
/// same name.
#[derive(Default, Debug, Clone)]
pub struct ColumnGroup {
    /// Indices of the columns in the group
    pub indices: HashSet<usize>,
    /// A non-nullable column is never NULL.
    /// If `None`, ambiguous references to the column name will be resolved to a `COALESCE(col1,
    /// col2, ..., coln)` over each column in the group
    pub non_nullable_column: Option<usize>,

    pub column_name: Option<String>,
}

impl BindContext {
    pub fn get_column_binding_index(
        &self,
        table_name: &Option<String>,
        column_name: &String,
    ) -> LiteResult<usize> {
        match &self.get_column_binding_indices(table_name, column_name)?[..] {
            [] => unreachable!(),
            [idx] => Ok(*idx),
            _ => Err(ErrorCode::InternalError(format!(
                "Ambiguous column name: {}",
                column_name
            ))),
        }
    }

    /// If return Vec has len > 1, it means we have an unqualified reference to a column which has
    /// been naturally joined upon, wherein none of the columns are min-nullable. This will be
    /// handled in downstream as a `COALESCE` expression
    pub fn get_column_binding_indices(
        &self,
        table_name: &Option<String>,
        column_name: &String,
    ) -> LiteResult<Vec<usize>> {
        match table_name {
            Some(table_name) => {
                if let Some(group_id_str) = table_name.strip_prefix(COLUMN_GROUP_PREFIX) {
                    let group_id = group_id_str.parse::<u32>().map_err(|_|ErrorCode::InternalError(
                        format!("Could not parse {:?} as virtual table name `{COLUMN_GROUP_PREFIX}[group_id]`", table_name)))?;
                    self.get_indices_with_group_id(group_id, column_name)
                } else {
                    Ok(vec![
                        self.get_index_with_table_name(column_name, table_name)?,
                    ])
                }
            }
            None => self.get_unqualified_indices(column_name),
        }
    }

    fn get_indices_with_group_id(
        &self,
        group_id: u32,
        column_name: &String,
    ) -> LiteResult<Vec<usize>> {
        let group = self.column_group_context.groups.get(&group_id).unwrap();
        if let Some(name) = &group.column_name {
            debug_assert_eq!(name, column_name);
        }
        if let Some(non_nullable) = &group.non_nullable_column {
            Ok(vec![*non_nullable])
        } else {
            // These will be converted to a `COALESCE(col1, col2, ..., coln)`
            let mut indices: Vec<_> = group.indices.iter().copied().collect();
            indices.sort(); // ensure a deterministic result
            Ok(indices)
        }
    }

    pub fn get_unqualified_indices(&self, column_name: &String) -> LiteResult<Vec<usize>> {
        let columns = self
            .indices_of
            .get(column_name)
            .ok_or_else(|| ErrorCode::ItemNotFound(format!("Invalid column: {column_name}")))?;
        if columns.len() > 1 {
            // If there is some group containing the columns and the ambiguous columns are all in
            // the group
            if let Some(group_id) = self.column_group_context.mapping.get(&columns[0]) {
                let group = self.column_group_context.groups.get(group_id).unwrap();
                if columns.iter().all(|idx| group.indices.contains(idx)) {
                    if let Some(non_nullable) = &group.non_nullable_column {
                        return Ok(vec![*non_nullable]);
                    } else {
                        // These will be converted to a `COALESCE(col1, col2, ..., coln)`
                        return Ok(columns.to_vec());
                    }
                }
            }
            Err(ErrorCode::InternalError(format!(
                "Ambiguous column name: {}",
                column_name
            )))
        } else {
            Ok(columns.to_vec())
        }
    }

    /// Identifies two columns as being in the same group. Additionally, possibly provides one of
    /// the columns as being `non_nullable`
    pub fn add_natural_columns(
        &mut self,
        left: usize,
        right: usize,
        non_nullable_column: Option<usize>,
    ) {
        match (
            self.column_group_context.mapping.get(&left).copied(),
            self.column_group_context.mapping.get(&right).copied(),
        ) {
            (None, None) => {
                let group_id = self.column_group_context.next_group_id;
                self.column_group_context.next_group_id += 1;

                let group = ColumnGroup {
                    indices: HashSet::from([left, right]),
                    non_nullable_column,
                    column_name: Some(self.columns[left].field.name.clone()),
                };
                self.column_group_context.groups.insert(group_id, group);
                self.column_group_context.mapping.insert(left, group_id);
                self.column_group_context.mapping.insert(right, group_id);
            }
            (Some(group_id), None) => {
                let group = self.column_group_context.groups.get_mut(&group_id).unwrap();
                group.indices.insert(right);
                if group.non_nullable_column.is_none() {
                    group.non_nullable_column = non_nullable_column;
                }
                self.column_group_context.mapping.insert(right, group_id);
            }
            (None, Some(group_id)) => {
                let group = self.column_group_context.groups.get_mut(&group_id).unwrap();
                group.indices.insert(left);
                if group.non_nullable_column.is_none() {
                    group.non_nullable_column = non_nullable_column;
                }
                self.column_group_context.mapping.insert(left, group_id);
            }
            (Some(l_group_id), Some(r_group_id)) => {
                if r_group_id == l_group_id {
                    return;
                }

                let r_group = self
                    .column_group_context
                    .groups
                    .remove(&r_group_id)
                    .unwrap();
                let l_group = self
                    .column_group_context
                    .groups
                    .get_mut(&l_group_id)
                    .unwrap();

                for idx in &r_group.indices {
                    *self.column_group_context.mapping.get_mut(idx).unwrap() = l_group_id;
                    l_group.indices.insert(*idx);
                }
                if l_group.non_nullable_column.is_none() {
                    l_group.non_nullable_column = if r_group.non_nullable_column.is_none() {
                        non_nullable_column
                    } else {
                        r_group.non_nullable_column
                    };
                }
            }
        }
    }

    fn get_index_with_table_name(
        &self,
        column_name: &String,
        table_name: &String,
    ) -> LiteResult<usize> {
        let column_indexes = self
            .indices_of
            .get(column_name)
            .ok_or_else(|| ErrorCode::ItemNotFound(format!("Invalid column: {}", column_name)))?;
        match column_indexes
            .iter()
            .find(|column_index| self.columns[**column_index].table_name == *table_name)
        {
            Some(column_index) => Ok(*column_index),
            None => Err(ErrorCode::ItemNotFound(format!(
                "missing FROM-clause entry for table \"{}\"",
                table_name
            ))),
        }
    }

    /// Merges two `BindContext`s which are adjacent. For instance, the `BindContext` of two
    /// adjacent cross-joined tables.
    pub fn merge_context(&mut self, other: Self) -> Result<()> {
        let begin = self.columns.len();
        self.columns.extend(other.columns.into_iter().map(|mut c| {
            c.index += begin;
            c
        }));
        for (k, v) in other.indices_of {
            let entry = self.indices_of.entry(k).or_default();
            entry.extend(v.into_iter().map(|x| x + begin));
        }
        for (k, (x, y)) in other.range_of {
            match self.range_of.entry(k) {
                Entry::Occupied(e) => {
                    return Err(ErrorCode::InternalError(format!(
                        "Duplicated table name while merging adjacent contexts: {}",
                        e.key()
                    ))
                    .into());
                }
                Entry::Vacant(entry) => {
                    entry.insert((begin + x, begin + y));
                }
            }
        }
        // To merge the column_group_contexts, we just need to offset RHS
        // with the next_group_id of LHS.
        let ColumnGroupContext {
            mapping,
            groups,
            next_group_id,
        } = other.column_group_context;

        let offset = self.column_group_context.next_group_id;
        for (idx, group_id) in mapping {
            self.column_group_context
                .mapping
                .insert(begin + idx, offset + group_id);
        }
        for (group_id, mut group) in groups {
            group.indices = group.indices.into_iter().map(|idx| idx + begin).collect();
            if let Some(col) = &mut group.non_nullable_column {
                *col += begin;
            }
            self.column_group_context
                .groups
                .insert(offset + group_id, group);
        }
        self.column_group_context.next_group_id += next_group_id;

        // we assume that the clause is contained in the outer-level context
        Ok(())
    }
}

impl BindContext {
    pub fn new() -> Self {
        Self::default()
    }
}
