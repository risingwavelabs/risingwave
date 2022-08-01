// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::{hash_map::Entry};
use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::cell::RefCell;

use risingwave_common::catalog::Field;
use risingwave_common::error::{ErrorCode, Result};

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

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Clause {
    Where,
    Values,
}

impl Display for Clause {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Clause::Where => write!(f, "WHERE"),
            Clause::Values => write!(f, "VALUES"),
        }
    }
}

/// A `BindContext` that is only visible if the `LATERAL` keyword
/// is provided.
pub struct LateralBindContext {
    pub is_visible: bool,
    pub context: BindContext,
}

#[derive(Default, Debug, Clone)]
pub struct BindContext {
    // Columns of all tables.
    pub columns: Vec<ColumnBinding>,
    // Mapping column name to indexs in `columns`.
    pub indexs_of: HashMap<String, Vec<usize>>,
    // Mapping table name to [begin, end) of its columns.
    pub range_of: HashMap<String, (usize, usize)>,
    // `clause` identifies in what clause we are binding.
    pub clause: Option<Clause>,
    // Maps naturally-joined columns to their natural column group
    natural_column_groups: HashMap<usize, RefCell<NaturalColumnGroup>>
}

#[derive(Default, Debug, Clone)]
struct NaturalColumnGroup {
    /// Indices of the columns in the group
    indices: HashSet<usize>,
    /// index for non-nullable column
    /// if None, ambiguous references to the column name
    /// will be resolved to a Coalesce(a, b, ..., z)
    /// over each column in the group
    non_nullable_column: Option<usize>
}

impl BindContext {
    /// If return Vec has len > 1, it means we have an unqualified reference to a column which has been
    /// naturally joined upon, wherein each input column is possibly nullable. This will be handled in 
    /// downstream as a `COALESCE` expression
    pub fn get_column_binding_index(
        &self,
        table_name: &Option<String>,
        column_name: &String,
    ) -> Result<Vec<usize>> {
        match table_name {
            Some(table_name) => Ok(vec![self.get_index_with_table_name(column_name, table_name)?]),
            None => self.get_unqualified_index(column_name),
        }
    }

    pub fn get_unqualified_index(&self, column_name: &String) -> Result<Vec<usize>> {
        let columns = self
            .indexs_of
            .get(column_name)
            .ok_or_else(|| ErrorCode::ItemNotFound(format!("Invalid column: {}", column_name)))?;
        if columns.len() > 1 {
            // If there is some group containing the columns and the ambiguous columns are all in the group
            if let Some(group) = self.natural_column_groups.get(&columns[0])
            && columns.iter().all(|idx| group.borrow().indices.contains(idx)) { 
                if let Some(non_nullable) = group.borrow().non_nullable_column {
                    return Ok(vec![non_nullable]);
                } else {
                    // These will be converted to a `COALESCE(col1, col2, ..., coln)`
                    return Ok(columns.to_vec());
                }
            }
            Err(ErrorCode::InternalError(format!("Ambiguous column name: {}", column_name)).into())
        } else {
            Ok(columns.to_vec())
        }
    }

    pub fn add_natural_columns(&mut self, left: usize, right: usize, non_nullable_column: Option<usize>) {
        let l_ref = self.natural_column_groups.get(&left).is_some();
        let r_ref = self.natural_column_groups.get(&right).is_some();
        match (l_ref, r_ref) {
            (false, false) => {
                let group = RefCell::new(NaturalColumnGroup {
                    indices: HashSet::from([left, right]),
                    non_nullable_column
                });
                self.natural_column_groups.insert(left, group.clone());
                self.natural_column_groups.insert(right, group);
            },
            (true, false) => {
                let group_ref = {
                    let group_ref = self.natural_column_groups.get_mut(&left).unwrap();
                    let group = group_ref.get_mut();
                    group.indices.insert(right);
                    if group.non_nullable_column.is_none() {
                        group.non_nullable_column = non_nullable_column;
                    }
                    group_ref.clone()
                };
                self.natural_column_groups.insert(right, group_ref);
            },
            (false, true) => {
                let group_ref = {
                    let group_ref = self.natural_column_groups.get_mut(&right).unwrap();
                    let group = group_ref.get_mut();
                    group.indices.insert(left);
                    if group.non_nullable_column.is_none() {
                        group.non_nullable_column = non_nullable_column;
                    }
                    group_ref.clone()
                };
                self.natural_column_groups.insert(left, group_ref);
            },
            (true, true) => {
                let r_group_ref = self.natural_column_groups.get(&right).unwrap().clone();
                let r_group = r_group_ref.borrow();
                let group_ref = {
                    let group_ref = self.natural_column_groups.get_mut(&left).unwrap();
                    let group = group_ref.get_mut();
                    for idx in &r_group.indices {
                        group.indices.insert(*idx);
                    }
                    group.indices.insert(right);
                    if group.non_nullable_column.is_none() {
                        group.non_nullable_column = if r_group.non_nullable_column.is_none() {
                            non_nullable_column
                        } else {
                            r_group.non_nullable_column.clone()
                        };
                    }
                    group_ref.clone()
                };
                for idx in &r_group.indices {
                    debug_assert!(self.natural_column_groups.insert(*idx, group_ref.clone()).is_some());
                }
            },
        }
    }

    fn get_index_with_table_name(
        &self,
        column_name: &String,
        table_name: &String,
    ) -> Result<usize> {
        let column_indexes = self
            .indexs_of
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
            ))
            .into()),
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
        for (k, v) in other.indexs_of {
            let entry = self.indexs_of.entry(k).or_insert_with(Vec::new);
            entry.extend(v.into_iter().map(|x| x + begin));
        }
        for (k, (x, y)) in other.range_of {
            match self.range_of.entry(k) {
                Entry::Occupied(e) => {
                    return Err(ErrorCode::InternalError(format!(
                        "Duplicated table name while binding context {}",
                        e.key()
                    ))
                    .into());
                }
                Entry::Vacant(entry) => {
                    entry.insert((begin + x, begin + y));
                }
            }
        }
        // we assume that the clause is contained in the outer-level context
        Ok(())
    }
}

impl BindContext {
    pub fn new() -> Self {
        Self::default()
    }
}
