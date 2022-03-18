use std::collections::HashMap;
use std::fmt::Display;

use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::DataType;

#[derive(Debug)]
pub struct ColumnBinding {
    pub table_name: String,
    pub column_name: String,
    pub index: usize,
    pub data_type: DataType,
}

impl ColumnBinding {
    pub fn new(table_name: String, column_name: String, index: usize, data_type: DataType) -> Self {
        ColumnBinding {
            table_name,
            column_name,
            index,
            data_type,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
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

#[derive(Default)]
pub struct BindContext {
    // Columns of all tables.
    pub columns: Vec<ColumnBinding>,
    // Mapping column name to indexs in `columns`.
    pub indexs_of: HashMap<String, Vec<usize>>,
    // Mapping table name to [begin, end) of its columns.
    pub range_of: HashMap<String, (usize, usize)>,
    // `clause` identifies in what clause we are binding.
    pub clause: Option<Clause>,
}

impl BindContext {
    pub fn get_index(&self, column_name: &String) -> Result<usize> {
        let columns = self
            .indexs_of
            .get(column_name)
            .ok_or_else(|| ErrorCode::ItemNotFound(format!("Invalid column: {}", column_name)))?;
        if columns.len() > 1 {
            Err(ErrorCode::InternalError("Ambiguous column name".into()).into())
        } else {
            Ok(columns[0])
        }
    }

    pub fn get_index_with_table_name(
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
}

impl BindContext {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        BindContext {
            columns: Vec::new(),
            indexs_of: HashMap::new(),
            range_of: HashMap::new(),
            clause: None,
        }
    }
}
