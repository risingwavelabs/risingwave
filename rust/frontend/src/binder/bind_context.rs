use std::collections::HashMap;
use std::fmt::Display;

use risingwave_common::types::DataType;

pub struct ColumnBinding {
    pub table_name: String,
    pub index: usize,
    pub data_type: DataType,
}

impl ColumnBinding {
    pub fn new(table_name: String, index: usize, data_type: DataType) -> Self {
        ColumnBinding {
            table_name,
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

pub struct BindContext {
    // Mapping column name to `ColumnBinding`
    pub columns: HashMap<String, Vec<ColumnBinding>>,
    // `clause` identifies in what clause we are binding.
    pub clause: Option<Clause>,
}

impl BindContext {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        BindContext {
            // tables: HashMap::new(),
            columns: HashMap::new(),
            clause: None,
        }
    }
}
