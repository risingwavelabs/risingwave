use std::collections::HashMap;

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

pub struct BindContext {
    // Mapping column name to `ColumnBinding`
    pub columns: HashMap<String, Vec<ColumnBinding>>,
    pub in_values_clause: bool,
}

impl BindContext {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        BindContext {
            // tables: HashMap::new(),
            columns: HashMap::new(),
            in_values_clause: false,
        }
    }
}
