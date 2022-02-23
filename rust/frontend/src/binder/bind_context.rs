use std::collections::HashMap;

use risingwave_common::types::DataType;

pub struct ColumnBinding {
    pub id: usize,
    pub data_type: DataType,
}

pub struct BindContext {
    // TODO: support multiple tables.

    // Mapping column name to column.
    pub columns: HashMap<String, ColumnBinding>,
}

impl BindContext {
    pub fn new() -> Self {
        BindContext {
            columns: HashMap::new(),
        }
    }
}
