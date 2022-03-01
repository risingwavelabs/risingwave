use std::collections::HashMap;

use risingwave_common::types::DataType;

pub struct ColumnBinding {
    pub id: usize,
    pub data_type: DataType,
}

pub struct BindContext {
    // Mapping table name to columns.
    pub tables: HashMap<String, HashMap<String, ColumnBinding>>,
}

impl BindContext {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        BindContext {
            tables: HashMap::new(),
        }
    }
}
