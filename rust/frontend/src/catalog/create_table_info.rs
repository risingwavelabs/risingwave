use crate::catalog::column_catalog::ColumnDesc;

pub struct CreateTableInfo {
    name: String,
    columns: Vec<(String, ColumnDesc)>,
    source: bool,
    append_only: bool,
}

impl CreateTableInfo {
    pub fn new(table_name: &str, columns: Vec<(String, ColumnDesc)>) -> Self {
        Self {
            name: table_name.to_string(),
            columns,
            source: false,
            append_only: false,
        }
    }

    pub fn get_name(&self) -> &str {
        &self.name
    }

    pub fn get_columns(&self) -> &Vec<(String, ColumnDesc)> {
        &self.columns
    }
}
