use risingwave_common::error::Result;
use risingwave_pb::plan::ColumnDesc as ColumnDescProst;

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

    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// Get table's all column desc in prost format.
    pub fn get_col_desc_prost(&self) -> Result<Vec<ColumnDescProst>> {
        let mut ret = Vec::with_capacity(self.columns.len());
        for (idx, (col_name, col_desc)) in self.columns.iter().enumerate() {
            ret.push(ColumnDescProst {
                column_type: Some(col_desc.data_type().to_protobuf()?),
                encoding: 1,
                is_primary: col_desc.is_primary(),
                name: col_name.into(),
                column_id: idx as i32,
            });
        }
        Ok(ret)
    }
}
