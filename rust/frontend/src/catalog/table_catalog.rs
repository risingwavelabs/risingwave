use std::collections::HashSet;

use itertools::Itertools;
use risingwave_common::array::RwError;
use risingwave_common::error::Result;
use risingwave_common::types::DataType;
use risingwave_pb::data::data_type::TypeName;
use risingwave_pb::meta::Table;
use risingwave_pb::plan::ColumnDesc as ProstColumnDesc;

use crate::catalog::column_catalog::{ColumnCatalog, ColumnDesc};
use crate::catalog::{CatalogError, ColumnId, TableId};

#[derive(Clone)]
pub struct TableCatalog {
    table_id: TableId,
    next_column_id: i32,
    columns: Vec<ColumnCatalog>,
}

pub const ROWID_NAME: &str = "_row_id";

impl TableCatalog {
    pub fn new(table_id: TableId) -> Self {
        Self {
            table_id,
            next_column_id: 0,
            columns: Vec::new(),
        }
    }

    pub fn add_column(&mut self, col_desc: &ProstColumnDesc) -> Result<()> {
        let col_catalog = self.prost_column_desc_to_catalog(col_desc);
        self.columns.push(col_catalog);
        Ok(())
    }

    pub fn columns(&self) -> &[ColumnCatalog] {
        &self.columns
    }

    pub fn id(&self) -> TableId {
        self.table_id
    }

    pub fn next_id(&mut self) -> i32 {
        let id = self.next_column_id;
        self.next_column_id += 1;
        id
    }

    pub fn prost_column_desc_to_catalog(&mut self, col: &ProstColumnDesc) -> ColumnCatalog {
        if col.column_type.as_ref().expect("wrong type").type_name == TypeName::Struct as i32 {
            let v: Vec<ColumnCatalog> = col
                .column_descs
                .iter()
                .map(|c| self.prost_column_desc_to_catalog(c))
                .collect_vec();
            let data_types = v.iter().map(|c| c.data_type()).collect_vec();
            let desc = ColumnDesc {
                data_type: DataType::Struct {
                    fields: data_types.into(),
                },
                type_name: Some(col.get_struct_name().to_string()),
            };
            ColumnCatalog::new(ColumnId::from(self.next_id()), col.name.clone(), desc, v)
        } else {
            let desc = ColumnDesc {
                data_type: col.get_column_type().expect("column type not found").into(),
                type_name: Some(col.get_struct_name().to_string()),
            };
            ColumnCatalog::new(
                ColumnId::from(self.next_id()),
                col.name.clone(),
                desc,
                vec![],
            )
        }
    }
}

impl TryFrom<&Table> for TableCatalog {
    type Error = RwError;

    fn try_from(tb: &Table) -> Result<Self> {
        let mut table_catalog = Self::new(TableId::from(&tb.table_ref_id));
        let mut names = HashSet::new();
        for col in &tb.column_descs {
            if !names.insert(col.name.clone()) {
                return Err(CatalogError::Duplicated("column", col.name.clone()).into());
            }
            table_catalog.add_column(col)?;
        }
        Ok(table_catalog)
    }
}
