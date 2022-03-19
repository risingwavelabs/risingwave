use itertools::Itertools;
use risingwave_common::catalog::{ColumnDesc, ColumnId};
use risingwave_common::types::DataType;
use risingwave_pb::plan::ColumnCatalog as ProstColumnCatalog;

#[derive(Debug, Clone)]
pub struct ColumnCatalog {
    pub column_desc: ColumnDesc,
    pub is_hidden: bool,
    pub catalogs: Vec<ColumnCatalog>,
    pub type_name: String,
}

impl ColumnCatalog {
    /// Get the column catalog's is hidden.
    pub fn is_hidden(&self) -> bool {
        self.is_hidden
    }

    /// Get a reference to the column desc's data type.
    pub fn data_type(&self) -> &DataType {
        &self.column_desc.data_type
    }

    /// Get the column desc's column id.
    pub fn column_id(&self) -> ColumnId {
        self.column_desc.column_id
    }

    /// Get a reference to the column desc's name.
    pub fn name(&self) -> &str {
        self.column_desc.name.as_ref()
    }

    pub fn get_column_descs(&self) -> Vec<ColumnDesc> {
        let mut descs = vec![self.column_desc.clone()];
        for catalog in &self.catalogs {
            descs.append(&mut catalog.get_column_descs());
        }
        descs
    }
}

impl From<ProstColumnCatalog> for ColumnCatalog {
    fn from(prost: ProstColumnCatalog) -> Self {
        let mut column_desc: ColumnDesc = prost.column_desc.unwrap().into();
        if let DataType::Struct { .. } = column_desc.data_type {
            let catalogs: Vec<ColumnCatalog> = prost
                .catalogs
                .into_iter()
                .map(ColumnCatalog::from)
                .collect();
            column_desc.data_type = DataType::Struct {
                fields: catalogs
                    .clone()
                    .into_iter()
                    .map(|c| c.data_type().clone())
                    .collect_vec()
                    .into(),
            };
            Self {
                column_desc,
                is_hidden: prost.is_hidden,
                catalogs,
                type_name: prost.type_name,
            }
        } else {
            Self {
                column_desc,
                is_hidden: prost.is_hidden,
                catalogs: vec![],
                type_name: prost.type_name,
            }
        }
    }
}
