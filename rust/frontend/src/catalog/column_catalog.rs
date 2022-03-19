use itertools::Itertools;
use risingwave_common::catalog::{ColumnDesc, ColumnId};
use risingwave_common::types::DataType;
use risingwave_pb::plan::ColumnCatalog as ProstColumnCatalog;

#[derive(Debug, Clone, PartialEq)]
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

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::{ColumnDesc, ColumnId};
    use risingwave_pb::plan::{ColumnCatalog as ProstColumnCatalog, ColumnDesc as ProstColumnDesc};
    use crate::catalog::column_catalog::ColumnCatalog;

    #[test]
    fn test_into_column_catalog() {
        use risingwave_common::types::*;

        let city = vec![
            ProstColumnCatalog {
                column_desc: Some(ProstColumnDesc {
                    column_type: Some(DataType::Varchar.to_protobuf()),
                    name: "country.city.address".to_string(),
                    column_id: 2,
                }),
                is_hidden: false,
                catalogs: vec![],
                ..Default::default()
            },
            ProstColumnCatalog {
                column_desc: Some(ProstColumnDesc {
                    column_type: Some(DataType::Varchar.to_protobuf()),
                    name: "country.city.zipcode".to_string(),
                    column_id: 3,
                }),
                is_hidden: false,
                catalogs: vec![],
                ..Default::default()
            },
        ];
        let country = vec![
            ProstColumnCatalog {
                column_desc: Some(ProstColumnDesc {
                    column_type: Some(DataType::Varchar.to_protobuf()),
                    name: "country.address".to_string(),
                    column_id: 1,
                }),
                is_hidden: false,
                catalogs: vec![],
                ..Default::default()
            },
            ProstColumnCatalog {
                column_desc: Some(ProstColumnDesc {
                    column_type: Some(
                        DataType::Struct {
                            fields: vec![].into(),
                        }
                        .to_protobuf(),
                    ),
                    name: "country.city".to_string(),
                    column_id: 4,
                }),
                is_hidden: false,
                catalogs: city,
                type_name: ".test.City".to_string(),
            },
        ];
        let catalog:ColumnCatalog = ProstColumnCatalog {
            column_desc: Some(ProstColumnDesc {
                column_type: Some(
                    DataType::Struct {
                        fields: vec![].into(),
                    }
                    .to_protobuf(),
                ),
                name: "country".to_string(),
                column_id: 5,
            }),
            is_hidden: false,
            catalogs: country,
            type_name: ".test.Country".to_string(),
        }.into();

        let city = vec![
            ColumnCatalog {
                column_desc: ColumnDesc {
                    data_type: DataType::Varchar,
                    name: "country.city.address".to_string(),
                    column_id: ColumnId::new(2),
                },
                is_hidden: false,
                catalogs: vec![],
                type_name: String::new(),
            },
            ColumnCatalog {
                column_desc: ColumnDesc {
                    data_type: DataType::Varchar,
                    name: "country.city.zipcode".to_string(),
                    column_id: ColumnId::new(3),
                },
                is_hidden: false,
                catalogs: vec![],
                type_name: String::new(),
            },
        ];
        let data_type = vec![DataType::Varchar,DataType::Varchar];
        let country = vec![
            ColumnCatalog {
                column_desc: ColumnDesc {
                    data_type: DataType::Varchar,
                    name: "country.address".to_string(),
                    column_id: ColumnId::new(1),
                },
                is_hidden: false,
                catalogs: vec![],
                type_name: String::new(),
            },
            ColumnCatalog {
                column_desc: ColumnDesc {
                    data_type: DataType::Struct {fields:data_type.clone().into()},
                    name: "country.city".to_string(),
                    column_id: ColumnId::new(4),
                },
                is_hidden: false,
                catalogs: city,
                type_name: ".test.City".to_string(),
            },
        ];

        assert_eq!(catalog,ColumnCatalog{
            column_desc: ColumnDesc {
                data_type: DataType::Struct {fields:vec![DataType::Varchar,DataType::Struct {fields:data_type.into()}].into()},
                column_id: ColumnId::new(5),
                name: "country".to_string()
            },
            is_hidden: false,
            catalogs: country,
            type_name: ".test.Country".to_string(),
        });
    }
}
