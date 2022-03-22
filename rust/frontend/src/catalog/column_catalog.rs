// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

use itertools::Itertools;
use risingwave_common::catalog::{ColumnDesc, ColumnId};
use risingwave_common::types::DataType;
use risingwave_pb::plan::ColumnCatalog as ProstColumnCatalog;

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnCatalog {
    pub column_desc: ColumnDesc,
    pub is_hidden: bool,
    pub field_catalogs: Vec<ColumnCatalog>,
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

    // Get all column descs by recursion
    pub fn get_column_descs(&self) -> Vec<ColumnDesc> {
        let mut descs = vec![self.column_desc.clone()];
        for catalog in &self.field_catalogs {
            descs.append(&mut catalog.get_column_descs());
        }
        descs
    }

    #[cfg(test)]
    pub fn new_atomic_catalog(data_type: DataType, name: &str, column_id: i32) -> Self {
        Self {
            column_desc: ColumnDesc {
                data_type,
                column_id: ColumnId::new(column_id),
                name: name.to_string(),
            },
            is_hidden: false,
            field_catalogs: vec![],
            type_name: String::new(),
        }
    }

    #[cfg(test)]
    pub fn new_struct_catalog(
        data_type: DataType,
        name: &str,
        column_id: i32,
        type_name: &str,
        fields: Vec<ColumnCatalog>,
    ) -> Self {
        Self {
            column_desc: ColumnDesc {
                data_type,
                column_id: ColumnId::new(column_id),
                name: name.to_string(),
            },
            is_hidden: false,
            field_catalogs: fields,
            type_name: type_name.to_string(),
        }
    }
}

impl From<ProstColumnCatalog> for ColumnCatalog {
    // If the DataType is struct, the column_catalog need to rebuild DataType Struct fields
    // according to its catalogs
    fn from(prost: ProstColumnCatalog) -> Self {
        let mut column_desc: ColumnDesc = prost.column_desc.unwrap().into();
        if let DataType::Struct { .. } = column_desc.data_type {
            let catalogs: Vec<ColumnCatalog> = prost
                .field_catalogs
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
                field_catalogs: catalogs,
                type_name: prost.type_name,
            }
        } else {
            Self {
                column_desc,
                is_hidden: prost.is_hidden,
                field_catalogs: vec![],
                type_name: prost.type_name,
            }
        }
    }
}

#[cfg(test)]
pub mod tests {
    use risingwave_common::types::*;
    use risingwave_pb::plan::ColumnCatalog as ProstColumnCatalog;

    use crate::catalog::column_catalog::ColumnCatalog;

    #[cfg(test)]
    pub fn build_prost_catalog() -> ProstColumnCatalog {
        let city = vec![
            ProstColumnCatalog::new_atomic(
                DataType::Varchar.to_protobuf(),
                "country.city.address",
                2,
            ),
            ProstColumnCatalog::new_atomic(
                DataType::Varchar.to_protobuf(),
                "country.city.zipcode",
                3,
            ),
        ];
        let country = vec![
            ProstColumnCatalog::new_atomic(DataType::Varchar.to_protobuf(), "country.address", 1),
            ProstColumnCatalog::new_struct(
                DataType::Struct {
                    fields: vec![].into(),
                }
                .to_protobuf(),
                "country.city",
                4,
                ".test.City",
                city,
            ),
        ];
        ProstColumnCatalog::new_struct(
            DataType::Struct {
                fields: vec![].into(),
            }
            .to_protobuf(),
            "country",
            5,
            ".test.Country",
            country,
        )
    }

    #[cfg(test)]
    pub fn build_catalog() -> ColumnCatalog {
        let city = vec![
            ColumnCatalog::new_atomic_catalog(DataType::Varchar, "country.city.address", 2),
            ColumnCatalog::new_atomic_catalog(DataType::Varchar, "country.city.zipcode", 3),
        ];
        let data_type = vec![DataType::Varchar, DataType::Varchar];
        let country = vec![
            ColumnCatalog::new_atomic_catalog(DataType::Varchar, "country.address", 1),
            ColumnCatalog::new_struct_catalog(
                DataType::Struct {
                    fields: data_type.clone().into(),
                },
                "country.city",
                4,
                ".test.City",
                city,
            ),
        ];
        ColumnCatalog::new_struct_catalog(
            DataType::Struct {
                fields: vec![
                    DataType::Varchar,
                    DataType::Struct {
                        fields: data_type.into(),
                    },
                ]
                .into(),
            },
            "country",
            5,
            ".test.Country",
            country,
        )
    }

    #[cfg(test)]
    fn test_into_column_catalog() {
        let catalog: ColumnCatalog = build_prost_catalog().into();
        assert_eq!(catalog, build_catalog());
    }
}
