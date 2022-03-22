use itertools::Itertools;
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
use risingwave_pb::plan::{
    ColumnDesc as ProstColumnDesc, OrderType as ProstOrderType,
    OrderedColumnDesc as ProstOrderedColumnDesc,
};

use crate::types::DataType;
use crate::util::sort_util::OrderType;

/// Column ID is the unique identifier of a column in a table. Different from table ID,
/// column ID is not globally unique.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct ColumnId(i32);

impl ColumnId {
    pub const fn new(column_id: i32) -> Self {
        Self(column_id)
    }
}

impl ColumnId {
    pub fn get_id(&self) -> i32 {
        self.0
    }
}

impl From<i32> for ColumnId {
    fn from(column_id: i32) -> Self {
        Self::new(column_id)
    }
}

impl std::fmt::Display for ColumnId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ColumnDesc {
    pub data_type: DataType,
    pub column_id: ColumnId,
    pub name: String, // for debugging
    pub field_descs: Vec<ColumnDesc>,
    pub type_name: String,
}
#[derive(Clone, Debug, PartialEq)]
pub struct OrderedColumnDesc {
    pub column_desc: ColumnDesc,
    pub order: OrderType,
}

impl ColumnDesc {
    pub fn unnamed(column_id: ColumnId, data_type: DataType) -> ColumnDesc {
        ColumnDesc {
            data_type,
            column_id,
            name: String::new(),
            field_descs: vec![],
            type_name: String::new(),
        }
    }

    // Get all column descs under field_descs
    pub fn get_column_descs(&self) -> Vec<ColumnDesc> {
        let mut descs = vec![self.clone()];
        for desc in &self.field_descs {
            descs.append(&mut desc.get_column_descs());
        }
        descs
    }

    #[cfg(test)]
    pub fn new_atomic(data_type: DataType, name: &str, column_id: i32) -> Self {
        Self {
            data_type,
            column_id: ColumnId::new(column_id),
            name: name.to_string(),
            field_descs: vec![],
            type_name: "".to_string(),
        }
    }

    #[cfg(test)]
    pub fn new_struct(
        data_type: DataType,
        name: &str,
        column_id: i32,
        type_name: &str,
        fields: Vec<ColumnDesc>,
    ) -> Self {
        Self {
            data_type,
            column_id: ColumnId::new(column_id),
            name: name.to_string(),
            field_descs: fields,
            type_name: type_name.to_string(),
        }
    }
}

impl From<ProstColumnDesc> for ColumnDesc {
    // Since the prost DataType struct doesn't have field, so it need to be reinit when into
    // ColumnDesc
    fn from(prost: ProstColumnDesc) -> Self {
        if let DataType::Struct { .. } = DataType::from(prost.column_type.as_ref().unwrap()) {
            let descs: Vec<ColumnDesc> = prost
                .field_descs
                .into_iter()
                .map(ColumnDesc::from)
                .collect();
            let date_type = DataType::Struct {
                fields: descs
                    .clone()
                    .into_iter()
                    .map(|c| c.data_type)
                    .collect_vec()
                    .into(),
            };
            Self {
                data_type: date_type,
                column_id: ColumnId::new(prost.column_id),
                name: prost.name,
                type_name: prost.type_name,
                field_descs: descs,
            }
        } else {
            Self {
                data_type: DataType::from(prost.column_type.as_ref().unwrap()),
                column_id: ColumnId::new(prost.column_id),
                name: prost.name,
                type_name: prost.type_name,
                field_descs: vec![],
            }
        }
    }
}

impl From<&ProstColumnDesc> for ColumnDesc {
    fn from(prost: &ProstColumnDesc) -> Self {
        prost.clone().into()
    }
}

impl From<ProstOrderedColumnDesc> for OrderedColumnDesc {
    fn from(prost: ProstOrderedColumnDesc) -> Self {
        Self {
            column_desc: prost.column_desc.unwrap().into(),
            order: OrderType::from_prost(&ProstOrderType::from_i32(prost.order).unwrap()),
        }
    }
}

#[cfg(test)]
pub mod tests {
    use risingwave_pb::plan::ColumnDesc as ProstColumnDesc;

    use crate::catalog::ColumnDesc;
    use crate::types::DataType;

    #[cfg(test)]
    pub fn build_prost_desc() -> ProstColumnDesc {
        let city = vec![
            ProstColumnDesc::new_atomic(DataType::Varchar.to_protobuf(), "country.city.address", 2),
            ProstColumnDesc::new_atomic(DataType::Varchar.to_protobuf(), "country.city.zipcode", 3),
        ];
        let country = vec![
            ProstColumnDesc::new_atomic(DataType::Varchar.to_protobuf(), "country.address", 1),
            ProstColumnDesc::new_struct(
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
        ProstColumnDesc::new_struct(
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
    pub fn build_desc() -> ColumnDesc {
        let city = vec![
            ColumnDesc::new_atomic(DataType::Varchar, "country.city.address", 2),
            ColumnDesc::new_atomic(DataType::Varchar, "country.city.zipcode", 3),
        ];
        let data_type = vec![DataType::Varchar, DataType::Varchar];
        let country = vec![
            ColumnDesc::new_atomic(DataType::Varchar, "country.address", 1),
            ColumnDesc::new_struct(
                DataType::Struct {
                    fields: data_type.clone().into(),
                },
                "country.city",
                4,
                ".test.City",
                city,
            ),
        ];
        ColumnDesc::new_struct(
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
        let desc: ColumnDesc = build_prost_desc().into();
        assert_eq!(desc, build_desc());
    }
}
