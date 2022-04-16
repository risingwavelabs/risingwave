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

use std::collections::HashMap;

use itertools::Itertools;
use risingwave_pb::plan::{
    ColumnDesc as ProstColumnDesc, OrderType as ProstOrderType,
    OrderedColumnDesc as ProstOrderedColumnDesc,
};

use crate::types::DataType;
use crate::util::sort_util::OrderType;

/// Column ID is the unique identifier of a column in a table. Different from table ID,
/// column ID is not globally unique.
#[derive(Clone, Copy, Eq, PartialEq, Hash)]
pub struct ColumnId(i32);

impl std::fmt::Debug for ColumnId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "#{}", self.0)
    }
}

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
impl From<ColumnId> for i32 {
    fn from(id: ColumnId) -> i32 {
        id.0
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
    pub field_descs: HashMap<String, ColumnDesc>,
    pub type_name: String,
    pub is_nested: bool,
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
            field_descs: HashMap::new(),
            type_name: String::new(),
            is_nested: false,
        }
    }

    pub fn with_name(data_type: DataType, name: impl Into<String>, column_id: ColumnId) -> Self {
        Self {
            data_type,
            column_id,
            name: name.into(),
            field_descs: HashMap::new(),
            type_name: "".to_string(),
            is_nested: false,
        }
    }

    /// Convert to proto
    pub fn to_protobuf(&self) -> ProstColumnDesc {
        ProstColumnDesc {
            column_type: Some(self.data_type.to_protobuf()),
            column_id: self.column_id.get_id(),
            name: self.name.clone(),
            field_descs: self
                .field_desc_iter()
                .map(|f| f.to_protobuf())
                .collect_vec(),
            type_name: self.type_name.clone(),
            is_nested: self.is_nested,
        }
    }

    pub fn field_desc_iter(&self) -> impl Iterator<Item = &ColumnDesc> {
        self.field_descs
            .values()
            .sorted_by(|a, b| a.name.cmp(&b.name))
    }

    /// Flatten a nested column to a list of columns (including itself).
    /// If the type is atomic, it returns simply itself.
    /// If the type has multiple nesting levels, it traverses for the tree-like schema,
    /// and returns every children node.
    pub fn flatten(&self) -> Vec<ColumnDesc> {
        let mut descs = vec![self.clone()];
        descs.append(
            &mut self
                .field_desc_iter()
                .flat_map(|d| {
                    let mut desc = d.clone();
                    desc.name = self.name.clone() + "." + &desc.name;
                    desc.flatten()
                })
                .collect_vec(),
        );
        descs
    }

    pub fn new_atomic(data_type: DataType, name: &str, column_id: i32, is_nested: bool) -> Self {
        Self {
            data_type,
            column_id: ColumnId::new(column_id),
            name: name.to_string(),
            field_descs: HashMap::new(),
            type_name: "".to_string(),
            is_nested,
        }
    }

    pub fn new_struct(
        name: &str,
        column_id: i32,
        type_name: &str,
        fields: Vec<ColumnDesc>,
        is_nested: bool,
    ) -> Self {
        let data_type = DataType::Struct {
            fields: fields
                .iter()
                .map(|f| f.data_type.clone())
                .collect_vec()
                .into(),
        };
        Self {
            data_type,
            column_id: ColumnId::new(column_id),
            name: name.to_string(),
            field_descs: fields
                .into_iter()
                .map(|f| (f.name.clone(), f))
                .collect::<HashMap<String, ColumnDesc>>(),
            type_name: type_name.to_string(),
            is_nested,
        }
    }

    /// Generate incremental `column_id` for `column_desc` and `field_descs`
    pub fn generate_increment_id(&mut self, index: &mut i32) {
        self.column_id = ColumnId::new(*index);
        *index += 1;
        for field in self
            .field_descs
            .values_mut()
            .sorted_by(|a, b| a.name.cmp(&b.name))
            .collect_vec()
        {
            field.generate_increment_id(index);
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
                field_descs: descs
                    .into_iter()
                    .map(|f| (f.name.clone(), f))
                    .collect::<HashMap<String, ColumnDesc>>(),
                is_nested: prost.is_nested,
            }
        } else {
            Self {
                data_type: DataType::from(prost.column_type.as_ref().unwrap()),
                column_id: ColumnId::new(prost.column_id),
                name: prost.name,
                type_name: prost.type_name,
                field_descs: HashMap::new(),
                is_nested: prost.is_nested,
            }
        }
    }
}

impl From<&ProstColumnDesc> for ColumnDesc {
    fn from(prost: &ProstColumnDesc) -> Self {
        prost.clone().into()
    }
}

impl From<&ColumnDesc> for ProstColumnDesc {
    fn from(c: &ColumnDesc) -> Self {
        Self {
            column_type: c.data_type.to_protobuf().into(),
            column_id: c.column_id.into(),
            name: c.name.clone(),
            field_descs: c.field_desc_iter().map(|c| c.to_protobuf()).collect(),
            type_name: c.type_name.clone(),
            is_nested: c.is_nested,
        }
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

    pub fn build_prost_desc() -> ProstColumnDesc {
        let city = vec![
            ProstColumnDesc::new_atomic(
                DataType::Varchar.to_protobuf(),
                "country.city.address",
                2,
                false,
            ),
            ProstColumnDesc::new_atomic(
                DataType::Varchar.to_protobuf(),
                "country.city.zipcode",
                3,
                false,
            ),
        ];
        let country = vec![
            ProstColumnDesc::new_atomic(
                DataType::Varchar.to_protobuf(),
                "country.address",
                1,
                false,
            ),
            ProstColumnDesc::new_struct("country.city", 4, ".test.City", city, false),
        ];
        ProstColumnDesc::new_struct("country", 5, ".test.Country", country, true)
    }

    pub fn build_desc() -> ColumnDesc {
        let city = vec![
            ColumnDesc::new_atomic(DataType::Varchar, "country.city.address", 2, true),
            ColumnDesc::new_atomic(DataType::Varchar, "country.city.zipcode", 3, true),
        ];
        let country = vec![
            ColumnDesc::new_atomic(DataType::Varchar, "country.address", 1, true),
            ColumnDesc::new_struct("country.city", 4, ".test.City", city, true),
        ];
        ColumnDesc::new_struct("country", 5, ".test.Country", country, false)
    }

    #[test]
    fn test_into_column_catalog() {
        let desc: ColumnDesc = build_prost_desc().into();
        assert_eq!(desc, build_desc());
    }
}
