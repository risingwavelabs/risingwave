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

impl std::fmt::Display for ColumnId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Clone, Debug)]
pub struct ColumnDesc {
    pub data_type: DataType,
    pub column_id: ColumnId,
    pub name: String, // for debugging
}
#[derive(Clone, Debug)]
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
        }
    }
}

impl From<ProstColumnDesc> for ColumnDesc {
    fn from(prost: ProstColumnDesc) -> Self {
        let ProstColumnDesc {
            column_type,
            column_id,
            name,
        } = prost;

        Self {
            data_type: DataType::from(&column_type.unwrap()),
            column_id: ColumnId::new(column_id),
            name,
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
