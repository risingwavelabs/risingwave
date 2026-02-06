// Copyright 2026 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Extension trait for RisingWave data types to check DataFusion compatibility.

use datafusion::arrow::datatypes::DataType as DFDataType;
use risingwave_common::array::arrow::IcebergArrowConvert;
use risingwave_common::types::DataType as RwDataType;

#[easy_ext::ext(RwDataTypeDataFusionExt)]
impl RwDataType {
    pub fn is_datafusion_native(&self) -> bool {
        match self {
            RwDataType::Boolean
            | RwDataType::Int32
            | RwDataType::Int64
            | RwDataType::Float32
            | RwDataType::Float64
            | RwDataType::Date
            | RwDataType::Time
            | RwDataType::Timestamp
            | RwDataType::Timestamptz
            | RwDataType::Varchar
            | RwDataType::Bytea
            | RwDataType::Serial
            | RwDataType::Decimal => true,
            RwDataType::Struct(v) => v.types().all(RwDataTypeDataFusionExt::is_datafusion_native),
            RwDataType::List(list) => list.elem().is_datafusion_native(),
            RwDataType::Map(map) => {
                map.key().is_datafusion_native() && map.value().is_datafusion_native()
            }
            _ => false,
        }
    }

    pub fn to_datafusion_native(&self) -> Option<DFDataType> {
        if !self.is_datafusion_native() {
            return None;
        }
        let arrow_field = IcebergArrowConvert.to_arrow_field("", self).ok()?;
        Some(arrow_field.data_type().clone())
    }
}
