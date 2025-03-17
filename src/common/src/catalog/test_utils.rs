// Copyright 2025 RisingWave Labs
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

use risingwave_pb::data::DataType;
use risingwave_pb::plan_common::{AdditionalColumn, ColumnDesc, ColumnDescVersion};

pub trait ColumnDescTestExt {
    /// Create a [`ColumnDesc`] with the given name and type.
    ///
    /// **Note: Only used for tests.**
    fn new(data_type: DataType, name: &str, column_id: i32) -> Self;
}

impl ColumnDescTestExt for ColumnDesc {
    fn new(data_type: DataType, name: &str, column_id: i32) -> Self {
        Self {
            column_type: Some(data_type),
            column_id,
            name: name.to_owned(),
            additional_column: Some(AdditionalColumn { column_type: None }),
            version: ColumnDescVersion::LATEST as _,
            nullable: true,
            ..Default::default()
        }
    }
}
