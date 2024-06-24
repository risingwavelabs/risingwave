// Copyright 2024 RisingWave Labs
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

pub use expect_test::{expect, Expect};
pub use itertools::Itertools;
pub use risingwave_common::catalog::ColumnDesc;
use risingwave_pb::plan_common::AdditionalColumn;

/// More concise display for `ColumnDesc`, to use in tests.
pub struct ColumnDescTestDisplay<'a>(pub &'a ColumnDesc);

impl<'a> std::fmt::Debug for ColumnDescTestDisplay<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let ColumnDesc {
            data_type,
            column_id,
            name,
            field_descs,
            type_name,
            generated_or_default_column,
            description,
            additional_column: AdditionalColumn { column_type },
            version: _,
        } = &self.0;

        write!(f, "{name}(#{column_id}): {data_type:?}")?;
        if !type_name.is_empty() {
            write!(f, ", type_name: {:?}", type_name)?;
        }
        if !field_descs.is_empty() {
            write!(f, ", field_descs: {:?}", field_descs)?;
        }
        if let Some(generated_or_default_column) = generated_or_default_column {
            write!(
                f,
                ", generated_or_default_column: {:?}",
                generated_or_default_column
            )?;
        }
        if let Some(description) = description {
            write!(f, ", description: {:?}", description)?;
        }
        if let Some(column_type) = column_type {
            write!(f, ", additional_column: {:?}", column_type)?;
        }
        Ok(())
    }
}
