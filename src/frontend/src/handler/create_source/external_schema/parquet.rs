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

//! Parquet-specific schema validation utilities

use risingwave_common::array::arrow::{IcebergArrowConvert, is_parquet_schema_match_source_schema};
use risingwave_common::types::DataType;

use super::*;

/// Check type compatibility for parquet sources using Arrow schema matching.
/// This provides more flexible type checking for parquet sources compared to exact type matching.
pub fn check_parquet_type_compatibility(
    parquet_type: &DataType,
    user_type: &DataType,
) -> Result<bool> {
    // Convert parquet type to Arrow field for comparison
    let arrow_field = IcebergArrowConvert
        .to_arrow_field("dummy", parquet_type)
        .map_err(|e| {
            RwError::from(ProtocolError(format!(
                "Failed to convert parquet type to arrow field: {}",
                e
            )))
        })?;

    Ok(is_parquet_schema_match_source_schema(
        arrow_field.data_type(),
        user_type,
    ))
}

#[cfg(test)]
mod tests {
    use risingwave_common::types::DataType;

    use super::*;

    #[test]
    fn test_parquet_type_compatibility() {
        // Test compatible types
        assert!(check_parquet_type_compatibility(&DataType::Int32, &DataType::Int32).unwrap());
        assert!(check_parquet_type_compatibility(&DataType::Varchar, &DataType::Varchar).unwrap());
        assert!(check_parquet_type_compatibility(&DataType::Float64, &DataType::Float64).unwrap());

        // Test incompatible types
        assert!(!check_parquet_type_compatibility(&DataType::Int32, &DataType::Varchar).unwrap());
        assert!(!check_parquet_type_compatibility(&DataType::Float64, &DataType::Int32).unwrap());
    }
}
