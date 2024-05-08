// Copyright 2024 RisingWave Labs
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

//! This is for arrow dependency named `arrow-xxx` such as `arrow-array` in the cargo workspace.
//!
//! This should the default arrow version to be used in our system.
//!
//! The corresponding version of arrow is currently used by `udf` and `iceberg` sink.

use std::sync::Arc;

pub use arrow_impl::{FromArrow, ToArrow};
use {arrow_array, arrow_buffer, arrow_cast, arrow_schema};

use crate::array::{ArrayError, ArrayImpl, DataType, DecimalArray, JsonbArray};

#[expect(clippy::duplicate_mod)]
#[path = "./arrow_impl.rs"]
mod arrow_impl;

/// Arrow conversion for the current version of UDF. This is in use but will be deprecated soon.
///
/// In the current version of UDF protocol, decimal and jsonb types are mapped to Arrow `LargeBinary` and `LargeUtf8` types.
pub struct UdfArrowConvert;

impl ToArrow for UdfArrowConvert {
    // Decimal values are stored as ASCII text representation in a large binary array.
    fn decimal_to_arrow(
        &self,
        _data_type: &arrow_schema::DataType,
        array: &DecimalArray,
    ) -> Result<arrow_array::ArrayRef, ArrayError> {
        Ok(Arc::new(arrow_array::LargeBinaryArray::from(array)))
    }

    // JSON values are stored as text representation in a large string array.
    fn jsonb_to_arrow(&self, array: &JsonbArray) -> Result<arrow_array::ArrayRef, ArrayError> {
        Ok(Arc::new(arrow_array::LargeStringArray::from(array)))
    }

    fn jsonb_type_to_arrow(&self, name: &str) -> arrow_schema::Field {
        arrow_schema::Field::new(name, arrow_schema::DataType::LargeUtf8, true)
    }

    fn decimal_type_to_arrow(&self, name: &str) -> arrow_schema::Field {
        arrow_schema::Field::new(name, arrow_schema::DataType::LargeBinary, true)
    }
}

impl FromArrow for UdfArrowConvert {
    fn from_large_utf8(&self) -> Result<DataType, ArrayError> {
        Ok(DataType::Jsonb)
    }

    fn from_large_binary(&self) -> Result<DataType, ArrayError> {
        Ok(DataType::Decimal)
    }

    fn from_large_utf8_array(
        &self,
        array: &arrow_array::LargeStringArray,
    ) -> Result<ArrayImpl, ArrayError> {
        Ok(ArrayImpl::Jsonb(array.try_into()?))
    }

    fn from_large_binary_array(
        &self,
        array: &arrow_array::LargeBinaryArray,
    ) -> Result<ArrayImpl, ArrayError> {
        Ok(ArrayImpl::Decimal(array.try_into()?))
    }
}

/// Arrow conversion for the next version of UDF. This is unused for now.
///
/// In the next version of UDF protocol, decimal and jsonb types will be mapped to Arrow extension types.
/// See <https://github.com/risingwavelabs/arrow-udf/tree/main#extension-types>.
pub struct NewUdfArrowConvert;

impl ToArrow for NewUdfArrowConvert {}
impl FromArrow for NewUdfArrowConvert {}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::array::*;
    use crate::buffer::Bitmap;

    #[test]
    fn struct_array() {
        // Empty array - risingwave to arrow conversion.
        let test_arr = StructArray::new(StructType::empty(), vec![], Bitmap::ones(0));
        assert_eq!(
            UdfArrowConvert
                .struct_to_arrow(
                    &arrow_schema::DataType::Struct(arrow_schema::Fields::empty()),
                    &test_arr
                )
                .unwrap()
                .len(),
            0
        );

        // Empty array - arrow to risingwave conversion.
        let test_arr_2 = arrow_array::StructArray::from(vec![]);
        assert_eq!(
            UdfArrowConvert
                .from_struct_array(&test_arr_2)
                .unwrap()
                .len(),
            0
        );

        // Struct array with primitive types. arrow to risingwave conversion.
        let test_arrow_struct_array = arrow_array::StructArray::try_from(vec![
            (
                "a",
                Arc::new(arrow_array::BooleanArray::from(vec![
                    Some(false),
                    Some(false),
                    Some(true),
                    None,
                ])) as arrow_array::ArrayRef,
            ),
            (
                "b",
                Arc::new(arrow_array::Int32Array::from(vec![
                    Some(42),
                    Some(28),
                    Some(19),
                    None,
                ])) as arrow_array::ArrayRef,
            ),
        ])
        .unwrap();
        let actual_risingwave_struct_array = UdfArrowConvert
            .from_struct_array(&test_arrow_struct_array)
            .unwrap()
            .into_struct();
        let expected_risingwave_struct_array = StructArray::new(
            StructType::new(vec![("a", DataType::Boolean), ("b", DataType::Int32)]),
            vec![
                BoolArray::from_iter([Some(false), Some(false), Some(true), None]).into_ref(),
                I32Array::from_iter([Some(42), Some(28), Some(19), None]).into_ref(),
            ],
            [true, true, true, true].into_iter().collect(),
        );
        assert_eq!(
            expected_risingwave_struct_array,
            actual_risingwave_struct_array
        );
    }

    #[test]
    fn list() {
        let array = ListArray::from_iter([None, Some(vec![0, -127, 127, 50]), Some(vec![0; 0])]);
        let data_type = arrow_schema::DataType::new_list(arrow_schema::DataType::Int32, true);
        let arrow = UdfArrowConvert.list_to_arrow(&data_type, &array).unwrap();
        let rw_array = UdfArrowConvert
            .from_list_array(arrow.as_any().downcast_ref().unwrap())
            .unwrap();
        assert_eq!(rw_array.as_list(), &array);
    }
}
