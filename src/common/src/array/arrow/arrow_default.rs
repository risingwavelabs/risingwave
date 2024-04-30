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

pub use arrow_impl::{FromArrow, ToArrow};
use {arrow_array, arrow_buffer, arrow_cast, arrow_schema};

#[expect(clippy::duplicate_mod)]
#[path = "./arrow_impl.rs"]
mod arrow_impl;

pub struct UdfArrowConvert;

impl ToArrow for UdfArrowConvert {}
impl FromArrow for UdfArrowConvert {}

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
        let rw_array = UdfArrowConvert.from_array(&arrow).unwrap();
        assert_eq!(rw_array.as_list(), &array);
    }
}
