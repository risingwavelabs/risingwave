// Copyright 2023 RisingWave Labs
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

use itertools::Itertools;

use crate::array::{ArrayImpl, ArrayRef};
use crate::for_all_type_pairs;
use crate::types::DataType;

/// Check if the schema of `columns` matches the expected `data_types`. Used for debugging.
pub fn schema_check<'a, T, C>(data_types: T, columns: C) -> Result<(), String>
where
    T: IntoIterator<Item = &'a DataType>,
    C: IntoIterator<Item = &'a ArrayRef>,
{
    for (i, pair) in data_types
        .into_iter()
        .zip_longest(columns.into_iter().map(|c| &**c))
        .enumerate()
    {
        macro_rules! matches {
            ($( { $DataType:ident, $PhysicalType:ident }),*) => {
                match (pair.as_ref().left(), pair.as_ref().right()) {
                    $( (Some(DataType::$DataType { .. }), Some(ArrayImpl::$PhysicalType(_))) => continue, )*
                    (data_type, array) => {
                        let array_ident = array.map(|a| a.get_ident());
                        return Err(format!(
                            "column type mismatched at position {i}: expected {data_type:?}, found {array_ident:?}"
                        ));
                    }
                }
            }
        }

        for_all_type_pairs! { matches }
    }

    Ok(())
}
