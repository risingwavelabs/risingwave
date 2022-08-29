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

use itertools::Itertools;

use crate::array::column::Column;
use crate::types::DataType;

/// Check if the schema of `columns` matches the expected `data_types`. Used for debugging.
pub fn schema_check(
    data_types: impl IntoIterator<Item = &'_ DataType> + Clone,
    columns: impl IntoIterator<Item = &'_ Column> + Clone,
) -> Result<(), String> {
    tracing::event!(
        tracing::Level::TRACE,
        "input schema = \n{:#?}\nexpected schema = \n{:#?}",
        columns
            .clone()
            .into_iter()
            .map(|col| col.array_ref().get_ident())
            .collect_vec(),
        data_types.clone().into_iter().collect_vec(),
    );

    for (i, pair) in columns.into_iter().zip_longest(data_types).enumerate() {
        let array = pair.as_ref().left().map(|c| c.array_ref());
        let builder = pair.as_ref().right().map(|d| d.create_array_builder(0)); // TODO: check `data_type` directly

        macro_rules! check_schema {
            ([], $( { $variant_name:ident, $suffix_name:ident, $array:ty, $builder:ty } ),*) => {
                use crate::array::ArrayBuilderImpl;
                use crate::array::ArrayImpl;

                match (array, &builder) {
                    $( (Some(ArrayImpl::$variant_name(_)), Some(ArrayBuilderImpl::$variant_name(_))) => {} ),*
                    _ => return Err(format!("column {} should be {:?}, while stream chunk gives {:?}",
                                            i, builder.map(|b| b.get_ident()), array.map(|a| a.get_ident()))),
                }
            };
        }

        for_all_variants! { check_schema };
    }

    Ok(())
}
