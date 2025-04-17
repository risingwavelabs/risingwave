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

use risingwave_common::types::{DataType, ListRef, ScalarRefImpl, StructType};
use risingwave_expr::{Result, function};

/// Returns the input array as a set of rows with an index.
///
/// ```slt
/// query II
/// select * from _pg_expandarray(array[1,2,null]);
/// ----
/// 1    1
/// 2    2
/// NULL 3
///
/// query TI
/// select * from _pg_expandarray(array['one', null, 'three']);
/// ----
/// one   1
/// NULL  2
/// three 3
/// ```
#[function(
    "_pg_expandarray(anyarray) -> setof struct<x any, n int4>",
    type_infer = "infer_type"
)]
fn _pg_expandarray(array: ListRef<'_>) -> impl Iterator<Item = (Option<ScalarRefImpl<'_>>, i32)> {
    #[allow(clippy::disallowed_methods)]
    array.iter().zip(1..)
}

fn infer_type(args: &[DataType]) -> Result<DataType> {
    Ok(DataType::Struct(StructType::new(vec![
        ("x", args[0].as_list().clone()),
        ("n", DataType::Int32),
    ])))
}
