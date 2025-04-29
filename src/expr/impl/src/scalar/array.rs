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

use risingwave_common::array::{ListValue, StructValue};
use risingwave_common::row::Row;
use risingwave_common::types::{
    DataType, ListRef, MapRef, MapType, MapValue, ScalarRef, ScalarRefImpl, ToOwnedDatum,
};
use risingwave_expr::expr::Context;
use risingwave_expr::{ExprError, function};

use super::array_positions::array_position;

#[function("array(...) -> anyarray", type_infer = "unreachable")]
fn array(row: impl Row, ctx: &Context) -> ListValue {
    ListValue::from_datum_iter(ctx.return_type.as_list_element_type(), row.iter())
}

#[function("row(...) -> struct", type_infer = "unreachable")]
fn row_(row: impl Row) -> StructValue {
    StructValue::new(row.iter().map(|d| d.to_owned_datum()).collect())
}

fn map_from_key_values_type_infer(args: &[DataType]) -> Result<DataType, ExprError> {
    let map = MapType::try_from_kv(
        args[0].as_list_element_type().clone(),
        args[1].as_list_element_type().clone(),
    )
    .map_err(ExprError::Custom)?;
    Ok(map.into())
}

fn map_from_entries_type_infer(args: &[DataType]) -> Result<DataType, ExprError> {
    let map = MapType::try_from_entries(args[0].as_list_element_type().clone())
        .map_err(ExprError::Custom)?;
    Ok(map.into())
}

/// # Example
///
/// ```slt
/// query T
/// select map_from_key_values(null::int[], array[1,2,3]);
/// ----
/// NULL
///
/// query T
/// select map_from_key_values(array['a','b','c'], array[1,2,3]);
/// ----
/// {a:1,b:2,c:3}
/// ```
#[function(
    "map_from_key_values(anyarray, anyarray) -> anymap",
    type_infer = "map_from_key_values_type_infer"
)]
fn map_from_key_values(key: ListRef<'_>, value: ListRef<'_>) -> Result<MapValue, ExprError> {
    MapValue::try_from_kv(key.to_owned(), value.to_owned()).map_err(ExprError::Custom)
}

#[function(
    "map_from_entries(anyarray) -> anymap",
    type_infer = "map_from_entries_type_infer"
)]
fn map_from_entries(entries: ListRef<'_>) -> Result<MapValue, ExprError> {
    MapValue::try_from_entries(entries.to_owned()).map_err(ExprError::Custom)
}

/// # Example
///
/// ```slt
/// query T
/// select map_access(map_from_key_values(array[1,2,3], array[100,200,300]), 3);
/// ----
/// 300
///
/// query T
/// select map_access(map_from_key_values(array[1,2,3], array[100,200,300]), '3');
/// ----
/// 300
///
/// query error
/// select map_access(map_from_key_values(array[1,2,3], array[100,200,300]), 1.0);
/// ----
/// db error: ERROR: Failed to run the query
///
/// Caused by these errors (recent errors listed first):
///   1: Failed to bind expression: map_access(map_from_key_values(ARRAY[1, 2, 3], ARRAY[100, 200, 300]), 1.0)
///   2: Bind error: Cannot access numeric in map(integer,integer)
///
///
/// query T
/// select map_access(map_from_key_values(array['a','b','c'], array[1,2,3]), 'a');
/// ----
/// 1
///
/// query T
/// select map_access(map_from_key_values(array['a','b','c'], array[1,2,3]), 'd');
/// ----
/// NULL
///
/// query T
/// select map_access(map_from_key_values(array['a','b','c'], array[1,2,3]), null);
/// ----
/// NULL
/// ```
#[function("map_access(anymap, any) -> any", type_infer = "unreachable")]
fn map_access<'a>(
    map: MapRef<'a>,
    key: ScalarRefImpl<'_>,
) -> Result<Option<ScalarRefImpl<'a>>, ExprError> {
    // FIXME: DatumRef in return value is not support by the macro yet.

    let (keys, values) = map.into_kv();
    let idx = array_position(keys, Some(key))?;
    match idx {
        Some(idx) => Ok(values.get((idx - 1) as usize).unwrap()),
        None => Ok(None),
    }
}

/// ```slt
/// query T
/// select
///     map_contains(MAP{1:1}, 1),
///     map_contains(MAP{1:1}, 2),
///     map_contains(MAP{1:1}, NULL::varchar),
///     map_contains(MAP{1:1}, 1.0)
/// ----
/// t f NULL f
/// ```
#[function("map_contains(anymap, any) -> boolean")]
fn map_contains(map: MapRef<'_>, key: ScalarRefImpl<'_>) -> Result<bool, ExprError> {
    let (keys, _values) = map.into_kv();
    let idx = array_position(keys, Some(key))?;
    Ok(idx.is_some())
}

/// ```slt
/// query I
/// select
///     map_length(NULL::map(int,int)),
///     map_length(MAP {}::map(int,int)),
///     map_length(MAP {1:1,2:2}::map(int,int))
/// ----
/// NULL 0 2
/// ```
#[function("map_length(anymap) -> int4")]
fn map_length<T: TryFrom<usize>>(map: MapRef<'_>) -> Result<T, ExprError> {
    map.len().try_into().map_err(|_| ExprError::NumericOverflow)
}

/// If both `m1` and `m2` have a value with the same key, then the output map contains the value from `m2`.
///
/// ```slt
/// query T
/// select map_cat(MAP{'a':1,'b':2},null::map(varchar,int));
/// ----
/// {a:1,b:2}
///
/// query T
/// select map_cat(MAP{'a':1,'b':2},MAP{'b':3,'c':4});
/// ----
/// {a:1,b:3,c:4}
///
/// # implicit type cast
/// query T
/// select map_cat(MAP{'a':1,'b':2},MAP{'b':3.0,'c':4.0});
/// ----
/// {a:1,b:3.0,c:4.0}
/// ```
#[function("map_cat(anymap, anymap) -> anymap")]
fn map_cat(m1: Option<MapRef<'_>>, m2: Option<MapRef<'_>>) -> Result<Option<MapValue>, ExprError> {
    match (m1, m2) {
        (None, None) => Ok(None),
        (Some(m), None) | (None, Some(m)) => Ok(Some(m.to_owned())),
        (Some(m1), Some(m2)) => Ok(Some(MapValue::concat(m1, m2))),
    }
}

/// Inserts a key-value pair into the map. If the key already exists, the value is updated.
///
/// # Example
///
/// ```slt
/// query T
/// select map_insert(map{'a':1, 'b':2}, 'c', 3);
/// ----
/// {a:1,b:2,c:3}
///
/// query T
/// select map_insert(map{'a':1, 'b':2}, 'b', 4);
/// ----
/// {a:1,b:4}
/// ```
///
/// TODO: support variadic arguments
#[function("map_insert(anymap, any, any) -> anymap")]
fn map_insert(
    map: MapRef<'_>,
    key: Option<ScalarRefImpl<'_>>,
    value: Option<ScalarRefImpl<'_>>,
) -> MapValue {
    let Some(key) = key else {
        return map.to_owned();
    };
    MapValue::insert(map, key.into_scalar_impl(), value.to_owned_datum())
}

/// Deletes a key-value pair from the map.
///
/// # Example
///
/// ```slt
/// query T
/// select map_delete(map{'a':1, 'b':2, 'c':3}, 'b');
/// ----
/// {a:1,c:3}
///
/// query T
/// select map_delete(map{'a':1, 'b':2, 'c':3}, 'd');
/// ----
/// {a:1,b:2,c:3}
/// ```
///
/// TODO: support variadic arguments
#[function("map_delete(anymap, any) -> anymap")]
fn map_delete(map: MapRef<'_>, key: Option<ScalarRefImpl<'_>>) -> MapValue {
    let Some(key) = key else {
        return map.to_owned();
    };
    MapValue::delete(map, key)
}

/// # Example
///
/// ```slt
/// query T
/// select map_keys(map{'a':1, 'b':2, 'c':3});
/// ----
/// {a,b,c}
/// ```
#[function(
    "map_keys(anymap) -> anyarray",
    type_infer = "|args|{
        Ok(DataType::List(Box::new(args[0].as_map().key().clone())))
    }"
)]
fn map_keys(map: MapRef<'_>) -> ListValue {
    map.into_kv().0.to_owned_scalar()
}

/// # Example
///
/// ```slt
/// query T
/// select map_values(map{'a':1, 'b':2, 'c':3});
/// ----
/// {1,2,3}
/// ```
#[function(
    "map_values(anymap) -> anyarray",
    type_infer = "|args|{
        Ok(DataType::List(Box::new(args[0].as_map().value().clone())))
    }"
)]
fn map_values(map: MapRef<'_>) -> ListValue {
    map.into_kv().1.to_owned_scalar()
}

/// # Example
///
/// ```slt
/// query T
/// select map_entries(map{'a':1, 'b':2, 'c':3});
/// ----
/// {"(a,1)","(b,2)","(c,3)"}
/// ```
#[function(
    "map_entries(anymap) -> anyarray",
    type_infer = "|args|{
        Ok(args[0].as_map().clone().into_list())
    }"
)]
fn map_entries(map: MapRef<'_>) -> ListValue {
    map.into_inner().to_owned()
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::DataChunk;
    use risingwave_common::row::Row;
    use risingwave_common::test_prelude::DataChunkTestExt;
    use risingwave_common::types::ToOwnedDatum;
    use risingwave_common::util::iter_util::ZipEqDebug;
    use risingwave_expr::expr::build_from_pretty;

    #[tokio::test]
    async fn test_row_expr() {
        let expr = build_from_pretty("(row:struct<a_int4,b_int4,c_int4> $0:int4 $1:int4 $2:int4)");
        let (input, expected) = DataChunk::from_pretty(
            "i i i <i,i,i>
             1 2 3 (1,2,3)
             4 2 1 (4,2,1)
             9 1 3 (9,1,3)
             1 1 1 (1,1,1)",
        )
        .split_column_at(3);

        // test eval
        let output = expr.eval(&input).await.unwrap();
        assert_eq!(&output, expected.column_at(0));

        // test eval_row
        for (row, expected) in input.rows().zip_eq_debug(expected.rows()) {
            let result = expr.eval_row(&row.to_owned_row()).await.unwrap();
            assert_eq!(result, expected.datum_at(0).to_owned_datum());
        }
    }
}
