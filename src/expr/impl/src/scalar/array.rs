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

use risingwave_common::array::{ListValue, StructValue};
use risingwave_common::row::Row;
use risingwave_common::types::{
    DataType, ListRef, MapRef, MapType, MapValue, ScalarRefImpl, ToOwnedDatum,
};
use risingwave_expr::expr::Context;
use risingwave_expr::{function, ExprError};

use super::array_positions::array_position;

#[function("array(...) -> anyarray", type_infer = "panic")]
fn array(row: impl Row, ctx: &Context) -> ListValue {
    ListValue::from_datum_iter(ctx.return_type.as_list(), row.iter())
}

#[function("row(...) -> struct", type_infer = "panic")]
fn row_(row: impl Row) -> StructValue {
    StructValue::new(row.iter().map(|d| d.to_owned_datum()).collect())
}

fn map_type_infer(args: &[DataType]) -> Result<DataType, ExprError> {
    let map = MapType::try_from_kv(args[0].as_list().clone(), args[1].as_list().clone())?;
    Ok(map.into())
}

/// # Example
///
/// ```slt
/// query T
/// select map_from_entries(null::int[], array[1,2,3]);
/// ----
/// NULL
///
/// query T
/// select map_from_entries(array['a','b','c'], array[1,2,3]);
/// ----
/// {"a":1,"b":2,"c":3}
/// ```
#[function(
    "map_from_entries(anyarray, anyarray) -> anymap",
    type_infer = "map_type_infer"
)]
fn map(key: ListRef<'_>, value: ListRef<'_>) -> Result<MapValue, ExprError> {
    MapValue::try_from_kv(key.to_owned(), value.to_owned()).map_err(ExprError::Custom)
}

/// # Example
///
/// ```slt
/// 
/// query T
/// select map_access(map_from_entries(array[1,2,3], array[100,200,300]), 3);
/// ----
/// 300
///
/// #TODO: support this  (Cannot implicitly cast ''3':Varchar' to polymorphic type Any)
/// query T
/// select map_access(map_from_entries(array[1,2,3], array[100,200,300]), '3');
/// ----
/// 300
///
/// #XXX: return error or NULL here?
/// query T
/// select map_access(map_from_entries(array[1,2,3], array[100,200,300]), 1.0);
/// ---
/// NULL
///
/// #TODO: support this  (Bind error: Cannot implicitly cast ''a':Varchar' to polymorphic type Any)
/// query T
/// select map_access(map_from_entries(array['a','b','c'], array[1,2,3]), 'a');
/// ----
/// 1
///
/// query T
/// select map_access(map_from_entries(array['a','b','c'], array[1,2,3]), 'd');
/// ----
/// NULL
///
/// query T
/// select map_access(map_from_entries(array['a','b','c'], array[1,2,3]), null);
/// ----
/// NULL
/// ```
#[function(
    "map_access(anymap, any) -> any",
    type_infer = "|args| Ok(args[0].as_map().value().clone())"
)]
fn map_access<'a, 'b>(
    map: MapRef<'a>,
    key: ScalarRefImpl<'b>,
) -> Result<Option<ScalarRefImpl<'a>>, ExprError> {
    // FIXME: DatumRef in return value is not support by the macro yet.

    let (keys, values) = map.into_kv();
    let idx = array_position(keys, Some(key))?;
    match idx {
        Some(idx) => Ok(values.get((idx - 1) as usize).unwrap()),
        None => Ok(None),
    }
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
