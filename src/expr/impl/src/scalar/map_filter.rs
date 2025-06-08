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

use std::sync::Arc;

use async_trait::async_trait;
use risingwave_common::array::{
    Array, ArrayBuilder, ArrayImpl, ArrayRef, DataChunk, ListValue, MapArrayBuilder, MapValue,
    StructValue,
};
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, Datum, Scalar, ScalarImpl, StructType, ToOwnedDatum};
use risingwave_expr::expr::{BoxedExpression, ExprError, Expression};
use risingwave_expr::{Result, build_function};

#[derive(Debug)]
struct MapFilterExpression {
    map: BoxedExpression,
    lambda: BoxedExpression,
    key_type: DataType,
    value_type: DataType,
}

#[async_trait]
impl Expression for MapFilterExpression {
    fn return_type(&self) -> DataType {
        self.map.return_type()
    }

    async fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        let map_input = self.map.eval(input).await?;
        let map_array = map_input.as_map();

        let mut builder = MapArrayBuilder::with_type(map_array.len(), self.return_type().clone());

        for idx in 0..map_array.len() {
            if map_array.is_null(idx) {
                builder.append_n(1, None);
                continue;
            }

            let map_ref = unsafe { map_array.raw_value_at_unchecked(idx) };
            let kv_count = map_ref.len();

            let mut key_builder = self.key_type.create_array_builder(kv_count);
            let mut value_builder = self.value_type.create_array_builder(kv_count);

            let mut kv_pairs = Vec::with_capacity(kv_count);

            for (key, value) in map_ref.iter() {
                key_builder.append(Some(key));
                value_builder.append(value);
                kv_pairs.push((key, value));
            }

            let key_array = key_builder.finish().into_ref();
            let value_array = value_builder.finish().into_ref();
            let chunk = DataChunk::new(vec![key_array.clone(), value_array.clone()], kv_count);

            let conditions = self.lambda.eval(&chunk).await?;
            let bool_array = conditions.as_bool();

            let mut filtered_entries = Vec::new();

            for idx in 0..kv_count {
                if bool_array.value_at(idx) == Some(true) {
                    let (key, value) = kv_pairs[idx];
                    filtered_entries.push(StructValue::new(vec![
                        key.to_owned_datum(),
                        value.to_owned_datum(),
                    ]));
                }
            }
            let elem_type = DataType::Struct(StructType::new(vec![
                ("key", self.key_type.clone()),
                ("value", self.value_type.clone()),
            ]));

            let map_value = if !filtered_entries.is_empty() {
                let new_list_value = ListValue::from_datum_iter(
                    &elem_type,
                    filtered_entries.iter().map(|s| {
                        let owned_val = s.to_owned();
                        let scalar: ScalarImpl = owned_val.into();
                        Some(scalar)
                    }),
                );
                Some(MapValue::from_entries(new_list_value))
            } else {
                None
            };

            builder.append_n(1, map_value.as_ref().map(|v| v.as_scalar_ref()));
        }

        Ok(Arc::new(ArrayImpl::Map(builder.finish())))
    }

    async fn eval_row(&self, input: &OwnedRow) -> Result<Datum> {
        let map_datum = self.map.eval_row(input).await?;
        let map_value = match map_datum {
            Some(scalar) => scalar.into_map(),
            None => return Ok(None),
        };

        let list_value = map_value.into_inner();
        let array_impl = list_value.into_array();
        let struct_array = match array_impl {
            ArrayImpl::Struct(arr) => arr,
            _ => return Err(ExprError::InvalidState("Expected StructArray".to_string())),
        };
        let mut key_builder = self.key_type.create_array_builder(struct_array.len());
        let mut value_builder = self.value_type.create_array_builder(struct_array.len());
        for idx in 0..struct_array.len() {
            let struct_ref = unsafe { struct_array.raw_value_at_unchecked(idx) };

            let key = struct_ref.field_at(0);
            let value = struct_ref.field_at(1);

            key_builder.append(key);
            value_builder.append(value);
        }

        let key_array = key_builder.finish().into_ref();
        let value_array = value_builder.finish().into_ref();
        let chunk = DataChunk::new(vec![key_array, value_array], struct_array.len());

        let condition = self.lambda.eval(&chunk).await?;
        let condition = condition.as_bool();

        let mut filtered_entries = Vec::new();

        for idx in 0..struct_array.len() {
            if condition.value_at(idx) == Some(true) {
                let struct_ref = unsafe { struct_array.raw_value_at_unchecked(idx) };

                let key = struct_ref.field_at(0);
                let value = struct_ref.field_at(1);

                let key_datum = key.to_owned_datum(); // 获取 key 的 owned 版本
                let value_datum = value.to_owned_datum(); // 获取 value 的 owned 版本

                let fields = vec![key_datum, value_datum];
                filtered_entries.push(StructValue::new(fields));
            }
        }

        let new_list_value = ListValue::from_datum_iter(
            &struct_array.data_type(),
            filtered_entries.iter().map(|s| {
                let owned_val = s.to_owned();
                let scalar: ScalarImpl = owned_val.into();
                Some(scalar) // 
            }),
        );
        let new_map_value = MapValue::from_entries(new_list_value);
        Ok(Some(new_map_value.into()))
    }
}

#[build_function("map_filter(anymap, any) -> anymap")]
fn build(return_type: DataType, children: Vec<BoxedExpression>) -> Result<BoxedExpression> {
    let [map, lambda] = <[BoxedExpression; 2]>::try_from(children).unwrap();

    let DataType::Map(map_type) = return_type else {
        panic!("Expected map type");
    };
    let key_type = map_type.key().clone();
    let value_type = map_type.value().clone();

    Ok(Box::new(MapFilterExpression {
        map,
        lambda,
        key_type,
        value_type,
    }))
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::{DataChunk, DataChunkTestExt};
    use risingwave_common::row::Row;
    use risingwave_common::types::ToOwnedDatum;
    use risingwave_common::util::iter_util::ZipEqDebug;
    use risingwave_expr::expr::build_from_pretty;

    #[tokio::test]
    async fn test_map_filter() {
        // The test doesn't make much sense for MapFilterExpression.
        // It is equivalent to `SELECT map_filter(MAP {1:1,3:2}, |k, v| 2 > 1)`.
        // Keep it to ensure `build_from_pretty` and `DataChunk::from_pretty` works for Map type.
        let expr = build_from_pretty(
            "(map_filter:map<int4,int4> $0:map<int4,int4> \
            (greater_than:boolean 2:int4 1:int4))",
        );
        let (input, expected) = DataChunk::from_pretty(
            "map<i,i>    map<i,i>
             {1:1,3:2}   {1:1,3:2}
             {5:3,7:4}   {5:3,7:4}
             {2:0,1:NULL}   {2:0,1:NULL}",
        )
        .split_column_at(1);

        let output = expr.eval(&input).await.unwrap();
        assert_eq!(&output, expected.column_at(0));

        for (row, expected_row) in input.rows().zip_eq_debug(expected.rows()) {
            let result = expr.eval_row(&row.to_owned_row()).await.unwrap();
            assert_eq!(result, expected_row.datum_at(0).to_owned_datum(),);
        }
    }
}
