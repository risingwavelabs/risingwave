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

use std::fmt::Write;
use std::str::FromStr;

use futures_util::FutureExt;
use itertools::Itertools;
use risingwave_common::array::{ListRef, ListValue, StructRef, StructValue};
use risingwave_common::cast;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, Int256, IntoOrdered, JsonbRef, ToText, F64};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_expr::expr::{build_func, Context, Expression, InputRefExpression};
use risingwave_expr::{function, ExprError, Result};
use risingwave_pb::expr::expr_node::PbType;

#[function("cast(varchar) -> *int")]
#[function("cast(varchar) -> decimal")]
#[function("cast(varchar) -> *float")]
#[function("cast(varchar) -> int256")]
#[function("cast(varchar) -> date")]
#[function("cast(varchar) -> time")]
#[function("cast(varchar) -> timestamp")]
#[function("cast(varchar) -> interval")]
#[function("cast(varchar) -> jsonb")]
pub fn str_parse<T>(elem: &str) -> Result<T>
where
    T: FromStr,
    <T as FromStr>::Err: std::fmt::Display,
{
    elem.trim()
        .parse()
        .map_err(|err: <T as FromStr>::Err| ExprError::Parse(err.to_string().into()))
}

#[function("cast(int2) -> int256")]
#[function("cast(int4) -> int256")]
#[function("cast(int8) -> int256")]
pub fn to_int256<T: TryInto<Int256>>(elem: T) -> Result<Int256> {
    elem.try_into()
        .map_err(|_| ExprError::CastOutOfRange("int256"))
}

#[function("cast(jsonb) -> boolean")]
pub fn jsonb_to_bool(v: JsonbRef<'_>) -> Result<bool> {
    v.as_bool().map_err(|e| ExprError::Parse(e.into()))
}

/// Note that PostgreSQL casts JSON numbers from arbitrary precision `numeric` but we use `f64`.
/// This is less powerful but still meets RFC 8259 interoperability.
#[function("cast(jsonb) -> int2")]
#[function("cast(jsonb) -> int4")]
#[function("cast(jsonb) -> int8")]
#[function("cast(jsonb) -> decimal")]
#[function("cast(jsonb) -> float4")]
#[function("cast(jsonb) -> float8")]
pub fn jsonb_to_number<T: TryFrom<F64>>(v: JsonbRef<'_>) -> Result<T> {
    v.as_number()
        .map_err(|e| ExprError::Parse(e.into()))?
        .into_ordered()
        .try_into()
        .map_err(|_| ExprError::NumericOutOfRange)
}

#[function("cast(int4) -> int2")]
#[function("cast(int8) -> int2")]
#[function("cast(int8) -> int4")]
#[function("cast(float4) -> int2")]
#[function("cast(float8) -> int2")]
#[function("cast(float4) -> int4")]
#[function("cast(float8) -> int4")]
#[function("cast(float4) -> int8")]
#[function("cast(float8) -> int8")]
#[function("cast(float8) -> float4")]
#[function("cast(decimal) -> int2")]
#[function("cast(decimal) -> int4")]
#[function("cast(decimal) -> int8")]
#[function("cast(decimal) -> float4")]
#[function("cast(decimal) -> float8")]
#[function("cast(float4) -> decimal")]
#[function("cast(float8) -> decimal")]
pub fn try_cast<T1, T2>(elem: T1) -> Result<T2>
where
    T1: TryInto<T2> + std::fmt::Debug + Copy,
{
    elem.try_into()
        .map_err(|_| ExprError::CastOutOfRange(std::any::type_name::<T2>()))
}

#[function("cast(boolean) -> int4")]
#[function("cast(int2) -> int4")]
#[function("cast(int2) -> int8")]
#[function("cast(int2) -> float4")]
#[function("cast(int2) -> float8")]
#[function("cast(int2) -> decimal")]
#[function("cast(int4) -> int8")]
#[function("cast(int4) -> float4")]
#[function("cast(int4) -> float8")]
#[function("cast(int4) -> decimal")]
#[function("cast(int8) -> float4")]
#[function("cast(int8) -> float8")]
#[function("cast(int8) -> decimal")]
#[function("cast(float4) -> float8")]
#[function("cast(date) -> timestamp")]
#[function("cast(time) -> interval")]
#[function("cast(timestamp) -> date")]
#[function("cast(timestamp) -> time")]
#[function("cast(interval) -> time")]
#[function("cast(varchar) -> varchar")]
#[function("cast(int256) -> float8")]
pub fn cast<T1, T2>(elem: T1) -> T2
where
    T1: Into<T2>,
{
    elem.into()
}

#[function("cast(varchar) -> boolean")]
pub fn str_to_bool(input: &str) -> Result<bool> {
    cast::str_to_bool(input).map_err(|err| ExprError::Parse(err.into()))
}

#[function("cast(int4) -> boolean")]
pub fn int_to_bool(input: i32) -> bool {
    input != 0
}

// For most of the types, cast them to varchar is similar to return their text format.
// So we use this function to cast type to varchar.
#[function("cast(*int) -> varchar")]
#[function("cast(decimal) -> varchar")]
#[function("cast(*float) -> varchar")]
#[function("cast(int256) -> varchar")]
#[function("cast(time) -> varchar")]
#[function("cast(date) -> varchar")]
#[function("cast(interval) -> varchar")]
#[function("cast(timestamp) -> varchar")]
#[function("cast(jsonb) -> varchar")]
#[function("cast(bytea) -> varchar")]
#[function("cast(anyarray) -> varchar")]
pub fn general_to_text(elem: impl ToText, mut writer: &mut impl Write) {
    elem.write(&mut writer).unwrap();
}

#[function("cast(boolean) -> varchar")]
pub fn bool_to_varchar(input: bool, writer: &mut impl Write) {
    writer
        .write_str(if input { "true" } else { "false" })
        .unwrap();
}

/// `bool_out` is different from `general_to_string<bool>` to produce a single char. `PostgreSQL`
/// uses different variants of bool-to-string in different situations.
#[function("bool_out(boolean) -> varchar")]
pub fn bool_out(input: bool, writer: &mut impl Write) {
    writer.write_str(if input { "t" } else { "f" }).unwrap();
}

#[function("cast(varchar) -> bytea")]
pub fn str_to_bytea(elem: &str) -> Result<Box<[u8]>> {
    cast::str_to_bytea(elem).map_err(|err| ExprError::Parse(err.into()))
}

// TODO(nanderstabel): optimize for multidimensional List. Depth can be given as a parameter to this
// function.
/// Takes a string input in the form of a comma-separated list enclosed in braces, and returns a
/// vector of strings containing the list items.
///
/// # Examples
/// - "{1, 2, 3}" => ["1", "2", "3"]
/// - "{1, {2, 3}}" => ["1", "{2, 3}"]
fn unnest(input: &str) -> Result<Vec<&str>> {
    let trimmed = input.trim();
    if !trimmed.starts_with('{') || !trimmed.ends_with('}') {
        return Err(ExprError::Parse("Input must be braced".into()));
    }
    let trimmed = &trimmed[1..trimmed.len() - 1];

    let mut items = Vec::new();
    let mut depth = 0;
    let mut start = 0;
    for (i, c) in trimmed.chars().enumerate() {
        match c {
            '{' => depth += 1,
            '}' => depth -= 1,
            ',' if depth == 0 => {
                let item = trimmed[start..i].trim();
                items.push(item);
                start = i + 1;
            }
            _ => {}
        }
    }
    if depth != 0 {
        return Err(ExprError::Parse("Unbalanced braces".into()));
    }
    let last = trimmed[start..].trim();
    if !last.is_empty() {
        items.push(last);
    }
    Ok(items)
}

#[function("cast(varchar) -> anyarray", type_infer = "panic")]
fn str_to_list(input: &str, ctx: &Context) -> Result<ListValue> {
    let cast = build_func(
        PbType::Cast,
        ctx.return_type.as_list().clone(),
        vec![InputRefExpression::new(DataType::Varchar, 0).boxed()],
    )
    .unwrap();
    let mut values = vec![];
    for item in unnest(input)? {
        let v = cast
            .eval_row(&OwnedRow::new(vec![Some(item.to_string().into())])) // TODO: optimize
            .now_or_never()
            .unwrap()?;
        values.push(v);
    }
    Ok(ListValue::new(values))
}

/// Cast array with `source_elem_type` into array with `target_elem_type` by casting each element.
#[function("cast(anyarray) -> anyarray", type_infer = "panic")]
fn list_cast(input: ListRef<'_>, ctx: &Context) -> Result<ListValue> {
    let cast = build_func(
        PbType::Cast,
        ctx.return_type.as_list().clone(),
        vec![InputRefExpression::new(ctx.arg_types[0].as_list().clone(), 0).boxed()],
    )
    .unwrap();
    let elements = input.iter();
    let mut values = Vec::with_capacity(elements.len());
    for item in elements {
        let v = cast
            .eval_row(&OwnedRow::new(vec![item.map(|s| s.into_scalar_impl())])) // TODO: optimize
            .now_or_never()
            .unwrap()?;
        values.push(v);
    }
    Ok(ListValue::new(values))
}

/// Cast struct of `source_elem_type` to `target_elem_type` by casting each element.
#[function("cast(struct) -> struct", type_infer = "panic")]
fn struct_cast(input: StructRef<'_>, ctx: &Context) -> Result<StructValue> {
    let fields = (input.iter_fields_ref())
        .zip_eq_fast(ctx.arg_types[0].as_struct().types())
        .zip_eq_fast(ctx.return_type.as_struct().types())
        .map(|((datum_ref, source_field_type), target_field_type)| {
            if source_field_type == target_field_type {
                return Ok(datum_ref.map(|scalar_ref| scalar_ref.into_scalar_impl()));
            }
            let cast = build_func(
                PbType::Cast,
                target_field_type.clone(),
                vec![InputRefExpression::new(source_field_type.clone(), 0).boxed()],
            )
            .unwrap();
            let value = match datum_ref {
                Some(scalar_ref) => cast
                    .eval_row(&OwnedRow::new(vec![Some(scalar_ref.into_scalar_impl())]))
                    .now_or_never()
                    .unwrap()?,
                None => None,
            };
            Ok(value) as Result<_>
        })
        .try_collect()?;
    Ok(StructValue::new(fields))
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDateTime;
    use itertools::Itertools;
    use risingwave_common::array::*;
    use risingwave_common::types::*;
    use risingwave_expr::expr::build_from_pretty;
    use risingwave_pb::expr::expr_node::PbType;

    use super::*;

    #[test]
    fn integer_cast_to_bool() {
        assert!(int_to_bool(32));
        assert!(int_to_bool(-32));
        assert!(!int_to_bool(0));
    }

    #[test]
    fn number_to_string() {
        macro_rules! test {
            ($fn:ident($value:expr), $right:literal) => {
                let mut writer = String::new();
                $fn($value, &mut writer);
                assert_eq!(writer, $right);
            };
        }

        test!(bool_to_varchar(true), "true");
        test!(bool_to_varchar(true), "true");
        test!(bool_to_varchar(false), "false");

        test!(general_to_text(32), "32");
        test!(general_to_text(-32), "-32");
        test!(general_to_text(i32::MIN), "-2147483648");
        test!(general_to_text(i32::MAX), "2147483647");

        test!(general_to_text(i16::MIN), "-32768");
        test!(general_to_text(i16::MAX), "32767");

        test!(general_to_text(i64::MIN), "-9223372036854775808");
        test!(general_to_text(i64::MAX), "9223372036854775807");

        test!(general_to_text(F64::from(32.12)), "32.12");
        test!(general_to_text(F64::from(-32.14)), "-32.14");

        test!(general_to_text(F32::from(32.12_f32)), "32.12");
        test!(general_to_text(F32::from(-32.14_f32)), "-32.14");

        test!(general_to_text(Decimal::try_from(1.222).unwrap()), "1.222");

        test!(general_to_text(Decimal::NaN), "NaN");
    }

    #[test]
    fn test_unnest() {
        assert_eq!(unnest("{ }").unwrap(), vec![] as Vec<String>);
        assert_eq!(
            unnest("{1, 2, 3}").unwrap(),
            vec!["1".to_string(), "2".to_string(), "3".to_string()]
        );
        assert_eq!(
            unnest("{{1, 2, 3}, {4, 5, 6}}").unwrap(),
            vec!["{1, 2, 3}".to_string(), "{4, 5, 6}".to_string()]
        );
        assert_eq!(
            unnest("{{{1, 2, 3}}, {{4, 5, 6}}}").unwrap(),
            vec!["{{1, 2, 3}}".to_string(), "{{4, 5, 6}}".to_string()]
        );
        assert_eq!(
            unnest("{{{1, 2, 3}, {4, 5, 6}}}").unwrap(),
            vec!["{{1, 2, 3}, {4, 5, 6}}".to_string()]
        );
        assert_eq!(
            unnest("{{{aa, bb, cc}, {dd, ee, ff}}}").unwrap(),
            vec!["{{aa, bb, cc}, {dd, ee, ff}}".to_string()]
        );
    }

    #[test]
    fn test_str_to_list() {
        // Empty List
        let ctx = Context {
            arg_types: vec![DataType::Varchar],
            return_type: DataType::from_str("int[]").unwrap(),
        };
        assert_eq!(str_to_list("{}", &ctx).unwrap(), ListValue::new(vec![]));

        let list123 = ListValue::new(vec![
            Some(1.to_scalar_value()),
            Some(2.to_scalar_value()),
            Some(3.to_scalar_value()),
        ]);

        // Single List
        let ctx = Context {
            arg_types: vec![DataType::Varchar],
            return_type: DataType::from_str("int[]").unwrap(),
        };
        assert_eq!(str_to_list("{1, 2, 3}", &ctx).unwrap(), list123);

        // Nested List
        let nested_list123 = ListValue::new(vec![Some(ScalarImpl::List(list123))]);
        let ctx = Context {
            arg_types: vec![DataType::Varchar],
            return_type: DataType::from_str("int[][]").unwrap(),
        };
        assert_eq!(str_to_list("{{1, 2, 3}}", &ctx).unwrap(), nested_list123);

        let nested_list445566 = ListValue::new(vec![Some(ScalarImpl::List(ListValue::new(vec![
            Some(44.to_scalar_value()),
            Some(55.to_scalar_value()),
            Some(66.to_scalar_value()),
        ])))]);

        let double_nested_list123_445566 = ListValue::new(vec![
            Some(ScalarImpl::List(nested_list123.clone())),
            Some(ScalarImpl::List(nested_list445566.clone())),
        ]);

        // Double nested List
        let ctx = Context {
            arg_types: vec![DataType::Varchar],
            return_type: DataType::from_str("int[][][]").unwrap(),
        };
        assert_eq!(
            str_to_list("{{{1, 2, 3}}, {{44, 55, 66}}}", &ctx).unwrap(),
            double_nested_list123_445566
        );

        // Cast previous double nested lists to double nested varchar lists
        let ctx = Context {
            arg_types: vec![DataType::from_str("int[][]").unwrap()],
            return_type: DataType::from_str("varchar[][]").unwrap(),
        };
        let double_nested_varchar_list123_445566 = ListValue::new(vec![
            Some(ScalarImpl::List(
                list_cast(
                    ListRef::ValueRef {
                        val: &nested_list123,
                    },
                    &ctx,
                )
                .unwrap(),
            )),
            Some(ScalarImpl::List(
                list_cast(
                    ListRef::ValueRef {
                        val: &nested_list445566,
                    },
                    &ctx,
                )
                .unwrap(),
            )),
        ]);

        // Double nested Varchar List
        let ctx = Context {
            arg_types: vec![DataType::Varchar],
            return_type: DataType::from_str("varchar[][][]").unwrap(),
        };
        assert_eq!(
            str_to_list("{{{1, 2, 3}}, {{44, 55, 66}}}", &ctx).unwrap(),
            double_nested_varchar_list123_445566
        );
    }

    #[test]
    fn test_invalid_str_to_list() {
        // Unbalanced input
        let ctx = Context {
            arg_types: vec![DataType::Varchar],
            return_type: DataType::from_str("int[]").unwrap(),
        };
        assert!(str_to_list("{{}", &ctx).is_err());
        assert!(str_to_list("{}}", &ctx).is_err());
        assert!(str_to_list("{{1, 2, 3}, {4, 5, 6}", &ctx).is_err());
        assert!(str_to_list("{{1, 2, 3}, 4, 5, 6}}", &ctx).is_err());
    }

    #[test]
    fn test_struct_cast() {
        let ctx = Context {
            arg_types: vec![DataType::Struct(StructType::new(vec![
                ("a", DataType::Varchar),
                ("b", DataType::Float32),
            ]))],
            return_type: DataType::Struct(StructType::new(vec![
                ("a", DataType::Int32),
                ("b", DataType::Int32),
            ])),
        };
        assert_eq!(
            struct_cast(
                StructValue::new(vec![
                    Some("1".into()),
                    Some(F32::from(0.0).to_scalar_value()),
                ])
                .as_scalar_ref(),
                &ctx,
            )
            .unwrap(),
            StructValue::new(vec![
                Some(1i32.to_scalar_value()),
                Some(0i32.to_scalar_value()),
            ])
        );
    }

    #[test]
    fn test_timestamp() {
        assert_eq!(
            try_cast::<_, Timestamp>(Date::from_ymd_uncheck(1994, 1, 1)).unwrap(),
            Timestamp::new(
                NaiveDateTime::parse_from_str("1994-1-1 0:0:0", "%Y-%m-%d %H:%M:%S").unwrap()
            )
        )
    }

    #[tokio::test]
    async fn test_unary() {
        test_unary_bool::<BoolArray, _>(|x| !x, PbType::Not).await;
        test_unary_date::<TimestampArray, _>(|x| try_cast(x).unwrap(), PbType::Cast).await;
        test_str_to_int16::<I16Array, _>(|x| str_parse(x).unwrap()).await;
    }

    #[tokio::test]
    async fn test_i16_to_i32() {
        let mut input = Vec::<Option<i16>>::new();
        let mut target = Vec::<Option<i32>>::new();
        for i in 0..100i16 {
            if i % 2 == 0 {
                target.push(Some(i as i32));
                input.push(Some(i));
            } else {
                input.push(None);
                target.push(None);
            }
        }
        let col1 = I16Array::from_iter(&input).into_ref();
        let data_chunk = DataChunk::new(vec![col1], 100);
        let expr = build_from_pretty("(cast:int4 $0:int2)");
        let res = expr.eval(&data_chunk).await.unwrap();
        let arr: &I32Array = res.as_ref().into();
        for (idx, item) in arr.iter().enumerate() {
            let x = target[idx].as_ref().map(|x| x.as_scalar_ref());
            assert_eq!(x, item);
        }

        for i in 0..input.len() {
            let row = OwnedRow::new(vec![input[i].map(|int| int.to_scalar_value())]);
            let result = expr.eval_row(&row).await.unwrap();
            let expected = target[i].map(|int| int.to_scalar_value());
            assert_eq!(result, expected);
        }
    }

    #[tokio::test]
    async fn test_neg() {
        let input = [Some(1), Some(0), Some(-1)];
        let target = [Some(-1), Some(0), Some(1)];

        let col1 = I32Array::from_iter(&input).into_ref();
        let data_chunk = DataChunk::new(vec![col1], 3);
        let expr = build_from_pretty("(neg:int4 $0:int4)");
        let res = expr.eval(&data_chunk).await.unwrap();
        let arr: &I32Array = res.as_ref().into();
        for (idx, item) in arr.iter().enumerate() {
            let x = target[idx].as_ref().map(|x| x.as_scalar_ref());
            assert_eq!(x, item);
        }

        for i in 0..input.len() {
            let row = OwnedRow::new(vec![input[i].map(|int| int.to_scalar_value())]);
            let result = expr.eval_row(&row).await.unwrap();
            let expected = target[i].map(|int| int.to_scalar_value());
            assert_eq!(result, expected);
        }
    }

    async fn test_str_to_int16<A, F>(f: F)
    where
        A: Array,
        for<'a> &'a A: std::convert::From<&'a ArrayImpl>,
        for<'a> <A as Array>::RefItem<'a>: PartialEq,
        F: Fn(&str) -> <A as Array>::OwnedItem,
    {
        let mut input = Vec::<Option<Box<str>>>::new();
        let mut target = Vec::<Option<<A as Array>::OwnedItem>>::new();
        for i in 0..1u32 {
            if i % 2 == 0 {
                let s = i.to_string().into_boxed_str();
                target.push(Some(f(&s)));
                input.push(Some(s));
            } else {
                input.push(None);
                target.push(None);
            }
        }
        let col1_data = &input.iter().map(|x| x.as_ref().map(|x| &**x)).collect_vec();
        let col1 = Utf8Array::from_iter(col1_data).into_ref();
        let data_chunk = DataChunk::new(vec![col1], 1);
        let expr = build_from_pretty("(cast:int2 $0:varchar)");
        let res = expr.eval(&data_chunk).await.unwrap();
        let arr: &A = res.as_ref().into();
        for (idx, item) in arr.iter().enumerate() {
            let x = target[idx].as_ref().map(|x| x.as_scalar_ref());
            assert_eq!(x, item);
        }

        for i in 0..input.len() {
            let row = OwnedRow::new(vec![input[i]
                .as_ref()
                .cloned()
                .map(|str| str.to_scalar_value())]);
            let result = expr.eval_row(&row).await.unwrap();
            let expected = target[i].as_ref().cloned().map(|x| x.to_scalar_value());
            assert_eq!(result, expected);
        }
    }

    async fn test_unary_bool<A, F>(f: F, kind: PbType)
    where
        A: Array,
        for<'a> &'a A: std::convert::From<&'a ArrayImpl>,
        for<'a> <A as Array>::RefItem<'a>: PartialEq,
        F: Fn(bool) -> <A as Array>::OwnedItem,
    {
        let mut input = Vec::<Option<bool>>::new();
        let mut target = Vec::<Option<<A as Array>::OwnedItem>>::new();
        for i in 0..100 {
            if i % 2 == 0 {
                input.push(Some(true));
                target.push(Some(f(true)));
            } else if i % 3 == 0 {
                input.push(Some(false));
                target.push(Some(f(false)));
            } else {
                input.push(None);
                target.push(None);
            }
        }

        let col1 = BoolArray::from_iter(&input).into_ref();
        let data_chunk = DataChunk::new(vec![col1], 100);
        let expr = build_from_pretty(format!("({kind:?}:boolean $0:boolean)"));
        let res = expr.eval(&data_chunk).await.unwrap();
        let arr: &A = res.as_ref().into();
        for (idx, item) in arr.iter().enumerate() {
            let x = target[idx].as_ref().map(|x| x.as_scalar_ref());
            assert_eq!(x, item);
        }

        for i in 0..input.len() {
            let row = OwnedRow::new(vec![input[i].map(|b| b.to_scalar_value())]);
            let result = expr.eval_row(&row).await.unwrap();
            let expected = target[i].as_ref().cloned().map(|x| x.to_scalar_value());
            assert_eq!(result, expected);
        }
    }

    async fn test_unary_date<A, F>(f: F, kind: PbType)
    where
        A: Array,
        for<'a> &'a A: std::convert::From<&'a ArrayImpl>,
        for<'a> <A as Array>::RefItem<'a>: PartialEq,
        F: Fn(Date) -> <A as Array>::OwnedItem,
    {
        let mut input = Vec::<Option<Date>>::new();
        let mut target = Vec::<Option<<A as Array>::OwnedItem>>::new();
        for i in 0..100 {
            if i % 2 == 0 {
                let date = Date::from_num_days_from_ce_uncheck(i);
                input.push(Some(date));
                target.push(Some(f(date)));
            } else {
                input.push(None);
                target.push(None);
            }
        }

        let col1 = DateArray::from_iter(&input).into_ref();
        let data_chunk = DataChunk::new(vec![col1], 100);
        let expr = build_from_pretty(format!("({kind:?}:timestamp $0:date)"));
        let res = expr.eval(&data_chunk).await.unwrap();
        let arr: &A = res.as_ref().into();
        for (idx, item) in arr.iter().enumerate() {
            let x = target[idx].as_ref().map(|x| x.as_scalar_ref());
            assert_eq!(x, item);
        }

        for i in 0..input.len() {
            let row = OwnedRow::new(vec![input[i].map(|d| d.to_scalar_value())]);
            let result = expr.eval_row(&row).await.unwrap();
            let expected = target[i].as_ref().cloned().map(|x| x.to_scalar_value());
            assert_eq!(result, expected);
        }
    }
}
