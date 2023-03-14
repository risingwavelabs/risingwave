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

#[cfg(test)]
mod tests {
    use risingwave_common::array::interval_array::IntervalArray;
    use risingwave_common::array::*;
    use risingwave_common::types::test_utils::IntervalUnitTestExt;
    use risingwave_common::types::{Decimal, IntervalUnit, NaiveDateWrapper, Scalar};
    use risingwave_pb::data::data_type::TypeName;
    use risingwave_pb::expr::expr_node::Type;

    use super::super::*;
    use crate::expr::test_utils::make_expression;
    use crate::vector_op::arithmetic_op::{date_interval_add, date_interval_sub};

    #[test]
    fn test_binary() {
        test_binary_i32::<I32Array, _>(|x, y| x + y, Type::Add);
        test_binary_i32::<I32Array, _>(|x, y| x - y, Type::Subtract);
        test_binary_i32::<I32Array, _>(|x, y| x * y, Type::Multiply);
        test_binary_i32::<I32Array, _>(|x, y| x / y, Type::Divide);
        test_binary_i32::<BoolArray, _>(|x, y| x == y, Type::Equal);
        test_binary_i32::<BoolArray, _>(|x, y| x != y, Type::NotEqual);
        test_binary_i32::<BoolArray, _>(|x, y| x > y, Type::GreaterThan);
        test_binary_i32::<BoolArray, _>(|x, y| x >= y, Type::GreaterThanOrEqual);
        test_binary_i32::<BoolArray, _>(|x, y| x < y, Type::LessThan);
        test_binary_i32::<BoolArray, _>(|x, y| x <= y, Type::LessThanOrEqual);
        test_binary_decimal::<DecimalArray, _>(|x, y| x + y, Type::Add);
        test_binary_decimal::<DecimalArray, _>(|x, y| x - y, Type::Subtract);
        test_binary_decimal::<DecimalArray, _>(|x, y| x * y, Type::Multiply);
        test_binary_decimal::<DecimalArray, _>(|x, y| x / y, Type::Divide);
        test_binary_decimal::<BoolArray, _>(|x, y| x == y, Type::Equal);
        test_binary_decimal::<BoolArray, _>(|x, y| x != y, Type::NotEqual);
        test_binary_decimal::<BoolArray, _>(|x, y| x > y, Type::GreaterThan);
        test_binary_decimal::<BoolArray, _>(|x, y| x >= y, Type::GreaterThanOrEqual);
        test_binary_decimal::<BoolArray, _>(|x, y| x < y, Type::LessThan);
        test_binary_decimal::<BoolArray, _>(|x, y| x <= y, Type::LessThanOrEqual);
        test_binary_interval::<NaiveDateTimeArray, _>(
            |x, y| date_interval_add(x, y).unwrap(),
            Type::Add,
        );
        test_binary_interval::<NaiveDateTimeArray, _>(
            |x, y| date_interval_sub(x, y).unwrap(),
            Type::Subtract,
        );
    }

    fn test_binary_i32<A, F>(f: F, kind: Type)
    where
        A: Array,
        for<'a> &'a A: std::convert::From<&'a ArrayImpl>,
        for<'a> <A as Array>::RefItem<'a>: PartialEq,
        F: Fn(i32, i32) -> <A as Array>::OwnedItem,
    {
        let mut lhs = Vec::<Option<i32>>::new();
        let mut rhs = Vec::<Option<i32>>::new();
        let mut target = Vec::<Option<<A as Array>::OwnedItem>>::new();
        for i in 0..100 {
            if i % 2 == 0 {
                lhs.push(Some(i));
                rhs.push(None);
                target.push(None);
            } else if i % 3 == 0 {
                lhs.push(Some(i));
                rhs.push(Some(i + 1));
                target.push(Some(f(i, i + 1)));
            } else if i % 5 == 0 {
                lhs.push(Some(i + 1));
                rhs.push(Some(i));
                target.push(Some(f(i + 1, i)));
            } else {
                lhs.push(Some(i));
                rhs.push(Some(i));
                target.push(Some(f(i, i)));
            }
        }

        let col1 = I32Array::from_iter(&lhs).into();
        let col2 = I32Array::from_iter(&rhs).into();
        let data_chunk = DataChunk::new(vec![col1, col2], 100);
        let expr = make_expression(kind, &[TypeName::Int32, TypeName::Int32], &[0, 1]);
        let vec_executor = build_from_prost(&expr).unwrap();
        let res = vec_executor.eval(&data_chunk).unwrap();
        let arr: &A = res.as_ref().into();
        for (idx, item) in arr.iter().enumerate() {
            let x = target[idx].as_ref().map(|x| x.as_scalar_ref());
            assert_eq!(x, item);
        }

        for i in 0..lhs.len() {
            let row = OwnedRow::new(vec![
                lhs[i].map(|int| int.to_scalar_value()),
                rhs[i].map(|int| int.to_scalar_value()),
            ]);
            let result = vec_executor.eval_row(&row).unwrap();
            let expected = target[i].as_ref().cloned().map(|x| x.to_scalar_value());
            assert_eq!(result, expected);
        }
    }

    fn test_binary_interval<A, F>(f: F, kind: Type)
    where
        A: Array,
        for<'a> &'a A: std::convert::From<&'a ArrayImpl>,
        for<'a> <A as Array>::RefItem<'a>: PartialEq,
        F: Fn(NaiveDateWrapper, IntervalUnit) -> <A as Array>::OwnedItem,
    {
        let mut lhs = Vec::<Option<NaiveDateWrapper>>::new();
        let mut rhs = Vec::<Option<IntervalUnit>>::new();
        let mut target = Vec::<Option<<A as Array>::OwnedItem>>::new();
        for i in 0..100 {
            if i % 2 == 0 {
                rhs.push(Some(IntervalUnit::from_ymd(0, i, i)));
                lhs.push(None);
                target.push(None);
            } else {
                rhs.push(Some(IntervalUnit::from_ymd(0, i, i)));
                lhs.push(Some(NaiveDateWrapper::from_num_days_from_ce_uncheck(i)));
                target.push(Some(f(
                    NaiveDateWrapper::from_num_days_from_ce_uncheck(i),
                    IntervalUnit::from_ymd(0, i, i),
                )));
            }
        }

        let col1 = NaiveDateArray::from_iter(&lhs).into();
        let col2 = IntervalArray::from_iter(&rhs).into();
        let data_chunk = DataChunk::new(vec![col1, col2], 100);
        let expr = make_expression(kind, &[TypeName::Date, TypeName::Interval], &[0, 1]);
        let vec_executor = build_from_prost(&expr).unwrap();
        let res = vec_executor.eval(&data_chunk).unwrap();
        let arr: &A = res.as_ref().into();
        for (idx, item) in arr.iter().enumerate() {
            let x = target[idx].as_ref().map(|x| x.as_scalar_ref());
            assert_eq!(x, item);
        }

        for i in 0..lhs.len() {
            let row = OwnedRow::new(vec![
                lhs[i].map(|date| date.to_scalar_value()),
                rhs[i].map(|date| date.to_scalar_value()),
            ]);
            let result = vec_executor.eval_row(&row).unwrap();
            let expected = target[i].as_ref().cloned().map(|x| x.to_scalar_value());
            assert_eq!(result, expected);
        }
    }

    fn test_binary_decimal<A, F>(f: F, kind: Type)
    where
        A: Array,
        for<'a> &'a A: std::convert::From<&'a ArrayImpl>,
        for<'a> <A as Array>::RefItem<'a>: PartialEq,
        F: Fn(Decimal, Decimal) -> <A as Array>::OwnedItem,
    {
        let mut lhs = Vec::<Option<Decimal>>::new();
        let mut rhs = Vec::<Option<Decimal>>::new();
        let mut target = Vec::<Option<<A as Array>::OwnedItem>>::new();
        for i in 0..100 {
            if i % 2 == 0 {
                lhs.push(Some(i.into()));
                rhs.push(None);
                target.push(None);
            } else if i % 3 == 0 {
                lhs.push(Some(i.into()));
                rhs.push(Some((i + 1).into()));
                target.push(Some(f((i).into(), (i + 1).into())));
            } else if i % 5 == 0 {
                lhs.push(Some((i + 1).into()));
                rhs.push(Some((i).into()));
                target.push(Some(f((i + 1).into(), (i).into())));
            } else {
                lhs.push(Some((i).into()));
                rhs.push(Some((i).into()));
                target.push(Some(f((i).into(), (i).into())));
            }
        }

        let col1 = DecimalArray::from_iter(&lhs).into();
        let col2 = DecimalArray::from_iter(&rhs).into();
        let data_chunk = DataChunk::new(vec![col1, col2], 100);
        let expr = make_expression(kind, &[TypeName::Decimal, TypeName::Decimal], &[0, 1]);
        let vec_executor = build_from_prost(&expr).unwrap();
        let res = vec_executor.eval(&data_chunk).unwrap();
        let arr: &A = res.as_ref().into();
        for (idx, item) in arr.iter().enumerate() {
            let x = target[idx].as_ref().map(|x| x.as_scalar_ref());
            assert_eq!(x, item);
        }

        for i in 0..lhs.len() {
            let row = OwnedRow::new(vec![
                lhs[i].map(|dec| dec.to_scalar_value()),
                rhs[i].map(|dec| dec.to_scalar_value()),
            ]);
            let result = vec_executor.eval_row(&row).unwrap();
            let expected = target[i].as_ref().cloned().map(|x| x.to_scalar_value());
            assert_eq!(result, expected);
        }
    }
}
