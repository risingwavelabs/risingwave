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

//! For expression that only accept one value as input (e.g. CAST)

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use risingwave_common::array::*;
    use risingwave_common::types::{NaiveDateWrapper, Scalar};
    use risingwave_pb::data::data_type::TypeName;
    use risingwave_pb::data::DataType;
    use risingwave_pb::expr::expr_node::{RexNode, Type};
    use risingwave_pb::expr::{ExprNode, FunctionCall};

    use super::super::*;
    use crate::expr::test_utils::{make_expression, make_input_ref};
    use crate::vector_op::cast::{str_parse, try_cast};

    #[tokio::test]
    async fn test_unary() {
        test_unary_bool::<BoolArray, _>(|x| !x, Type::Not).await;
        test_unary_date::<NaiveDateTimeArray, _>(|x| try_cast(x).unwrap(), Type::Cast).await;
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
        let col1 = I16Array::from_iter(&input).into();
        let data_chunk = DataChunk::new(vec![col1], 100);
        let return_type = DataType {
            type_name: TypeName::Int32 as i32,
            is_nullable: false,
            ..Default::default()
        };
        let expr = ExprNode {
            expr_type: Type::Cast as i32,
            return_type: Some(return_type),
            rex_node: Some(RexNode::FuncCall(FunctionCall {
                children: vec![make_input_ref(0, TypeName::Int16)],
            })),
        };
        let vec_executor = build_from_prost(&expr).unwrap();
        let res = vec_executor.eval(&data_chunk).await.unwrap();
        let arr: &I32Array = res.as_ref().into();
        for (idx, item) in arr.iter().enumerate() {
            let x = target[idx].as_ref().map(|x| x.as_scalar_ref());
            assert_eq!(x, item);
        }

        for i in 0..input.len() {
            let row = OwnedRow::new(vec![input[i].map(|int| int.to_scalar_value())]);
            let result = vec_executor.eval_row(&row).await.unwrap();
            let expected = target[i].map(|int| int.to_scalar_value());
            assert_eq!(result, expected);
        }
    }

    #[tokio::test]
    async fn test_neg() {
        let mut input = Vec::<Option<i32>>::new();
        let mut target = Vec::<Option<i32>>::new();

        input.push(Some(1));
        input.push(Some(0));
        input.push(Some(-1));

        target.push(Some(-1));
        target.push(Some(0));
        target.push(Some(1));

        let col1 = I32Array::from_iter(&input).into();
        let data_chunk = DataChunk::new(vec![col1], 3);
        let return_type = DataType {
            type_name: TypeName::Int32 as i32,
            is_nullable: false,
            ..Default::default()
        };
        let expr = ExprNode {
            expr_type: Type::Neg as i32,
            return_type: Some(return_type),
            rex_node: Some(RexNode::FuncCall(FunctionCall {
                children: vec![make_input_ref(0, TypeName::Int32)],
            })),
        };
        let vec_executor = build_from_prost(&expr).unwrap();
        let res = vec_executor.eval(&data_chunk).await.unwrap();
        let arr: &I32Array = res.as_ref().into();
        for (idx, item) in arr.iter().enumerate() {
            let x = target[idx].as_ref().map(|x| x.as_scalar_ref());
            assert_eq!(x, item);
        }

        for i in 0..input.len() {
            let row = OwnedRow::new(vec![input[i].map(|int| int.to_scalar_value())]);
            let result = vec_executor.eval_row(&row).await.unwrap();
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
        let col1 = Utf8Array::from_iter(col1_data).into();
        let data_chunk = DataChunk::new(vec![col1], 1);
        let return_type = DataType {
            type_name: TypeName::Int16 as i32,
            is_nullable: false,
            ..Default::default()
        };
        let expr = ExprNode {
            expr_type: Type::Cast as i32,
            return_type: Some(return_type),
            rex_node: Some(RexNode::FuncCall(FunctionCall {
                children: vec![make_input_ref(0, TypeName::Varchar)],
            })),
        };
        let vec_executor = build_from_prost(&expr).unwrap();
        let res = vec_executor.eval(&data_chunk).await.unwrap();
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
            let result = vec_executor.eval_row(&row).await.unwrap();
            let expected = target[i].as_ref().cloned().map(|x| x.to_scalar_value());
            assert_eq!(result, expected);
        }
    }

    async fn test_unary_bool<A, F>(f: F, kind: Type)
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

        let col1 = BoolArray::from_iter(&input).into();
        let data_chunk = DataChunk::new(vec![col1], 100);
        let prost = make_expression(
            kind,
            TypeName::Boolean,
            vec![make_input_ref(0, TypeName::Boolean)],
        );
        let vec_executor = build_from_prost(&prost).unwrap();
        let res = vec_executor.eval(&data_chunk).await.unwrap();
        let arr: &A = res.as_ref().into();
        for (idx, item) in arr.iter().enumerate() {
            let x = target[idx].as_ref().map(|x| x.as_scalar_ref());
            assert_eq!(x, item);
        }

        for i in 0..input.len() {
            let row = OwnedRow::new(vec![input[i].map(|b| b.to_scalar_value())]);
            let result = vec_executor.eval_row(&row).await.unwrap();
            let expected = target[i].as_ref().cloned().map(|x| x.to_scalar_value());
            assert_eq!(result, expected);
        }
    }

    async fn test_unary_date<A, F>(f: F, kind: Type)
    where
        A: Array,
        for<'a> &'a A: std::convert::From<&'a ArrayImpl>,
        for<'a> <A as Array>::RefItem<'a>: PartialEq,
        F: Fn(NaiveDateWrapper) -> <A as Array>::OwnedItem,
    {
        let mut input = Vec::<Option<NaiveDateWrapper>>::new();
        let mut target = Vec::<Option<<A as Array>::OwnedItem>>::new();
        for i in 0..100 {
            if i % 2 == 0 {
                let date = NaiveDateWrapper::from_num_days_from_ce_uncheck(i);
                input.push(Some(date));
                target.push(Some(f(date)));
            } else {
                input.push(None);
                target.push(None);
            }
        }

        let col1 = NaiveDateArray::from_iter(&input).into();
        let data_chunk = DataChunk::new(vec![col1], 100);
        let prost = make_expression(
            kind,
            TypeName::Timestamp,
            vec![make_input_ref(0, TypeName::Date)],
        );
        let vec_executor = build_from_prost(&prost).unwrap();
        let res = vec_executor.eval(&data_chunk).await.unwrap();
        let arr: &A = res.as_ref().into();
        for (idx, item) in arr.iter().enumerate() {
            let x = target[idx].as_ref().map(|x| x.as_scalar_ref());
            assert_eq!(x, item);
        }

        for i in 0..input.len() {
            let row = OwnedRow::new(vec![input[i].map(|d| d.to_scalar_value())]);
            let result = vec_executor.eval_row(&row).await.unwrap();
            let expected = target[i].as_ref().cloned().map(|x| x.to_scalar_value());
            assert_eq!(result, expected);
        }
    }
}
