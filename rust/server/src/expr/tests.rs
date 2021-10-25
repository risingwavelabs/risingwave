use super::*;
use crate::array::column::Column;
use crate::array::*;
use crate::types::{BoolType, DecimalType, Int32Type, Scalar};
use pb_construct::make_proto;
use protobuf::well_known_types::Any as AnyProto;
use protobuf::RepeatedField;
use risingwave_proto::data::DataType as DataTypeProto;
use risingwave_proto::expr::ExprNode_Type::INPUT_REF;
use risingwave_proto::expr::InputRefExpr;
use risingwave_proto::expr::{ExprNode, ExprNode_Type, FunctionCall};
use rust_decimal::Decimal;

#[test]
fn test_binary() {
    test_binary_i32::<I32Array, _>(|x, y| x + y, ExprNode_Type::ADD);
    test_binary_i32::<I32Array, _>(|x, y| x - y, ExprNode_Type::SUBTRACT);
    test_binary_i32::<I32Array, _>(|x, y| x * y, ExprNode_Type::MULTIPLY);
    test_binary_i32::<I32Array, _>(|x, y| x / y, ExprNode_Type::DIVIDE);
    test_binary_i32::<BoolArray, _>(|x, y| x == y, ExprNode_Type::EQUAL);
    test_binary_i32::<BoolArray, _>(|x, y| x != y, ExprNode_Type::NOT_EQUAL);
    test_binary_i32::<BoolArray, _>(|x, y| x > y, ExprNode_Type::GREATER_THAN);
    test_binary_i32::<BoolArray, _>(|x, y| x >= y, ExprNode_Type::GREATER_THAN_OR_EQUAL);
    test_binary_i32::<BoolArray, _>(|x, y| x < y, ExprNode_Type::LESS_THAN);
    test_binary_i32::<BoolArray, _>(|x, y| x <= y, ExprNode_Type::LESS_THAN_OR_EQUAL);
    test_binary_decimal::<DecimalArray, _>(|x, y| x + y, ExprNode_Type::ADD);
    test_binary_decimal::<DecimalArray, _>(|x, y| x - y, ExprNode_Type::SUBTRACT);
    test_binary_decimal::<DecimalArray, _>(|x, y| x * y, ExprNode_Type::MULTIPLY);
    test_binary_decimal::<DecimalArray, _>(|x, y| x / y, ExprNode_Type::DIVIDE);
    test_binary_decimal::<BoolArray, _>(|x, y| x == y, ExprNode_Type::EQUAL);
    test_binary_decimal::<BoolArray, _>(|x, y| x != y, ExprNode_Type::NOT_EQUAL);
    test_binary_decimal::<BoolArray, _>(|x, y| x > y, ExprNode_Type::GREATER_THAN);
    test_binary_decimal::<BoolArray, _>(|x, y| x >= y, ExprNode_Type::GREATER_THAN_OR_EQUAL);
    test_binary_decimal::<BoolArray, _>(|x, y| x < y, ExprNode_Type::LESS_THAN);
    test_binary_decimal::<BoolArray, _>(|x, y| x <= y, ExprNode_Type::LESS_THAN_OR_EQUAL);
    test_binary_bool::<BoolArray, _>(|x, y| x && y, ExprNode_Type::AND);
    test_binary_bool::<BoolArray, _>(|x, y| x || y, ExprNode_Type::OR);
}

#[test]
fn test_unary() {
    test_unary_bool::<BoolArray, _>(|x| !x, ExprNode_Type::NOT);
}

fn test_binary_i32<A, F>(f: F, kind: ExprNode_Type)
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

    let col1 = Column::new(
        I32Array::from_slice(&lhs)
            .map(|x| Arc::new(x.into()))
            .unwrap(),
        Int32Type::create(true),
    );
    let col2 = Column::new(
        I32Array::from_slice(&rhs)
            .map(|x| Arc::new(x.into()))
            .unwrap(),
        Int32Type::create(true),
    );
    let data_chunk = DataChunk::builder().columns(vec![col1, col2]).build();
    let expr = make_expression(
        kind,
        risingwave_proto::data::DataType_TypeName::INT32,
        &[0, 1],
    );
    let mut vec_excutor = build_from_proto(&expr).unwrap();
    let res = vec_excutor.eval(&data_chunk).unwrap();
    let arr: &A = res.as_ref().into();
    for (idx, item) in arr.iter().enumerate() {
        let x = target[idx].as_ref().map(|x| x.as_scalar_ref());
        assert_eq!(x, item);
    }
}

fn test_binary_decimal<A, F>(f: F, kind: ExprNode_Type)
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

    let col1 = Column::new(
        DecimalArray::from_slice(&lhs)
            .map(|x| Arc::new(x.into()))
            .unwrap(),
        DecimalType::create(true, 10, 5).unwrap(),
    );
    let col2 = Column::new(
        DecimalArray::from_slice(&rhs)
            .map(|x| Arc::new(x.into()))
            .unwrap(),
        DecimalType::create(true, 10, 5).unwrap(),
    );
    let data_chunk = DataChunk::builder().columns(vec![col1, col2]).build();
    let expr = make_expression(
        kind,
        risingwave_proto::data::DataType_TypeName::DECIMAL,
        &[0, 1],
    );
    let mut vec_excutor = build_from_proto(&expr).unwrap();
    let res = vec_excutor.eval(&data_chunk).unwrap();
    let arr: &A = res.as_ref().into();
    for (idx, item) in arr.iter().enumerate() {
        let x = target[idx].as_ref().map(|x| x.as_scalar_ref());
        assert_eq!(x, item);
    }
}

fn test_binary_bool<A, F>(f: F, kind: ExprNode_Type)
where
    A: Array,
    for<'a> &'a A: std::convert::From<&'a ArrayImpl>,
    for<'a> <A as Array>::RefItem<'a>: PartialEq,
    F: Fn(bool, bool) -> <A as Array>::OwnedItem,
{
    let mut lhs = Vec::<Option<bool>>::new();
    let mut rhs = Vec::<Option<bool>>::new();
    let mut target = Vec::<Option<<A as Array>::OwnedItem>>::new();
    for i in 0..100 {
        if i % 2 == 0 {
            lhs.push(Some(true));
            rhs.push(None);
            target.push(None);
        } else if i % 3 == 0 {
            lhs.push(Some(true));
            rhs.push(Some(false));
            target.push(Some(f(true, false)));
        } else if i % 5 == 0 {
            lhs.push(Some(false));
            rhs.push(Some(false));
            target.push(Some(f(false, false)));
        } else {
            lhs.push(Some(true));
            rhs.push(Some(true));
            target.push(Some(f(true, true)));
        }
    }

    let col1 = Column::new(
        BoolArray::from_slice(&lhs)
            .map(|x| Arc::new(x.into()))
            .unwrap(),
        BoolType::create(true),
    );
    let col2 = Column::new(
        BoolArray::from_slice(&rhs)
            .map(|x| Arc::new(x.into()))
            .unwrap(),
        BoolType::create(true),
    );
    let data_chunk = DataChunk::builder().columns(vec![col1, col2]).build();
    let expr = make_expression(
        kind,
        risingwave_proto::data::DataType_TypeName::DECIMAL,
        &[0, 1],
    );
    let mut vec_excutor = build_from_proto(&expr).unwrap();
    let res = vec_excutor.eval(&data_chunk).unwrap();
    let arr: &A = res.as_ref().into();
    for (idx, item) in arr.iter().enumerate() {
        let x = target[idx].as_ref().map(|x| x.as_scalar_ref());
        assert_eq!(x, item);
    }
}

fn test_unary_bool<A, F>(f: F, kind: ExprNode_Type)
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

    let col1 = Column::new(
        BoolArray::from_slice(&input)
            .map(|x| Arc::new(x.into()))
            .unwrap(),
        BoolType::create(true),
    );
    let data_chunk = DataChunk::builder().columns(vec![col1]).build();
    let expr = make_expression(
        kind,
        risingwave_proto::data::DataType_TypeName::DECIMAL,
        &[0],
    );
    let mut vec_excutor = build_from_proto(&expr).unwrap();
    let res = vec_excutor.eval(&data_chunk).unwrap();
    let arr: &A = res.as_ref().into();
    for (idx, item) in arr.iter().enumerate() {
        let x = target[idx].as_ref().map(|x| x.as_scalar_ref());
        assert_eq!(x, item);
    }
}

fn make_expression(
    kind: ExprNode_Type,
    ret: risingwave_proto::data::DataType_TypeName,
    indices: &[i32],
) -> ExprNode {
    let mut exprs = Vec::new();
    for idx in indices {
        exprs.push(make_inputref(*idx, ret));
    }
    make_proto!(ExprNode, {
      expr_type: kind,
      body: AnyProto::pack(
        &make_proto!(FunctionCall, {
          children: RepeatedField::from_slice(exprs.as_slice())
        })
      ).unwrap(),
      return_type: make_proto!(DataTypeProto, {
        type_name: risingwave_proto::data::DataType_TypeName::BOOLEAN
      })
    })
}

fn make_inputref(idx: i32, ret: risingwave_proto::data::DataType_TypeName) -> ExprNode {
    make_proto!(ExprNode, {
      expr_type: INPUT_REF,
      body: AnyProto::pack(
        &make_proto!(InputRefExpr, {column_idx: idx})
      ).unwrap(),
      return_type: make_proto!(DataTypeProto, {
        type_name: ret
      })
    })
}
