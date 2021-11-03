use super::*;
use crate::array::column::Column;
use crate::array::interval_array::IntervalArray;
use crate::array::*;
use crate::types::{
    BoolType, DateType, DecimalType, Int32Type, IntervalType, IntervalUnit, Scalar,
};
use crate::vector_op::arithmetic_op::{date_interval_add, date_interval_sub};
use crate::vector_op::cast::date_to_timestamp;
use pb_construct::make_proto;
use protobuf::well_known_types::Any as AnyProto;
use risingwave_pb::expr::expr_node::RexNode;
use risingwave_pb::expr::expr_node::Type as ProstExprType;
use risingwave_pb::expr::FunctionCall;
use risingwave_proto::data::DataType as DataTypeProto;
use risingwave_proto::data::DataType_TypeName;
use risingwave_proto::expr::ExprNode;
use risingwave_proto::expr::ExprNode_Type::INPUT_REF;
use risingwave_proto::expr::InputRefExpr;
use rust_decimal::Decimal;

#[test]
fn test_binary() {
    test_binary_i32::<I32Array, _>(|x, y| x + y, ProstExprType::Add);
    test_binary_i32::<I32Array, _>(|x, y| x - y, ProstExprType::Subtract);
    test_binary_i32::<I32Array, _>(|x, y| x * y, ProstExprType::Multiply);
    test_binary_i32::<I32Array, _>(|x, y| x / y, ProstExprType::Divide);
    test_binary_i32::<BoolArray, _>(|x, y| x == y, ProstExprType::Equal);
    test_binary_i32::<BoolArray, _>(|x, y| x != y, ProstExprType::NotEqual);
    test_binary_i32::<BoolArray, _>(|x, y| x > y, ProstExprType::GreaterThan);
    test_binary_i32::<BoolArray, _>(|x, y| x >= y, ProstExprType::GreaterThanOrEqual);
    test_binary_i32::<BoolArray, _>(|x, y| x < y, ProstExprType::LessThan);
    test_binary_i32::<BoolArray, _>(|x, y| x <= y, ProstExprType::LessThanOrEqual);
    test_binary_decimal::<DecimalArray, _>(|x, y| x + y, ProstExprType::Add);
    test_binary_decimal::<DecimalArray, _>(|x, y| x - y, ProstExprType::Subtract);
    test_binary_decimal::<DecimalArray, _>(|x, y| x * y, ProstExprType::Multiply);
    test_binary_decimal::<DecimalArray, _>(|x, y| x / y, ProstExprType::Divide);
    test_binary_decimal::<BoolArray, _>(|x, y| x == y, ProstExprType::Equal);
    test_binary_decimal::<BoolArray, _>(|x, y| x != y, ProstExprType::NotEqual);
    test_binary_decimal::<BoolArray, _>(|x, y| x > y, ProstExprType::GreaterThan);
    test_binary_decimal::<BoolArray, _>(|x, y| x >= y, ProstExprType::GreaterThanOrEqual);
    test_binary_decimal::<BoolArray, _>(|x, y| x < y, ProstExprType::LessThan);
    test_binary_decimal::<BoolArray, _>(|x, y| x <= y, ProstExprType::LessThanOrEqual);
    test_binary_bool::<BoolArray, _>(|x, y| x && y, ProstExprType::And);
    test_binary_bool::<BoolArray, _>(|x, y| x || y, ProstExprType::Or);
    test_binary_interval::<I64Array, _>(
        |x, y| date_interval_add::<i32, i32, i64>(x, y).unwrap(),
        ProstExprType::Add,
    );
    test_binary_interval::<I64Array, _>(
        |x, y| date_interval_sub::<i32, i32, i64>(x, y).unwrap(),
        ProstExprType::Subtract,
    );
}

#[test]
fn test_unary() {
    test_unary_bool::<BoolArray, _>(|x| !x, ProstExprType::Not);
    test_unary_date::<I64Array, _>(|x| date_to_timestamp(x).unwrap(), ProstExprType::Cast);
}

fn test_binary_i32<A, F>(f: F, kind: ProstExprType)
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
        &[DataType_TypeName::INT32, DataType_TypeName::INT32],
        &[0, 1],
    );
    let mut vec_excutor = build_from_prost(&expr).unwrap();
    let res = vec_excutor.eval(&data_chunk).unwrap();
    let arr: &A = res.as_ref().into();
    for (idx, item) in arr.iter().enumerate() {
        let x = target[idx].as_ref().map(|x| x.as_scalar_ref());
        assert_eq!(x, item);
    }
}

fn test_binary_interval<A, F>(f: F, kind: ProstExprType)
where
    A: Array,
    for<'a> &'a A: std::convert::From<&'a ArrayImpl>,
    for<'a> <A as Array>::RefItem<'a>: PartialEq,
    F: Fn(i32, IntervalUnit) -> <A as Array>::OwnedItem,
{
    let mut lhs = Vec::<Option<i32>>::new();
    let mut rhs = Vec::<Option<IntervalUnit>>::new();
    let mut target = Vec::<Option<<A as Array>::OwnedItem>>::new();
    for i in 0..100 {
        if i % 2 == 0 {
            rhs.push(Some(IntervalUnit::from_ymd(0, i, i)));
            lhs.push(None);
            target.push(None);
        } else {
            rhs.push(Some(IntervalUnit::from_ymd(0, i, i)));
            lhs.push(Some(i));
            target.push(Some(f(i, IntervalUnit::from_ymd(0, i, i))));
        }
    }

    let col1 = Column::new(
        I32Array::from_slice(&lhs)
            .map(|x| Arc::new(x.into()))
            .unwrap(),
        DateType::create(true),
    );
    let col2 = Column::new(
        IntervalArray::from_slice(&rhs)
            .map(|x| Arc::new(x.into()))
            .unwrap(),
        IntervalType::create(true),
    );
    let data_chunk = DataChunk::builder().columns(vec![col1, col2]).build();
    let expr = make_expression(
        kind,
        &[DataType_TypeName::DATE, DataType_TypeName::INTERVAL],
        &[0, 1],
    );
    let mut vec_excutor = build_from_prost(&expr).unwrap();
    let res = vec_excutor.eval(&data_chunk).unwrap();
    let arr: &A = res.as_ref().into();
    for (idx, item) in arr.iter().enumerate() {
        let x = target[idx].as_ref().map(|x| x.as_scalar_ref());
        assert_eq!(x, item);
    }
}

fn test_binary_decimal<A, F>(f: F, kind: ProstExprType)
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
        &[DataType_TypeName::DECIMAL, DataType_TypeName::DECIMAL],
        &[0, 1],
    );
    let mut vec_excutor = build_from_prost(&expr).unwrap();
    let res = vec_excutor.eval(&data_chunk).unwrap();
    let arr: &A = res.as_ref().into();
    for (idx, item) in arr.iter().enumerate() {
        let x = target[idx].as_ref().map(|x| x.as_scalar_ref());
        assert_eq!(x, item);
    }
}

fn test_binary_bool<A, F>(f: F, kind: ProstExprType)
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
        &[DataType_TypeName::DECIMAL, DataType_TypeName::DECIMAL],
        &[0, 1],
    );
    let mut vec_excutor = build_from_prost(&expr).unwrap();
    let res = vec_excutor.eval(&data_chunk).unwrap();
    let arr: &A = res.as_ref().into();
    for (idx, item) in arr.iter().enumerate() {
        let x = target[idx].as_ref().map(|x| x.as_scalar_ref());
        assert_eq!(x, item);
    }
}

fn test_unary_bool<A, F>(f: F, kind: ProstExprType)
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
    let expr = make_expression(kind, &[DataType_TypeName::BOOLEAN], &[0]);
    let mut vec_excutor = build_from_prost(&expr).unwrap();
    let res = vec_excutor.eval(&data_chunk).unwrap();
    let arr: &A = res.as_ref().into();
    for (idx, item) in arr.iter().enumerate() {
        let x = target[idx].as_ref().map(|x| x.as_scalar_ref());
        assert_eq!(x, item);
    }
}

fn test_unary_date<A, F>(f: F, kind: ProstExprType)
where
    A: Array,
    for<'a> &'a A: std::convert::From<&'a ArrayImpl>,
    for<'a> <A as Array>::RefItem<'a>: PartialEq,
    F: Fn(i32) -> <A as Array>::OwnedItem,
{
    let mut input = Vec::<Option<i32>>::new();
    let mut target = Vec::<Option<<A as Array>::OwnedItem>>::new();
    for i in 0..100 {
        if i % 2 == 0 {
            input.push(Some(i));
            target.push(Some(f(i)));
        } else {
            input.push(None);
            target.push(None);
        }
    }

    let col1 = Column::new(
        I32Array::from_slice(&input)
            .map(|x| Arc::new(x.into()))
            .unwrap(),
        DateType::create(true),
    );
    let data_chunk = DataChunk::builder().columns(vec![col1]).build();
    let expr = make_expression(kind, &[DataType_TypeName::DATE], &[0]);
    let mut vec_excutor = build_from_prost(&expr).unwrap();
    let res = vec_excutor.eval(&data_chunk).unwrap();
    let arr: &A = res.as_ref().into();
    for (idx, item) in arr.iter().enumerate() {
        let x = target[idx].as_ref().map(|x| x.as_scalar_ref());
        assert_eq!(x, item);
    }
}

fn make_expression(
    kind: ProstExprType,
    rets: &[DataType_TypeName],
    indices: &[i32],
) -> ProstExprNode {
    let mut exprs = Vec::new();
    for (idx, ret) in indices.iter().zip(rets.iter()) {
        exprs.push(make_inputref(*idx, *ret).to_prost::<ProstExprNode>());
    }
    let function_call = FunctionCall { children: exprs };
    let return_type = risingwave_pb::data::DataType {
        type_name: risingwave_pb::data::data_type::TypeName::Timestamp as i32,
        precision: 0,
        scale: 0,
        is_nullable: false,
        interval_type: 0,
    };
    ProstExprNode {
        expr_type: kind as i32,
        body: None,
        return_type: Some(return_type),
        rex_node: Some(RexNode::FuncCall(function_call)),
    }
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
