use std::collections::HashMap;

use itertools::Itertools as _;
use risingwave_common::types::DataType;

use crate::expr::{Expr as _, ExprImpl, ExprType, FunctionCall};

pub fn new_func(func_type: ExprType, inputs: Vec<ExprImpl>) -> Option<FunctionCall> {
    let f = FUNC_H_MAP.get(&func_type).unwrap_or(&(new_simple as H));
    f(func_type, inputs)
}

fn build_type_derive_map() -> HashMap<ExprType, (DataType, Vec<DataType>)> {
    use {DataType as T, ExprType as E};
    let mut m = HashMap::new();
    // logical expressions
    m.insert(E::And, (T::Boolean, vec![T::Boolean, T::Boolean]));
    m.insert(E::Or, (T::Boolean, vec![T::Boolean, T::Boolean]));
    m.insert(E::Not, (T::Boolean, vec![T::Boolean]));
    m.insert(E::IsTrue, (T::Boolean, vec![T::Boolean]));
    m.insert(E::IsNotTrue, (T::Boolean, vec![T::Boolean]));
    m.insert(E::IsFalse, (T::Boolean, vec![T::Boolean]));
    m.insert(E::IsNotFalse, (T::Boolean, vec![T::Boolean]));
    // arithmetic expressions
    m.insert(E::RoundDigit, (T::Decimal, vec![T::Decimal, T::Int32]));
    // string expressions
    m.insert(
        E::Replace,
        (T::Varchar, vec![T::Varchar, T::Varchar, T::Varchar]),
    );
    m.insert(
        E::Translate,
        (T::Varchar, vec![T::Varchar, T::Varchar, T::Varchar]),
    );
    m.insert(E::Upper, (T::Varchar, vec![T::Varchar]));
    m.insert(E::Lower, (T::Varchar, vec![T::Varchar]));
    m.insert(E::Trim, (T::Varchar, vec![T::Varchar]));
    m.insert(E::Ltrim, (T::Varchar, vec![T::Varchar]));
    m.insert(E::Rtrim, (T::Varchar, vec![T::Varchar]));
    m.insert(E::Position, (T::Int32, vec![T::Varchar, T::Varchar]));
    m.insert(E::Length, (T::Int32, vec![T::Varchar]));
    m.insert(E::Ascii, (T::Int32, vec![T::Varchar]));
    m.insert(E::Like, (T::Boolean, vec![T::Varchar, T::Varchar]));
    m
}

fn build_fh_map() -> HashMap<ExprType, H> {
    let mut m = HashMap::<_, H>::new();
    // comparison
    m.insert(ExprType::IsNull, new_cmp_unary);
    m.insert(ExprType::IsNotNull, new_cmp_unary);
    m.insert(ExprType::Equal, new_cmp);
    m.insert(ExprType::NotEqual, new_cmp);
    m.insert(ExprType::GreaterThan, new_cmp);
    m.insert(ExprType::GreaterThanOrEqual, new_cmp);
    m.insert(ExprType::LessThan, new_cmp);
    m.insert(ExprType::LessThanOrEqual, new_cmp);
    // arithmetic
    m.insert(ExprType::Neg, new_neg);
    m.insert(ExprType::Add, new_add);
    m.insert(ExprType::Subtract, new_sub);
    m.insert(ExprType::Multiply, new_mul);
    m.insert(ExprType::Divide, new_div);
    m.insert(ExprType::Modulus, new_mod);
    // temporal
    m.insert(ExprType::Extract, new_extract);
    m
}

type H = fn(ExprType, Vec<ExprImpl>) -> Option<FunctionCall>;

pub fn new_simple(func_type: ExprType, inputs: Vec<ExprImpl>) -> Option<FunctionCall> {
    let (return_type, operand_types) = FUNC_SIG_MAP.get(&func_type)?;
    if inputs.len() != operand_types.len() {
        return None;
    }
    let args = inputs
        .into_iter()
        .zip_eq(operand_types)
        .map(|(e, t)| implicit_cast(e, t))
        .collect::<Option<_>>()?;
    Some(FunctionCall::new_with_return_type(
        func_type,
        args,
        return_type.clone(),
    ))
}

fn new_cmp_unary(func_type: ExprType, inputs: Vec<ExprImpl>) -> Option<FunctionCall> {
    let arg = as_unary(inputs)?;
    Some(FunctionCall::new_with_return_type(
        func_type,
        vec![arg],
        DataType::Boolean,
    ))
}

fn new_cmp(func_type: ExprType, inputs: Vec<ExprImpl>) -> Option<FunctionCall> {
    let (lhs, rhs) = as_binary(inputs)?;
    crate::binder::Binder::find_compat(lhs.return_type(), rhs.return_type()).ok()?;
    Some(FunctionCall::new_with_return_type(
        func_type,
        vec![lhs, rhs],
        DataType::Boolean,
    ))
}

fn new_neg(func_type: ExprType, inputs: Vec<ExprImpl>) -> Option<FunctionCall> {
    let arg = as_unary(inputs)?;
    let t = arg.return_type();
    if !(t.is_numeric() || t == DataType::Interval) {
        return None;
    }
    Some(FunctionCall::new_with_return_type(func_type, vec![arg], t))
}

fn new_add(func_type: ExprType, inputs: Vec<ExprImpl>) -> Option<FunctionCall> {
    let (lhs, rhs) = as_binary(inputs)?;
    let (lt, rt) = (lhs.return_type(), rhs.return_type());
    if lt.is_numeric() && rt.is_numeric() {
        return new_num_atm(func_type, lhs, rhs);
    }
    if lt.is_date_or_timestamp() && rt == DataType::Interval {
        return Some(FunctionCall::new_with_return_type(
            func_type,
            vec![lhs, rhs],
            DataType::Timestamp,
        ));
    }
    if rt.is_date_or_timestamp() && lt == DataType::Interval {
        return Some(FunctionCall::new_with_return_type(
            func_type,
            vec![lhs, rhs],
            DataType::Timestamp,
        ));
    }
    None
}

fn new_sub(func_type: ExprType, inputs: Vec<ExprImpl>) -> Option<FunctionCall> {
    let (lhs, rhs) = as_binary(inputs)?;
    let (lt, rt) = (lhs.return_type(), rhs.return_type());
    if lt.is_numeric() && rt.is_numeric() {
        return new_num_atm(func_type, lhs, rhs);
    }
    if lt.is_date_or_timestamp() && rt == DataType::Interval {
        return Some(FunctionCall::new_with_return_type(
            func_type,
            vec![lhs, rhs],
            DataType::Timestamp,
        ));
    }
    None
}

fn new_mul(func_type: ExprType, inputs: Vec<ExprImpl>) -> Option<FunctionCall> {
    let (lhs, rhs) = as_binary(inputs)?;
    let (lt, rt) = (lhs.return_type(), rhs.return_type());
    if lt.is_numeric() && rt.is_numeric() {
        return new_num_atm(func_type, lhs, rhs);
    }
    None
}

fn new_div(func_type: ExprType, inputs: Vec<ExprImpl>) -> Option<FunctionCall> {
    let (lhs, rhs) = as_binary(inputs)?;
    let (lt, rt) = (lhs.return_type(), rhs.return_type());
    if lt.is_numeric() && rt.is_numeric() {
        return new_num_atm(func_type, lhs, rhs);
    }
    None
}

fn new_mod(func_type: ExprType, inputs: Vec<ExprImpl>) -> Option<FunctionCall> {
    let (lhs, rhs) = as_binary(inputs)?;
    let (lt, rt) = (lhs.return_type(), rhs.return_type());
    if lt.is_numeric() && rt.is_numeric() {
        return new_num_atm(func_type, lhs, rhs);
    }
    None
}

fn new_extract(func_type: ExprType, inputs: Vec<ExprImpl>) -> Option<FunctionCall> {
    let (lhs, rhs) = as_binary(inputs)?;
    let (lt, rt) = (lhs.return_type(), rhs.return_type());
    if lt.is_string() && rt.is_date_or_timestamp() {
        return Some(FunctionCall::new_with_return_type(
            func_type,
            vec![lhs, rhs],
            DataType::Decimal,
        ));
    }
    None
}

lazy_static::lazy_static! {
    static ref FUNC_SIG_MAP: HashMap<ExprType, (DataType, Vec<DataType>)> = {
        build_type_derive_map()
    };
}

fn implicit_cast(e: ExprImpl, t: &DataType) -> Option<ExprImpl> {
    let tt = e.return_type();
    if tt == *t {
        return Some(e);
    }
    if tt.is_string() || t.is_string() {
        return None;
    }
    Some(e.ensure_type(t.clone()))
}

lazy_static::lazy_static! {
    static ref FUNC_H_MAP: HashMap<ExprType, H> = {
        build_fh_map()
    };
}

fn new_num_atm(func_type: ExprType, lhs: ExprImpl, rhs: ExprImpl) -> Option<FunctionCall> {
    let return_type =
        crate::binder::Binder::find_compat(lhs.return_type(), rhs.return_type()).ok()?;
    Some(FunctionCall::new_with_return_type(
        func_type,
        vec![lhs, rhs],
        return_type,
    ))
}

fn as_unary(inputs: Vec<ExprImpl>) -> Option<ExprImpl> {
    let mut iter = inputs.into_iter().fuse();
    let arg = iter.next()?;
    if iter.next().is_some() {
        return None;
    }
    Some(arg)
}

fn as_binary(inputs: Vec<ExprImpl>) -> Option<(ExprImpl, ExprImpl)> {
    let mut iter = inputs.into_iter().fuse();
    let lhs = iter.next()?;
    let rhs = iter.next()?;
    if iter.next().is_some() {
        return None;
    }
    Some((lhs, rhs))
}
