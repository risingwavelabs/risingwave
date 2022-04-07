use std::collections::HashMap;

use itertools::Itertools as _;
use risingwave_common::types::DataType;

use crate::expr::{Expr as _, ExprImpl, ExprType, FunctionCall};

lazy_static::lazy_static! {
    static ref FUNC_SIG_MAP: HashMap<ExprType, (DataType, Vec<DataType>)> = {
        build_type_derive_map()
    };
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

pub fn new_simple(func_type: ExprType, inputs: Vec<ExprImpl>) -> Option<FunctionCall> {
    let (return_type, operand_types) = FUNC_SIG_MAP.get(&func_type)?;
    let args = inputs
        .into_iter()
        .zip_eq(operand_types)
        .map(|(e, t)| e.ensure_type(t.clone()))
        .collect();
    Some(FunctionCall::new_with_return_type(
        func_type,
        args,
        return_type.clone(),
    ))
}

type H = fn(ExprType, Vec<ExprImpl>) -> Option<FunctionCall>;
lazy_static::lazy_static! {
    static ref FUNC_H_MAP: HashMap<ExprType, H> = {
        build_fh_map()
    };
}

fn build_fh_map() -> HashMap<ExprType, H> {
    let mut m = HashMap::<_, H>::new();
    m.insert(ExprType::Equal, new_cmp);
    m.insert(ExprType::NotEqual, new_cmp);
    m.insert(ExprType::GreaterThan, new_cmp);
    m.insert(ExprType::GreaterThanOrEqual, new_cmp);
    m.insert(ExprType::LessThan, new_cmp);
    m.insert(ExprType::LessThanOrEqual, new_cmp);
    m.insert(ExprType::Add, new_add);
    m.insert(ExprType::Subtract, new_sub);
    m
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

fn new_add(func_type: ExprType, inputs: Vec<ExprImpl>) -> Option<FunctionCall> {
    let (lhs, rhs) = as_binary(inputs)?;
    let (lt, rt) = (lhs.return_type(), rhs.return_type());
    if lt.is_numeric() && rt.is_numeric() {
        return new_num_atm(func_type, lhs, rhs);
    }
    None
}

fn new_sub(func_type: ExprType, inputs: Vec<ExprImpl>) -> Option<FunctionCall> {
    let (lhs, rhs) = as_binary(inputs)?;
    let (lt, rt) = (lhs.return_type(), rhs.return_type());
    if lt.is_numeric() && rt.is_numeric() {
        return new_num_atm(func_type, lhs, rhs);
    }
    None
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

fn as_binary(inputs: Vec<ExprImpl>) -> Option<(ExprImpl, ExprImpl)> {
    let mut iter = inputs.into_iter().fuse();
    let lhs = iter.next()?;
    let rhs = iter.next()?;
    Some((lhs, rhs))
}

pub fn new_func(func_type: ExprType, inputs: Vec<ExprImpl>) -> Option<FunctionCall> {
    let f = FUNC_H_MAP.get(&func_type).unwrap_or(&(new_simple as H));
    f(func_type, inputs)
}
