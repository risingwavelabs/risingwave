use std::collections::HashMap;

use itertools::Itertools as _;
use risingwave_common::types::DataType;

use crate::expr::{ExprImpl, ExprType, FunctionCall};

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

pub fn new_func(func_type: ExprType, inputs: Vec<ExprImpl>) -> Option<FunctionCall> {
    if let Some((return_type, operand_types)) = FUNC_SIG_MAP.get(&func_type) {
        let args = inputs
            .into_iter()
            .zip_eq(operand_types)
            .map(|(e, t)| e.ensure_type(t.clone()))
            .collect();
        return Some(FunctionCall::new_with_return_type(
            func_type,
            args,
            return_type.clone(),
        ));
    }
    None
}
