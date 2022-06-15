use std::collections::HashMap;

use itertools::iproduct;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::DataType;

use super::{name_of, DataTypeName};
use crate::expr::ExprType;

/// Infers the return type of a function. Returns `Err` if the function with specified data types
/// is not supported on backend.
pub fn infer_type(func_type: ExprType, inputs_type: Vec<DataType>) -> Result<DataType> {
    // With our current simplified type system, where all types are nullable and not parameterized
    // by things like length or precision, the inference can be done with a map lookup.
    let input_type_names = inputs_type.iter().map(name_of).collect();
    infer_type_name(func_type, input_type_names).map(|type_name| match type_name {
        DataTypeName::Boolean => DataType::Boolean,
        DataTypeName::Int16 => DataType::Int16,
        DataTypeName::Int32 => DataType::Int32,
        DataTypeName::Int64 => DataType::Int64,
        DataTypeName::Decimal => DataType::Decimal,
        DataTypeName::Float32 => DataType::Float32,
        DataTypeName::Float64 => DataType::Float64,
        DataTypeName::Varchar => DataType::Varchar,
        DataTypeName::Date => DataType::Date,
        DataTypeName::Timestamp => DataType::Timestamp,
        DataTypeName::Timestampz => DataType::Timestampz,
        DataTypeName::Time => DataType::Time,
        DataTypeName::Interval => DataType::Interval,
        DataTypeName::Struct | DataTypeName::List => {
            panic!("Functions returning struct or list can not be inferred. Please use `FunctionCall::new_unchecked`.")
        }
    })
}

/// Infer the return type name without parameters like length or precision.
fn infer_type_name(func_type: ExprType, inputs_type: Vec<DataTypeName>) -> Result<DataTypeName> {
    FUNC_SIG_MAP
        .get(&FuncSign::new(func_type, inputs_type.clone()))
        .cloned()
        .ok_or_else(|| {
            ErrorCode::NotImplemented(format!("{:?}{:?}", func_type, inputs_type), 112.into())
                .into()
        })
}

#[derive(PartialEq, Hash)]
struct FuncSign {
    func: ExprType,
    inputs_type: Vec<DataTypeName>,
}

impl Eq for FuncSign {}

impl FuncSign {
    pub fn new(func: ExprType, inputs_type: Vec<DataTypeName>) -> Self {
        FuncSign { func, inputs_type }
    }
}

fn build_binary_cmp_funcs(
    map: &mut HashMap<FuncSign, DataTypeName>,
    exprs: &[ExprType],
    args: &[DataTypeName],
) {
    for (e, lt, rt) in iproduct!(exprs, args, args) {
        map.insert(FuncSign::new(*e, vec![*lt, *rt]), DataTypeName::Boolean);
    }
}

fn build_binary_atm_funcs(
    map: &mut HashMap<FuncSign, DataTypeName>,
    exprs: &[ExprType],
    args: &[DataTypeName],
) {
    for e in exprs {
        for (li, lt) in args.iter().enumerate() {
            for (ri, rt) in args.iter().enumerate() {
                let ret = if li <= ri { rt } else { lt };
                map.insert(FuncSign::new(*e, vec![*lt, *rt]), *ret);
            }
        }
    }
}

fn build_unary_atm_funcs(
    map: &mut HashMap<FuncSign, DataTypeName>,
    exprs: &[ExprType],
    args: &[DataTypeName],
) {
    for (e, arg) in iproduct!(exprs, args) {
        map.insert(FuncSign::new(*e, vec![*arg]), *arg);
    }
}

fn build_commutative_funcs(
    map: &mut HashMap<FuncSign, DataTypeName>,
    expr: ExprType,
    arg0: DataTypeName,
    arg1: DataTypeName,
    ret: DataTypeName,
) {
    map.insert(FuncSign::new(expr, vec![arg0, arg1]), ret);
    map.insert(FuncSign::new(expr, vec![arg1, arg0]), ret);
}

fn build_round_funcs(map: &mut HashMap<FuncSign, DataTypeName>, expr: ExprType) {
    map.insert(
        FuncSign::new(expr, vec![DataTypeName::Float64]),
        DataTypeName::Float64,
    );
    map.insert(
        FuncSign::new(expr, vec![DataTypeName::Decimal]),
        DataTypeName::Decimal,
    );
}

/// This function builds type derived map for all built-in functions that take a fixed number
/// of arguments.  They can be determined to have one or more type signatures since some are
/// compatible with more than one type.
/// Type signatures and arities of variadic functions are checked
/// [elsewhere](crate::expr::FunctionCall::new).
fn build_type_derive_map() -> HashMap<FuncSign, DataTypeName> {
    use {DataTypeName as T, ExprType as E};
    let mut map = HashMap::new();
    let all_types = [
        T::Boolean,
        T::Int16,
        T::Int32,
        T::Int64,
        T::Decimal,
        T::Float32,
        T::Float64,
        T::Varchar,
        T::Date,
        T::Timestamp,
        T::Timestampz,
        T::Time,
        T::Interval,
    ];
    let num_types = [
        T::Int16,
        T::Int32,
        T::Int64,
        T::Decimal,
        T::Float32,
        T::Float64,
    ];

    // logical expressions
    for e in [E::Not, E::IsTrue, E::IsNotTrue, E::IsFalse, E::IsNotFalse] {
        map.insert(FuncSign::new(e, vec![T::Boolean]), T::Boolean);
    }
    for e in [E::And, E::Or] {
        map.insert(FuncSign::new(e, vec![T::Boolean, T::Boolean]), T::Boolean);
    }
    map.insert(FuncSign::new(E::BoolOut, vec![T::Boolean]), T::Varchar);

    // comparison expressions
    for e in [E::IsNull, E::IsNotNull] {
        for t in all_types {
            map.insert(FuncSign::new(e, vec![t]), T::Boolean);
        }
    }
    let cmp_exprs = &[
        E::Equal,
        E::NotEqual,
        E::LessThan,
        E::LessThanOrEqual,
        E::GreaterThan,
        E::GreaterThanOrEqual,
        E::IsDistinctFrom,
    ];
    build_binary_cmp_funcs(&mut map, cmp_exprs, &num_types);
    build_binary_cmp_funcs(&mut map, cmp_exprs, &[T::Struct, T::List]);
    build_binary_cmp_funcs(&mut map, cmp_exprs, &[T::Date, T::Timestamp, T::Timestampz]);
    build_binary_cmp_funcs(&mut map, cmp_exprs, &[T::Time, T::Interval]);
    for e in cmp_exprs {
        for t in [T::Boolean, T::Varchar] {
            map.insert(FuncSign::new(*e, vec![t, t]), T::Boolean);
        }
    }

    let unary_atm_exprs = &[E::Abs, E::Neg];

    build_unary_atm_funcs(&mut map, unary_atm_exprs, &num_types);
    build_binary_atm_funcs(
        &mut map,
        &[E::Add, E::Subtract, E::Multiply, E::Divide],
        &num_types,
    );
    build_binary_atm_funcs(
        &mut map,
        &[E::Modulus],
        &[T::Int16, T::Int32, T::Int64, T::Decimal],
    );
    map.insert(
        FuncSign::new(E::RoundDigit, vec![T::Decimal, T::Int32]),
        T::Decimal,
    );

    // build bitwise operator
    // bitwise operator
    let integral_types = [T::Int16, T::Int32, T::Int64]; // reusable for and/or/xor/not

    build_binary_atm_funcs(
        &mut map,
        &[E::BitwiseAnd, E::BitwiseOr, E::BitwiseXor],
        &integral_types,
    );

    // Shift Operator is not using `build_binary_atm_funcs` because
    // allowed rhs is different from allowed lhs
    // return type is lhs rather than larger of the two
    for (e, lt, rt) in iproduct!(
        &[E::BitwiseShiftLeft, E::BitwiseShiftRight],
        &integral_types,
        &[T::Int16, T::Int32]
    ) {
        map.insert(FuncSign::new(*e, vec![*lt, *rt]), *lt);
    }

    build_unary_atm_funcs(&mut map, &[E::BitwiseNot], &[T::Int16, T::Int32, T::Int64]);

    build_round_funcs(&mut map, E::Round);
    build_round_funcs(&mut map, E::Ceil);
    build_round_funcs(&mut map, E::Floor);

    // temporal expressions
    for (base, delta) in [
        (T::Date, T::Int32),
        (T::Timestamp, T::Interval),
        (T::Timestampz, T::Interval),
        (T::Time, T::Interval),
        (T::Interval, T::Interval),
    ] {
        build_commutative_funcs(&mut map, E::Add, base, delta, base);
        map.insert(FuncSign::new(E::Subtract, vec![base, delta]), base);
        map.insert(FuncSign::new(E::Subtract, vec![base, base]), delta);
    }

    // date + interval = timestamp, date - interval = timestamp
    build_commutative_funcs(&mut map, E::Add, T::Date, T::Interval, T::Timestamp);
    map.insert(
        FuncSign::new(E::Subtract, vec![T::Date, T::Interval]),
        T::Timestamp,
    );
    // date + time = timestamp
    build_commutative_funcs(&mut map, E::Add, T::Date, T::Time, T::Timestamp);
    // interval * float8 = interval, interval / float8 = interval
    for t in num_types {
        build_commutative_funcs(&mut map, E::Multiply, T::Interval, t, T::Interval);
        map.insert(FuncSign::new(E::Divide, vec![T::Interval, t]), T::Interval);
    }

    for t in [T::Timestamp, T::Time, T::Date] {
        map.insert(FuncSign::new(E::Extract, vec![T::Varchar, t]), T::Decimal);
    }
    for t in [T::Timestamp, T::Date] {
        map.insert(
            FuncSign::new(E::TumbleStart, vec![t, T::Interval]),
            T::Timestamp,
        );
    }

    // string expressions
    for e in [E::Trim, E::Ltrim, E::Rtrim, E::Lower, E::Upper, E::Md5] {
        map.insert(FuncSign::new(e, vec![T::Varchar]), T::Varchar);
    }
    for e in [E::Trim, E::Ltrim, E::Rtrim] {
        map.insert(FuncSign::new(e, vec![T::Varchar, T::Varchar]), T::Varchar);
    }
    for e in [E::Repeat, E::Substr] {
        map.insert(FuncSign::new(e, vec![T::Varchar, T::Int32]), T::Varchar);
    }
    map.insert(
        FuncSign::new(E::Substr, vec![T::Varchar, T::Int32, T::Int32]),
        T::Varchar,
    );
    for e in [E::Replace, E::Translate] {
        map.insert(
            FuncSign::new(e, vec![T::Varchar, T::Varchar, T::Varchar]),
            T::Varchar,
        );
    }
    for e in [E::Length, E::Ascii, E::CharLength] {
        map.insert(FuncSign::new(e, vec![T::Varchar]), T::Int32);
    }
    map.insert(
        FuncSign::new(E::Position, vec![T::Varchar, T::Varchar]),
        T::Int32,
    );
    map.insert(
        FuncSign::new(E::Like, vec![T::Varchar, T::Varchar]),
        T::Boolean,
    );
    map.insert(
        FuncSign::new(E::SplitPart, vec![T::Varchar, T::Varchar, T::Int32]),
        T::Varchar,
    );
    // TODO: Support more `to_char` types.
    map.insert(
        FuncSign::new(E::ToChar, vec![T::Timestamp, T::Varchar]),
        T::Varchar,
    );

    map
}

lazy_static::lazy_static! {
    static ref FUNC_SIG_MAP: HashMap<FuncSign, DataTypeName> = {
        build_type_derive_map()
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_simple_infer_type(
        func_type: ExprType,
        inputs_type: Vec<DataType>,
        expected_type_name: DataType,
    ) {
        let ret = infer_type(func_type, inputs_type).unwrap();
        assert_eq!(ret, expected_type_name);
    }

    fn test_infer_type_not_exist(func_type: ExprType, inputs_type: Vec<DataType>) {
        let ret = infer_type(func_type, inputs_type);
        assert!(ret.is_err());
    }

    #[test]
    fn test_arithmetics() {
        use DataType::*;
        let atm_exprs = vec![
            ExprType::Add,
            ExprType::Subtract,
            ExprType::Multiply,
            ExprType::Divide,
        ];
        let num_promote_table = vec![
            (Int16, Int16, Int16),
            (Int16, Int32, Int32),
            (Int16, Int64, Int64),
            (Int16, Decimal, Decimal),
            (Int16, Float32, Float32),
            (Int16, Float64, Float64),
            (Int32, Int16, Int32),
            (Int32, Int32, Int32),
            (Int32, Int64, Int64),
            (Int32, Decimal, Decimal),
            (Int32, Float32, Float32),
            (Int32, Float64, Float64),
            (Int64, Int16, Int64),
            (Int64, Int32, Int64),
            (Int64, Int64, Int64),
            (Int64, Decimal, Decimal),
            (Int64, Float32, Float32),
            (Int64, Float64, Float64),
            (Decimal, Int16, Decimal),
            (Decimal, Int32, Decimal),
            (Decimal, Int64, Decimal),
            (Decimal, Decimal, Decimal),
            (Decimal, Float32, Float32),
            (Decimal, Float64, Float64),
            (Float32, Int16, Float32),
            (Float32, Int32, Float32),
            (Float32, Int64, Float32),
            (Float32, Decimal, Float32),
            (Float32, Float32, Float32),
            (Float32, Float64, Float64),
            (Float64, Int16, Float64),
            (Float64, Int32, Float64),
            (Float64, Int64, Float64),
            (Float64, Decimal, Float64),
            (Float64, Float32, Float64),
            (Float64, Float64, Float64),
        ];
        for (expr, (t1, t2, tr)) in iproduct!(atm_exprs, num_promote_table) {
            test_simple_infer_type(expr, vec![t1, t2], tr);
        }
    }

    #[test]
    fn test_bitwise() {
        use DataType::*;
        let bitwise_exprs = vec![
            ExprType::BitwiseAnd,
            ExprType::BitwiseOr,
            ExprType::BitwiseXor,
        ];
        let num_promote_table = vec![
            (Int16, Int16, Int16),
            (Int16, Int32, Int32),
            (Int16, Int64, Int64),
            (Int32, Int16, Int32),
            (Int32, Int32, Int32),
            (Int32, Int64, Int64),
            (Int64, Int16, Int64),
            (Int64, Int32, Int64),
            (Int64, Int64, Int64),
        ];
        for (expr, (t1, t2, tr)) in iproduct!(bitwise_exprs, num_promote_table) {
            test_simple_infer_type(expr, vec![t1, t2], tr);
        }

        for (expr, (t1, t2, tr)) in iproduct!(
            vec![ExprType::BitwiseShiftLeft, ExprType::BitwiseShiftRight,],
            vec![
                (Int16, Int16, Int16),
                (Int32, Int16, Int32),
                (Int64, Int16, Int64),
                (Int16, Int32, Int16),
                (Int64, Int32, Int64),
                (Int32, Int32, Int32),
            ]
        ) {
            test_simple_infer_type(expr, vec![t1, t2], tr);
        }
    }
    #[test]
    fn test_bool_num_not_exist() {
        let exprs = vec![
            ExprType::Add,
            ExprType::Subtract,
            ExprType::Multiply,
            ExprType::Divide,
            ExprType::Modulus,
            ExprType::Equal,
            ExprType::NotEqual,
            ExprType::LessThan,
            ExprType::LessThanOrEqual,
            ExprType::GreaterThan,
            ExprType::GreaterThanOrEqual,
            ExprType::And,
            ExprType::Or,
            ExprType::Not,
        ];
        let num_types = vec![
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::Float32,
            DataType::Float64,
            DataType::Decimal,
        ];

        for (expr, num_t) in iproduct!(exprs, num_types) {
            test_infer_type_not_exist(expr, vec![num_t, DataType::Boolean]);
        }
    }
}
