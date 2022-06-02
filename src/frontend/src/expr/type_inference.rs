// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! This type inference is just to infer the return type of function calls, and make sure the
//! functionCall expressions have same input type requirement and return type definition as backend.
use std::collections::HashMap;
use std::vec;

use itertools::{iproduct, Itertools};
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::DataType;

use crate::expr::{Expr as _, ExprImpl, ExprType};

/// `DataTypeName` is designed for type derivation here. In other scenarios,
/// use `DataType` instead.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Copy)]
enum DataTypeName {
    Boolean,
    Int16,
    Int32,
    Int64,
    Decimal,
    Float32,
    Float64,
    Varchar,
    Date,
    Timestamp,
    Timestampz,
    Time,
    Interval,
    Struct,
    List,
}

fn name_of(ty: &DataType) -> DataTypeName {
    match ty {
        DataType::Boolean => DataTypeName::Boolean,
        DataType::Int16 => DataTypeName::Int16,
        DataType::Int32 => DataTypeName::Int32,
        DataType::Int64 => DataTypeName::Int64,
        DataType::Decimal => DataTypeName::Decimal,
        DataType::Float32 => DataTypeName::Float32,
        DataType::Float64 => DataTypeName::Float64,
        DataType::Varchar => DataTypeName::Varchar,
        DataType::Date => DataTypeName::Date,
        DataType::Timestamp => DataTypeName::Timestamp,
        DataType::Timestampz => DataTypeName::Timestampz,
        DataType::Time => DataTypeName::Time,
        DataType::Interval => DataTypeName::Interval,
        DataType::Struct { .. } => DataTypeName::Struct,
        DataType::List { .. } => DataTypeName::List,
    }
}

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

#[allow(dead_code)]
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

// Same as build_binary_atm_funcs except the RHS are limited to Int16 and Int32
fn build_binary_shift_funcs(
    map: &mut HashMap<FuncSign, DataTypeName>,
    exprs: &[ExprType],
    argsl: &[DataTypeName],
    argsr: &[DataTypeName],
) {
    for e in exprs {
        for (_li, lt) in argsl.iter().enumerate() {
            for (_ri, rt) in argsr.iter().enumerate() {
                map.insert(FuncSign::new(*e, vec![*lt, *rt]), *lt);
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
    build_binary_atm_funcs(
        &mut map,
        &[E::BitwiseAnd, E::BitwiseOr, E::BitwiseXor],
        &[T::Int16, T::Int32, T::Int64],
    );

    build_binary_shift_funcs(
        &mut map,
        &[E::BitwiseShiftLeft, E::BitwiseShiftRight],
        &[T::Int16, T::Int32, T::Int64],
        &[T::Int16, T::Int32],
    );

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
    map.insert(
        FuncSign::new(E::Substr, vec![T::Varchar, T::Int32]),
        T::Varchar,
    );
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
    for e in [E::Length, E::Ascii] {
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

/// Find the least restrictive type. Used by `VALUES`, `CASE`, `UNION`, etc.
/// It is a simplified version of the rule used in
/// [PG](https://www.postgresql.org/docs/current/typeconv-union-case.html).
///
/// If you also need to cast them to this type, and there are more than 2 exprs, check out
/// [`align_types`].
pub fn least_restrictive(lhs: DataType, rhs: DataType) -> Result<DataType> {
    if lhs == rhs {
        Ok(lhs)
    } else if cast_ok(&lhs, &rhs, &CastContext::Implicit) {
        Ok(rhs)
    } else if cast_ok(&rhs, &lhs, &CastContext::Implicit) {
        Ok(lhs)
    } else {
        Err(ErrorCode::BindError(format!("types {:?} and {:?} cannot be matched", lhs, rhs)).into())
    }
}

/// Find the `least_restrictive` type over a list of `exprs`, and add implicit cast when necessary.
/// Used by `VALUES`, `CASE`, `UNION`, etc. See [PG](https://www.postgresql.org/docs/current/typeconv-union-case.html).
pub fn align_types<'a>(exprs: impl Iterator<Item = &'a mut ExprImpl>) -> Result<DataType> {
    use std::mem::swap;

    let exprs = exprs.collect_vec();
    // Essentially a filter_map followed by a try_reduce, which is unstable.
    let mut ret_type = None;
    for e in &exprs {
        if e.is_null() {
            continue;
        }
        ret_type = match ret_type {
            None => Some(e.return_type()),
            Some(t) => Some(least_restrictive(t, e.return_type())?),
        };
    }
    let ret_type = ret_type.unwrap_or(DataType::Varchar);
    for e in exprs {
        let mut dummy = ExprImpl::literal_bool(false);
        swap(&mut dummy, e);
        *e = dummy.cast_implicit(ret_type.clone())?;
    }
    Ok(ret_type)
}

/// The context a cast operation is invoked in. An implicit cast operation is allowed in a context
/// that allows explicit casts, but not vice versa. See details in
/// [PG](https://www.postgresql.org/docs/current/catalog-pg-cast.html).
#[derive(Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum CastContext {
    Implicit,
    Assign,
    Explicit,
}

/// Checks whether casting from `source` to `target` is ok in `allows` context.
pub fn cast_ok(source: &DataType, target: &DataType, allows: &CastContext) -> bool {
    let k = (name_of(source), name_of(target));
    matches!(CAST_MAP.get(&k), Some(context) if context <= allows)
}

fn build_cast_map() -> HashMap<(DataTypeName, DataTypeName), CastContext> {
    use DataTypeName as T;

    // Implicit cast operations in PG are organized in 3 sequences, with the reverse direction being
    // assign cast operations.
    // https://github.com/postgres/postgres/blob/e0064f0ff6dfada2695330c6bc1945fa7ae813be/src/include/catalog/pg_cast.dat#L18-L20
    let mut m = HashMap::new();
    insert_cast_seq(
        &mut m,
        &[
            T::Int16,
            T::Int32,
            T::Int64,
            T::Decimal,
            T::Float32,
            T::Float64,
        ],
    );
    insert_cast_seq(&mut m, &[T::Date, T::Timestamp, T::Timestampz]);
    insert_cast_seq(&mut m, &[T::Time, T::Interval]);
    // Allow explicit cast operation between the same type, for types not included above.
    // Ideally we should remove all such useless casts. But for now we just forbid them in contexts
    // that only allow implicit or assign cast operations, and the user can still write them
    // explicitly.
    //
    // Note this is different in PG, where same type cast is used for sizing (e.g. `NUMERIC(18,3)`
    // to `NUMERIC(20,4)`). Sizing casts are only available for `numeric`, `timestamp`,
    // `timestamptz`, `time`, `interval` and these are implicit. https://www.postgresql.org/docs/current/typeconv-query.html
    //
    // As we do not support size parameters in types, there are no sizing casts.
    m.insert((T::Boolean, T::Boolean), CastContext::Explicit);
    m.insert((T::Varchar, T::Varchar), CastContext::Explicit);

    // Casting to and from string type.
    for t in [
        T::Boolean,
        T::Int16,
        T::Int32,
        T::Int64,
        T::Decimal,
        T::Float32,
        T::Float64,
        T::Date,
        T::Timestamp,
        T::Timestampz,
        T::Time,
        T::Interval,
    ] {
        m.insert((t, T::Varchar), CastContext::Assign);
        // Casting from string is explicit-only in PG.
        // But as we bind string literals to `varchar` rather than `unknown`, allowing them in
        //  assign context enables this shorter statement:
        // `insert into t values ('2022-01-01')`
        // If it was explicit:
        // `insert into t values ('2022-01-01'::date)`
        // `insert into t values (date '2022-01-01')`
        m.insert((T::Varchar, t), CastContext::Assign);
    }

    // Misc casts allowed by PG that are neither in implicit cast sequences nor from/to string.
    m.insert((T::Timestamp, T::Time), CastContext::Assign);
    m.insert((T::Timestampz, T::Time), CastContext::Assign);
    m.insert((T::Boolean, T::Int32), CastContext::Explicit);
    m.insert((T::Int32, T::Boolean), CastContext::Explicit);
    m
}

fn insert_cast_seq(
    m: &mut HashMap<(DataTypeName, DataTypeName), CastContext>,
    types: &[DataTypeName],
) {
    for (source_index, source_type) in types.iter().enumerate() {
        for (target_index, target_type) in types.iter().enumerate() {
            let cast_context = match source_index.cmp(&target_index) {
                std::cmp::Ordering::Less => CastContext::Implicit,
                // See comments in `build_cast_map` for why same type cast is marked as explicit.
                std::cmp::Ordering::Equal => CastContext::Explicit,
                std::cmp::Ordering::Greater => CastContext::Assign,
            };
            m.insert((*source_type, *target_type), cast_context);
        }
    }
}

lazy_static::lazy_static! {
    static ref CAST_MAP: HashMap<(DataTypeName, DataTypeName), CastContext> = {
        build_cast_map()
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

    fn gen_cast_table(allows: CastContext) -> Vec<String> {
        use itertools::Itertools as _;
        use DataType as T;
        let all_types = &[
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
        all_types
            .iter()
            .map(|source| {
                all_types
                    .iter()
                    .map(|target| match cast_ok(source, target, &allows) {
                        false => ' ',
                        true => 'T',
                    })
                    .collect::<String>()
            })
            .collect_vec()
    }

    #[test]
    fn test_cast_ok() {
        // With the help of a script we can obtain the 3 expected cast tables from PG. They are
        // slightly modified on same-type cast and from-string cast for reasons explained above in
        // `build_cast_map`.

        let actual = gen_cast_table(CastContext::Implicit);
        assert_eq!(
            actual,
            vec![
                "             ", // bool
                "  TTTTT      ",
                "   TTTT      ",
                "    TTT      ",
                "     TT      ",
                "      T      ",
                "             ",
                "             ", // varchar
                "         TT  ",
                "          T  ",
                "             ",
                "            T",
                "             ",
            ]
        );
        let actual = gen_cast_table(CastContext::Assign);
        assert_eq!(
            actual,
            vec![
                "       T     ", // bool
                "  TTTTTT     ",
                " T TTTTT     ",
                " TT TTTT     ",
                " TTT TTT     ",
                " TTTT TT     ",
                " TTTTT T     ",
                "TTTTTTT TTTTT", // varchar
                "       T TT  ",
                "       TT TT ",
                "       TTT T ",
                "       T    T",
                "       T   T ",
            ]
        );
        let actual = gen_cast_table(CastContext::Explicit);
        assert_eq!(
            actual,
            vec![
                "T T    T     ", // bool
                " TTTTTTT     ",
                "TTTTTTTT     ",
                " TTTTTTT     ",
                " TTTTTTT     ",
                " TTTTTTT     ",
                " TTTTTTT     ",
                "TTTTTTTTTTTTT", // varchar
                "       TTTT  ",
                "       TTTTT ",
                "       TTTTT ",
                "       T   TT",
                "       T   TT",
            ]
        );
    }
}
