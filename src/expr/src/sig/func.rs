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

//! Function signatures.

use std::collections::HashMap;
use std::ops::Deref;
use std::sync::LazyLock;

use itertools::iproduct;
use risingwave_common::types::DataTypeName;
use risingwave_pb::expr::expr_node::Type as ExprType;

pub static FUNC_SIG_MAP: LazyLock<FuncSigMap> = LazyLock::new(build_type_derive_map);

/// The table of function signatures.
pub fn func_sigs() -> impl Iterator<Item = &'static FuncSign> {
    FUNC_SIG_MAP.0.values().flatten()
}

/// A function signature.
#[derive(PartialEq, Eq, Hash, Clone, Debug)]
pub struct FuncSign {
    pub func: ExprType,
    pub inputs_type: Vec<DataTypeName>,
    pub ret_type: DataTypeName,
}

impl FuncSign {
    /// Returns a string describing the function without return type.
    pub fn to_string_no_return(&self) -> String {
        format!(
            "{}({})",
            self.func.as_str_name(),
            self.inputs_type
                .iter()
                .map(|t| format!("{t:?}"))
                .collect::<Vec<_>>()
                .join(",")
        )
        .to_lowercase()
    }
}

#[derive(Default)]
pub struct FuncSigMap(HashMap<(ExprType, usize), Vec<FuncSign>>);

impl Deref for FuncSigMap {
    type Target = HashMap<(ExprType, usize), Vec<FuncSign>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl FuncSigMap {
    pub fn insert(
        &mut self,
        func: ExprType,
        param_types: Vec<DataTypeName>,
        ret_type: DataTypeName,
    ) {
        let arity = param_types.len();
        let inputs_type = param_types.into_iter().map(Into::into).collect();
        let sig = FuncSign {
            func,
            inputs_type,
            ret_type,
        };
        self.0.entry((func, arity)).or_default().push(sig)
    }
}

/// This function builds type derived map for all built-in functions that take a fixed number
/// of arguments.  They can be determined to have one or more type signatures since some are
/// compatible with more than one type.
/// Type signatures and arities of variadic functions are checked
/// [elsewhere](crate::expr::FunctionCall::new).
fn build_type_derive_map() -> FuncSigMap {
    use {DataTypeName as T, ExprType as E};
    let mut map = FuncSigMap::default();
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
        T::Timestamptz,
        T::Time,
        T::Interval,
        T::Jsonb,
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
        map.insert(e, vec![T::Boolean], T::Boolean);
    }
    for e in [E::And, E::Or] {
        map.insert(e, vec![T::Boolean, T::Boolean], T::Boolean);
    }
    map.insert(E::BoolOut, vec![T::Boolean], T::Varchar);

    // comparison expressions
    for e in [E::IsNull, E::IsNotNull] {
        for t in all_types {
            map.insert(e, vec![t], T::Boolean);
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
        E::IsNotDistinctFrom,
    ];
    build_binary_cmp_funcs(&mut map, cmp_exprs, &num_types);
    build_binary_cmp_funcs(&mut map, cmp_exprs, &[T::Struct]);
    build_binary_cmp_funcs(
        &mut map,
        cmp_exprs,
        &[T::Date, T::Timestamp, T::Timestamptz],
    );
    build_binary_cmp_funcs(&mut map, cmp_exprs, &[T::Time, T::Interval]);
    for e in cmp_exprs {
        for t in [T::Boolean, T::Varchar] {
            map.insert(*e, vec![t, t], T::Boolean);
        }
    }

    let unary_atm_exprs = &[E::Abs, E::Neg];

    build_unary_atm_funcs(&mut map, unary_atm_exprs, &num_types);
    build_binary_atm_funcs(
        &mut map,
        &[E::Add, E::Subtract, E::Multiply, E::Divide],
        &[T::Int16, T::Int32, T::Int64, T::Decimal],
    );
    build_binary_atm_funcs(
        &mut map,
        &[E::Add, E::Subtract, E::Multiply, E::Divide],
        &[T::Float32, T::Float64],
    );
    build_binary_atm_funcs(
        &mut map,
        &[E::Modulus],
        &[T::Int16, T::Int32, T::Int64, T::Decimal],
    );
    map.insert(E::RoundDigit, vec![T::Decimal, T::Int32], T::Decimal);
    map.insert(E::Pow, vec![T::Float64, T::Float64], T::Float64);
    map.insert(E::Exp, vec![T::Float64], T::Float64);

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
        map.insert(*e, vec![*lt, *rt], *lt);
    }

    build_unary_atm_funcs(&mut map, &[E::BitwiseNot], &[T::Int16, T::Int32, T::Int64]);

    build_round_funcs(&mut map, E::Round);
    build_round_funcs(&mut map, E::Ceil);
    build_round_funcs(&mut map, E::Floor);

    // temporal expressions
    for (base, delta) in [
        (T::Date, T::Int32),
        (T::Timestamp, T::Interval),
        (T::Timestamptz, T::Interval),
        (T::Time, T::Interval),
    ] {
        build_commutative_funcs(&mut map, E::Add, base, delta, base);
        map.insert(E::Subtract, vec![base, delta], base);
        map.insert(E::Subtract, vec![base, base], delta);
    }
    map.insert(E::Add, vec![T::Interval, T::Interval], T::Interval);
    map.insert(E::Subtract, vec![T::Interval, T::Interval], T::Interval);

    // date + interval = timestamp, date - interval = timestamp
    build_commutative_funcs(&mut map, E::Add, T::Date, T::Interval, T::Timestamp);
    map.insert(E::Subtract, vec![T::Date, T::Interval], T::Timestamp);
    // date + time = timestamp
    build_commutative_funcs(&mut map, E::Add, T::Date, T::Time, T::Timestamp);
    // interval * float8 = interval, interval / float8 = interval
    for t in num_types {
        build_commutative_funcs(&mut map, E::Multiply, T::Interval, t, T::Interval);
        map.insert(E::Divide, vec![T::Interval, t], T::Interval);
    }

    for t in [T::Timestamptz, T::Timestamp, T::Time, T::Date] {
        map.insert(E::Extract, vec![T::Varchar, t], T::Decimal);
    }
    for t in [T::Timestamp, T::Date] {
        map.insert(
            E::TumbleStart,
            vec![t, T::Interval, T::Interval, T::Interval],
            T::Timestamp,
        );
    }
    map.insert(
        E::TumbleStart,
        vec![T::Timestamptz, T::Interval, T::Interval],
        T::Timestamptz,
    );
    map.insert(E::ToTimestamp, vec![T::Float64], T::Timestamptz);
    map.insert(E::ToTimestamp1, vec![T::Varchar, T::Varchar], T::Timestamp);
    map.insert(
        E::AtTimeZone,
        vec![T::Timestamp, T::Varchar],
        T::Timestamptz,
    );
    map.insert(
        E::AtTimeZone,
        vec![T::Timestamptz, T::Varchar],
        T::Timestamp,
    );
    map.insert(E::DateTrunc, vec![T::Varchar, T::Timestamp], T::Timestamp);
    map.insert(
        E::DateTrunc,
        vec![T::Varchar, T::Timestamptz, T::Varchar],
        T::Timestamptz,
    );
    map.insert(E::DateTrunc, vec![T::Varchar, T::Interval], T::Interval);

    // string expressions
    for e in [E::Trim, E::Ltrim, E::Rtrim, E::Lower, E::Upper, E::Md5] {
        map.insert(e, vec![T::Varchar], T::Varchar);
    }
    for e in [E::Trim, E::Ltrim, E::Rtrim] {
        map.insert(e, vec![T::Varchar, T::Varchar], T::Varchar);
    }
    for e in [E::Repeat, E::Substr] {
        map.insert(e, vec![T::Varchar, T::Int32], T::Varchar);
    }
    map.insert(E::Substr, vec![T::Varchar, T::Int32, T::Int32], T::Varchar);
    for e in [E::Replace, E::Translate] {
        map.insert(e, vec![T::Varchar, T::Varchar, T::Varchar], T::Varchar);
    }
    map.insert(E::FormatType, vec![T::Int32, T::Int32], T::Varchar);
    map.insert(
        E::Overlay,
        vec![T::Varchar, T::Varchar, T::Int32],
        T::Varchar,
    );
    map.insert(
        E::Overlay,
        vec![T::Varchar, T::Varchar, T::Int32, T::Int32],
        T::Varchar,
    );
    for e in [
        E::Length,
        E::Ascii,
        E::CharLength,
        E::OctetLength,
        E::BitLength,
    ] {
        map.insert(e, vec![T::Varchar], T::Int32);
    }
    map.insert(E::Position, vec![T::Varchar, T::Varchar], T::Int32);
    map.insert(E::Like, vec![T::Varchar, T::Varchar], T::Boolean);
    map.insert(
        E::SplitPart,
        vec![T::Varchar, T::Varchar, T::Int32],
        T::Varchar,
    );
    // TODO: Support more `to_char` types.
    map.insert(E::ToChar, vec![T::Timestamp, T::Varchar], T::Varchar);
    // array_to_string
    map.insert(E::ArrayToString, vec![T::List, T::Varchar], T::Varchar);
    map.insert(
        E::ArrayToString,
        vec![T::List, T::Varchar, T::Varchar],
        T::Varchar,
    );

    map.insert(E::JsonbAccessInner, vec![T::Jsonb, T::Int32], T::Jsonb);
    map.insert(E::JsonbAccessInner, vec![T::Jsonb, T::Varchar], T::Jsonb);
    map.insert(E::JsonbAccessStr, vec![T::Jsonb, T::Int32], T::Varchar);
    map.insert(E::JsonbAccessStr, vec![T::Jsonb, T::Varchar], T::Varchar);
    map.insert(E::JsonbTypeof, vec![T::Jsonb], T::Varchar);
    map.insert(E::JsonbArrayLength, vec![T::Jsonb], T::Int32);

    map
}

fn build_binary_cmp_funcs(map: &mut FuncSigMap, exprs: &[ExprType], args: &[DataTypeName]) {
    for (e, lt, rt) in iproduct!(exprs, args, args) {
        map.insert(*e, vec![*lt, *rt], DataTypeName::Boolean);
    }
}

fn build_binary_atm_funcs(map: &mut FuncSigMap, exprs: &[ExprType], args: &[DataTypeName]) {
    for e in exprs {
        for (li, lt) in args.iter().enumerate() {
            for (ri, rt) in args.iter().enumerate() {
                let ret = if li <= ri { rt } else { lt };
                map.insert(*e, vec![*lt, *rt], *ret);
            }
        }
    }
}

fn build_unary_atm_funcs(map: &mut FuncSigMap, exprs: &[ExprType], args: &[DataTypeName]) {
    for (e, arg) in iproduct!(exprs, args) {
        map.insert(*e, vec![*arg], *arg);
    }
}

fn build_commutative_funcs(
    map: &mut FuncSigMap,
    expr: ExprType,
    arg0: DataTypeName,
    arg1: DataTypeName,
    ret: DataTypeName,
) {
    map.insert(expr, vec![arg0, arg1], ret);
    map.insert(expr, vec![arg1, arg0], ret);
}

fn build_round_funcs(map: &mut FuncSigMap, expr: ExprType) {
    map.insert(expr, vec![DataTypeName::Float64], DataTypeName::Float64);
    map.insert(expr, vec![DataTypeName::Decimal], DataTypeName::Decimal);
}
