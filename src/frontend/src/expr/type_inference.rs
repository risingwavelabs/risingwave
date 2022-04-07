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
use std::sync::Arc;
use std::vec;

use itertools::iproduct;
use risingwave_common::types::DataType;

use crate::expr::ExprType;

/// `DataTypeName` is designed for type derivation here. In other scenarios,
/// use `DataType` instead.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Copy)]
enum DataTypeName {
    Boolean,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    Decimal,
    Date,
    Varchar,
    Time,
    Timestamp,
    Timestampz,
    Interval,
    Struct,
    List,
}

fn name_of(ty: &DataType) -> DataTypeName {
    match ty {
        DataType::Int16 => DataTypeName::Int16,
        DataType::Int32 => DataTypeName::Int32,
        DataType::Int64 => DataTypeName::Int64,
        DataType::Float32 => DataTypeName::Float32,
        DataType::Float64 => DataTypeName::Float64,
        DataType::Boolean => DataTypeName::Boolean,
        DataType::Varchar => DataTypeName::Varchar,
        DataType::Date => DataTypeName::Date,
        DataType::Time => DataTypeName::Time,
        DataType::Timestamp => DataTypeName::Timestamp,
        DataType::Timestampz => DataTypeName::Timestampz,
        DataType::Decimal => DataTypeName::Decimal,
        DataType::Interval => DataTypeName::Interval,
        DataType::Struct { .. } => DataTypeName::Struct,
        DataType::List { .. } => DataTypeName::List,
    }
}

/// Infers the return type of a function. Returns `None` if the function with specified data types
/// is not supported on backend.
pub fn infer_type(func_type: ExprType, inputs_type: Vec<DataType>) -> Option<DataType> {
    // With our current simplified type system, where all types are nullable and not parameterized
    // by things like length or precision, the inference can be done with a map lookup.
    let input_type_names = inputs_type.iter().map(name_of).collect();
    infer_type_name(func_type, input_type_names).map(|type_name| match type_name {
        DataTypeName::Int16 => DataType::Int16,
        DataTypeName::Int32 => DataType::Int32,
        DataTypeName::Int64 => DataType::Int64,
        DataTypeName::Float32 => DataType::Float32,
        DataTypeName::Float64 => DataType::Float64,
        DataTypeName::Boolean => DataType::Boolean,
        DataTypeName::Varchar => DataType::Varchar,
        DataTypeName::Date => DataType::Date,
        DataTypeName::Time => DataType::Time,
        DataTypeName::Timestamp => DataType::Timestamp,
        DataTypeName::Timestampz => DataType::Timestampz,
        DataTypeName::Decimal => DataType::Decimal,
        DataTypeName::Interval => DataType::Interval,
        DataTypeName::Struct => DataType::Struct {
            fields: Arc::new([]),
        },
        DataTypeName::List => DataType::List {
            datatype: Box::new(DataType::Int32),
        },
    })
}

/// Infer the return type name without parameters like length or precision.
fn infer_type_name(func_type: ExprType, inputs_type: Vec<DataTypeName>) -> Option<DataTypeName> {
    FUNC_SIG_MAP
        .get(&FuncSign {
            func: func_type,
            inputs_type,
        })
        .cloned()
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
    pub fn new_no_input(func: ExprType) -> Self {
        FuncSign {
            func,
            inputs_type: vec![],
        }
    }
    pub fn new_unary(func: ExprType, p1: DataTypeName) -> Self {
        FuncSign {
            func,
            inputs_type: vec![p1],
        }
    }
    pub fn new_binary(func: ExprType, p1: DataTypeName, p2: DataTypeName) -> Self {
        FuncSign {
            func,
            inputs_type: vec![p1, p2],
        }
    }
    pub fn new_ternary(
        func: ExprType,
        p1: DataTypeName,
        p2: DataTypeName,
        p3: DataTypeName,
    ) -> Self {
        FuncSign {
            func,
            inputs_type: vec![p1, p2, p3],
        }
    }
}
fn arithmetic_type_derive(t1: DataTypeName, t2: DataTypeName) -> DataTypeName {
    if t2 as i32 > t1 as i32 {
        t2
    } else {
        t1
    }
}
fn build_unary_funcs(
    map: &mut HashMap<FuncSign, DataTypeName>,
    exprs: &[ExprType],
    arg1: &[DataTypeName],
    ret: DataTypeName,
) {
    for (expr, a1) in iproduct!(exprs, arg1) {
        map.insert(FuncSign::new_unary(*expr, *a1), ret);
    }
}
fn build_binary_funcs(
    map: &mut HashMap<FuncSign, DataTypeName>,
    exprs: &[ExprType],
    arg1: &[DataTypeName],
    arg2: &[DataTypeName],
    ret: DataTypeName,
) {
    for (expr, a1, a2) in iproduct!(exprs, arg1, arg2) {
        map.insert(FuncSign::new_binary(*expr, *a1, *a2), ret);
    }
}
fn build_ternary_funcs(
    map: &mut HashMap<FuncSign, DataTypeName>,
    exprs: &[ExprType],
    arg1: &[DataTypeName],
    arg2: &[DataTypeName],
    arg3: &[DataTypeName],
    ret: DataTypeName,
) {
    for (expr, a1, a2, a3) in iproduct!(exprs, arg1, arg2, arg3) {
        map.insert(FuncSign::new_ternary(*expr, *a1, *a2, *a3), ret);
    }
}
fn build_type_derive_map() -> HashMap<FuncSign, DataTypeName> {
    use {DataTypeName as T, ExprType as E};
    let mut map = HashMap::new();
    let num_types = vec![
        T::Int16,
        T::Int32,
        T::Int64,
        T::Float32,
        T::Float64,
        T::Decimal,
    ];
    let all_types = vec![
        T::Int16,
        T::Int32,
        T::Int64,
        T::Float32,
        T::Float64,
        T::Boolean,
        T::Varchar,
        T::Decimal,
        T::Time,
        T::Timestamp,
        T::Interval,
        T::Date,
        T::Timestampz,
    ];
    let str_types = vec![T::Varchar];
    let atm_exprs = vec![E::Add, E::Subtract, E::Multiply, E::Divide, E::Modulus];
    let cmp_exprs = vec![
        E::Equal,
        E::NotEqual,
        E::LessThan,
        E::LessThanOrEqual,
        E::GreaterThan,
        E::GreaterThanOrEqual,
    ];
    for (expr, t1, t2) in iproduct!(atm_exprs, num_types.clone(), num_types.clone()) {
        map.insert(
            FuncSign::new_binary(expr, t1, t2),
            arithmetic_type_derive(t1, t2),
        );
    }
    for t in num_types.clone() {
        map.insert(FuncSign::new_unary(E::Neg, t), t);
    }
    build_binary_funcs(&mut map, &cmp_exprs, &num_types, &num_types, T::Boolean);
    build_binary_funcs(&mut map, &cmp_exprs, &str_types, &str_types, T::Boolean);
    build_binary_funcs(
        &mut map,
        &cmp_exprs,
        &[T::Boolean],
        &[T::Boolean],
        T::Boolean,
    );

    // Date comparisons
    build_binary_funcs(
        &mut map,
        &cmp_exprs,
        &[T::Date, T::Timestamp],
        &[T::Date, T::Timestamp],
        T::Boolean,
    );
    // Date/Timestamp/Interval arithmetic
    map.insert(
        FuncSign::new_binary(E::Add, T::Interval, T::Date),
        T::Timestamp,
    );
    map.insert(
        FuncSign::new_binary(E::Add, T::Date, T::Interval),
        T::Timestamp,
    );
    map.insert(
        FuncSign::new_binary(E::Subtract, T::Date, T::Interval),
        T::Timestamp,
    );
    map.insert(
        FuncSign::new_binary(E::Add, T::Interval, T::Timestamp),
        T::Timestamp,
    );
    map.insert(
        FuncSign::new_binary(E::Add, T::Timestamp, T::Interval),
        T::Timestamp,
    );
    map.insert(
        FuncSign::new_binary(E::Subtract, T::Timestamp, T::Interval),
        T::Timestamp,
    );
    build_binary_funcs(
        &mut map,
        &[E::Multiply],
        &[T::Interval],
        &[T::Int16, T::Int32, T::Int64],
        T::Interval,
    );
    build_binary_funcs(
        &mut map,
        &[E::Multiply],
        &[T::Int16, T::Int32, T::Int64],
        &[T::Interval],
        T::Interval,
    );

    build_binary_funcs(
        &mut map,
        &[E::And, E::Or],
        &[T::Boolean],
        &[T::Boolean],
        T::Boolean,
    );
    build_unary_funcs(
        &mut map,
        &[E::IsTrue, E::IsNotTrue, E::IsFalse, E::IsNotFalse, E::Not],
        &[T::Boolean],
        T::Boolean,
    );
    build_unary_funcs(
        &mut map,
        &[E::IsNull, E::IsNotNull, E::StreamNullByRowCount],
        &all_types,
        T::Boolean,
    );
    build_binary_funcs(&mut map, &[E::Substr], &str_types, &num_types, T::Varchar);
    build_ternary_funcs(
        &mut map,
        &[E::Substr],
        &str_types,
        &num_types,
        &num_types,
        T::Varchar,
    );
    build_unary_funcs(&mut map, &[E::Length], &str_types, T::Int32);
    build_unary_funcs(
        &mut map,
        &[E::Trim, E::Ltrim, E::Rtrim, E::Lower, E::Upper],
        &str_types,
        T::Varchar,
    );
    build_binary_funcs(
        &mut map,
        &[E::Trim, E::Ltrim, E::Rtrim, E::Position],
        &str_types,
        &str_types,
        T::Varchar,
    );
    build_binary_funcs(&mut map, &[E::Like], &str_types, &str_types, T::Boolean);
    build_ternary_funcs(
        &mut map,
        &[E::Replace],
        &str_types,
        &str_types,
        &str_types,
        T::Varchar,
    );
    build_binary_funcs(
        &mut map,
        &[E::RoundDigit],
        &[T::Decimal],
        &[T::Int32],
        T::Decimal,
    );
    build_binary_funcs(
        &mut map,
        &[E::Extract],
        &[T::Varchar], // Time field, "YEAR", "DAY", etc
        &[T::Timestamp, T::Time, T::Date],
        T::Decimal,
    );
    build_binary_funcs(
        &mut map,
        &[E::TumbleStart],
        &[T::Date, T::Timestamp],
        &[T::Interval],
        T::Timestamp,
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
        assert_eq!(ret, None);
    }

    #[test]
    fn test_arithmetics() {
        use DataType::*;
        let atm_exprs = vec![
            ExprType::Add,
            ExprType::Subtract,
            ExprType::Multiply,
            ExprType::Divide,
            ExprType::Modulus,
        ];
        let num_promote_table = vec![
            (Int16, Int16, Int16),
            (Int16, Int32, Int32),
            (Int16, Int64, Int64),
            (Int16, Float32, Float32),
            (Int16, Float64, Float64),
            (Int16, Decimal, Decimal),
            (Int32, Int16, Int32),
            (Int32, Int32, Int32),
            (Int32, Int64, Int64),
            (Int32, Float32, Float32),
            (Int32, Float64, Float64),
            (Int32, Decimal, Decimal),
            (Int64, Int16, Int64),
            (Int64, Int32, Int64),
            (Int64, Int64, Int64),
            (Int64, Float32, Float32),
            (Int64, Float64, Float64),
            (Int64, Decimal, Decimal),
            (Float32, Int16, Float32),
            (Float32, Int32, Float32),
            (Float32, Int64, Float32),
            (Float32, Float32, Float32),
            (Float32, Float64, Float64),
            (Float32, Decimal, Decimal),
            (Float64, Int16, Float64),
            (Float64, Int32, Float64),
            (Float64, Int64, Float64),
            (Float64, Float32, Float64),
            (Float64, Float64, Float64),
            (Float64, Decimal, Decimal),
            (Decimal, Int16, Decimal),
            (Decimal, Int32, Decimal),
            (Decimal, Int64, Decimal),
            (Decimal, Float32, Decimal),
            (Decimal, Float64, Decimal),
            (Decimal, Decimal, Decimal),
        ];
        for (expr, (t1, t2, tr)) in iproduct!(atm_exprs, num_promote_table) {
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
