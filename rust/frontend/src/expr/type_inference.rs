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
    Char,
    Varchar,
    Time,
    Timestamp,
    Timestampz,
    Interval,
    Struct,
}

fn name_of(ty: &DataType) -> DataTypeName {
    match ty {
        DataType::Int16 => DataTypeName::Int16,
        DataType::Int32 => DataTypeName::Int32,
        DataType::Int64 => DataTypeName::Int64,
        DataType::Float32 => DataTypeName::Float32,
        DataType::Float64 => DataTypeName::Float64,
        DataType::Boolean => DataTypeName::Boolean,
        DataType::Char => DataTypeName::Char,
        DataType::Varchar => DataTypeName::Varchar,
        DataType::Date => DataTypeName::Date,
        DataType::Time => DataTypeName::Time,
        DataType::Timestamp => DataTypeName::Timestamp,
        DataType::Timestampz => DataTypeName::Timestampz,
        DataType::Decimal => DataTypeName::Decimal,
        DataType::Interval => DataTypeName::Interval,
        DataType::Struct { .. } => DataTypeName::Struct,
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
        DataTypeName::Char => DataType::Char,
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
fn build_type_derive_map() -> HashMap<FuncSign, DataTypeName> {
    let mut map = HashMap::new();
    let num_types = vec![
        DataTypeName::Int16,
        DataTypeName::Int32,
        DataTypeName::Int64,
        DataTypeName::Float32,
        DataTypeName::Float64,
        DataTypeName::Decimal,
    ];
    let all_types = vec![
        DataTypeName::Int16,
        DataTypeName::Int32,
        DataTypeName::Int64,
        DataTypeName::Float32,
        DataTypeName::Float64,
        DataTypeName::Boolean,
        DataTypeName::Char,
        DataTypeName::Varchar,
        DataTypeName::Decimal,
        DataTypeName::Time,
        DataTypeName::Timestamp,
        DataTypeName::Interval,
        DataTypeName::Date,
        DataTypeName::Timestampz,
    ];
    let atm_exprs = vec![
        ExprType::Add,
        ExprType::Subtract,
        ExprType::Multiply,
        ExprType::Divide,
        ExprType::Modulus,
    ];

    let cmp_exprs = vec![
        ExprType::Equal,
        ExprType::NotEqual,
        ExprType::LessThan,
        ExprType::LessThanOrEqual,
        ExprType::GreaterThan,
        ExprType::GreaterThanOrEqual,
    ];
    let logical_exprs = vec![ExprType::And, ExprType::Or, ExprType::Not];
    let bool_check_exprs = vec![
        ExprType::IsTrue,
        ExprType::IsNotTrue,
        ExprType::IsFalse,
        ExprType::IsNotFalse,
    ];
    let null_check_exprs = vec![
        ExprType::IsNull,
        ExprType::IsNotNull,
        ExprType::StreamNullByRowCount,
    ];

    for (expr, t1, t2) in iproduct!(atm_exprs, num_types.clone(), num_types.clone()) {
        map.insert(
            FuncSign::new_binary(expr, t1, t2),
            arithmetic_type_derive(t1, t2),
        );
    }
    for (expr, t1, t2) in iproduct!(cmp_exprs.clone(), num_types.clone(), num_types) {
        map.insert(FuncSign::new_binary(expr, t1, t2), DataTypeName::Boolean);
    }
    for expr in cmp_exprs {
        map.insert(
            FuncSign::new_binary(expr, DataTypeName::Boolean, DataTypeName::Boolean),
            DataTypeName::Boolean,
        );
    }
    for expr in logical_exprs {
        map.insert(
            FuncSign::new_binary(expr, DataTypeName::Boolean, DataTypeName::Boolean),
            DataTypeName::Boolean,
        );
    }
    for expr in bool_check_exprs {
        map.insert(
            FuncSign::new_binary(expr, DataTypeName::Boolean, DataTypeName::Boolean),
            DataTypeName::Boolean,
        );
    }
    for (expr, t) in iproduct!(null_check_exprs, all_types) {
        map.insert(FuncSign::new_unary(expr, t), DataTypeName::Boolean);
    }

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
