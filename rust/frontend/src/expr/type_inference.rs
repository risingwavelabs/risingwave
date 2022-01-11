//! this type inference is just to infer the return type of function calls and make sure the
//! functionCall expressions have same input type requirement and retrun type definition.
use std::collections::HashMap;
use std::vec;

use itertools::iproduct;
use risingwave_pb::data::data_type::TypeName;
use risingwave_pb::data::DataType;
use risingwave_pb::expr::expr_node;

/// infer the return type of a functional, if the backend's expression implementation can't recive
/// the datatypes, return Null.
pub fn infer_type(func_type: expr_node::Type, inputs_type: Vec<DataType>) -> Option<DataType> {
    // we will do a simple infer at first, which just infer the type name. if the return type need
    // other description, like data length for char, max data length for varchar or precision for
    // time, decimal, we will do the further infer.

    let ret_type = infer_type_name(
        func_type,
        inputs_type.iter().map(|t| t.get_type_name()).collect(),
    )?;
    match ret_type.get_type_name() {
        TypeName::Decimal => infer_decimal(func_type, inputs_type),
        TypeName::Interval => infer_interval(func_type, inputs_type),
        _ => Some(ret_type),
    }
}

/// infer the return type of expressions whose retrun type is decimal, specifically, return the
/// Decimal type with precision and scale.
fn infer_decimal(_func_type: expr_node::Type, _inputs_type: Vec<DataType>) -> Option<DataType> {
    // TODO:
    Some(DataType {
        type_name: TypeName::Decimal as i32,
        precision: 28,
        scale: 10,
        is_nullable: false,
        interval_type: 0,
    })
}

/// infer the return type of expressions whose retrun type is interval.
fn infer_interval(_func_type: expr_node::Type, _inputs_typee: Vec<DataType>) -> Option<DataType> {
    todo!()
}

/// the first inference just infer the `type_name` and `null_able` for the expression
fn infer_type_name(func_type: expr_node::Type, inputs_type: Vec<TypeName>) -> Option<DataType> {
    FUNC_SIG_MAP
        .get(&FuncSign {
            func: func_type,
            inputs_type,
        })
        .cloned()
}

#[derive(PartialEq, Hash)]
struct FuncSign {
    func: expr_node::Type,
    inputs_type: Vec<TypeName>,
}
impl Eq for FuncSign {}
#[allow(dead_code)]
impl FuncSign {
    pub fn new(func: expr_node::Type, inputs_type: Vec<TypeName>) -> Self {
        FuncSign { func, inputs_type }
    }
    pub fn new_no_input(func: expr_node::Type) -> Self {
        FuncSign {
            func,
            inputs_type: vec![],
        }
    }
    pub fn new_unary(func: expr_node::Type, p1: TypeName) -> Self {
        FuncSign {
            func,
            inputs_type: vec![p1],
        }
    }
    pub fn new_binary(func: expr_node::Type, p1: TypeName, p2: TypeName) -> Self {
        FuncSign {
            func,
            inputs_type: vec![p1, p2],
        }
    }
    pub fn new_ternary(func: expr_node::Type, p1: TypeName, p2: TypeName, p3: TypeName) -> Self {
        FuncSign {
            func,
            inputs_type: vec![p1, p2, p3],
        }
    }
}
fn new_data_type(type_name: TypeName, is_nullable: bool) -> DataType {
    DataType {
        type_name: type_name as i32,
        precision: 0,
        scale: 0,
        is_nullable,
        interval_type: 0,
    }
}
fn arithmetic_type_derive(t1: TypeName, t2: TypeName) -> TypeName {
    if t2 as i32 > t1 as i32 {
        t2
    } else {
        t1
    }
}
fn build_type_derive_map() -> HashMap<FuncSign, DataType> {
    let mut map = HashMap::new();
    let num_types = vec![
        TypeName::Int16,
        TypeName::Int32,
        TypeName::Int64,
        TypeName::Float,
        TypeName::Double,
        TypeName::Decimal,
    ];
    let all_types = vec![
        TypeName::Unknown,
        TypeName::Int16,
        TypeName::Int32,
        TypeName::Int64,
        TypeName::Float,
        TypeName::Double,
        TypeName::Boolean,
        TypeName::Char,
        TypeName::Varchar,
        TypeName::Decimal,
        TypeName::Time,
        TypeName::Timestamp,
        TypeName::Interval,
        TypeName::Date,
        TypeName::Null,
        TypeName::Timestampz,
        TypeName::Symbol,
        TypeName::Cursor,
    ];
    let atm_exprs = vec![
        expr_node::Type::Add,
        expr_node::Type::Subtract,
        expr_node::Type::Multiply,
        expr_node::Type::Divide,
        expr_node::Type::Modulus,
    ];

    let cmp_exprs = vec![
        expr_node::Type::Equal,
        expr_node::Type::NotEqual,
        expr_node::Type::LessThan,
        expr_node::Type::LessThanOrEqual,
        expr_node::Type::GreaterThan,
        expr_node::Type::GreaterThanOrEqual,
    ];
    let logical_exprs = vec![
        expr_node::Type::And,
        expr_node::Type::Or,
        expr_node::Type::Not,
    ];
    let bool_check_exprs = vec![
        expr_node::Type::IsTrue,
        expr_node::Type::IsNotTrue,
        expr_node::Type::IsFalse,
        expr_node::Type::IsNotFalse,
    ];
    let null_check_exprs = vec![
        expr_node::Type::IsNull,
        expr_node::Type::IsNotNull,
        expr_node::Type::StreamNullByRowCount,
    ];

    for (expr, t1, t2) in iproduct!(atm_exprs, num_types.clone(), num_types.clone()) {
        map.insert(
            FuncSign::new_binary(expr, t1, t2),
            new_data_type(arithmetic_type_derive(t1, t2), false),
        );
    }
    for (expr, t1, t2) in iproduct!(cmp_exprs.clone(), num_types.clone(), num_types) {
        map.insert(
            FuncSign::new_binary(expr, t1, t2),
            new_data_type(TypeName::Boolean, false),
        );
    }
    for expr in cmp_exprs {
        map.insert(
            FuncSign::new_binary(expr, TypeName::Boolean, TypeName::Boolean),
            new_data_type(TypeName::Boolean, false),
        );
    }
    for expr in logical_exprs {
        map.insert(
            FuncSign::new_binary(expr, TypeName::Boolean, TypeName::Boolean),
            new_data_type(TypeName::Boolean, false),
        );
    }
    for expr in bool_check_exprs {
        map.insert(
            FuncSign::new_binary(expr, TypeName::Boolean, TypeName::Boolean),
            new_data_type(TypeName::Boolean, false),
        );
    }
    for (expr, t) in iproduct!(null_check_exprs, all_types) {
        map.insert(
            FuncSign::new_unary(expr, t),
            new_data_type(TypeName::Boolean, false),
        );
    }

    map
}
lazy_static::lazy_static! {
  static ref FUNC_SIG_MAP: HashMap<FuncSign, DataType> = {
    build_type_derive_map()
  };
}
#[cfg(test)]
mod tests {
    use itertools::iproduct;
    use risingwave_pb::data::data_type::TypeName;
    use risingwave_pb::data::DataType;
    use risingwave_pb::expr::expr_node;

    use super::{infer_type, new_data_type};

    fn test_simple_infer_type(
        func_type: expr_node::Type,
        inputs_type: Vec<DataType>,
        expected_type_name: TypeName,
        expected_nullable: bool,
    ) {
        let ret = infer_type(func_type, inputs_type).unwrap();
        assert_eq!(ret.get_type_name(), expected_type_name);
        assert_eq!(ret.get_is_nullable(), expected_nullable);
    }
    fn test_infer_type_not_exist(func_type: expr_node::Type, inputs_type: Vec<DataType>) {
        let ret = infer_type(func_type, inputs_type);
        assert_eq!(ret, None);
    }

    #[test]
    fn test_arithmetics() {
        let atm_exprs = vec![
            expr_node::Type::Add,
            expr_node::Type::Subtract,
            expr_node::Type::Multiply,
            expr_node::Type::Divide,
            expr_node::Type::Modulus,
        ];
        let num_promote_table = vec![
            (TypeName::Int16, TypeName::Int16, TypeName::Int16),
            (TypeName::Int16, TypeName::Int32, TypeName::Int32),
            (TypeName::Int16, TypeName::Int64, TypeName::Int64),
            (TypeName::Int16, TypeName::Float, TypeName::Float),
            (TypeName::Int16, TypeName::Double, TypeName::Double),
            (TypeName::Int16, TypeName::Decimal, TypeName::Decimal),
            (TypeName::Int32, TypeName::Int16, TypeName::Int32),
            (TypeName::Int32, TypeName::Int32, TypeName::Int32),
            (TypeName::Int32, TypeName::Int64, TypeName::Int64),
            (TypeName::Int32, TypeName::Float, TypeName::Float),
            (TypeName::Int32, TypeName::Double, TypeName::Double),
            (TypeName::Int32, TypeName::Decimal, TypeName::Decimal),
            (TypeName::Int64, TypeName::Int16, TypeName::Int64),
            (TypeName::Int64, TypeName::Int32, TypeName::Int64),
            (TypeName::Int64, TypeName::Int64, TypeName::Int64),
            (TypeName::Int64, TypeName::Float, TypeName::Float),
            (TypeName::Int64, TypeName::Double, TypeName::Double),
            (TypeName::Int64, TypeName::Decimal, TypeName::Decimal),
            (TypeName::Float, TypeName::Int16, TypeName::Float),
            (TypeName::Float, TypeName::Int32, TypeName::Float),
            (TypeName::Float, TypeName::Int64, TypeName::Float),
            (TypeName::Float, TypeName::Float, TypeName::Float),
            (TypeName::Float, TypeName::Double, TypeName::Double),
            (TypeName::Float, TypeName::Decimal, TypeName::Decimal),
            (TypeName::Double, TypeName::Int16, TypeName::Double),
            (TypeName::Double, TypeName::Int32, TypeName::Double),
            (TypeName::Double, TypeName::Int64, TypeName::Double),
            (TypeName::Double, TypeName::Float, TypeName::Double),
            (TypeName::Double, TypeName::Double, TypeName::Double),
            (TypeName::Double, TypeName::Decimal, TypeName::Decimal),
            (TypeName::Decimal, TypeName::Int16, TypeName::Decimal),
            (TypeName::Decimal, TypeName::Int32, TypeName::Decimal),
            (TypeName::Decimal, TypeName::Int64, TypeName::Decimal),
            (TypeName::Decimal, TypeName::Float, TypeName::Decimal),
            (TypeName::Decimal, TypeName::Double, TypeName::Decimal),
            (TypeName::Decimal, TypeName::Decimal, TypeName::Decimal),
        ];
        for (expr, (t1, t2, tr)) in iproduct!(atm_exprs, num_promote_table) {
            test_simple_infer_type(
                expr,
                vec![new_data_type(t1, true), new_data_type(t2, false)],
                tr,
                false,
            );
        }
    }

    #[test]
    fn test_bool_num_not_exist() {
        let exprs = vec![
            expr_node::Type::Add,
            expr_node::Type::Subtract,
            expr_node::Type::Multiply,
            expr_node::Type::Divide,
            expr_node::Type::Modulus,
            expr_node::Type::Equal,
            expr_node::Type::NotEqual,
            expr_node::Type::LessThan,
            expr_node::Type::LessThanOrEqual,
            expr_node::Type::GreaterThan,
            expr_node::Type::GreaterThanOrEqual,
            expr_node::Type::And,
            expr_node::Type::Or,
            expr_node::Type::Not,
        ];
        let num_types = vec![
            TypeName::Int16,
            TypeName::Int32,
            TypeName::Int64,
            TypeName::Float,
            TypeName::Double,
            TypeName::Decimal,
        ];

        for (expr, num_t) in iproduct!(exprs, num_types) {
            test_infer_type_not_exist(
                expr,
                vec![
                    new_data_type(num_t, false),
                    new_data_type(TypeName::Boolean, false),
                ],
            );
        }
    }
}
