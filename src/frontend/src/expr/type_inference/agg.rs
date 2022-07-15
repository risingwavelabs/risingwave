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

use std::collections::HashMap;

use risingwave_common::types::DataType;
use risingwave_expr::expr::AggKind;

use super::super::AggCall;
use super::DataTypeName;

#[derive(PartialEq, Eq, Hash, Clone, Debug)]
pub struct AggFuncSign {
    pub func: AggKind,
    pub inputs_type: Vec<DataTypeName>,
    pub ret_type: DataTypeName,
}

#[derive(Default)]
pub struct AggFuncSigMap(HashMap<(AggKind, usize), Vec<AggFuncSign>>);
impl AggFuncSigMap {
    fn insert(&mut self, func: AggKind, param_types: Vec<DataTypeName>, ret_type: DataTypeName) {
        let arity = param_types.len();
        let inputs_type = param_types.into_iter().map(Into::into).collect();
        let sig = AggFuncSign {
            func: func.clone(),
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
fn build_type_derive_map() -> AggFuncSigMap {
    use {AggKind as A, DataTypeName as T};
    let mut map = AggFuncSigMap::default();

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

    // Call infer_return_type to check the return type. If it throw error shows that the type is not
    // inferred.
    for agg in [
        A::Sum,
        A::Min,
        A::Max,
        A::Count,
        A::Avg,
        A::StringAgg,
        A::SingleValue,
        A::ApproxCountDistinct,
    ] {
        for input in all_types {
            match AggCall::infer_return_type(&agg, &[DataType::from(input)]) {
                Ok(v) => map.insert(agg.clone(), vec![input], DataTypeName::from(v)),
                Err(_e) => continue,
            }
        }
    }
    map
}

lazy_static::lazy_static! {
    static ref AGG_FUNC_SIG_MAP: AggFuncSigMap = {
        build_type_derive_map()
    };
}

/// The table of function signatures.
pub fn agg_func_sigs() -> impl Iterator<Item = &'static AggFuncSign> {
    AGG_FUNC_SIG_MAP.0.values().flatten()
}

// #[cfg(test)]
// mod tests {
//     use super::*;

//     fn infer_type_v0(func_type: ExprType, inputs_type: Vec<DataType>) -> Result<DataType> {
//         let inputs = inputs_type
//             .into_iter()
//             .map(|t| {
//                 crate::expr::Literal::new(
//                     Some(match t {
//                         DataType::Boolean => true.into(),
//                         DataType::Int16 => 1i16.into(),
//                         DataType::Int32 => 1i32.into(),
//                         DataType::Int64 => 1i64.into(),
//                         DataType::Float32 => 1f32.into(),
//                         DataType::Float64 => 1f64.into(),
//                         DataType::Decimal => risingwave_common::types::Decimal::NaN.into(),
//                         _ => unimplemented!(),
//                     }),
//                     t,
//                 )
//                 .into()
//             })
//             .collect();
//         let (_, ret) = infer_type(func_type, inputs)?;
//         Ok(ret)
//     }

//     fn test_simple_infer_type(
//         func_type: ExprType,
//         inputs_type: Vec<DataType>,
//         expected_type_name: DataType,
//     ) {
//         let ret = infer_type_v0(func_type, inputs_type).unwrap();
//         assert_eq!(ret, expected_type_name);
//     }

//     fn test_infer_type_not_exist(func_type: ExprType, inputs_type: Vec<DataType>) {
//         let ret = infer_type_v0(func_type, inputs_type);
//         assert!(ret.is_err());
//     }

//     #[test]
//     fn test_arithmetics() {
//         use DataType::*;
//         let atm_exprs = vec![
//             ExprType::Add,
//             ExprType::Subtract,
//             ExprType::Multiply,
//             ExprType::Divide,
//         ];
//         let num_promote_table = vec![
//             (Int16, Int16, Int16),
//             (Int16, Int32, Int32),
//             (Int16, Int64, Int64),
//             (Int16, Decimal, Decimal),
//             (Int16, Float32, Float32),
//             (Int16, Float64, Float64),
//             (Int32, Int16, Int32),
//             (Int32, Int32, Int32),
//             (Int32, Int64, Int64),
//             (Int32, Decimal, Decimal),
//             (Int32, Float32, Float32),
//             (Int32, Float64, Float64),
//             (Int64, Int16, Int64),
//             (Int64, Int32, Int64),
//             (Int64, Int64, Int64),
//             (Int64, Decimal, Decimal),
//             (Int64, Float32, Float32),
//             (Int64, Float64, Float64),
//             (Decimal, Int16, Decimal),
//             (Decimal, Int32, Decimal),
//             (Decimal, Int64, Decimal),
//             (Decimal, Decimal, Decimal),
//             (Decimal, Float32, Float32),
//             (Decimal, Float64, Float64),
//             (Float32, Int16, Float32),
//             (Float32, Int32, Float32),
//             (Float32, Int64, Float32),
//             (Float32, Decimal, Float32),
//             (Float32, Float32, Float32),
//             (Float32, Float64, Float64),
//             (Float64, Int16, Float64),
//             (Float64, Int32, Float64),
//             (Float64, Int64, Float64),
//             (Float64, Decimal, Float64),
//             (Float64, Float32, Float64),
//             (Float64, Float64, Float64),
//         ];
//         for (expr, (t1, t2, tr)) in iproduct!(atm_exprs, num_promote_table) {
//             test_simple_infer_type(expr, vec![t1, t2], tr);
//         }
//     }

//     #[test]
//     fn test_bitwise() {
//         use DataType::*;
//         let bitwise_exprs = vec![
//             ExprType::BitwiseAnd,
//             ExprType::BitwiseOr,
//             ExprType::BitwiseXor,
//         ];
//         let num_promote_table = vec![
//             (Int16, Int16, Int16),
//             (Int16, Int32, Int32),
//             (Int16, Int64, Int64),
//             (Int32, Int16, Int32),
//             (Int32, Int32, Int32),
//             (Int32, Int64, Int64),
//             (Int64, Int16, Int64),
//             (Int64, Int32, Int64),
//             (Int64, Int64, Int64),
//         ];
//         for (expr, (t1, t2, tr)) in iproduct!(bitwise_exprs, num_promote_table) {
//             test_simple_infer_type(expr, vec![t1, t2], tr);
//         }

//         for (expr, (t1, t2, tr)) in iproduct!(
//             vec![ExprType::BitwiseShiftLeft, ExprType::BitwiseShiftRight,],
//             vec![
//                 (Int16, Int16, Int16),
//                 (Int32, Int16, Int32),
//                 (Int64, Int16, Int64),
//                 (Int16, Int32, Int16),
//                 (Int64, Int32, Int64),
//                 (Int32, Int32, Int32),
//             ]
//         ) {
//             test_simple_infer_type(expr, vec![t1, t2], tr);
//         }
//     }
//     #[test]
//     fn test_bool_num_not_exist() {
//         let exprs = vec![
//             ExprType::Add,
//             ExprType::Subtract,
//             ExprType::Multiply,
//             ExprType::Divide,
//             ExprType::Modulus,
//             ExprType::Equal,
//             ExprType::NotEqual,
//             ExprType::LessThan,
//             ExprType::LessThanOrEqual,
//             ExprType::GreaterThan,
//             ExprType::GreaterThanOrEqual,
//             ExprType::And,
//             ExprType::Or,
//             ExprType::Not,
//         ];
//         let num_types = vec![
//             DataType::Int16,
//             DataType::Int32,
//             DataType::Int64,
//             DataType::Float32,
//             DataType::Float64,
//             DataType::Decimal,
//         ];

//         for (expr, num_t) in iproduct!(exprs, num_types) {
//             test_infer_type_not_exist(expr, vec![num_t, DataType::Boolean]);
//         }
//     }

//     #[test]
//     fn test_match_implicit() {
//         use DataTypeName as T;
//         // func_name and ret_type does not affect the overload resolution logic
//         const DUMMY_FUNC: ExprType = ExprType::Add;
//         const DUMMY_RET: T = T::Int32;
//         let testcases = [
//             (
//                 "Binary special rule prefers arguments of same type.",
//                 vec![
//                     vec![T::Int32, T::Int32],
//                     vec![T::Int32, T::Varchar],
//                     vec![T::Int32, T::Float64],
//                 ],
//                 &[Some(T::Int32), None] as &[_],
//                 Ok(&[T::Int32, T::Int32] as &[_]),
//             ),
//             (
//                 "Without binary special rule, Rule 4e selects varchar.",
//                 vec![
//                     vec![T::Int32, T::Int32, T::Int32],
//                     vec![T::Int32, T::Int32, T::Varchar],
//                     vec![T::Int32, T::Int32, T::Float64],
//                 ],
//                 &[Some(T::Int32), Some(T::Int32), None] as &[_],
//                 Ok(&[T::Int32, T::Int32, T::Varchar] as &[_]),
//             ),
//             (
//                 "Without binary special rule, Rule 4e selects preferred type.",
//                 vec![
//                     vec![T::Int32, T::Int32, T::Int32],
//                     vec![T::Int32, T::Int32, T::Float64],
//                 ],
//                 &[Some(T::Int32), Some(T::Int32), None] as &[_],
//                 Ok(&[T::Int32, T::Int32, T::Float64] as &[_]),
//             ),
//             (
//                 "Without binary special rule, Rule 4f treats exact-match and cast-match
// equally.",                 vec![
//                     vec![T::Int32, T::Int32, T::Int32],
//                     vec![T::Int32, T::Int32, T::Float32],
//                 ],
//                 &[Some(T::Int32), Some(T::Int32), None] as &[_],
//                 Err("not unique"),
//             ),
//             (
//                 "`top_matches` ranks by exact count then preferred count",
//                 vec![
//                     vec![T::Float64, T::Float64, T::Float64, T::Timestampz], // 0 exact 3
// preferred                     vec![T::Float64, T::Int32, T::Float32, T::Timestamp],    // 1 exact
// 1 preferred                     vec![T::Float32, T::Float32, T::Int32, T::Timestampz],   // 1
// exact 0 preferred                     vec![T::Int32, T::Float64, T::Float32, T::Timestampz],   //
// 1 exact 1 preferred                     vec![T::Int32, T::Int16, T::Int32, T::Timestampz], // 2
// exact 1 non-castable                     vec![T::Int32, T::Float64, T::Float32, T::Date],   // 1
// exact 1 preferred                 ],
//                 &[Some(T::Int32), Some(T::Int32), Some(T::Int32), None] as &[_],
//                 Ok(&[T::Int32, T::Float64, T::Float32, T::Timestampz] as &[_]),
//             ),
//             (
//                 "Rule 4e fails and Rule 4f unique.",
//                 vec![
//                     vec![T::Int32, T::Int32, T::Time],
//                     vec![T::Int32, T::Int32, T::Int32],
//                 ],
//                 &[None, Some(T::Int32), None] as &[_],
//                 Ok(&[T::Int32, T::Int32, T::Int32] as &[_]),
//             ),
//             (
//                 "Rule 4e empty and Rule 4f unique.",
//                 vec![
//                     vec![T::Int32, T::Int32, T::Varchar],
//                     vec![T::Int32, T::Int32, T::Int32],
//                     vec![T::Varchar, T::Int32, T::Int32],
//                 ],
//                 &[None, Some(T::Int32), None] as &[_],
//                 Ok(&[T::Int32, T::Int32, T::Int32] as &[_]),
//             ),
//             (
//                 "Rule 4e varchar resolves prior category conflict.",
//                 vec![
//                     vec![T::Int32, T::Int32, T::Float32],
//                     vec![T::Time, T::Int32, T::Int32],
//                     vec![T::Varchar, T::Int32, T::Int32],
//                 ],
//                 &[None, Some(T::Int32), None] as &[_],
//                 Ok(&[T::Varchar, T::Int32, T::Int32] as &[_]),
//             ),
//             (
//                 "Rule 4f fails.",
//                 vec![
//                     vec![T::Float32, T::Float32, T::Float32, T::Float32],
//                     vec![T::Decimal, T::Decimal, T::Int64, T::Decimal],
//                 ],
//                 &[Some(T::Int16), Some(T::Int32), None, Some(T::Int64)] as &[_],
//                 Err("not unique"),
//             ),
//             (
//                 "Rule 4f all unknown.",
//                 vec![
//                     vec![T::Float32, T::Float32, T::Float32, T::Float32],
//                     vec![T::Decimal, T::Decimal, T::Int64, T::Decimal],
//                 ],
//                 &[None, None, None, None] as &[_],
//                 Err("not unique"),
//             ),
//         ];
//         for (desc, candidates, inputs, expected) in testcases {
//             let mut sig_map = FuncSigMap::default();
//             candidates
//                 .into_iter()
//                 .for_each(|formals| sig_map.insert(DUMMY_FUNC, formals, DUMMY_RET));
//             let result = infer_type_name(&sig_map, DUMMY_FUNC, inputs);
//             match (expected, result) {
//                 (Ok(expected), Ok(found)) => {
//                     assert_eq!(expected, found.inputs_type, "case `{}`", desc)
//                 }
//                 (Ok(_), Err(err)) => panic!("case `{}` unexpected error: {:?}", desc, err),
//                 (Err(_), Ok(f)) => panic!(
//                     "case `{}` expect error but found: {:?}",
//                     desc, f.inputs_type
//                 ),
//                 (Err(expected), Err(err)) => assert!(
//                     err.to_string().contains(expected),
//                     "case `{}` expect err `{}` != {:?}",
//                     desc,
//                     expected,
//                     err
//                 ),
//             }
//         }
//     }
// }
