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

use itertools::{iproduct, Itertools as _};
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::DataType;

use super::{cast_ok_base, CastContext, DataTypeName};
use crate::expr::{Expr as _, ExprImpl, ExprType};

/// Infers the return type of a function. Returns `Err` if the function with specified data types
/// is not supported on backend.
pub fn infer_type(func_type: ExprType, inputs: Vec<ExprImpl>) -> Result<(Vec<ExprImpl>, DataType)> {
    let sig = infer_type_name(&FUNC_SIG_MAP, func_type, &inputs)?;
    let inputs = inputs
        .into_iter()
        .zip_eq(&sig.inputs_type)
        .map(|(expr, t)| {
            if DataTypeName::from(expr.return_type()) != *t {
                return expr.cast_implicit((*t).into());
            }
            Ok(expr)
        })
        .try_collect()?;
    let ret_type = sig.ret_type.into();
    Ok((inputs, ret_type))
}

/// From all available functions in `sig_map`, find and return the best matching `FuncSign` for the
/// provided `func_type` and `inputs`. This not only support exact function signature match, but can
/// also match `substr(varchar, smallint)` or even `substr(varchar, unknown)` to `substr(varchar,
/// int)`.
///
/// This correponds to the `PostgreSQL` rules on operators and functions here:
/// * <https://www.postgresql.org/docs/current/typeconv-oper.html>
/// * <https://www.postgresql.org/docs/current/typeconv-func.html>
///
/// To summarize,
/// 1. Find all functions with matching `func_type` and argument count.
/// 2. For binary operator with unknown on exactly one side, try to find an exact match assuming
///    both sides are same type.
/// 3. Rank candidates based on most matching positions. This covers Rule 2, 4a, 4c and 4d in
///    `PostgreSQL`. See [`top_matches`] for details.
/// 4. Attempt to narrow down candidates by selecting type catagories for unknowns. This covers Rule
///    4e in `PostgreSQL`. See [`narrow_category`] for details.
/// 5. Attempt to narrow down candidates by assuming all arguments are same type. This covers Rule
///    4f in `PostgreSQL`. See [`narrow_same_type`] for details.
fn infer_type_name<'a, 'b>(
    sig_map: &'a FuncSigMap,
    func_type: ExprType,
    inputs: &'b [ExprImpl],
) -> Result<&'a FuncSign> {
    let candidates = sig_map
        .0
        .get(&(func_type, inputs.len()))
        .map(std::ops::Deref::deref)
        .unwrap_or_default();

    // Binary operators have a special unknown rule for exact match.
    // ~~But it is just speed up and does not affect correctness.~~
    // Exact match has to be prioritized here over rule `f`, which allows casting
    // and resolves `int < unknown` to {`int < float8`, `int < int`, etc}
    if inputs.len() == 2 {
        let t = match (inputs[0].is_null(), inputs[1].is_null()) {
            (true, true) => None,
            (true, false) => Some(inputs[1].return_type().into()),
            (false, true) => Some(inputs[0].return_type().into()),
            (false, false) => None,
        };
        if let Some(t) = t {
            let exact = candidates
                .iter()
                .find(|sig| sig.inputs_type[0] == t && sig.inputs_type[1] == t);
            if let Some(sig) = exact {
                return Ok(sig);
            }
        }
    }

    let mut candidates = top_matches(candidates, inputs);

    if candidates.is_empty() {
        return Err(ErrorCode::NotImplemented(
            format!(
                "{:?}{:?}",
                func_type,
                inputs.iter().map(|e| e.return_type()).collect_vec()
            ),
            112.into(),
        )
        .into());
    }

    candidates = narrow_category(candidates, inputs);

    if candidates.len() > 1 {
        candidates = narrow_same_type(candidates, inputs);
    }

    match &candidates[..] {
        [] => unreachable!(),
        [sig] => Ok(sig),
        _ => Err(ErrorCode::BindError(format!(
            "multi func match: {:?} {:?}",
            func_type,
            inputs.iter().map(|e| e.return_type()).collect_vec(),
        ))
        .into()),
    }
}

/// Checks if `t` is a preferred type in any type category, as defined by `PostgreSQL`:
/// <https://www.postgresql.org/docs/current/catalog-pg-type.html>.
fn is_preferred(t: DataTypeName) -> bool {
    use DataTypeName as T;
    matches!(
        t,
        T::Float64 | T::Boolean | T::Varchar | T::Timestampz | T::Interval
    )
}

/// Find the top `candidates` that match `inputs` on most non-null positions. This covers Rule 2,
/// 4a, 4c and 4d in [`PostgreSQL`](https://www.postgresql.org/docs/current/typeconv-func.html).
///
/// * Rule 2 & 4c: Keep candidates that have most exact type matches. Exact match on all posistions
///   is just a special case.
/// * Rule 4d: Break ties by selecting those that accept preferred types at most positions.
/// * Rule 4a: If the input cannot implicit cast to expected type at any position, this candidate is
///   discarded.
///
/// Correponding implementation in `PostgreSQL`:
/// * Rule 2 on operators: `OpernameGetOprid()` in `namespace.c` [14.0/86a4dc1][rule 2 oper src]
///   * Note that unknown-handling logic of `binary_oper_exact()` in `parse_oper.c` is in
///     [`infer_type_name`].
/// * Rule 2 on functions: Line 1427 - Line 1439 of `func_get_detail()` in `parse_func.c`
///   [14.0/86a4dc1][rule 2 func src]
/// * Rule 4a: `func_match_argtypes()` in `parse_func.c` [14.0/86a4dc1][rule 4a src]
/// * Rule 4c: Line 1062 - Line 1104 of `func_select_candidate()` in `parse_func.c`
///   [14.0/86a4dc1][rule 4c src]
/// * Rule 4d: Line 1106 - Line 1153 of `func_select_candidate()` in `parse_func.c`
///   [14.0/86a4dc1][rule 4d src]
///
/// [rule 2 oper src]: https://github.com/postgres/postgres/blob/86a4dc1e6f29d1992a2afa3fac1a0b0a6e84568c/src/backend/catalog/namespace.c#L1516-L1611
/// [rule 2 func src]: https://github.com/postgres/postgres/blob/86a4dc1e6f29d1992a2afa3fac1a0b0a6e84568c/src/backend/parser/parse_func.c#L1427-L1439
/// [rule 4a src]: https://github.com/postgres/postgres/blob/86a4dc1e6f29d1992a2afa3fac1a0b0a6e84568c/src/backend/parser/parse_func.c#L907-L947
/// [rule 4c src]: https://github.com/postgres/postgres/blob/86a4dc1e6f29d1992a2afa3fac1a0b0a6e84568c/src/backend/parser/parse_func.c#L1062-L1104
/// [rule 4d src]: https://github.com/postgres/postgres/blob/86a4dc1e6f29d1992a2afa3fac1a0b0a6e84568c/src/backend/parser/parse_func.c#L1106-L1153
fn top_matches<'a, 'b>(candidates: &'a [FuncSign], inputs: &'b [ExprImpl]) -> Vec<&'a FuncSign> {
    let mut best_exact = 0;
    let mut best_preferred = 0;
    let mut best_candidate = Vec::new();

    for sig in candidates {
        let mut n_exact = 0;
        let mut n_preferred = 0;
        let mut castable = true;
        for (a, p) in inputs.iter().zip_eq(&sig.inputs_type) {
            if !a.is_null() {
                let at = a.return_type().into();
                if at == *p {
                    n_exact += 1;
                } else if !cast_ok_base(at, *p, CastContext::Implicit) {
                    castable = false;
                    break;
                }
                // Only count non-nulls. Example:
                // ```
                // create function xxx(text, int, int) returns text language sql return 1;
                // create function xxx(int, text, int) returns text language sql return 2;
                // create function xxx(int, int, int) returns text language sql return 3;
                // select xxx(null, null, null);
                // select xxx(null, null, 1::smallint);  -- 3
                // ```
                // If we count null positions, the first 2 wins because text is preferred.
                if is_preferred(*p) {
                    n_preferred += 1;
                }
            }
        }
        if !castable {
            continue;
        }
        if n_exact > best_exact || n_exact == best_exact && n_preferred > best_preferred {
            best_exact = n_exact;
            best_preferred = n_preferred;
            best_candidate.clear();
        }
        if n_exact == best_exact && n_preferred == best_preferred {
            best_candidate.push(sig);
        }
    }
    best_candidate
}

/// Attempt to narrow down candidates by selecting type catagories for unknowns. This covers Rule 4e
/// in [`PostgreSQL`](https://www.postgresql.org/docs/current/typeconv-func.html).
///
/// This is done in 2 phases:
/// * First, chech each unknown position individually and try to find a type category for it.
///   * If any candidate accept varchar, varchar is selected;
///   * otherwise, if all candidate accept the same category, select this category.
///     * Also record whether any candidate accept the preferred type within this category.
/// * When all unknown positions are assigned their type categories, discard a candidate if at any
///   position
///   * it does not agree with the type category assignment;
///   * the assigned type category contains a preferred type but the candidate is not preferred.
///
/// If the first phase fails or the second phase gives an empty set, this attempt preserves orginal
/// list untouched.
///
/// Correponding implementation in `PostgreSQL`:
/// * Line 1164 - Line 1298 of `func_select_candidate()` in `parse_func.c` [14.0/86a4dc1][rule 4e
///   src]
///
/// [rule 4e src]: https://github.com/postgres/postgres/blob/86a4dc1e6f29d1992a2afa3fac1a0b0a6e84568c/src/backend/parser/parse_func.c#L1164-L1298
fn narrow_category<'a, 'b>(
    best_candidate: Vec<&'a FuncSign>,
    inputs: &'b [ExprImpl],
) -> Vec<&'a FuncSign> {
    let mut ets = Vec::new();
    for (i, arg) in inputs.iter().enumerate() {
        if !arg.is_null() {
            continue;
        }
        let mut t = Some(best_candidate[0].inputs_type[i]);
        for sig in &best_candidate[1..] {
            let tc = sig.inputs_type[i];
            if tc == DataTypeName::Varchar {
                t = Some(DataTypeName::Varchar);
            } else if let Some(tt) = t {
                if tt == DataTypeName::Varchar
                    || tc == tt
                    || cast_ok_base(tc, tt, CastContext::Implicit)
                {
                } else if cast_ok_base(tt, tc, CastContext::Implicit) {
                    t = Some(tc);
                } else {
                    t = None;
                }
            }
        }
        if let Some(t) = t {
            ets.push(t);
        } else {
            break;
        }
    }
    let cands_temp = best_candidate
        .iter()
        .filter(|sig| {
            let mut ets_iter = ets.iter();
            for (i, p) in sig.inputs_type.iter().enumerate() {
                if !inputs[i].is_null() {
                    continue;
                }
                let Some(t) = ets_iter.next() else {return false};
                if is_preferred(*t) {
                    if *p != *t {
                        return false;
                    }
                } else if !cast_ok_base(*p, *t, CastContext::Implicit) {
                    return false;
                }
            }
            true
        })
        .cloned()
        .collect_vec();
    if !cands_temp.is_empty() {
        cands_temp
    } else {
        best_candidate
    }
}

/// Attempt to narrow down candidates by assuming all arguments are same type. This covers Rule 4f
/// in [`PostgreSQL`](https://www.postgresql.org/docs/current/typeconv-func.html).
///
/// If all non-null arguments are same type, assume all unknown arguments are of this type as well.
/// Discard a candidate if its paramater type cannot be casted from this type.
///
/// If the condition is not met or the result is empty, this attempt preserves orginal list
/// untouched.
///
/// Note this rule cannot replace special treatment given to binary operators in [Rule 2], because
/// this runs after varchar-biased Rule 4e ([`narrow_category`]), and has no preference between
/// exact-match and castable-match.
///
/// Correponding implementation in `PostgreSQL`:
/// * Line 1300 - Line 1355 of `func_select_candidate()` in `parse_func.c` [14.0/86a4dc1][rule 4f
///   src]
///
/// [rule 4f src]: https://github.com/postgres/postgres/blob/86a4dc1e6f29d1992a2afa3fac1a0b0a6e84568c/src/backend/parser/parse_func.c#L1300-L1355
/// [Rule 2]: https://www.postgresql.org/docs/current/typeconv-oper.html#:~:text=then%20assume%20it%20is%20the%20same%20type%20as%20the%20other%20argument%20for%20this%20check
fn narrow_same_type<'a, 'b>(
    best_candidate: Vec<&'a FuncSign>,
    inputs: &'b [ExprImpl],
) -> Vec<&'a FuncSign> {
    let mut t = None;
    for e in inputs {
        if e.is_null() {
            continue;
        }
        let tc = e.return_type().into();
        match t {
            None => {
                t = Some(tc);
            }
            Some(tt) => {
                if tt != tc {
                    t = None;
                    break;
                }
            }
        }
    }
    if let Some(t) = t {
        let cand_temp = best_candidate
            .iter()
            .filter(|sig| {
                sig.inputs_type
                    .iter()
                    .all(|p| cast_ok_base(t, *p, CastContext::Implicit))
            })
            .cloned()
            .collect_vec();
        if !cand_temp.is_empty() {
            return cand_temp;
        }
    }
    best_candidate
}

#[derive(PartialEq, Eq, Hash, Clone, Debug)]
pub struct FuncSign {
    pub func: ExprType,
    pub inputs_type: Vec<DataTypeName>,
    pub ret_type: DataTypeName,
}

#[derive(Default)]
pub struct FuncSigMap(HashMap<(ExprType, usize), Vec<FuncSign>>);
impl FuncSigMap {
    fn insert(&mut self, func: ExprType, param_types: Vec<DataTypeName>, ret_type: DataTypeName) {
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
    ];
    build_binary_cmp_funcs(&mut map, cmp_exprs, &num_types);
    build_binary_cmp_funcs(&mut map, cmp_exprs, &[T::Struct, T::List]);
    build_binary_cmp_funcs(&mut map, cmp_exprs, &[T::Date, T::Timestamp, T::Timestampz]);
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
        &num_types,
    );
    build_binary_atm_funcs(
        &mut map,
        &[E::Modulus],
        &[T::Int16, T::Int32, T::Int64, T::Decimal],
    );
    map.insert(E::RoundDigit, vec![T::Decimal, T::Int32], T::Decimal);

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
        (T::Timestampz, T::Interval),
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

    for t in [T::Timestamp, T::Time, T::Date] {
        map.insert(E::Extract, vec![T::Varchar, t], T::Decimal);
    }
    for t in [T::Timestamp, T::Date] {
        map.insert(E::TumbleStart, vec![t, T::Interval], T::Timestamp);
    }

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
    for e in [E::Length, E::Ascii, E::CharLength] {
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

    map
}

lazy_static::lazy_static! {
    static ref FUNC_SIG_MAP: FuncSigMap = {
        build_type_derive_map()
    };
}

/// The table of function signatures.
pub fn func_sigs() -> impl Iterator<Item = &'static FuncSign> {
    FUNC_SIG_MAP.0.values().flatten()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn infer_type_v0(func_type: ExprType, inputs_type: Vec<DataType>) -> Result<DataType> {
        let inputs = inputs_type
            .into_iter()
            .map(|t| {
                crate::expr::Literal::new(
                    Some(match t {
                        DataType::Boolean => true.into(),
                        DataType::Int16 => 1i16.into(),
                        DataType::Int32 => 1i32.into(),
                        DataType::Int64 => 1i64.into(),
                        DataType::Float32 => 1f32.into(),
                        DataType::Float64 => 1f64.into(),
                        DataType::Decimal => risingwave_common::types::Decimal::NaN.into(),
                        _ => unimplemented!(),
                    }),
                    t,
                )
                .into()
            })
            .collect();
        let (_, ret) = infer_type(func_type, inputs)?;
        Ok(ret)
    }

    fn test_simple_infer_type(
        func_type: ExprType,
        inputs_type: Vec<DataType>,
        expected_type_name: DataType,
    ) {
        let ret = infer_type_v0(func_type, inputs_type).unwrap();
        assert_eq!(ret, expected_type_name);
    }

    fn test_infer_type_not_exist(func_type: ExprType, inputs_type: Vec<DataType>) {
        let ret = infer_type_v0(func_type, inputs_type);
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
