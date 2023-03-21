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

use itertools::Itertools as _;
use num_integer::Integer as _;
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_common::types::struct_type::StructType;
use risingwave_common::types::{DataType, DataTypeName, ScalarImpl};
use risingwave_common::util::iter_util::ZipEqFast;
pub use risingwave_expr::sig::func::*;

use super::{align_types, cast_ok_base, CastContext};
use crate::expr::type_inference::cast::align_array_and_element;
use crate::expr::{cast_ok, is_row_function, Expr as _, ExprImpl, ExprType, FunctionCall};

/// Infers the return type of a function. Returns `Err` if the function with specified data types
/// is not supported on backend.
///
/// It also mutates the `inputs` by adding necessary casts.
pub fn infer_type(func_type: ExprType, inputs: &mut Vec<ExprImpl>) -> Result<DataType> {
    if let Some(res) = infer_type_for_special(func_type, inputs).transpose() {
        return res;
    }

    let actuals = inputs
        .iter()
        .map(|e| match e.is_unknown() {
            true => None,
            false => Some(e.return_type().into()),
        })
        .collect_vec();
    let sig = infer_type_name(&FUNC_SIG_MAP, func_type, &actuals)?;
    let inputs_owned = std::mem::take(inputs);
    *inputs = inputs_owned
        .into_iter()
        .zip_eq_fast(sig.inputs_type)
        .map(|(expr, t)| {
            if DataTypeName::from(expr.return_type()) != *t {
                if t.is_scalar() {
                    return expr.cast_implicit((*t).into()).map_err(Into::into);
                } else {
                    return Err(ErrorCode::BindError(format!(
                        "Cannot implicitly cast '{:?}' to polymorphic type {:?}",
                        &expr, t
                    ))
                    .into());
                }
            }
            Ok(expr)
        })
        .try_collect::<_, _, RwError>()?;
    Ok(sig.ret_type.into())
}

pub fn infer_some_all(
    mut func_types: Vec<ExprType>,
    inputs: &mut Vec<ExprImpl>,
) -> Result<DataType> {
    let element_type = if inputs[1].is_unknown() {
        None
    } else if let DataType::List { datatype } = inputs[1].return_type() {
        Some(DataTypeName::from(*datatype))
    } else {
        return Err(ErrorCode::BindError(
            "op ANY/ALL (array) requires array on right side".to_string(),
        )
        .into());
    };

    let final_type = func_types.pop().unwrap();
    let actuals = vec![
        (!inputs[0].is_unknown()).then_some(inputs[0].return_type().into()),
        element_type,
    ];
    let sig = infer_type_name(&FUNC_SIG_MAP, final_type, &actuals)?;
    if DataTypeName::from(inputs[0].return_type()) != sig.inputs_type[0] {
        inputs[0] = inputs[0].clone().cast_implicit(sig.inputs_type[0].into())?;
    }
    if element_type != Some(sig.inputs_type[1]) {
        inputs[1] = inputs[1].clone().cast_implicit(DataType::List {
            datatype: Box::new(sig.inputs_type[1].into()),
        })?;
    }

    let inputs_owned = std::mem::take(inputs);
    let mut func_call =
        FunctionCall::new_unchecked(final_type, inputs_owned, sig.ret_type.into()).into();
    while let Some(func_type) = func_types.pop() {
        func_call = FunctionCall::new(func_type, vec![func_call])?.into();
    }
    let return_type = func_call.return_type();
    *inputs = vec![func_call];
    Ok(return_type)
}

macro_rules! ensure_arity {
    ($func:literal, $lower:literal <= | $inputs:ident | <= $upper:literal) => {
        if !($lower <= $inputs.len() && $inputs.len() <= $upper) {
            return Err(ErrorCode::BindError(format!(
                "Function `{}` takes {} to {} arguments ({} given)",
                $func,
                $lower,
                $upper,
                $inputs.len(),
            ))
            .into());
        }
    };
    ($func:literal, $lower:literal <= | $inputs:ident |) => {
        if !($lower <= $inputs.len()) {
            return Err(ErrorCode::BindError(format!(
                "Function `{}` takes at least {} arguments ({} given)",
                $func,
                $lower,
                $inputs.len(),
            ))
            .into());
        }
    };
    ($func:literal, | $inputs:ident | == $num:literal) => {
        if !($inputs.len() == $num) {
            return Err(ErrorCode::BindError(format!(
                "Function `{}` takes {} arguments ({} given)",
                $func,
                $num,
                $inputs.len(),
            ))
            .into());
        }
    };
    ($func:literal, | $inputs:ident | <= $upper:literal) => {
        if !($inputs.len() <= $upper) {
            return Err(ErrorCode::BindError(format!(
                "Function `{}` takes at most {} arguments ({} given)",
                $func,
                $upper,
                $inputs.len(),
            ))
            .into());
        }
    };
}

/// An intermediate representation of struct type when resolving the type to cast.
#[derive(Debug)]
pub enum NestedType {
    /// A type that can should inferred (will never be struct).
    Infer(DataType),
    /// A concrete data type.
    Type(DataType),
    /// A struct type.
    Struct(Vec<NestedType>),
}

/// Convert struct type to a nested type
fn extract_struct_nested_type(ty: &StructType) -> Result<NestedType> {
    let fields = ty
        .fields
        .iter()
        .map(|f| match f {
            DataType::Struct(s) => extract_struct_nested_type(s),
            _ => Ok(NestedType::Type(f.clone())),
        })
        .try_collect()?;
    Ok(NestedType::Struct(fields))
}

/// Decompose expression into a nested type to be inferred.
fn extract_expr_nested_type(expr: &ExprImpl) -> Result<NestedType> {
    if expr.is_unknown() {
        Ok(NestedType::Infer(expr.return_type()))
    } else if is_row_function(expr) {
        // For row function, recursively get the type requirement of each field.
        let func = expr.as_function_call().unwrap();
        let ret = func
            .inputs()
            .iter()
            .map(extract_expr_nested_type)
            .collect::<Result<Vec<_>>>()?;
        Ok(NestedType::Struct(ret))
    } else {
        match expr.return_type() {
            DataType::Struct(ty) => extract_struct_nested_type(&ty),
            ty => Ok(NestedType::Type(ty)),
        }
    }
}

/// Handle struct comparisons for [`infer_type_for_special`].
fn infer_struct_cast_target_type(
    func_type: ExprType,
    lexpr: NestedType,
    rexpr: NestedType,
) -> Result<(bool, bool, DataType)> {
    match (lexpr, rexpr) {
        (NestedType::Struct(lty), NestedType::Struct(rty)) => {
            // If both sides are structs, resolve the final data type recursively.
            if lty.len() != rty.len() {
                return Err(ErrorCode::BindError(format!(
                    "cannot infer type because of different number of fields: left={:?} right={:?}",
                    lty, rty
                ))
                .into());
            }
            let mut tys = vec![];
            let mut lcasts = false;
            let mut rcasts = false;
            tys.reserve(lty.len());
            for (lf, rf) in lty.into_iter().zip_eq_fast(rty) {
                let (lcast, rcast, ty) = infer_struct_cast_target_type(func_type, lf, rf)?;
                lcasts |= lcast;
                rcasts |= rcast;
                tys.push((ty, "".to_string())); // TODO(chi): generate field name
            }
            Ok((
                lcasts,
                rcasts,
                DataType::Struct(StructType::new(tys).into()),
            ))
        }
        (l, r @ NestedType::Struct(_)) | (l @ NestedType::Struct(_), r) => {
            // If only one side is nested type, these two types can never be casted.
            Err(ErrorCode::BindError(format!(
                "cannot infer type because unmatched types: left={:?} right={:?}",
                l, r
            ))
            .into())
        }
        (NestedType::Type(l), NestedType::Type(r)) => {
            // If both sides are concrete types, try cast in either direction.
            if l == r {
                Ok((false, false, r))
            } else if cast_ok(&l, &r, CastContext::Implicit) {
                Ok((true, false, r))
            } else if cast_ok(&r, &l, CastContext::Implicit) {
                Ok((false, true, l))
            } else {
                return Err(ErrorCode::BindError(format!(
                    "cannot cast {} to {} or {} to {}",
                    l, r, r, l
                ))
                .into());
            }
        }
        (NestedType::Type(ty), NestedType::Infer(ity)) => {
            // If one side is *unknown*, cast to another type.
            Ok((false, ity != ty, ty))
        }
        (NestedType::Infer(ity), NestedType::Type(ty)) => {
            // If one side is *unknown*, cast to another type.
            Ok((ity != ty, false, ty))
        }
        (NestedType::Infer(l), NestedType::Infer(r)) => {
            // Both sides are *unknown*, using the sig_map to infer the return type.
            let actuals = vec![None, None];
            let sig = infer_type_name(&FUNC_SIG_MAP, func_type, &actuals)?;
            Ok((
                sig.ret_type != l.into(),
                sig.ret_type != r.into(),
                sig.ret_type.into(),
            ))
        }
    }
}

/// Special exprs that cannot be handled by [`infer_type_name`] and [`FuncSigMap`] are handled here.
/// These include variadic functions, list and struct type, as well as non-implicit cast.
///
/// We should aim for enhancing the general inferring framework and reduce the special cases here.
///
/// Returns:
/// * `Err` when we are sure this expr is invalid
/// * `Ok(Some(t))` when the return type should be `t` under these special rules
/// * `Ok(None)` when no special rule matches and it should try general rules later
fn infer_type_for_special(
    func_type: ExprType,
    inputs: &mut Vec<ExprImpl>,
) -> Result<Option<DataType>> {
    match func_type {
        ExprType::Case => {
            let len = inputs.len();
            align_types(inputs.iter_mut().enumerate().filter_map(|(i, e)| {
                // `Case` organize `inputs` as (cond, res) pairs with a possible `else` res at
                // the end. So we align exprs at odd indices as well as the last one when length
                // is odd.
                match i.is_odd() || len.is_odd() && i == len - 1 {
                    true => Some(e),
                    false => None,
                }
            }))
            .map(Some)
            .map_err(Into::into)
        }
        ExprType::In => {
            align_types(inputs.iter_mut())?;
            Ok(Some(DataType::Boolean))
        }
        ExprType::Coalesce => {
            ensure_arity!("coalesce", 1 <= | inputs |);
            align_types(inputs.iter_mut()).map(Some).map_err(Into::into)
        }
        ExprType::ConcatWs => {
            ensure_arity!("concat_ws", 2 <= | inputs |);
            let inputs_owned = std::mem::take(inputs);
            *inputs = inputs_owned
                .into_iter()
                .enumerate()
                .map(|(i, input)| match i {
                    // 0-th arg must be string
                    0 => input.cast_implicit(DataType::Varchar).map_err(Into::into),
                    // subsequent can be any type, using the output format
                    _ => input.cast_output(),
                })
                .try_collect()?;
            Ok(Some(DataType::Varchar))
        }
        ExprType::ConcatOp => {
            let inputs_owned = std::mem::take(inputs);
            *inputs = inputs_owned
                .into_iter()
                .map(|input| input.cast_explicit(DataType::Varchar))
                .try_collect()?;
            Ok(Some(DataType::Varchar))
        }
        ExprType::IsNotNull => {
            ensure_arity!("is_not_null", | inputs | == 1);
            match inputs[0].return_type() {
                DataType::Struct(_) | DataType::List { .. } => Ok(Some(DataType::Boolean)),
                _ => Ok(None),
            }
        }
        ExprType::IsNull => {
            ensure_arity!("is_null", | inputs | == 1);
            match inputs[0].return_type() {
                DataType::Struct(_) | DataType::List { .. } => Ok(Some(DataType::Boolean)),
                _ => Ok(None),
            }
        }
        ExprType::Equal
        | ExprType::NotEqual
        | ExprType::LessThan
        | ExprType::LessThanOrEqual
        | ExprType::GreaterThan
        | ExprType::GreaterThanOrEqual
        | ExprType::IsDistinctFrom
        | ExprType::IsNotDistinctFrom => {
            ensure_arity!("cmp", | inputs | == 2);
            match (inputs[0].is_unknown(), inputs[1].is_unknown()) {
                // `'a' = null` handled by general rules later
                (true, true) => return Ok(None),
                // `null = array[1]` where null should have same type as right side
                // `null = 1` can use the general rule, but return `Ok(None)` here is less readable
                (true, false) => {
                    let owned = std::mem::replace(&mut inputs[0], ExprImpl::literal_bool(true));
                    inputs[0] = owned.cast_implicit(inputs[1].return_type())?;
                    return Ok(Some(DataType::Boolean));
                }
                (false, true) => {
                    let owned = std::mem::replace(&mut inputs[1], ExprImpl::literal_bool(true));
                    inputs[1] = owned.cast_implicit(inputs[0].return_type())?;
                    return Ok(Some(DataType::Boolean));
                }
                // Types of both sides are known. Continue.
                (false, false) => {}
            }
            let ok = match (inputs[0].return_type(), inputs[1].return_type()) {
                // Cast each field of the struct to their corresponding field in the other struct.
                (DataType::Struct(_), DataType::Struct(_)) => {
                    let (lcast, rcast, ret) = infer_struct_cast_target_type(
                        func_type,
                        extract_expr_nested_type(&inputs[0])?,
                        extract_expr_nested_type(&inputs[1])?,
                    )?;
                    if lcast {
                        let owned0 =
                            std::mem::replace(&mut inputs[0], ExprImpl::literal_bool(true));
                        inputs[0] =
                            FunctionCall::new_unchecked(ExprType::Cast, vec![owned0], ret.clone())
                                .into();
                    }
                    if rcast {
                        let owned1 =
                            std::mem::replace(&mut inputs[1], ExprImpl::literal_bool(true));
                        inputs[1] =
                            FunctionCall::new_unchecked(ExprType::Cast, vec![owned1], ret).into();
                    }
                    true
                }
                // Unlike auto-cast in struct, PostgreSQL disallows `int[] = bigint[]` for array.
                // They have to match exactly.
                (l @ DataType::List { .. }, r @ DataType::List { .. }) => l == r,
                // use general rule unless `struct = struct` or `array = array`
                _ => return Ok(None),
            };
            if ok {
                Ok(Some(DataType::Boolean))
            } else {
                Err(ErrorCode::BindError(format!(
                    "cannot compare {} and {}",
                    inputs[0].return_type(),
                    inputs[1].return_type()
                ))
                .into())
            }
        }
        ExprType::RegexpMatch => {
            ensure_arity!("regexp_match", 2 <= | inputs | <= 3);
            if inputs.len() == 3 {
                match &inputs[2] {
                    ExprImpl::Literal(flag) => {
                        match flag.get_data() {
                            Some(flag) => {
                                let ScalarImpl::Utf8(flag) = flag else {
                                    return Err(ErrorCode::BindError(
                                        "flag in regexp_match must be a literal string".to_string(),
                                    ).into());
                                };
                                for c in flag.chars() {
                                    if c == 'g' {
                                        return Err(ErrorCode::InvalidInputSyntax(
                                            "regexp_match() does not support the \"global\" option. Use the regexp_matches function instead."
                                                .to_string(),
                                        )
                                        .into());
                                    }
                                    if !"ic".contains(c) {
                                        return Err(ErrorCode::NotImplemented(
                                            format!("invalid regular expression option: \"{c}\""),
                                            None.into(),
                                        )
                                        .into());
                                    }
                                }
                            }
                            None => {
                                // flag is NULL. Will return NULL.
                            }
                        }
                    }
                    _ => {
                        return Err(ErrorCode::BindError(
                            "flag in regexp_match must be a literal string".to_string(),
                        )
                        .into())
                    }
                }
            }
            Ok(Some(DataType::List {
                datatype: Box::new(DataType::Varchar),
            }))
        }
        ExprType::ArrayCat => {
            ensure_arity!("array_cat", | inputs | == 2);
            let left_type = inputs[0].return_type();
            let right_type = inputs[1].return_type();
            let return_type = match (&left_type, &right_type) {
                (
                    DataType::List {
                        datatype: left_elem_type,
                    },
                    DataType::List {
                        datatype: right_elem_type,
                    },
                ) => {
                    if let Ok(res) = align_types(inputs.iter_mut()) {
                        Some(res)
                    } else if **left_elem_type == right_type {
                        Some(left_type.clone())
                    } else if left_type == **right_elem_type {
                        Some(right_type.clone())
                    } else {
                        let common_type = align_array_and_element(0, 1, inputs)
                            .or_else(|_| align_array_and_element(1, 0, inputs));
                        match common_type {
                            Ok(casted) => Some(casted),
                            Err(err) => return Err(err.into()),
                        }
                    }
                }
                _ => None,
            };
            Ok(Some(return_type.ok_or_else(|| {
                ErrorCode::BindError(format!(
                    "Cannot concatenate {} and {}",
                    left_type, right_type
                ))
            })?))
        }
        ExprType::ArrayAppend => {
            ensure_arity!("array_append", | inputs | == 2);
            let common_type = align_array_and_element(0, 1, inputs);
            match common_type {
                Ok(casted) => Ok(Some(casted)),
                Err(_) => Err(ErrorCode::BindError(format!(
                    "Cannot append {} to {}",
                    inputs[0].return_type(),
                    inputs[1].return_type()
                ))
                .into()),
            }
        }
        ExprType::ArrayPrepend => {
            ensure_arity!("array_prepend", | inputs | == 2);
            let common_type = align_array_and_element(1, 0, inputs);
            match common_type {
                Ok(casted) => Ok(Some(casted)),
                Err(_) => Err(ErrorCode::BindError(format!(
                    "Cannot prepend {} to {}",
                    inputs[0].return_type(),
                    inputs[1].return_type()
                ))
                .into()),
            }
        }
        ExprType::ArrayDistinct => {
            ensure_arity!("array_distinct", | inputs | == 1);
            let ret_type = inputs[0].return_type();
            if inputs[0].is_unknown() {
                return Err(ErrorCode::BindError(
                    "could not determine polymorphic type because input has type unknown"
                        .to_string(),
                )
                .into());
            }
            match ret_type {
                DataType::List {
                    datatype: list_elem_type,
                } => Ok(Some(DataType::List {
                    datatype: list_elem_type,
                })),
                _ => Ok(None),
            }
        }
        ExprType::Vnode => {
            ensure_arity!("vnode", 1 <= | inputs |);
            Ok(Some(DataType::Int16))
        }
        ExprType::Now => {
            ensure_arity!("now", | inputs | <= 1);
            Ok(Some(DataType::Timestamptz))
        }
        _ => Ok(None),
    }
}

/// From all available functions in `sig_map`, find and return the best matching `FuncSign` for the
/// provided `func_type` and `inputs`. This not only support exact function signature match, but can
/// also match `substr(varchar, smallint)` or even `substr(varchar, unknown)` to `substr(varchar,
/// int)`.
///
/// This corresponds to the `PostgreSQL` rules on operators and functions here:
/// * <https://www.postgresql.org/docs/current/typeconv-oper.html>
/// * <https://www.postgresql.org/docs/current/typeconv-func.html>
///
/// To summarize,
/// 1. Find all functions with matching `func_type` and argument count.
/// 2. For binary operator with unknown on exactly one side, try to find an exact match assuming
///    both sides are same type.
/// 3. Rank candidates based on most matching positions. This covers Rule 2, 4a, 4c and 4d in
///    `PostgreSQL`. See [`top_matches`] for details.
/// 4. Attempt to narrow down candidates by selecting type categories for unknowns. This covers Rule
///    4e in `PostgreSQL`. See [`narrow_category`] for details.
/// 5. Attempt to narrow down candidates by assuming all arguments are same type. This covers Rule
///    4f in `PostgreSQL`. See [`narrow_same_type`] for details.
fn infer_type_name<'a>(
    sig_map: &'a FuncSigMap,
    func_type: ExprType,
    inputs: &[Option<DataTypeName>],
) -> Result<&'a FuncSign> {
    let candidates = sig_map.get_with_arg_nums(func_type, inputs.len());

    // Binary operators have a special `unknown` handling rule for exact match. We do not
    // distinguish operators from functions as of now.
    if inputs.len() == 2 {
        let t = match (inputs[0], inputs[1]) {
            (None, t) => Ok(t),
            (t, None) => Ok(t),
            (Some(_), Some(_)) => Err(()),
        };
        if let Ok(Some(t)) = t {
            let exact = candidates.iter().find(|sig| sig.inputs_type == [t, t]);
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
                inputs.iter().map(TypeDebug).collect_vec()
            ),
            112.into(),
        )
        .into());
    }

    // After this line `candidates` will never be empty, as the narrow rules will retain original
    // list when no desirable candidates can be selected.

    candidates = narrow_category(candidates, inputs);

    candidates = narrow_same_type(candidates, inputs);

    match &candidates[..] {
        [] => unreachable!(),
        [sig] => Ok(sig),
        _ => Err(ErrorCode::BindError(format!(
            "function {:?}{:?} is not unique\nHINT:  Could not choose a best candidate function. You might need to add explicit type casts.",
            func_type,
            inputs.iter().map(TypeDebug).collect_vec(),
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
        T::Float64 | T::Boolean | T::Varchar | T::Timestamptz | T::Interval
    )
}

/// Checks if `source` can be implicitly casted to `target`. Generally we do not consider it being a
/// valid cast when they are of the same type, because it may deserve special treatment. This is
/// also the behavior of underlying [`cast_ok_base`].
///
/// Sometimes it is more convenient to include equality when checking whether a formal parameter can
/// accept an actual argument. So we introduced `eq_ok` to control this behavior.
fn implicit_ok(source: DataTypeName, target: DataTypeName, eq_ok: bool) -> bool {
    eq_ok && source == target || cast_ok_base(source, target, CastContext::Implicit)
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
/// Corresponding implementation in `PostgreSQL`:
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
fn top_matches<'a>(
    candidates: &'a [FuncSign],
    inputs: &[Option<DataTypeName>],
) -> Vec<&'a FuncSign> {
    let mut best_exact = 0;
    let mut best_preferred = 0;
    let mut best_candidates = Vec::new();

    for sig in candidates {
        let mut n_exact = 0;
        let mut n_preferred = 0;
        let mut castable = true;
        for (formal, actual) in sig.inputs_type.iter().zip_eq_fast(inputs) {
            let Some(actual) = actual else { continue };
            if formal == actual {
                n_exact += 1;
            } else if !implicit_ok(*actual, *formal, false) {
                castable = false;
                break;
            }
            if is_preferred(*formal) {
                n_preferred += 1;
            }
        }
        if !castable {
            continue;
        }
        if n_exact > best_exact || n_exact == best_exact && n_preferred > best_preferred {
            best_exact = n_exact;
            best_preferred = n_preferred;
            best_candidates.clear();
        }
        if n_exact == best_exact && n_preferred == best_preferred {
            best_candidates.push(sig);
        }
    }
    best_candidates
}

/// Attempt to narrow down candidates by selecting type categories for unknowns. This covers Rule 4e
/// in [`PostgreSQL`](https://www.postgresql.org/docs/current/typeconv-func.html).
///
/// This is done in 2 phases:
/// * First, check each unknown position individually and try to find a type category for it.
///   * If any candidate accept varchar, varchar is selected;
///   * otherwise, if all candidate accept the same category, select this category.
///     * Also record whether any candidate accept the preferred type within this category.
/// * When all unknown positions are assigned their type categories, discard a candidate if at any
///   position
///   * it does not agree with the type category assignment;
///   * the assigned type category contains a preferred type but the candidate is not preferred.
///
/// If the first phase fails or the second phase gives an empty set, this attempt preserves original
/// list untouched.
///
/// Corresponding implementation in `PostgreSQL`:
/// * Line 1164 - Line 1298 of `func_select_candidate()` in `parse_func.c` [14.0/86a4dc1][rule 4e
///   src]
///
/// [rule 4e src]: https://github.com/postgres/postgres/blob/86a4dc1e6f29d1992a2afa3fac1a0b0a6e84568c/src/backend/parser/parse_func.c#L1164-L1298
fn narrow_category<'a>(
    candidates: Vec<&'a FuncSign>,
    inputs: &[Option<DataTypeName>],
) -> Vec<&'a FuncSign> {
    const BIASED_TYPE: DataTypeName = DataTypeName::Varchar;
    let Ok(categories) = inputs.iter().enumerate().map(|(i, actual)| {
        // This closure returns
        // * Err(()) when a category cannot be selected
        // * Ok(None) when actual argument is non-null and can skip selection
        // * Ok(Some(t)) when the selected category is `t`
        //
        // Here `t` is actually just one type within that selected category, rather than the
        // category itself. It is selected to be the [`super::least_restrictive`] over all
        // candidates. This makes sure that `t` is the preferred type if any candidate accept it.
        if actual.is_some() {
            return Ok(None);
        }
        let mut category = Ok(candidates[0].inputs_type[i]);
        for sig in &candidates[1..] {
            let formal = sig.inputs_type[i];
            if formal == BIASED_TYPE || category == Ok(BIASED_TYPE) {
                category = Ok(BIASED_TYPE);
                break;
            }
            // formal != BIASED_TYPE && category.is_err():
            // - Category conflict err can only be solved by a later varchar. Skip this candidate.
            let Ok(selected) = category else { continue };
            // least_restrictive or mark temporary conflict err
            if implicit_ok(formal, selected, true) {
                // noop
            } else if implicit_ok(selected, formal, false) {
                category = Ok(formal);
            } else {
                category = Err(());
            }
        }
        category.map(Some)
    }).try_collect::<_, Vec<_>, _>() else {
        // First phase failed.
        return candidates;
    };
    let cands_temp = candidates
        .iter()
        .filter(|sig| {
            sig.inputs_type
                .iter()
                .zip_eq_fast(&categories)
                .all(|(formal, category)| {
                    // category.is_none() means the actual argument is non-null and skipped category
                    // selection.
                    let Some(selected) = category else { return true };
                    *formal == *selected
                        || !is_preferred(*selected) && implicit_ok(*formal, *selected, false)
                })
        })
        .copied()
        .collect_vec();
    match cands_temp.is_empty() {
        true => candidates,
        false => cands_temp,
    }
}

/// Attempt to narrow down candidates by assuming all arguments are same type. This covers Rule 4f
/// in [`PostgreSQL`](https://www.postgresql.org/docs/current/typeconv-func.html).
///
/// If all non-null arguments are same type, assume all unknown arguments are of this type as well.
/// Discard a candidate if its parameter type cannot be casted from this type.
///
/// If the condition is not met or the result is empty, this attempt preserves original list
/// untouched.
///
/// Note this rule cannot replace special treatment given to binary operators in [Rule 2], because
/// this runs after varchar-biased Rule 4e ([`narrow_category`]), and has no preference between
/// exact-match and castable-match.
///
/// Corresponding implementation in `PostgreSQL`:
/// * Line 1300 - Line 1355 of `func_select_candidate()` in `parse_func.c` [14.0/86a4dc1][rule 4f
///   src]
///
/// [rule 4f src]: https://github.com/postgres/postgres/blob/86a4dc1e6f29d1992a2afa3fac1a0b0a6e84568c/src/backend/parser/parse_func.c#L1300-L1355
/// [Rule 2]: https://www.postgresql.org/docs/current/typeconv-oper.html#:~:text=then%20assume%20it%20is%20the%20same%20type%20as%20the%20other%20argument%20for%20this%20check
fn narrow_same_type<'a>(
    candidates: Vec<&'a FuncSign>,
    inputs: &[Option<DataTypeName>],
) -> Vec<&'a FuncSign> {
    let Ok(Some(same_type)) = inputs.iter().try_fold(None, |acc, cur| match (acc, cur) {
        (None, t) => Ok(*t),
        (t, None) => Ok(t),
        (Some(l), Some(r)) if l == *r => Ok(Some(l)),
        _ => Err(())
    }) else {
        return candidates;
    };
    let cands_temp = candidates
        .iter()
        .filter(|sig| {
            sig.inputs_type
                .iter()
                .all(|formal| implicit_ok(same_type, *formal, true))
        })
        .copied()
        .collect_vec();
    match cands_temp.is_empty() {
        true => candidates,
        false => cands_temp,
    }
}

struct TypeDebug<'a>(&'a Option<DataTypeName>);
impl<'a> std::fmt::Debug for TypeDebug<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            Some(t) => t.fmt(f),
            None => write!(f, "unknown"),
        }
    }
}

#[cfg(test)]
mod tests {
    use itertools::iproduct;

    use super::*;

    fn infer_type_v0(func_type: ExprType, inputs_type: Vec<DataType>) -> Result<DataType> {
        let mut inputs = inputs_type
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
        infer_type(func_type, &mut inputs)
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
            (Int16, Float32, Float64),
            (Int16, Float64, Float64),
            (Int32, Int16, Int32),
            (Int32, Int32, Int32),
            (Int32, Int64, Int64),
            (Int32, Decimal, Decimal),
            (Int32, Float32, Float64),
            (Int32, Float64, Float64),
            (Int64, Int16, Int64),
            (Int64, Int32, Int64),
            (Int64, Int64, Int64),
            (Int64, Decimal, Decimal),
            (Int64, Float32, Float64),
            (Int64, Float64, Float64),
            (Decimal, Int16, Decimal),
            (Decimal, Int32, Decimal),
            (Decimal, Int64, Decimal),
            (Decimal, Decimal, Decimal),
            (Decimal, Float32, Float64),
            (Decimal, Float64, Float64),
            (Float32, Int16, Float64),
            (Float32, Int32, Float64),
            (Float32, Int64, Float64),
            (Float32, Decimal, Float64),
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

    #[test]
    fn test_match_implicit() {
        use DataTypeName as T;
        // func_name and ret_type does not affect the overload resolution logic
        const DUMMY_FUNC: ExprType = ExprType::Add;
        const DUMMY_RET: T = T::Int32;
        let testcases: [(
            &'static str,
            &'static [&'static [T]],
            &'static [Option<T>],
            std::result::Result<&'static [T], &'static str>,
        ); 10] = [
            (
                "Binary special rule prefers arguments of same type.",
                &[
                    &[T::Int32, T::Int32],
                    &[T::Int32, T::Varchar],
                    &[T::Int32, T::Float64],
                ],
                &[Some(T::Int32), None],
                Ok(&[T::Int32, T::Int32]),
            ),
            (
                "Without binary special rule, Rule 4e selects varchar.",
                &[
                    &[T::Int32, T::Int32, T::Int32],
                    &[T::Int32, T::Int32, T::Varchar],
                    &[T::Int32, T::Int32, T::Float64],
                ],
                &[Some(T::Int32), Some(T::Int32), None],
                Ok(&[T::Int32, T::Int32, T::Varchar]),
            ),
            (
                "Without binary special rule, Rule 4e selects preferred type.",
                &[
                    &[T::Int32, T::Int32, T::Int32],
                    &[T::Int32, T::Int32, T::Float64],
                ],
                &[Some(T::Int32), Some(T::Int32), None],
                Ok(&[T::Int32, T::Int32, T::Float64]),
            ),
            (
                "Without binary special rule, Rule 4f treats exact-match and cast-match equally.",
                &[
                    &[T::Int32, T::Int32, T::Int32],
                    &[T::Int32, T::Int32, T::Float32],
                ],
                &[Some(T::Int32), Some(T::Int32), None],
                Err("not unique"),
            ),
            (
                "`top_matches` ranks by exact count then preferred count",
                &[
                    &[T::Float64, T::Float64, T::Float64, T::Timestamptz], /* 0 exact 3 preferred */
                    &[T::Float64, T::Int32, T::Float32, T::Timestamp],     // 1 exact 1 preferred
                    &[T::Float32, T::Float32, T::Int32, T::Timestamptz],   // 1 exact 0 preferred
                    &[T::Int32, T::Float64, T::Float32, T::Timestamptz],   // 1 exact 1 preferred
                    &[T::Int32, T::Int16, T::Int32, T::Timestamptz], // 2 exact 1 non-castable
                    &[T::Int32, T::Float64, T::Float32, T::Date],    // 1 exact 1 preferred
                ],
                &[Some(T::Int32), Some(T::Int32), Some(T::Int32), None],
                Ok(&[T::Int32, T::Float64, T::Float32, T::Timestamptz]),
            ),
            (
                "Rule 4e fails and Rule 4f unique.",
                &[
                    &[T::Int32, T::Int32, T::Time],
                    &[T::Int32, T::Int32, T::Int32],
                ],
                &[None, Some(T::Int32), None],
                Ok(&[T::Int32, T::Int32, T::Int32]),
            ),
            (
                "Rule 4e empty and Rule 4f unique.",
                &[
                    &[T::Int32, T::Int32, T::Varchar],
                    &[T::Int32, T::Int32, T::Int32],
                    &[T::Varchar, T::Int32, T::Int32],
                ],
                &[None, Some(T::Int32), None],
                Ok(&[T::Int32, T::Int32, T::Int32]),
            ),
            (
                "Rule 4e varchar resolves prior category conflict.",
                &[
                    &[T::Int32, T::Int32, T::Float32],
                    &[T::Time, T::Int32, T::Int32],
                    &[T::Varchar, T::Int32, T::Int32],
                ],
                &[None, Some(T::Int32), None],
                Ok(&[T::Varchar, T::Int32, T::Int32]),
            ),
            (
                "Rule 4f fails.",
                &[
                    &[T::Float32, T::Float32, T::Float32, T::Float32],
                    &[T::Decimal, T::Decimal, T::Int64, T::Decimal],
                ],
                &[Some(T::Int16), Some(T::Int32), None, Some(T::Int64)],
                Err("not unique"),
            ),
            (
                "Rule 4f all unknown.",
                &[
                    &[T::Float32, T::Float32, T::Float32, T::Float32],
                    &[T::Decimal, T::Decimal, T::Int64, T::Decimal],
                ],
                &[None, None, None, None],
                Err("not unique"),
            ),
        ];
        for (desc, candidates, inputs, expected) in testcases {
            let mut sig_map = FuncSigMap::default();
            candidates.into_iter().for_each(|formals| {
                sig_map.insert(FuncSign {
                    name: "add",
                    func: DUMMY_FUNC,
                    inputs_type: formals,
                    ret_type: DUMMY_RET,
                    build: |_, _| unreachable!(),
                })
            });
            let result = infer_type_name(&sig_map, DUMMY_FUNC, inputs);
            match (expected, result) {
                (Ok(expected), Ok(found)) => {
                    assert_eq!(expected, found.inputs_type, "case `{}`", desc)
                }
                (Ok(_), Err(err)) => panic!("case `{}` unexpected error: {:?}", desc, err),
                (Err(_), Ok(f)) => panic!(
                    "case `{}` expect error but found: {:?}",
                    desc, f.inputs_type
                ),
                (Err(expected), Err(err)) => assert!(
                    err.to_string().contains(expected),
                    "case `{}` expect err `{}` != {:?}",
                    desc,
                    expected,
                    err
                ),
            }
        }
    }
}
