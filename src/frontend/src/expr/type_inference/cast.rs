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
use risingwave_common::error::ErrorCode;
use risingwave_common::types::{DataType, DataTypeName};
use risingwave_common::util::iter_util::ZipEqFast;
pub use risingwave_expr::sig::cast::*;

use crate::expr::{Expr as _, ExprImpl, InputRef};

/// Find the least restrictive type. Used by `VALUES`, `CASE`, `UNION`, etc.
/// It is a simplified version of the rule used in
/// [PG](https://www.postgresql.org/docs/current/typeconv-union-case.html).
///
/// If you also need to cast them to this type, and there are more than 2 exprs, check out
/// [`align_types`].
pub fn least_restrictive(lhs: DataType, rhs: DataType) -> std::result::Result<DataType, ErrorCode> {
    if lhs == rhs {
        Ok(lhs)
    } else if cast_ok(&lhs, &rhs, CastContext::Implicit) {
        Ok(rhs)
    } else if cast_ok(&rhs, &lhs, CastContext::Implicit) {
        Ok(lhs)
    } else {
        Err(ErrorCode::BindError(format!(
            "types {:?} and {:?} cannot be matched",
            lhs, rhs
        )))
    }
}

/// Find the `least_restrictive` type over a list of `exprs`, and add implicit cast when necessary.
/// Used by `VALUES`, `CASE`, `UNION`, etc. See [PG](https://www.postgresql.org/docs/current/typeconv-union-case.html).
pub fn align_types<'a>(
    exprs: impl Iterator<Item = &'a mut ExprImpl>,
) -> std::result::Result<DataType, ErrorCode> {
    use std::mem::swap;

    let exprs = exprs.collect_vec();
    // Essentially a filter_map followed by a try_reduce, which is unstable.
    let mut ret_type = None;
    for e in &exprs {
        if e.is_unknown() {
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
        // unwrap: cast to least_restrictive type always succeeds
        *e = dummy.cast_implicit(ret_type.clone()).unwrap();
    }
    Ok(ret_type)
}

/// Aligns an array and an element by returning a possible common array type and casting them into
/// the common type.
///
/// `array_idx` and `element_idx` indicate which element in inputs is the array and which the
/// element.
///
/// Example: `align_array_and_element(numeric[], int) -> numeric[]`
pub fn align_array_and_element(
    array_idx: usize,
    element_indices: &[usize],
    inputs: &mut [ExprImpl],
) -> std::result::Result<DataType, ErrorCode> {
    let mut dummy_element = match inputs[array_idx].is_unknown() {
        // when array is unknown type, make an unknown typed value (e.g. null)
        true => ExprImpl::literal_null(DataType::Varchar),
        false => {
            let array_element_type = match inputs[array_idx].return_type() {
                DataType::List(t) => *t,
                t => return Err(ErrorCode::BindError(format!("expects array but got {t}"))),
            };
            // use InputRef rather than literal_null so it is always typed, even for varchar
            InputRef::new(0, array_element_type).into()
        }
    };
    assert_eq!(dummy_element.is_unknown(), inputs[array_idx].is_unknown());

    let common_element_type = align_types(
        inputs
            .iter_mut()
            .enumerate()
            .filter_map(|(i, e)| element_indices.contains(&i).then_some(e))
            .chain(std::iter::once(&mut dummy_element)),
    )?;
    let array_type = DataType::List(Box::new(common_element_type));

    // elements are already casted by `align_types`, we cast the array argument here
    let array_owned = std::mem::replace(&mut inputs[array_idx], ExprImpl::literal_bool(false));
    inputs[array_idx] = array_owned.cast_implicit(array_type.clone())?;

    Ok(array_type)
}

/// Checks whether casting from `source` to `target` is ok in `allows` context.
pub fn cast_ok(source: &DataType, target: &DataType, allows: CastContext) -> bool {
    cast_ok_struct(source, target, allows)
        || cast_ok_array(source, target, allows)
        || cast_ok_base(source.into(), target.into(), allows)
}

pub fn cast_ok_base(source: DataTypeName, target: DataTypeName, allows: CastContext) -> bool {
    matches!(CAST_MAP.get(&(source, target)), Some(context) if *context <= allows)
}

fn cast_ok_struct(source: &DataType, target: &DataType, allows: CastContext) -> bool {
    match (source, target) {
        (DataType::Struct(lty), DataType::Struct(rty)) => {
            if lty.is_empty() || rty.is_empty() {
                unreachable!("record type should be already processed at this point");
            }
            if lty.len() != rty.len() {
                // only cast structs of the same length
                return false;
            }
            // ... and all fields are castable
            lty.types()
                .zip_eq_fast(rty.types())
                .all(|(src, dst)| src == dst || cast_ok(src, dst, allows))
        }
        // The automatic casts to string types are treated as assignment casts, while the automatic
        // casts from string types are explicit-only.
        // https://www.postgresql.org/docs/14/sql-createcast.html#id-1.9.3.58.7.4
        (DataType::Varchar, DataType::Struct(_)) => CastContext::Explicit <= allows,
        (DataType::Struct(_), DataType::Varchar) => CastContext::Assign <= allows,
        _ => false,
    }
}

fn cast_ok_array(source: &DataType, target: &DataType, allows: CastContext) -> bool {
    match (source, target) {
        (DataType::List(source_elem), DataType::List(target_elem)) => {
            cast_ok(source_elem, target_elem, allows)
        }
        // The automatic casts to string types are treated as assignment casts, while the automatic
        // casts from string types are explicit-only.
        // https://www.postgresql.org/docs/14/sql-createcast.html#id-1.9.3.58.7.4
        (DataType::Varchar, DataType::List(_)) => CastContext::Explicit <= allows,
        (DataType::List(_), DataType::Varchar) => CastContext::Assign <= allows,
        _ => false,
    }
}

pub fn cast_map_array() -> Vec<(DataTypeName, DataTypeName, CastContext)> {
    CAST_MAP
        .iter()
        .map(|((src, target), ctx)| (*src, *target, *ctx))
        .collect_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

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
            T::Timestamptz,
            T::Time,
            T::Interval,
        ];
        all_types
            .iter()
            .map(|source| {
                all_types
                    .iter()
                    .map(|target| match cast_ok(source, target, allows) {
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
                "             ", // varchar
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
                "  T    T     ", // bool
                "  TTTTTT     ",
                "TT TTTTT     ",
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
    }
}
