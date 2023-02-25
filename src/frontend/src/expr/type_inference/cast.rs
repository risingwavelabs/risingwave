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
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::{DataType, DataTypeName};
use risingwave_common::util::iter_util::ZipEqFast;
pub use risingwave_expr::sig::cast::*;

use crate::expr::{Expr as _, ExprImpl};

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
    element_idx: usize,
    inputs: &mut Vec<ExprImpl>,
) -> std::result::Result<DataType, ErrorCode> {
    let array = inputs[array_idx].return_type();
    let element = inputs[element_idx].return_type();
    let array_ele_type_opt = match &array {
        DataType::List { datatype: array_et } => Some(array_et),
        _ => None,
    };
    let array_ele_type = array_ele_type_opt.ok_or_else(|| {
        ErrorCode::BindError(format!("cannot combine {} with {}", array, element))
    })?;

    // cast to least restrictive type or return error
    let common_ele_type = least_restrictive(*array_ele_type.clone(), element.clone());
    if common_ele_type.is_err() {
        return Err(ErrorCode::BindError(format!(
            "unable to find least restrictive type between {} and {}",
            element, array
        )));
    }

    // found common type
    let common_ele_type = common_ele_type.unwrap();
    let array_type = DataType::List {
        datatype: Box::new(common_ele_type.clone()),
    };

    // try to cast inputs to inputs to common type
    let inputs_owned = std::mem::take(inputs);

    let casted_res: Result<Vec<ExprImpl>> = inputs_owned
        .into_iter()
        .enumerate()
        .map(|(idx, input)| {
            if idx == array_idx {
                input.cast_implicit(array_type.clone()).map_err(Into::into)
            } else {
                input
                    .cast_implicit(common_ele_type.clone())
                    .map_err(Into::into)
            }
        })
        .try_collect();

    let casted = casted_res.map_err(|_| {
        ErrorCode::BindError(format!("unable to align between {} and {}", element, array))
    })?;

    *inputs = casted;
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
            if lty.fields.is_empty() || rty.fields.is_empty() {
                unreachable!("record type should be already processed at this point");
            }
            if lty.fields.len() != rty.fields.len() {
                // only cast structs of the same length
                return false;
            }
            // ... and all fields are castable
            lty.fields
                .iter()
                .zip_eq_fast(rty.fields.iter())
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
        (
            DataType::List {
                datatype: source_elem,
            },
            DataType::List {
                datatype: target_elem,
            },
        ) => cast_ok(source_elem, target_elem, allows),
        // The automatic casts to string types are treated as assignment casts, while the automatic
        // casts from string types are explicit-only.
        // https://www.postgresql.org/docs/14/sql-createcast.html#id-1.9.3.58.7.4
        (DataType::Varchar, DataType::List { datatype: _ }) => CastContext::Explicit <= allows,
        (DataType::List { datatype: _ }, DataType::Varchar) => CastContext::Assign <= allows,
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
