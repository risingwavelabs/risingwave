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

use std::collections::BTreeMap;
use std::sync::LazyLock;

use itertools::Itertools as _;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::{DataType, DataTypeName};

use crate::expr::{Expr as _, ExprImpl};

// helper for determine_nesting_level
fn calc_nesting_level_inner(dt: DataType, level: i32) -> i32 {
    let return_val: i32 = match dt {
        DataType::List { datatype: inner } => calc_nesting_level_inner(*inner, level + 1),
        _ => level,
    };
    return_val
}

/// True if lhs is more nested, else false
///
/// Examples:
/// `calc_nesting_level(DataType::Boolean) -> 0`
/// `calc_nesting_level(List{DataType::Int16}) -> 1`
/// `calc_nesting_level(List{List{DataType::Boolean}}) -> 2`
pub fn calc_nesting_level(dt: DataType) -> i32 {
    calc_nesting_level_inner(dt, 0)
}

/// True if lhs is more nested, else false
///
/// Examples:
/// `lhs_is_more_nested(DataType::Boolean, DataType::Boolean) -> false`
/// `lhs_is_more_nested(List{List{DataType::Boolean}}, DataType::Date) -> true`
/// `lhs_is_more_nested(List{DataType::Int16}, List{List{DataType::Boolean}}) -> false`
pub fn lhs_is_more_nested(lhs: DataType, rhs: DataType) -> bool {
    let lhs_level = calc_nesting_level(lhs);
    let rhs_level = calc_nesting_level(rhs);
    lhs_level > rhs_level
}

/// Get the more nested element. Returns rhs if both are equally nested
///
/// Examples:
/// `get_most_nested(DataType::Boolean, DataType::Boolean) -> DataType::Boolean`
/// `get_most_nested(DataType::Date, List{List{DataType::Boolean}}) ->
/// List{List{DataType::Boolean}}` `get_most_nested(List{DataType::Int16},
/// List{List{DataType::Boolean}}) -> List{List{DataType::Boolean}}`
pub fn get_most_nested(lhs: DataType, rhs: DataType) -> DataType {
    if lhs_is_more_nested(lhs.clone(), rhs.clone()) {
        return lhs;
    }
    rhs
}

// helper for add_nesting
pub fn add_nesting_inner(target_dt: DataType, add_levels: i32) -> DataType {
    if add_levels <= 0 {
        return target_dt;
    }
    add_nesting_inner(
        DataType::List {
            datatype: Box::new(target_dt),
        },
        add_levels - 1,
    )
}

/// Add levels to target dt until it is as nested as `target_nesting`
///
/// Examples:
/// `add_nesting(DataType::Boolean, DataType::Boolean) -> DataType::Boolean`
/// `add_nesting(DataType::Date, List{List{DataType::Boolean}}) -> List{List{DataType::Date}}`
/// `add_nesting(List{List{DataType::Int16}}, DataType::Interval) -> List{List{DataType::Int16}} //
/// already more nested`
pub fn add_nesting(target_dt: DataType, target_nesting: DataType) -> DataType {
    let target_dt_level = calc_nesting_level(target_dt.clone());
    let target_nesting_level = calc_nesting_level(target_nesting);
    add_nesting_inner(target_dt, target_nesting_level - target_dt_level)
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
    } else if cast_ok(&lhs, &rhs, CastContext::Implicit) {
        Ok(rhs)
    } else if cast_ok(&rhs, &lhs, CastContext::Implicit) {
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
        *e = dummy.cast_implicit(ret_type.clone())?;
    }
    Ok(ret_type)
}

/// aligns an array and an element by returning a possible common array type and casting them into
/// the common type
/// array_idx and element_idx indicate which element in inputs is the array and which the element
///  Example: align_array_and_element(numeric[], int) -> numeric[]
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
    let most_nested = get_most_nested(element.clone(), array.clone());
    let array_type = add_nesting(common_ele_type.clone(), most_nested);

    // try to cast inputs to inputs to common type
    let inputs_owned = std::mem::take(inputs);
    let casted_res: Result<Vec<ExprImpl>> = inputs_owned
        .into_iter()
        .map(|input| {
            let x = input.return_type();
            input.cast_implicit(add_nesting(common_ele_type.clone(), x))
        })
        .try_collect();

    let casted = casted_res.map_err(|_| {
        ErrorCode::BindError(format!("unable to align between {} and {}", element, array))
    })?;

    *inputs = casted;
    Ok(array_type)
}

/// The context a cast operation is invoked in. An implicit cast operation is allowed in a context
/// that allows explicit casts, but not vice versa. See details in
/// [PG](https://www.postgresql.org/docs/current/catalog-pg-cast.html).
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum CastContext {
    Implicit,
    Assign,
    Explicit,
}

pub type CastMap = BTreeMap<(DataTypeName, DataTypeName), CastContext>;

impl From<&CastContext> for String {
    fn from(c: &CastContext) -> Self {
        match c {
            CastContext::Implicit => "IMPLICIT".to_string(),
            CastContext::Assign => "ASSIGN".to_string(),
            CastContext::Explicit => "EXPLICIT".to_string(),
        }
    }
}

/// Checks whether casting from `source` to `target` is ok in `allows` context.
pub fn cast_ok(source: &DataType, target: &DataType, allows: CastContext) -> bool {
    cast_ok_array(source, target, allows) || cast_ok_base(source.into(), target.into(), allows)
}

pub fn cast_ok_base(source: DataTypeName, target: DataTypeName, allows: CastContext) -> bool {
    matches!(CAST_MAP.get(&(source, target)), Some(context) if *context <= allows)
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

pub static CAST_MAP: LazyLock<CastMap> = LazyLock::new(|| {
    use DataTypeName as T;

    // Implicit cast operations in PG are organized in 3 sequences, with the reverse direction being
    // assign cast operations.
    // https://github.com/postgres/postgres/blob/e0064f0ff6dfada2695330c6bc1945fa7ae813be/src/include/catalog/pg_cast.dat#L18-L20
    let mut m = BTreeMap::new();
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
        m.insert((T::Varchar, t), CastContext::Explicit);
    }

    // Misc casts allowed by PG that are neither in implicit cast sequences nor from/to string.
    m.insert((T::Timestamp, T::Time), CastContext::Assign);
    m.insert((T::Timestampz, T::Time), CastContext::Assign);
    m.insert((T::Boolean, T::Int32), CastContext::Explicit);
    m.insert((T::Int32, T::Boolean), CastContext::Explicit);
    m
});

fn insert_cast_seq(
    m: &mut BTreeMap<(DataTypeName, DataTypeName), CastContext>,
    types: &[DataTypeName],
) {
    for (source_index, source_type) in types.iter().enumerate() {
        for (target_index, target_type) in types.iter().enumerate() {
            let cast_context = match source_index.cmp(&target_index) {
                std::cmp::Ordering::Less => CastContext::Implicit,
                // Unnecessary cast between the same type should have been removed.
                // Note that sizing cast between `NUMERIC(18, 3)` and `NUMERIC(20, 4)` or between
                // `int` and `int not null` may still be necessary. But we do not have such types
                // yet.
                std::cmp::Ordering::Equal => continue,
                std::cmp::Ordering::Greater => CastContext::Assign,
            };
            m.insert((*source_type, *target_type), cast_context);
        }
    }
}

pub fn cast_map_array() -> Vec<(DataTypeName, DataTypeName, CastContext)> {
    CAST_MAP
        .iter()
        .map(|((src, target), ctx)| (*src, *target, *ctx))
        .collect_vec()
}

#[derive(Clone)]
pub struct CastSig {
    pub from_type: DataTypeName,
    pub to_type: DataTypeName,
    pub context: CastContext,
}

pub fn cast_sigs() -> impl Iterator<Item = CastSig> {
    CAST_MAP
        .iter()
        .map(|((from_type, to_type), context)| CastSig {
            from_type: *from_type,
            to_type: *to_type,
            context: *context,
        })
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
            T::Timestampz,
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

    #[test]
    fn test_nesting_level_ok() {
        let dt = DataType::Boolean;
        let nested_1 = DataType::List {
            datatype: Box::new(dt.clone()),
        };
        let nested_2 = DataType::List {
            datatype: Box::new(nested_1.clone()),
        };
        let nested_3 = DataType::List {
            datatype: Box::new(nested_2.clone()),
        };
        assert_eq!(calc_nesting_level(dt), 0);
        assert_eq!(calc_nesting_level(nested_1), 1);
        assert_eq!(calc_nesting_level(nested_2), 2);
        assert_eq!(calc_nesting_level(nested_3), 3);
    }

    #[test]
    fn test_is_more_nested_ok() {
        let dt = DataType::Boolean;
        let nested_1 = DataType::List {
            datatype: Box::new(dt.clone()),
        };
        let nested_2 = DataType::List {
            datatype: Box::new(nested_1.clone()),
        };
        let nested_3 = DataType::List {
            datatype: Box::new(nested_2.clone()),
        };
        assert!(lhs_is_more_nested(nested_3.clone(), dt.clone()));
        assert!(lhs_is_more_nested(nested_1.clone(), dt.clone()));
        assert!(lhs_is_more_nested(nested_2.clone(), dt.clone()));
        assert!(lhs_is_more_nested(nested_3.clone(), nested_2.clone()));
        assert!(lhs_is_more_nested(nested_3.clone(), nested_1.clone()));

        // negated
        assert!(!lhs_is_more_nested(dt.clone(), nested_3.clone()));
        assert!(!lhs_is_more_nested(dt.clone(), nested_1.clone()));
        assert!(!lhs_is_more_nested(dt, nested_2.clone()));
        assert!(!lhs_is_more_nested(nested_2, nested_3.clone()));
        assert!(!lhs_is_more_nested(nested_1, nested_3));
    }
}
