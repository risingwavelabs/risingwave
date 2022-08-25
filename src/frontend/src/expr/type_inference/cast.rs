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

use itertools::Itertools as _;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::DataType;

use super::DataTypeName;
use crate::expr::{Expr as _, ExprImpl};

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

/// The context a cast operation is invoked in. An implicit cast operation is allowed in a context
/// that allows explicit casts, but not vice versa. See details in
/// [PG](https://www.postgresql.org/docs/current/catalog-pg-cast.html).
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum CastContext {
    Implicit,
    Assign,
    Explicit,
}

pub type CastMap = HashMap<(DataTypeName, DataTypeName), CastContext>;

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
        (
            DataType::Varchar,
            DataType::List {
                datatype: target_elem,
            },
        ) if target_elem == &Box::new(DataType::Varchar) => true,
        (
            DataType::Varchar,
            DataType::List {
                datatype: target_elem,
            },
        ) => cast_ok(&DataType::Varchar, target_elem, allows),
        _ => false,
    }
}

fn build_cast_map() -> CastMap {
    use DataTypeName as T;

    // Implicit cast operations in PG are organized in 3 sequences, with the reverse direction being
    // assign cast operations.
    // https://github.com/postgres/postgres/blob/e0064f0ff6dfada2695330c6bc1945fa7ae813be/src/include/catalog/pg_cast.dat#L18-L20
    let mut m = HashMap::new();
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
}

fn insert_cast_seq(
    m: &mut HashMap<(DataTypeName, DataTypeName), CastContext>,
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

lazy_static::lazy_static! {
    pub static ref CAST_MAP: CastMap = {
        build_cast_map()
    };
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
}
