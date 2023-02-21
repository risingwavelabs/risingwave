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

use std::collections::BTreeMap;
use std::sync::LazyLock;

use parse_display::Display;
use risingwave_common::types::DataTypeName;

#[derive(Clone, Debug)]
pub struct CastSig {
    pub from_type: DataTypeName,
    pub to_type: DataTypeName,
    pub context: CastContext,
}

/// The context a cast operation is invoked in. An implicit cast operation is allowed in a context
/// that allows explicit casts, but not vice versa. See details in
/// [PG](https://www.postgresql.org/docs/current/catalog-pg-cast.html).
#[derive(Clone, Copy, Debug, Display, Eq, Ord, PartialEq, PartialOrd)]
#[display(style = "UPPERCASE")]
pub enum CastContext {
    Implicit,
    Assign,
    Explicit,
}

impl CastSig {
    /// Returns a string describing the cast.
    pub fn to_string_no_return(&self) -> String {
        format!("cast({:?}->{:?})", self.from_type, self.to_type).to_lowercase()
    }
}

pub type CastMap = BTreeMap<(DataTypeName, DataTypeName), CastContext>;

pub fn cast_sigs() -> impl Iterator<Item = CastSig> {
    CAST_MAP
        .iter()
        .map(|((from_type, to_type), context)| CastSig {
            from_type: *from_type,
            to_type: *to_type,
            context: *context,
        })
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
    insert_cast_seq(&mut m, &[T::Date, T::Timestamp, T::Timestamptz]);
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
        T::Timestamptz,
        T::Time,
        T::Interval,
        T::Jsonb,
    ] {
        m.insert((t, T::Varchar), CastContext::Assign);
        m.insert((T::Varchar, t), CastContext::Explicit);
    }

    // Misc casts allowed by PG that are neither in implicit cast sequences nor from/to string.
    m.insert((T::Timestamp, T::Time), CastContext::Assign);
    m.insert((T::Timestamptz, T::Time), CastContext::Assign);
    m.insert((T::Boolean, T::Int32), CastContext::Explicit);
    m.insert((T::Int32, T::Boolean), CastContext::Explicit);

    // Casting from jsonb to bool / number.
    for t in [
        T::Boolean,
        T::Int16,
        T::Int32,
        T::Int64,
        T::Decimal,
        T::Float32,
        T::Float64,
    ] {
        m.insert((T::Jsonb, t), CastContext::Explicit);
    }

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
