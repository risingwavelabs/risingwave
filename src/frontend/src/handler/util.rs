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

use bytes::Bytes;
use itertools::Itertools;
use pgwire::pg_field_descriptor::{PgFieldDescriptor, TypeOid};
use pgwire::types::Row;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::{ColumnDesc, Field};
use risingwave_common::types::{DataType, ScalarRefImpl};

/// Format scalars according to postgres convention.
fn pg_value_format(d: ScalarRefImpl<'_>, format: bool) -> Bytes {
    // format == false means TEXT format
    // format == true means BINARY format
    if !format {
        match d {
            ScalarRefImpl::Bool(b) => if b { "t" } else { "f" }.into(),
            _ => d.to_string().into(),
        }
    } else {
        d.binary_serialize()
    }
}

pub fn to_pg_rows(chunk: DataChunk, format: bool) -> Vec<Row> {
    chunk
        .rows()
        .map(|r| {
            Row::new(
                r.values()
                    .map(|data| data.map(|data| pg_value_format(data, format)))
                    .collect_vec(),
            )
        })
        .collect_vec()
}

/// Convert column descs to rows which conclude name and type
pub fn col_descs_to_rows(columns: Vec<ColumnDesc>) -> Vec<Row> {
    columns
        .iter()
        .flat_map(|col| {
            col.flatten()
                .into_iter()
                .map(|c| {
                    let type_name = if let DataType::Struct { .. } = c.data_type {
                        c.type_name.clone()
                    } else {
                        format!("{:?}", &c.data_type)
                    };
                    Row::new(vec![Some(c.name.into()), Some(type_name.into())])
                })
                .collect_vec()
        })
        .collect_vec()
}

/// Convert from [`Field`] to [`PgFieldDescriptor`].
pub fn to_pg_field(f: &Field) -> PgFieldDescriptor {
    PgFieldDescriptor::new(f.name.clone(), data_type_to_type_oid(f.data_type()))
}

pub fn data_type_to_type_oid(data_type: DataType) -> TypeOid {
    match data_type {
        DataType::Int16 => TypeOid::SmallInt,
        DataType::Int32 => TypeOid::Int,
        DataType::Int64 => TypeOid::BigInt,
        DataType::Float32 => TypeOid::Float4,
        DataType::Float64 => TypeOid::Float8,
        DataType::Boolean => TypeOid::Boolean,
        DataType::Varchar => TypeOid::Varchar,
        DataType::Date => TypeOid::Date,
        DataType::Time => TypeOid::Time,
        DataType::Timestamp => TypeOid::Timestamp,
        DataType::Timestampz => TypeOid::Timestampz,
        DataType::Decimal => TypeOid::Decimal,
        DataType::Interval => TypeOid::Interval,
        DataType::Struct { .. } => TypeOid::Varchar,
        DataType::List { .. } => TypeOid::Varchar,
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::*;

    use super::*;

    #[test]
    fn test_to_pg_field() {
        let field = Field::with_name(DataType::Int32, "v1");
        let pg_field = to_pg_field(&field);
        assert_eq!(pg_field.get_name(), "v1");
        assert_eq!(
            pg_field.get_type_oid().as_number(),
            TypeOid::Int.as_number()
        );
    }

    #[test]
    fn test_to_pg_rows() {
        let chunk = DataChunk::from_pretty(
            "i I f    T
             1 6 6.01 aaa
             2 . .    .
             3 7 7.01 vvv
             4 . .    .  ",
        );
        let rows = to_pg_rows(chunk, false);
        let expected: Vec<Vec<Option<Bytes>>> = vec![
            vec![
                Some("1".into()),
                Some("6".into()),
                Some("6.01".into()),
                Some("aaa".into()),
            ],
            vec![Some("2".into()), None, None, None],
            vec![
                Some("3".into()),
                Some("7".into()),
                Some("7.01".into()),
                Some("vvv".into()),
            ],
            vec![Some("4".into()), None, None, None],
        ];
        let vec = rows
            .into_iter()
            .map(|r| r.values().iter().cloned().collect_vec())
            .collect_vec();

        assert_eq!(vec, expected);
    }

    #[test]
    fn test_value_format() {
        use ScalarRefImpl as S;

        let f = pg_value_format;
        assert_eq!(&f(S::Float32(1_f32.into()), false), "1");
        assert_eq!(&f(S::Float32(f32::NAN.into()), false), "NaN");
        assert_eq!(&f(S::Float64(f64::NAN.into()), false), "NaN");
        assert_eq!(&f(S::Float32(f32::INFINITY.into()), false), "Infinity");
        assert_eq!(&f(S::Float32(f32::NEG_INFINITY.into()), false), "-Infinity");
        assert_eq!(&f(S::Float64(f64::INFINITY.into()), false), "Infinity");
        assert_eq!(&f(S::Float64(f64::NEG_INFINITY.into()), false), "-Infinity");
        assert_eq!(&f(S::Bool(true), false), "t");
        assert_eq!(&f(S::Bool(false), false), "f");
    }
}
