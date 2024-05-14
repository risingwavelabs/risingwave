// Copyright 2024 RisingWave Labs
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

use std::str::FromStr;

use anyhow::anyhow;
use bytes::BytesMut;
use pg_bigdecimal::PgNumeric;
use risingwave_common::types::{DataType, Decimal, Int256, ScalarImpl, ScalarRefImpl};
use thiserror_ext::AsReport;
use tokio_postgres::types::{to_sql_checked, FromSql, IsNull, Kind, ToSql, Type};

use crate::error::ConnectorResult;

#[derive(Clone, Debug)]
pub struct EnumString(pub String);

impl<'a> FromSql<'a> for EnumString {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + 'static + Sync + Send>> {
        Ok(EnumString(String::from_utf8_lossy(raw).into_owned()))
    }

    fn accepts(ty: &Type) -> bool {
        matches!(ty.kind(), Kind::Enum(_))
    }
}

impl ToSql for EnumString {
    to_sql_checked!();

    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>>
    where
        Self: Sized,
    {
        match ty.kind() {
            Kind::Enum(e) => {
                if e.contains(&self.0) {
                    out.extend_from_slice(self.0.as_bytes());
                    Ok(IsNull::No)
                } else {
                    Err(format!(
                        "EnumString value {} is not in the enum type {:?}",
                        self.0, e
                    )
                    .into())
                }
            }
            _ => Err("EnumString can only be used with ENUM types".into()),
        }
    }

    fn accepts(ty: &Type) -> bool {
        matches!(ty.kind(), Kind::Enum(_))
    }
}

/// Adapter for `ScalarImpl` to Postgres data type,
/// which can be used to encode/decode to/from Postgres value.
#[derive(Debug)]
pub(crate) enum ScalarAdapter<'a> {
    Builtin(ScalarRefImpl<'a>),
    Uuid(uuid::Uuid),
    Numeric(PgNumeric),
    Enum(EnumString),
}

impl ToSql for ScalarAdapter<'_> {
    to_sql_checked!();

    fn to_sql(
        &self,
        ty: &Type,
        out: &mut bytes::BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        match self {
            ScalarAdapter::Builtin(v) => v.to_sql(ty, out),
            ScalarAdapter::Uuid(v) => v.to_sql(ty, out),
            ScalarAdapter::Numeric(v) => v.to_sql(ty, out),
            ScalarAdapter::Enum(v) => v.to_sql(ty, out),
        }
    }

    fn accepts(_ty: &Type) -> bool {
        true
    }
}

/// convert from Postgres uuid, numeric and enum to `ScalarAdapter`
impl<'a> FromSql<'a> for ScalarAdapter<'_> {
    fn from_sql(
        ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        match ty.kind() {
            Kind::Simple => match *ty {
                Type::UUID => {
                    let uuid = uuid::Uuid::from_sql(ty, raw)?;
                    Ok(ScalarAdapter::Uuid(uuid))
                }
                Type::NUMERIC => {
                    let numeric = PgNumeric::from_sql(ty, raw)?;
                    Ok(ScalarAdapter::Numeric(numeric))
                }
                _ => Err(anyhow!("failed to convert type {:?} to ScalarAdapter", ty).into()),
            },
            Kind::Enum(_) => Ok(ScalarAdapter::Enum(EnumString::from_sql(ty, raw)?)),
            _ => Err(anyhow!("failed to convert type {:?} to ScalarAdapter", ty).into()),
        }
    }

    fn accepts(ty: &Type) -> bool {
        matches!(ty, &Type::UUID | &Type::NUMERIC) || <EnumString as FromSql>::accepts(ty)
    }
}

impl ScalarAdapter<'_> {
    pub fn name(&self) -> &'static str {
        match self {
            ScalarAdapter::Builtin(_) => "Builtin",
            ScalarAdapter::Uuid(_) => "Uuid",
            ScalarAdapter::Numeric(_) => "Numeric",
            ScalarAdapter::Enum(_) => "Enum",
        }
    }

    /// convert `ScalarRefImpl` to `ScalarAdapter` so that we can correctly encode to postgres value
    pub(crate) fn from_scalar<'a>(
        scalar: ScalarRefImpl<'a>,
        ty: &Type,
    ) -> ConnectorResult<ScalarAdapter<'a>> {
        Ok(match (scalar, ty, ty.kind()) {
            (ScalarRefImpl::Utf8(s), &Type::UUID, _) => ScalarAdapter::Uuid(s.parse()?),
            (ScalarRefImpl::Utf8(s), &Type::NUMERIC, _) => {
                ScalarAdapter::Numeric(string_to_pg_numeric(s)?)
            }
            (ScalarRefImpl::Int256(s), &Type::NUMERIC, _) => {
                ScalarAdapter::Numeric(string_to_pg_numeric(&s.to_string())?)
            }
            (ScalarRefImpl::Utf8(s), _, Kind::Enum(_)) => {
                ScalarAdapter::Enum(EnumString(s.to_owned()))
            }
            _ => ScalarAdapter::Builtin(scalar),
        })
    }

    pub fn into_scalar(self, ty: DataType) -> Option<ScalarImpl> {
        match (&self, &ty) {
            (ScalarAdapter::Builtin(scalar), _) => Some(scalar.into_scalar_impl()),
            (ScalarAdapter::Uuid(uuid), &DataType::Varchar) => {
                Some(ScalarImpl::from(uuid.to_string()))
            }
            (ScalarAdapter::Numeric(numeric), &DataType::Varchar) => {
                Some(ScalarImpl::from(pg_numeric_to_string(numeric)))
            }
            (ScalarAdapter::Numeric(numeric), &DataType::Int256) => {
                pg_numeric_to_rw_int256(numeric)
            }
            (ScalarAdapter::Enum(EnumString(s)), &DataType::Varchar) => Some(ScalarImpl::from(s)),
            _ => {
                tracing::error!(
                    adapter = self.name(),
                    rw_type = ty.pg_name(),
                    "failed to convert from ScalarAdapter: invalid conversion"
                );
                None
            }
        }
    }

    pub fn build_scalar_in_list(
        vec: Vec<Option<ScalarAdapter<'_>>>,
        ty: DataType,
        mut builder: risingwave_common::array::ArrayBuilderImpl,
    ) -> Option<risingwave_common::array::ArrayBuilderImpl> {
        for val in vec {
            let scalar = match (val, &ty) {
                (Some(ScalarAdapter::Numeric(numeric)), &DataType::Varchar) => {
                    if pg_numeric_is_special(&numeric) {
                        return None;
                    } else {
                        Some(ScalarImpl::from(pg_numeric_to_string(&numeric)))
                    }
                }
                (Some(ScalarAdapter::Numeric(numeric)), &DataType::Int256) => match numeric {
                    PgNumeric::Normalized(big_decimal) => {
                        match Int256::from_str(big_decimal.to_string().as_str()) {
                            Ok(num) => Some(ScalarImpl::from(num)),
                            Err(err) => {
                                tracing::error!(error = %err.as_report(), "parse pg-numeric as rw_int256 failed");
                                return None;
                            }
                        }
                    }
                    _ => return None,
                },
                (Some(ScalarAdapter::Numeric(numeric)), &DataType::Decimal) => match numeric {
                    PgNumeric::Normalized(big_decimal) => {
                        match Decimal::from_str(big_decimal.to_string().as_str()) {
                            Ok(num) => Some(ScalarImpl::from(num)),
                            Err(err) => {
                                tracing::error!(error = %err.as_report(), "parse pg-numeric as rw-numeric failed (likely out-of-range");
                                return None;
                            }
                        }
                    }
                    _ => return None,
                },
                (Some(_), _) => unreachable!(
                    "into_scalar_in_list should only be called with ScalarAdapter::Numeric types"
                ),
                (None, _) => None,
            };
            builder.append(scalar);
        }
        Some(builder)
    }
}

fn pg_numeric_is_special(val: &PgNumeric) -> bool {
    matches!(
        val,
        PgNumeric::NegativeInf | PgNumeric::PositiveInf | PgNumeric::NaN
    )
}

fn pg_numeric_to_rw_int256(val: &PgNumeric) -> Option<ScalarImpl> {
    match Int256::from_str(pg_numeric_to_string(val).as_str()) {
        Ok(num) => Some(ScalarImpl::from(num)),
        Err(err) => {
            tracing::error!(error = %err.as_report(), "failed to convert PgNumeric to Int256");
            None
        }
    }
}

fn pg_numeric_to_string(val: &PgNumeric) -> String {
    // TODO(kexiang): NEGATIVE_INFINITY -> -Infinity, POSITIVE_INFINITY -> Infinity, NAN -> NaN
    // The current implementation is to ensure consistency with the behavior of cdc event parsor.
    match val {
        PgNumeric::NegativeInf => String::from("NEGATIVE_INFINITY"),
        PgNumeric::Normalized(big_decimal) => big_decimal.to_string(),
        PgNumeric::PositiveInf => String::from("POSITIVE_INFINITY"),
        PgNumeric::NaN => String::from("NAN"),
    }
}

fn string_to_pg_numeric(s: &str) -> super::ConnectorResult<PgNumeric> {
    Ok(match s {
        "NEGATIVE_INFINITY" => PgNumeric::NegativeInf,
        "POSITIVE_INFINITY" => PgNumeric::PositiveInf,
        "NAN" => PgNumeric::NaN,
        _ => PgNumeric::Normalized(s.parse().unwrap()),
    })
}
