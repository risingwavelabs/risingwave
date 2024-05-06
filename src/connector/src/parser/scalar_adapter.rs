use std::str::FromStr;

use anyhow::anyhow;
use bytes::BytesMut;
use pg_bigdecimal::PgNumeric;
use risingwave_common::types::{DataType, Int256, ScalarImpl, ScalarRefImpl};
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

    fn accepts(ty: &Type) -> bool {
        matches!(ty.kind(), Kind::Enum(_))
    }

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
}

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
        match *ty {
            Type::UUID => {
                let uuid = uuid::Uuid::from_sql(ty, raw)?;
                Ok(ScalarAdapter::Uuid(uuid))
            }
            Type::NUMERIC => {
                let numeric = PgNumeric::from_sql(ty, raw)?;
                Ok(ScalarAdapter::Numeric(numeric))
            }
            Type::ANYENUM => {
                let s = String::from_utf8(raw.to_vec())?;
                Ok(ScalarAdapter::Enum(EnumString(s)))
            }
            _ => Err(anyhow!("failed to convert type {:?} to ScalarAdapter", ty).into()),
        }
    }

    fn accepts(ty: &Type) -> bool {
        matches!(ty, &Type::UUID | &Type::NUMERIC | &Type::ANYENUM)
    }
}

impl ScalarAdapter<'_> {
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

    pub fn into_scalar(self, ty: DataType) -> ConnectorResult<ScalarImpl> {
        match (&self, &ty) {
            (ScalarAdapter::Builtin(scalar), _) => Ok(scalar.into_scalar_impl()),
            (ScalarAdapter::Uuid(uuid), &DataType::Varchar) => {
                Ok(ScalarImpl::from(uuid.to_string()))
            }
            (ScalarAdapter::Numeric(numeric), &DataType::Varchar) => {
                Ok(ScalarImpl::from(pg_numeric_to_string(numeric)))
            }
            (ScalarAdapter::Numeric(numeric), &DataType::Int256) => {
                pg_numeric_to_rw_int256(numeric)
            }
            (ScalarAdapter::Enum(EnumString(s)), &DataType::Varchar) => Ok(ScalarImpl::from(s)),
            _ => Err(anyhow!(
                "failed to convert ScalarAdapter from {:?} to {:?}",
                self,
                ty
            )
            .into()),
        }
    }
}

fn pg_numeric_to_rw_int256(val: &PgNumeric) -> ConnectorResult<ScalarImpl> {
    match Int256::from_str(pg_numeric_to_string(val).as_str()) {
        Ok(num) => Ok(ScalarImpl::from(num)),
        Err(err) => Err(anyhow!("failed to convert PgNumeric to Int256: {}", err).into()),
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
