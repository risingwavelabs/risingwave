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

use risingwave_common::types::{
    Date, Decimal, Interval, JsonbVal, ScalarImpl, Time, Timestamp, Timestamptz,
};

impl<'a> postgres_types::FromSql<'a> for ScalarImpl {
    fn from_sql(
        ty: &postgres_types::Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(match *ty {
            postgres_types::Type::BOOL => {
                ScalarImpl::from(<bool as postgres_types::FromSql>::from_sql(ty, raw)?)
            }
            postgres_types::Type::INT2 => {
                ScalarImpl::from(<i16 as postgres_types::FromSql>::from_sql(ty, raw)?)
            }
            postgres_types::Type::INT4 => {
                ScalarImpl::from(<i32 as postgres_types::FromSql>::from_sql(ty, raw)?)
            }
            postgres_types::Type::INT8 => {
                ScalarImpl::from(<i64 as postgres_types::FromSql>::from_sql(ty, raw)?)
            }
            postgres_types::Type::FLOAT4 => {
                ScalarImpl::from(<f32 as postgres_types::FromSql>::from_sql(ty, raw)?)
            }
            postgres_types::Type::FLOAT8 => {
                ScalarImpl::from(<f64 as postgres_types::FromSql>::from_sql(ty, raw)?)
            }
            postgres_types::Type::DATE => {
                ScalarImpl::from(<Date as postgres_types::FromSql>::from_sql(ty, raw)?)
            }
            postgres_types::Type::TIME => {
                ScalarImpl::from(<Time as postgres_types::FromSql>::from_sql(ty, raw)?)
            }
            postgres_types::Type::TIMESTAMP => {
                ScalarImpl::from(<Timestamp as postgres_types::FromSql>::from_sql(ty, raw)?)
            }
            postgres_types::Type::TIMESTAMPTZ => {
                ScalarImpl::from(<Timestamptz as postgres_types::FromSql>::from_sql(ty, raw)?)
            }
            postgres_types::Type::JSONB => ScalarImpl::from(JsonbVal::from_sql(ty, raw)?),
            postgres_types::Type::INTERVAL => ScalarImpl::from(Interval::from_sql(ty, raw)?),
            postgres_types::Type::BYTEA => {
                ScalarImpl::from(Vec::<u8>::from_sql(ty, raw)?.into_boxed_slice())
            }
            postgres_types::Type::VARCHAR | postgres_types::Type::TEXT => {
                ScalarImpl::from(String::from_sql(ty, raw)?)
            }
            // Serial, Int256, Struct, List and Decimal are not supported here
            // Note: The Decimal type is specially handled in the `ScalarAdapter`.
            _ => bail_not_implemented!("the postgres decoding for {ty} is unsupported"),
        })
    }

    fn accepts(ty: &postgres_types::Type) -> bool {
        matches!(
            *ty,
            postgres_types::Type::BOOL
                | postgres_types::Type::INT2
                | postgres_types::Type::INT4
                | postgres_types::Type::INT8
                | postgres_types::Type::FLOAT4
                | postgres_types::Type::FLOAT8
                | postgres_types::Type::DATE
                | postgres_types::Type::TIME
                | postgres_types::Type::TIMESTAMP
                | postgres_types::Type::TIMESTAMPTZ
                | postgres_types::Type::JSONB
                | postgres_types::Type::INTERVAL
                | postgres_types::Type::BYTEA
                | postgres_types::Type::VARCHAR
                | postgres_types::Type::TEXT
        )
    }
}

/// The following table shows the mapping between Rust types and Sql Server types in tiberius.
/// |Rust Type|Sql Server Type|
/// |`u8`|`tinyint`|
/// |`i16`|`smallint`|
/// |`i32`|`int`|
/// |`i64`|`bigint`|
/// |`f32`|`float(24)`|
/// |`f64`|`float(53)`|
/// |`bool`|`bit`|
/// |`String`/`&str`|`nvarchar`/`varchar`/`nchar`/`char`/`ntext`/`text`|
/// |`Vec<u8>`/`&[u8]`|`binary`/`varbinary`/`image`|
/// |[`Uuid`]|`uniqueidentifier`|
/// |[`Numeric`]|`numeric`/`decimal`|
/// |[`Decimal`] (with feature flag `rust_decimal`)|`numeric`/`decimal`|
/// |[`XmlData`]|`xml`|
/// |[`NaiveDateTime`] (with feature flag `chrono`)|`datetime`/`datetime2`/`smalldatetime`|
/// |[`NaiveDate`] (with feature flag `chrono`)|`date`|
/// |[`NaiveTime`] (with feature flag `chrono`)|`time`|
/// |[`DateTime`] (with feature flag `chrono`)|`datetimeoffset`|
///
/// See the [`time`] module for more information about the date and time structs.
///
/// [`Row#get`]: struct.Row.html#method.get
/// [`Row#try_get`]: struct.Row.html#method.try_get
/// [`time`]: time/index.html
/// [`Uuid`]: struct.Uuid.html
/// [`Numeric`]: numeric/struct.Numeric.html
/// [`Decimal`]: numeric/struct.Decimal.html
/// [`XmlData`]: xml/struct.XmlData.html
/// [`NaiveDateTime`]: time/chrono/struct.NaiveDateTime.html
/// [`NaiveDate`]: time/chrono/struct.NaiveDate.html
/// [`NaiveTime`]: time/chrono/struct.NaiveTime.html
/// [`DateTime`]: time/chrono/struct.DateTime.html
impl<'a> tiberius::FromSql<'a> for ScalarImpl {
    fn from_sql(value: &'a tiberius::ColumnData<'static>) -> tiberius::Result<Option<Self>> {
        Ok(match &value {
            tiberius::ColumnData::U8(_) => {
                <u8 as tiberius::FromSql>::from_sql(value)?.map(|v| ScalarImpl::from(v as i16))
            }
            tiberius::ColumnData::I16(_) => {
                <i16 as tiberius::FromSql>::from_sql(value)?.map(ScalarImpl::from)
            }
            tiberius::ColumnData::I32(_) => {
                <i32 as tiberius::FromSql>::from_sql(value)?.map(ScalarImpl::from)
            }
            tiberius::ColumnData::I64(_) => {
                <i64 as tiberius::FromSql>::from_sql(value)?.map(ScalarImpl::from)
            }
            tiberius::ColumnData::F32(_) => {
                <f32 as tiberius::FromSql>::from_sql(value)?.map(ScalarImpl::from)
            }
            tiberius::ColumnData::F64(_) => {
                <f64 as tiberius::FromSql>::from_sql(value)?.map(ScalarImpl::from)
            }
            tiberius::ColumnData::Bit(_) => {
                <bool as tiberius::FromSql>::from_sql(value)?.map(ScalarImpl::from)
            }
            tiberius::ColumnData::String(_) => {
                <&str as tiberius::FromSql>::from_sql(value)?.map(ScalarImpl::from)
            }
            tiberius::ColumnData::Numeric(_) => {
                <Decimal as tiberius::FromSql>::from_sql(value)?.map(ScalarImpl::from)
            }
            tiberius::ColumnData::DateTime(_)
            | tiberius::ColumnData::DateTime2(_)
            | tiberius::ColumnData::SmallDateTime(_) => {
                <Timestamp as tiberius::FromSql>::from_sql(value)?.map(ScalarImpl::from)
            }
            tiberius::ColumnData::Time(_) => {
                <Time as tiberius::FromSql>::from_sql(value)?.map(ScalarImpl::from)
            }
            tiberius::ColumnData::Date(_) => {
                <Date as tiberius::FromSql>::from_sql(value)?.map(ScalarImpl::from)
            }
            tiberius::ColumnData::DateTimeOffset(_) => {
                <Timestamptz as tiberius::FromSql>::from_sql(value)?.map(ScalarImpl::from)
            }
            tiberius::ColumnData::Binary(_) => {
                <&[u8] as tiberius::FromSql>::from_sql(value)?.map(ScalarImpl::from)
            }
            tiberius::ColumnData::Guid(_) | tiberius::ColumnData::Xml(_) => {
                return Err(tiberius::error::Error::Conversion(
                    format!(
                        "the sql server decoding for {:?} is unsupported",
                        value.type_name()
                    )
                    .into(),
                ))
            }
        })
    }
}
