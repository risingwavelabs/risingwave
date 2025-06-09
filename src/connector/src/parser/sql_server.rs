// Copyright 2025 RisingWave Labs
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

use std::collections::HashSet;
use std::str::FromStr;
use std::sync::LazyLock;

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use risingwave_common::catalog::Schema;
use risingwave_common::log::LogSuppresser;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, Date, Decimal, ScalarImpl, Time, Timestamp, Timestamptz};
use rust_decimal::Decimal as RustDecimal;
use thiserror_ext::AsReport;
use tiberius::Row;
use tiberius::xml::XmlData;
use uuid::Uuid;

use crate::parser::utils::log_error;

static LOG_SUPPERSSER: LazyLock<LogSuppresser> = LazyLock::new(LogSuppresser::default);

pub fn sql_server_row_to_owned_row(row: &mut Row, schema: &Schema) -> OwnedRow {
    let mut datums: Vec<Option<ScalarImpl>> = vec![];
    let mut money_fields: HashSet<&str> = HashSet::new();
    // Special handling of the money field, as the third-party library Tiberius converts the money type to i64.
    for (column, _) in row.cells() {
        if column.column_type() == tiberius::ColumnType::Money {
            money_fields.insert(column.name());
        }
    }
    for i in 0..schema.fields.len() {
        let rw_field = &schema.fields[i];
        let name = rw_field.name.as_str();
        let datum = match money_fields.contains(name) {
            true => match row.try_get::<i64, usize>(i) {
                Ok(Some(value)) => Some(convert_money_i64_to_type(value, &rw_field.data_type)),
                Ok(None) => None,
                Err(err) => {
                    log_error!(name, err, "parse column failed");
                    None
                }
            },
            false => match row.try_get::<ScalarImplTiberiusWrapper, usize>(i) {
                Ok(datum) => datum.map(|d| d.0),
                Err(err) => {
                    log_error!(name, err, "parse column failed");
                    None
                }
            },
        };

        datums.push(datum);
    }
    OwnedRow::new(datums)
}

pub fn convert_money_i64_to_type(value: i64, data_type: &DataType) -> ScalarImpl {
    match data_type {
        DataType::Decimal => {
            ScalarImpl::Decimal(Decimal::from(value) / Decimal::from_str("10000").unwrap())
        }
        _ => {
            panic!(
                "Conversion of Money type to {:?} is not supported",
                data_type
            );
        }
    }
}
macro_rules! impl_tiberius_wrapper {
    ($wrapper_name:ident, $variant_name:ident) => {
        pub struct $wrapper_name($variant_name);

        impl From<$variant_name> for $wrapper_name {
            fn from(value: $variant_name) -> Self {
                Self(value)
            }
        }
    };
}

impl_tiberius_wrapper!(ScalarImplTiberiusWrapper, ScalarImpl);
impl_tiberius_wrapper!(TimeTiberiusWrapper, Time);
impl_tiberius_wrapper!(DateTiberiusWrapper, Date);
impl_tiberius_wrapper!(TimestampTiberiusWrapper, Timestamp);
impl_tiberius_wrapper!(TimestamptzTiberiusWrapper, Timestamptz);
impl_tiberius_wrapper!(DecimalTiberiusWrapper, Decimal);

macro_rules! impl_chrono_tiberius_wrapper {
    ($wrapper_name:ident, $variant_name:ident, $chrono:ty) => {
        impl<'a> tiberius::IntoSql<'a> for $wrapper_name {
            fn into_sql(self) -> tiberius::ColumnData<'a> {
                self.0.0.into_sql()
            }
        }

        impl<'a> tiberius::FromSql<'a> for $wrapper_name {
            fn from_sql(
                value: &'a tiberius::ColumnData<'static>,
            ) -> tiberius::Result<Option<Self>> {
                let instant = <$chrono>::from_sql(value)?;
                let time = instant.map($variant_name::from).map($wrapper_name::from);
                tiberius::Result::Ok(time)
            }
        }
    };
}

impl_chrono_tiberius_wrapper!(TimeTiberiusWrapper, Time, NaiveTime);
impl_chrono_tiberius_wrapper!(DateTiberiusWrapper, Date, NaiveDate);
impl_chrono_tiberius_wrapper!(TimestampTiberiusWrapper, Timestamp, NaiveDateTime);

impl<'a> tiberius::IntoSql<'a> for DecimalTiberiusWrapper {
    fn into_sql(self) -> tiberius::ColumnData<'a> {
        match self.0 {
            Decimal::Normalized(d) => d.into_sql(),
            Decimal::NaN => tiberius::ColumnData::Numeric(None),
            Decimal::PositiveInf => tiberius::ColumnData::Numeric(None),
            Decimal::NegativeInf => tiberius::ColumnData::Numeric(None),
        }
    }
}

impl<'a> tiberius::FromSql<'a> for DecimalTiberiusWrapper {
    // TODO(kexiang): will sql server have inf/-inf/nan for decimal?
    fn from_sql(value: &'a tiberius::ColumnData<'static>) -> tiberius::Result<Option<Self>> {
        tiberius::Result::Ok(
            RustDecimal::from_sql(value)?
                .map(Decimal::Normalized)
                .map(DecimalTiberiusWrapper::from),
        )
    }
}

impl<'a> tiberius::IntoSql<'a> for TimestamptzTiberiusWrapper {
    fn into_sql(self) -> tiberius::ColumnData<'a> {
        self.0.to_datetime_utc().into_sql()
    }
}

impl<'a> tiberius::FromSql<'a> for TimestamptzTiberiusWrapper {
    fn from_sql(value: &'a tiberius::ColumnData<'static>) -> tiberius::Result<Option<Self>> {
        let instant = DateTime::<Utc>::from_sql(value)?;
        let time = instant
            .map(Timestamptz::from)
            .map(TimestamptzTiberiusWrapper::from);
        tiberius::Result::Ok(time)
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
impl<'a> tiberius::FromSql<'a> for ScalarImplTiberiusWrapper {
    fn from_sql(value: &'a tiberius::ColumnData<'static>) -> tiberius::Result<Option<Self>> {
        Ok(match &value {
            tiberius::ColumnData::U8(_) => u8::from_sql(value)?
                .map(|v| ScalarImplTiberiusWrapper::from(ScalarImpl::from(v as i16))),
            tiberius::ColumnData::I16(_) => i16::from_sql(value)?
                .map(ScalarImpl::from)
                .map(ScalarImplTiberiusWrapper::from),
            tiberius::ColumnData::I32(_) => i32::from_sql(value)?
                .map(ScalarImpl::from)
                .map(ScalarImplTiberiusWrapper::from),
            tiberius::ColumnData::I64(_) => i64::from_sql(value)?
                .map(ScalarImpl::from)
                .map(ScalarImplTiberiusWrapper::from),
            tiberius::ColumnData::F32(_) => f32::from_sql(value)?
                .map(ScalarImpl::from)
                .map(ScalarImplTiberiusWrapper::from),
            tiberius::ColumnData::F64(_) => f64::from_sql(value)?
                .map(ScalarImpl::from)
                .map(ScalarImplTiberiusWrapper::from),
            tiberius::ColumnData::Bit(_) => bool::from_sql(value)?
                .map(ScalarImpl::from)
                .map(ScalarImplTiberiusWrapper::from),
            tiberius::ColumnData::String(_) => <&str>::from_sql(value)?
                .map(ScalarImpl::from)
                .map(ScalarImplTiberiusWrapper::from),
            tiberius::ColumnData::Numeric(_) => DecimalTiberiusWrapper::from_sql(value)?
                .map(|w| ScalarImpl::from(w.0))
                .map(ScalarImplTiberiusWrapper::from),
            tiberius::ColumnData::DateTime(_)
            | tiberius::ColumnData::DateTime2(_)
            | tiberius::ColumnData::SmallDateTime(_) => TimestampTiberiusWrapper::from_sql(value)?
                .map(|w| ScalarImpl::from(w.0))
                .map(ScalarImplTiberiusWrapper::from),
            tiberius::ColumnData::Time(_) => TimeTiberiusWrapper::from_sql(value)?
                .map(|w| ScalarImpl::from(w.0))
                .map(ScalarImplTiberiusWrapper::from),
            tiberius::ColumnData::Date(_) => DateTiberiusWrapper::from_sql(value)?
                .map(|w| ScalarImpl::from(w.0))
                .map(ScalarImplTiberiusWrapper::from),
            tiberius::ColumnData::DateTimeOffset(_) => TimestamptzTiberiusWrapper::from_sql(value)?
                .map(|w| ScalarImpl::from(w.0))
                .map(ScalarImplTiberiusWrapper::from),
            tiberius::ColumnData::Binary(_) => <&[u8]>::from_sql(value)?
                .map(ScalarImpl::from)
                .map(ScalarImplTiberiusWrapper::from),
            tiberius::ColumnData::Guid(_) => <Uuid>::from_sql(value)?
                .map(|uuid| uuid.to_string().to_uppercase())
                .map(ScalarImpl::from)
                .map(ScalarImplTiberiusWrapper::from),
            tiberius::ColumnData::Xml(_) => <&XmlData>::from_sql(value)?
                .map(|xml| xml.clone().into_string())
                .map(ScalarImpl::from)
                .map(ScalarImplTiberiusWrapper::from),
        })
    }
}

/// The following table shows the mapping between Rust types and Sql Server types in tiberius.
/// |Rust type|Sql Server type|
/// |--------|--------|
/// |`u8`|`tinyint`|
/// |`i16`|`smallint`|
/// |`i32`|`int`|
/// |`i64`|`bigint`|
/// |`f32`|`float(24)`|
/// |`f64`|`float(53)`|
/// |`bool`|`bit`|
/// |`String`/`&str` (< 4000 characters)|`nvarchar(4000)`|
/// |`String`/`&str`|`nvarchar(max)`|
/// |`Vec<u8>`/`&[u8]` (< 8000 bytes)|`varbinary(8000)`|
/// |`Vec<u8>`/`&[u8]`|`varbinary(max)`|
/// |[`Uuid`]|`uniqueidentifier`|
/// |[`Numeric`]|`numeric`/`decimal`|
/// |[`Decimal`] (with feature flag `rust_decimal`)|`numeric`/`decimal`|
/// |[`BigDecimal`] (with feature flag `bigdecimal`)|`numeric`/`decimal`|
/// |[`XmlData`]|`xml`|
/// |[`NaiveDate`] (with `chrono` feature, TDS 7.3 >)|`date`|
/// |[`NaiveTime`] (with `chrono` feature, TDS 7.3 >)|`time`|
/// |[`DateTime`] (with `chrono` feature, TDS 7.3 >)|`datetimeoffset`|
/// |[`NaiveDateTime`] (with `chrono` feature, TDS 7.3 >)|`datetime2`|
/// |[`NaiveDateTime`] (with `chrono` feature, TDS 7.2)|`datetime`|
///
/// It is possible to use some of the types to write into columns that are not
/// of the same type. For example on systems following the TDS 7.3 standard (SQL
/// Server 2008 and later), the chrono type `NaiveDateTime` can also be used to
/// write to `datetime`, `datetime2` and `smalldatetime` columns. All string
/// types can also be used with `ntext`, `text`, `varchar`, `nchar` and `char`
/// columns. All binary types can also be used with `binary` and `image`
/// columns.
///
/// See the [`time`] module for more information about the date and time structs.
///
/// [`Client#query`]: struct.Client.html#method.query
/// [`Client#execute`]: struct.Client.html#method.execute
/// [`time`]: time/index.html
/// [`Uuid`]: struct.Uuid.html
/// [`Numeric`]: numeric/struct.Numeric.html
/// [`Decimal`]: numeric/struct.Decimal.html
/// [`BigDecimal`]: numeric/struct.BigDecimal.html
/// [`XmlData`]: xml/struct.XmlData.html
/// [`NaiveDateTime`]: time/chrono/struct.NaiveDateTime.html
/// [`NaiveDate`]: time/chrono/struct.NaiveDate.html
/// [`NaiveTime`]: time/chrono/struct.NaiveTime.html
/// [`DateTime`]: time/chrono/struct.DateTime.html
impl<'a> tiberius::IntoSql<'a> for ScalarImplTiberiusWrapper {
    fn into_sql(self) -> tiberius::ColumnData<'a> {
        match self.0 {
            ScalarImpl::Int16(v) => v.into_sql(),
            ScalarImpl::Int32(v) => v.into_sql(),
            ScalarImpl::Int64(v) => v.into_sql(),
            ScalarImpl::Float32(v) => v.0.into_sql(),
            ScalarImpl::Float64(v) => v.0.into_sql(),
            ScalarImpl::Bool(v) => v.into_sql(),
            ScalarImpl::Decimal(v) => DecimalTiberiusWrapper::from(v).into_sql(),
            ScalarImpl::Date(v) => DateTiberiusWrapper::from(v).into_sql(),
            ScalarImpl::Timestamp(v) => TimestampTiberiusWrapper::from(v).into_sql(),
            ScalarImpl::Timestamptz(v) => TimestamptzTiberiusWrapper::from(v).into_sql(),
            ScalarImpl::Time(v) => TimeTiberiusWrapper::from(v).into_sql(),
            ScalarImpl::Utf8(v) => String::from(v).into_sql(),
            // ScalarImpl::Bytea(v) => (*v.clone()).into_sql(),
            value => {
                // Utf8, Serial, Interval, Jsonb, Int256, Struct, List are not supported yet
                unimplemented!("the sql server decoding for {:?} is unsupported", value);
            }
        }
    }
}
