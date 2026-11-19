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

use std::sync::LazyLock;

use anyhow::{Context, bail, ensure};
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use risingwave_common::catalog::Schema;
use risingwave_common::log::LogSuppressor;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{
    DataType, Date, Datum, Decimal, ScalarImpl, Time, Timestamp, Timestamptz,
};
use risingwave_common::util::iter_util::ZipEqFast;
use rust_decimal::Decimal as RustDecimal;
use thiserror_ext::AsReport;
use tiberius::xml::XmlData;
use tiberius::{Column, ColumnType, Row};
use uuid::Uuid;

use crate::parser::utils::log_error;

static LOG_SUPPRESSOR: LazyLock<LogSuppressor> = LazyLock::new(LogSuppressor::default);

/// Decoder built from the TDS metadata that precedes a SQL Server result set.
///
/// Tiberius keeps the payload representation (`i64` for MONEY) separate from
/// the TDS column type. Build the MONEY classification from the latter once
/// per result set and reuse it for all rows. `mssql_type_to_rw_type` maps the
/// corresponding schema type to [`DataType::Decimal`].
pub struct SqlServerRowDecoder {
    schema: Schema,
    money_columns: Vec<bool>,
}

impl SqlServerRowDecoder {
    pub fn new(columns: &[Column], schema: &Schema) -> anyhow::Result<Self> {
        ensure!(
            columns.len() == schema.len(),
            "SQL Server returned {} columns, but the planned schema has {} columns",
            columns.len(),
            schema.len()
        );

        let money_columns = columns
            .iter()
            .zip_eq_fast(&schema.fields)
            .map(|(column, field)| {
                let column_type = column.column_type();
                match (column_type, &field.data_type) {
                    (ColumnType::Money | ColumnType::Money4, DataType::Decimal) => Ok(true),
                    (ColumnType::Money | ColumnType::Money4, data_type) => bail!(
                        "SQL Server MONEY column `{}` has incompatible RisingWave type {}",
                        field.name,
                        data_type
                    ),
                    (ColumnType::Decimaln | ColumnType::Numericn, DataType::Decimal) => Ok(false),
                    (_, DataType::Decimal) => bail!(
                        "SQL Server column `{}` has TDS type {:?}, which is incompatible with the planned RisingWave Decimal type",
                        field.name,
                        column_type
                    ),
                    _ => Ok(false),
                }
            })
            .collect::<anyhow::Result<Vec<_>>>()?;

        Ok(Self {
            schema: schema.clone(),
            money_columns,
        })
    }

    fn validate_row(&self, row: &Row) -> anyhow::Result<()> {
        ensure!(
            row.result_index() == 0,
            "mssql_query returned more than one result set"
        );
        ensure!(
            row.columns().len() == self.schema.len(),
            "SQL Server row has {} columns, but the planned schema has {} columns",
            row.columns().len(),
            self.schema.len()
        );
        Ok(())
    }

    pub fn decode(&self, row: &mut Row) -> anyhow::Result<OwnedRow> {
        self.validate_row(row)?;

        let mut datums = Vec::with_capacity(self.schema.len());
        for (i, rw_field) in self.schema.fields.iter().enumerate() {
            let name = rw_field.name.as_str();
            let datum = match sql_server_cell_to_rw_datum(
                row,
                i,
                name,
                &rw_field.data_type,
                self.money_columns[i],
            ) {
                Ok(datum) => datum,
                Err(err) => {
                    log_error!(name, err, "parse column failed");
                    None
                }
            };
            datums.push(datum);
        }
        Ok(OwnedRow::new(datums))
    }

    /// Decode primary-key columns strictly while preserving the legacy
    /// lenient behavior for all other columns in a SQL Server CDC snapshot.
    pub fn decode_with_strict_pk(
        &self,
        row: &mut Row,
        pk_indices: &[usize],
    ) -> anyhow::Result<OwnedRow> {
        self.validate_row(row)?;

        super::decode_row_with_strict_pk(
            "SQL Server",
            &self.schema,
            pk_indices,
            |index, field| {
                sql_server_cell_to_rw_datum(
                    row,
                    index,
                    &field.name,
                    &field.data_type,
                    self.money_columns[index],
                )
            },
            |name, err| log_error!(name, err, "parse column failed"),
        )
    }
}

fn sql_server_cell_to_rw_datum(
    row: &Row,
    index: usize,
    name: &str,
    data_type: &DataType,
    is_money: bool,
) -> anyhow::Result<Datum> {
    if is_money {
        return row
            .try_get::<i64, usize>(index)
            .with_context(|| format!("failed to decode SQL Server money column `{name}`"))?
            .map(|value| try_convert_money_i64_to_type(value, data_type))
            .transpose();
    }

    Ok(row
        .try_get::<ScalarImplTiberiusWrapper, usize>(index)
        .with_context(|| format!("failed to decode SQL Server snapshot column `{name}`"))?
        .map(|datum| datum.0)
        .map(|scalar| coerce_scalar_to_target_type(scalar, data_type)))
}

fn coerce_scalar_to_target_type(scalar: ScalarImpl, target_type: &DataType) -> ScalarImpl {
    match (scalar, target_type) {
        // SQL Server validator allows integer upcast (e.g. `int` -> `BIGINT`).
        // Coerce snapshot values to the target RW type to keep validation and execution consistent.
        (ScalarImpl::Int16(v), DataType::Int32) => ScalarImpl::Int32(v as i32),
        (ScalarImpl::Int16(v), DataType::Int64) => ScalarImpl::Int64(v as i64),
        (ScalarImpl::Int32(v), DataType::Int64) => ScalarImpl::Int64(v as i64),
        // SQL Server `real` may map to `FLOAT` in RW validator.
        (ScalarImpl::Float32(v), DataType::Float64) => ScalarImpl::Float64((v.0 as f64).into()),
        (scalar, _) => scalar,
    }
}

fn try_convert_money_i64_to_type(value: i64, data_type: &DataType) -> anyhow::Result<ScalarImpl> {
    match data_type {
        DataType::Decimal => {
            // SQL Server MONEY and SMALLMONEY use a fixed scale of four.
            Ok(ScalarImpl::Decimal(Decimal::from_i128_with_scale(
                value as i128,
                4,
            )))
        }
        _ => bail!("conversion of SQL Server money to {data_type} is not supported"),
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::Field;
    use risingwave_common::types::F32;

    use super::*;

    #[test]
    fn test_row_decoder_uses_tds_money_metadata() {
        let columns = vec![
            Column::new("money".into(), ColumnType::Money),
            Column::new("decimal".into(), ColumnType::Decimaln),
        ];
        let schema = Schema::new(vec![
            Field::with_name(DataType::Decimal, "money"),
            Field::with_name(DataType::Decimal, "decimal"),
        ]);

        let decoder = SqlServerRowDecoder::new(&columns, &schema).unwrap();
        assert_eq!(decoder.money_columns, vec![true, false]);

        assert!(SqlServerRowDecoder::new(&columns[..1], &schema).is_err());

        let intn_column = [Column::new("not_money".into(), ColumnType::Intn)];
        let decimal_schema = Schema::new(vec![Field::with_name(DataType::Decimal, "not_money")]);
        assert!(SqlServerRowDecoder::new(&intn_column, &decimal_schema).is_err());
    }

    #[test]
    fn test_integer_upcast_coercion() {
        let v = coerce_scalar_to_target_type(ScalarImpl::Int32(7), &DataType::Int64);
        assert_eq!(v, ScalarImpl::Int64(7));

        let v = coerce_scalar_to_target_type(ScalarImpl::Int16(7), &DataType::Int32);
        assert_eq!(v, ScalarImpl::Int32(7));

        let v = coerce_scalar_to_target_type(ScalarImpl::Int16(7), &DataType::Int64);
        assert_eq!(v, ScalarImpl::Int64(7));
    }

    #[test]
    fn test_float_upcast_coercion() {
        let v =
            coerce_scalar_to_target_type(ScalarImpl::Float32(F32::from(1.25)), &DataType::Float64);
        assert_eq!(v, ScalarImpl::Float64(1.25.into()));
    }

    #[test]
    fn test_non_upcast_keeps_original() {
        let v = coerce_scalar_to_target_type(ScalarImpl::Int32(7), &DataType::Int32);
        assert_eq!(v, ScalarImpl::Int32(7));
    }

    /// `MONEY` / `SMALLMONEY` are stored by SQL Server / Tiberius as a
    /// scaled `i64` with a fixed scale of four.
    /// Covers the common cases used by the SLT suite.
    #[test]
    fn test_money_i64_to_decimal() {
        fn assert_money(raw: i64, expected: Decimal) {
            let ScalarImpl::Decimal(actual) =
                try_convert_money_i64_to_type(raw, &DataType::Decimal).unwrap()
            else {
                unreachable!()
            };
            assert_eq!(actual, expected);
            assert_eq!(actual.scale(), Some(4));
        }

        assert_money(10000, Decimal::from(1));

        // $1.23 -> 12300 cents of 1/10000 -> 1.23
        assert_money(12300, Decimal::from(123) / Decimal::from(100));

        // $1234.56 -> 12345600 -> 1234.56
        assert_money(12345600, Decimal::from(123456) / Decimal::from(100));

        // 0 must not panic.
        assert_money(0, Decimal::from(0));
    }

    /// Money columns must not be coerced into non-Decimal targets: that
    /// would silently throw away the scaled i64 instead of producing a
    /// value with the right scale.
    #[test]
    fn test_money_i64_to_unsupported_type_errors() {
        for unsupported in [
            DataType::Int32,
            DataType::Int64,
            DataType::Float64,
            DataType::Varchar,
        ] {
            assert!(
                try_convert_money_i64_to_type(12345600, &unsupported).is_err(),
                "expected error for money -> {unsupported:?}"
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
        let instant = time::OffsetDateTime::from_sql(value)?;
        instant
            .map(|instant| {
                let timestamptz = instant
                    .unix_timestamp_nanos()
                    .checked_div(1000)
                    .and_then(|micros| i64::try_from(micros).ok())
                    .and_then(Timestamptz::from_micros)
                    .ok_or_else(|| {
                        tiberius::error::Error::Conversion(
                            "datetimeoffset is out of range for RisingWave timestamptz".into(),
                        )
                    })?;
                Ok(TimestamptzTiberiusWrapper::from(timestamptz))
            })
            .transpose()
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
            ScalarImpl::Bytea(v) => {
                let value: Vec<u8> = (*v).to_vec();
                value.into_sql()
            }
            ScalarImpl::Utf8(v) => String::from(v).into_sql(),
            value => {
                // Serial, Interval, Jsonb, Int256, Struct, List are not supported yet
                unimplemented!("the sql server decoding for {:?} is unsupported", value);
            }
        }
    }
}
