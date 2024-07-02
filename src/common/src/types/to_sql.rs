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

use std::error::Error;

use bytes::BytesMut;
use postgres_types::{to_sql_checked, IsNull};

use crate::types::ScalarImpl;

impl postgres_types::ToSql for ScalarImpl {
    to_sql_checked!();

    fn to_sql(
        &self,
        ty: &postgres_types::Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>>
    where
        Self: Sized,
    {
        match self {
            ScalarImpl::Int16(v) => v.to_sql(ty, out),
            ScalarImpl::Int32(v) => v.to_sql(ty, out),
            ScalarImpl::Int64(v) => v.to_sql(ty, out),
            ScalarImpl::Serial(v) => v.to_sql(ty, out),
            ScalarImpl::Float32(v) => v.to_sql(ty, out),
            ScalarImpl::Float64(v) => v.to_sql(ty, out),
            ScalarImpl::Utf8(v) => v.to_sql(ty, out),
            ScalarImpl::Bool(v) => v.to_sql(ty, out),
            ScalarImpl::Decimal(v) => v.to_sql(ty, out),
            ScalarImpl::Interval(v) => v.to_sql(ty, out),
            ScalarImpl::Date(v) => v.to_sql(ty, out),
            ScalarImpl::Timestamp(v) => v.to_sql(ty, out),
            ScalarImpl::Timestamptz(v) => v.to_sql(ty, out),
            ScalarImpl::Time(v) => v.to_sql(ty, out),
            ScalarImpl::Bytea(v) => (&**v).to_sql(ty, out),
            ScalarImpl::Jsonb(v) => v.to_sql(ty, out),
            ScalarImpl::Int256(_) | ScalarImpl::Struct(_) | ScalarImpl::List(_) => {
                bail_not_implemented!("the postgres encoding for {ty} is unsupported")
            }
        }
    }

    // return true to accept all types
    fn accepts(_ty: &postgres_types::Type) -> bool
    where
        Self: Sized,
    {
        true
    }
}

/// |Rust type|Server type|
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
impl<'a> tiberius::IntoSql<'a> for ScalarImpl {
    fn into_sql(self) -> tiberius::ColumnData<'a> {
        match self {
            ScalarImpl::Int16(v) => v.into_sql(),
            ScalarImpl::Int32(v) => v.into_sql(),
            ScalarImpl::Int64(v) => v.into_sql(),
            ScalarImpl::Float32(v) => v.into_sql(),
            ScalarImpl::Float64(v) => v.into_sql(),
            ScalarImpl::Bool(v) => v.into_sql(),
            ScalarImpl::Decimal(v) => v.into_sql(),
            ScalarImpl::Date(v) => v.into_sql(),
            ScalarImpl::Timestamp(v) => v.into_sql(),
            ScalarImpl::Timestamptz(v) => v.to_datetime_utc().into_sql(),
            ScalarImpl::Time(v) => v.into_sql(),
            // ScalarImpl::Bytea(v) => (*v.clone()).into_sql(),
            value => {
                // Utf8, Serial, Interval, Timestamptz, Jsonb, Int256, Struct, List are not supported yet
                unimplemented!("the sql server decoding for {:?} is unsupported", value);
            }
        }
    }
}

// impl<'a> crate::IntoSql<'a> for Option<$ty> {
//     fn into_sql(self) -> crate::tds::codec::ColumnData<'a> {
//         match self {
//             Some(val) => {
//                 let $target = val;
//                 $variant(Some($val))
//             },
//             None => $variant(None)
//         }
//     }
// }
