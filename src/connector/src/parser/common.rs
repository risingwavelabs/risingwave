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

use std::borrow::Cow;
use std::str::FromStr;

use anyhow::{anyhow, Result};
use base64::Engine as _;
use risingwave_common::array::{ListValue, StructValue};
use risingwave_common::cast::{
    i64_to_timestamp, i64_to_timestamptz, str_to_bytea, str_to_date, str_to_time, str_to_timestamp,
    str_with_time_zone_to_timestamptz,
};
use risingwave_common::types::{
    DataType, Date, Datum, Decimal, Int256, Interval, JsonbVal, ScalarImpl, Time,
};
use risingwave_common::util::iter_util::ZipEqFast;
use simd_json::value::StaticNode;
use simd_json::{BorrowedValue, ValueAccess};

use super::unified::json::JsonParseOptions;
use crate::source::SourceFormat;
use crate::{ensure_i16, ensure_i32, ensure_i64, ensure_str, simd_json_ensure_float};
pub(crate) fn json_object_smart_get_value<'a, 'b>(
    v: &'b simd_json::BorrowedValue<'a>,
    key: Cow<'b, str>,
) -> Option<&'b BorrowedValue<'a>> {
    let obj = v.as_object()?;
    if obj.contains_key(key.as_ref()) {
        return obj.get(key.as_ref());
    }
    for (k, v) in obj {
        if k.eq_ignore_ascii_case(key.as_ref()) {
            return Some(v);
        }
    }
    None
}

fn do_parse_simd_json_value(
    format: &SourceFormat,
    dtype: &DataType,
    v: &BorrowedValue<'_>,
) -> Result<ScalarImpl> {
    let options = match format {
        SourceFormat::DebeziumJson => JsonParseOptions::DEBEZIUM,
        _ => Default::default(),
    };
    Ok(options.parse(v, dtype)?.unwrap())
}

#[inline]
pub(crate) fn simd_json_parse_value(
    // column: &ColumnDesc,
    format: &SourceFormat,
    dtype: &DataType,
    value: Option<&BorrowedValue<'_>>,
) -> Result<Datum> {
    match value {
        None | Some(BorrowedValue::Static(StaticNode::Null)) => Ok(None),
        Some(v) => Ok(Some(do_parse_simd_json_value(format, dtype, v).map_err(
            |e| anyhow!("failed to parse type '{}' from json: {}", dtype, e),
        )?)),
    }
}
