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

use std::fmt::Debug;

use anyhow::anyhow;
use jsonbb::{Builder, Value};
use risingwave_common::types::{
    DataType, JsonbVal, ListRef, ScalarRefImpl, StructRef, Timestamptz, ToText,
};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_expr::expr::{BoxedExpression, Context};
use risingwave_expr::{build_function, function, ExprError, Result};

use super::timestamptz::time_zone_err;

// TODO(Kexiang): We can unify all the to_jsonb functions if we include ctx and time zone for all types. Should we?
#[function("to_jsonb(boolean) -> jsonb")]
#[function("to_jsonb(*int) -> jsonb")]
#[function("to_jsonb(decimal) -> jsonb")]
#[function("to_jsonb(serial) -> jsonb")]
#[function("to_jsonb(*float) -> jsonb")]
#[function("to_jsonb(int256) -> jsonb")]
#[function("to_jsonb(time) -> jsonb")]
#[function("to_jsonb(date) -> jsonb")]
#[function("to_jsonb(timestamp) -> jsonb")]
#[function("to_jsonb(interval) -> jsonb")]
#[function("to_jsonb(varchar) -> jsonb")]
#[function("to_jsonb(jsonb) -> jsonb")]
pub fn to_jsonb(input: Option<impl Into<JsonbVal>>) -> JsonbVal {
    match input {
        Some(inner) => inner.into(),
        None => JsonbVal::null(),
    }
}

#[function("to_jsonb(bytea) -> jsonb")]
pub fn bytea_to_jsonb(input: Option<&[u8]>) -> JsonbVal {
    match input {
        Some(inner) => bytea_to_value(inner).into(),
        None => JsonbVal::null(),
    }
}

// Only to register this signature to function signature map.
#[build_function("to_jsonb(timestamptz) -> jsonb")]
#[build_function("to_jsonb(struct) -> jsonb")]
#[build_function("to_jsonb(anyarray) -> jsonb")]
fn build_timestamptz_to_jsonb(
    _return_type: DataType,
    _children: Vec<BoxedExpression>,
) -> Result<BoxedExpression> {
    Err(ExprError::UnsupportedFunction(
        "to_jsonb of timestamptz/struct/anyarray should have been rewritten to include timezone"
            .into(),
    ))
}

#[function("to_jsonb(timestamptz, varchar) -> jsonb")]
pub fn timestamptz_to_jsonb(input: Option<Timestamptz>, zone: Option<&str>) -> Result<JsonbVal> {
    match input {
        Some(inner) => zone
            .ok_or_else(|| {
                ExprError::Internal(anyhow!(
                    "to_char(timestamptz, varchar) have a null zone_str"
                ))
            })
            .and_then(|zone_str| {
                let value = timestamptz_to_value(inner, zone_str)?;
                Ok(value.into())
            }),
        None => Ok(JsonbVal::null()),
    }
}

#[function("to_jsonb(struct, varchar) -> jsonb")]
pub fn struct_to_jsonb(
    input: Option<StructRef<'_>>,
    zone: Option<&str>,
    ctx: &Context,
) -> Result<JsonbVal> {
    match input {
        Some(inner) => zone
            .ok_or_else(|| {
                ExprError::Internal(anyhow!("to_char(struct, varchar) have a null zone_str"))
            })
            .and_then(|zone_str| {
                let mut builder = Builder::default();
                add_struct_to_builder(inner, zone_str, &ctx.arg_types[0], &mut builder)?;
                Ok(builder.finish().into())
            }),
        None => Ok(JsonbVal::null()),
    }
}

#[function("to_jsonb(anyarray, varchar) -> jsonb")]
pub fn list_to_jsonb(
    input: Option<ListRef<'_>>,
    zone: Option<&str>,
    ctx: &Context,
) -> Result<JsonbVal> {
    match input {
        Some(inner) => zone
            .ok_or_else(|| {
                ExprError::Internal(anyhow!("to_char(anyarray, varchar) have a null zone_str"))
            })
            .and_then(|zone_str| {
                let mut builder = Builder::default();
                add_anyarrary_to_builder(inner, zone_str, &ctx.arg_types[0], &mut builder)?;
                Ok(builder.finish().into())
            }),
        None => Ok(JsonbVal::null()),
    }
}

fn bytea_to_value(input: &[u8]) -> Value {
    input.to_text().as_str().into()
}

fn timestamptz_to_value(input: Timestamptz, zone: &str) -> Result<Value> {
    let time_zone = Timestamptz::lookup_time_zone(zone).map_err(time_zone_err)?;
    let instant_local = input.to_datetime_in_zone(time_zone);
    Ok(instant_local.to_rfc3339().as_str().into())
}

fn add_anyarrary_to_builder(
    input: ListRef<'_>,
    zone: &str,
    data_type: &DataType,
    builder: &mut Builder,
) -> Result<()> {
    let data_type_in_list = data_type.as_list();
    builder.begin_array();
    input
        .iter()
        .map(|x| add_scalar_to_builder(x, zone, data_type_in_list, builder))
        .try_collect()?;
    builder.end_array();
    Ok(())
}

fn add_struct_to_builder(
    input: StructRef<'_>,
    zone: &str,
    data_type: &DataType,
    builder: &mut Builder,
) -> Result<()> {
    builder.begin_object();
    let names: Vec<&str> = data_type.as_struct().names().collect();
    input
        .iter_fields_ref()
        .zip_eq_fast(data_type.as_struct().types())
        .enumerate()
        .map(|(idx, (scalar, data_type))| {
            if names.is_empty() {
                builder.display(format_args!("f{}", idx + 1));
            } else {
                builder.add_string(names[idx]);
            };
            add_scalar_to_builder(scalar, zone, data_type, builder)?;
            Ok::<(), ExprError>(())
        })
        .try_collect()?;
    builder.end_object();
    Ok(())
}

fn add_scalar_to_builder(
    input: Option<ScalarRefImpl<'_>>,
    zone: &str,
    data_type: &DataType,
    builder: &mut Builder,
) -> Result<()> {
    match input {
        Some(input) => match input {
            ScalarRefImpl::Int16(v) => builder.add_value(Value::from(v).as_ref()),
            ScalarRefImpl::Int32(v) => builder.add_value(Value::from(v).as_ref()),
            ScalarRefImpl::Int64(v) => builder.add_value(Value::from(v).as_ref()),
            ScalarRefImpl::Int256(v) => builder.add_value(Value::from(v).as_ref()),
            ScalarRefImpl::Float32(v) => builder.add_value(Value::from(v).as_ref()),
            ScalarRefImpl::Float64(v) => builder.add_value(Value::from(v).as_ref()),
            ScalarRefImpl::Utf8(v) => builder.add_value(Value::from(v).as_ref()),
            ScalarRefImpl::Bool(v) => builder.add_value(Value::from(v).as_ref()),
            ScalarRefImpl::Decimal(v) => builder.add_value(Value::from(v).as_ref()),
            ScalarRefImpl::Interval(v) => builder.add_value(Value::from(v).as_ref()),
            ScalarRefImpl::Date(v) => builder.add_value(Value::from(v).as_ref()),
            ScalarRefImpl::Time(v) => builder.add_value(Value::from(v).as_ref()),
            ScalarRefImpl::Timestamp(v) => builder.add_value(Value::from(v).as_ref()),
            ScalarRefImpl::Jsonb(v) => builder.add_value(Value::from(v).as_ref()),
            ScalarRefImpl::Serial(v) => builder.add_value(Value::from(v).as_ref()),
            ScalarRefImpl::Bytea(v) => builder.add_value(bytea_to_value(v).as_ref()),
            ScalarRefImpl::Timestamptz(v) => {
                builder.add_value(timestamptz_to_value(v, zone)?.as_ref())
            }
            ScalarRefImpl::Struct(v) => add_struct_to_builder(v, zone, data_type, builder)?,
            ScalarRefImpl::List(v) => add_anyarrary_to_builder(v, zone, data_type, builder)?,
        },
        None => builder.add_null(),
    };
    Ok(())
}
