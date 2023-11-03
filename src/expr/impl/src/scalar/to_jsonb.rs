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

use jsonbb::Builder;
use risingwave_common::types::{
    DataType, Date, Decimal, Int256Ref, Interval, JsonbRef, JsonbVal, ListRef, ScalarRefImpl,
    Serial, StructRef, Time, Timestamp, Timestamptz, ToText, F32, F64,
};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_expr::expr::Context;
use risingwave_expr::{function, ExprError, Result};

#[function("to_jsonb(boolean) -> jsonb")]
#[function("to_jsonb(*int) -> jsonb")]
#[function("to_jsonb(decimal) -> jsonb")]
#[function("to_jsonb(serial) -> jsonb")]
#[function("to_jsonb(*float) -> jsonb")]
#[function("to_jsonb(int256) -> jsonb")]
#[function("to_jsonb(time) -> jsonb")]
#[function("to_jsonb(date) -> jsonb")]
#[function("to_jsonb(timestamp) -> jsonb")]
#[function("to_jsonb(timestamptz) -> jsonb")]
#[function("to_jsonb(interval) -> jsonb")]
#[function("to_jsonb(varchar) -> jsonb")]
#[function("to_jsonb(jsonb) -> jsonb")]
#[function("to_jsonb(bytea) -> jsonb")]
pub fn to_jsonb(input: Option<impl ToJsonb>) -> Result<JsonbVal> {
    match input {
        Some(inner) => {
            let mut builder = Builder::default();
            inner.add_to(&mut builder)?;
            Ok(builder.finish().into())
        }
        None => Ok(JsonbVal::null()),
    }
}

#[function("to_jsonb(struct) -> jsonb")]
pub fn struct_to_jsonb(input: Option<StructRef<'_>>, ctx: &Context) -> Result<JsonbVal> {
    match input {
        Some(inner) => {
            let mut builder = Builder::default();
            add_struct_to_builder(inner, &ctx.arg_types[0], &mut builder)?;
            Ok(builder.finish().into())
        }
        None => Ok(JsonbVal::null()),
    }
}

#[function("to_jsonb(anyarray) -> jsonb")]
pub fn list_to_jsonb(input: Option<ListRef<'_>>, ctx: &Context) -> Result<JsonbVal> {
    match input {
        Some(inner) => {
            let mut builder = Builder::default();
            add_anyarrary_to_builder(inner, &ctx.arg_types[0], &mut builder)?;
            Ok(builder.finish().into())
        }
        None => Ok(JsonbVal::null()),
    }
}

fn add_anyarrary_to_builder(
    input: ListRef<'_>,
    data_type: &DataType,
    builder: &mut Builder,
) -> Result<()> {
    let data_type_in_list = data_type.as_list();
    builder.begin_array();
    input
        .iter()
        .map(|x| add_scalar_to_builder(x, data_type_in_list, builder))
        .try_collect()?;
    builder.end_array();
    Ok(())
}

fn add_struct_to_builder(
    input: StructRef<'_>,
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
            add_scalar_to_builder(scalar, data_type, builder)?;
            Ok::<(), ExprError>(())
        })
        .try_collect()?;
    builder.end_object();
    Ok(())
}

fn add_scalar_to_builder(
    input: Option<ScalarRefImpl<'_>>,
    data_type: &DataType,
    builder: &mut Builder,
) -> Result<()> {
    match input {
        Some(input) => match input {
            ScalarRefImpl::Int16(v) => v.add_to(builder)?,
            ScalarRefImpl::Int32(v) => v.add_to(builder)?,
            ScalarRefImpl::Int64(v) => v.add_to(builder)?,
            ScalarRefImpl::Int256(v) => v.add_to(builder)?,
            ScalarRefImpl::Float32(v) => v.add_to(builder)?,
            ScalarRefImpl::Float64(v) => v.add_to(builder)?,
            ScalarRefImpl::Utf8(v) => v.add_to(builder)?,
            ScalarRefImpl::Bool(v) => v.add_to(builder)?,
            ScalarRefImpl::Decimal(v) => v.add_to(builder)?,
            ScalarRefImpl::Interval(v) => v.add_to(builder)?,
            ScalarRefImpl::Date(v) => v.add_to(builder)?,
            ScalarRefImpl::Time(v) => v.add_to(builder)?,
            ScalarRefImpl::Timestamp(v) => v.add_to(builder)?,
            ScalarRefImpl::Jsonb(v) => v.add_to(builder)?,
            ScalarRefImpl::Serial(v) => v.add_to(builder)?,
            ScalarRefImpl::Bytea(v) => v.add_to(builder)?,
            ScalarRefImpl::Timestamptz(v) => v.add_to(builder)?,
            ScalarRefImpl::Struct(v) => add_struct_to_builder(v, data_type, builder)?,
            ScalarRefImpl::List(v) => add_anyarrary_to_builder(v, data_type, builder)?,
        },
        None => builder.add_null(),
    };
    Ok(())
}

/// Values that can be converted to JSONB.
pub trait ToJsonb {
    fn add_to(self, builder: &mut Builder) -> Result<()>;
}

impl ToJsonb for bool {
    fn add_to(self, builder: &mut Builder) -> Result<()> {
        builder.add_bool(self);
        Ok(())
    }
}

impl ToJsonb for i16 {
    fn add_to(self, builder: &mut Builder) -> Result<()> {
        builder.add_i64(self as _);
        Ok(())
    }
}

impl ToJsonb for i32 {
    fn add_to(self, builder: &mut Builder) -> Result<()> {
        builder.add_i64(self as _);
        Ok(())
    }
}

impl ToJsonb for i64 {
    fn add_to(self, builder: &mut Builder) -> Result<()> {
        let res: F64 = self
            .try_into()
            .map_err(|_| ExprError::CastOutOfRange("IEEE 754 double"))?;
        res.add_to(builder)?;
        Ok(())
    }
}

impl ToJsonb for F32 {
    fn add_to(self, builder: &mut Builder) -> Result<()> {
        if self.0 == f32::INFINITY {
            "Infinity".add_to(builder)?;
        } else if self.0 == f32::NEG_INFINITY {
            "-Infinity".add_to(builder)?;
        } else if self.0.is_nan() {
            "NaN".add_to(builder)?;
        } else {
            builder.add_f64(self.0 as f64);
        }
        Ok(())
    }
}

impl ToJsonb for F64 {
    fn add_to(self, builder: &mut Builder) -> Result<()> {
        if self.0 == f64::INFINITY {
            "Infinity".add_to(builder)?;
        } else if self.0 == f64::NEG_INFINITY {
            "-Infinity".add_to(builder)?;
        } else if self.0.is_nan() {
            "NaN".add_to(builder)?;
        } else {
            builder.add_f64(self.0);
        }
        Ok(())
    }
}

impl ToJsonb for Decimal {
    fn add_to(self, builder: &mut Builder) -> Result<()> {
        let res: F64 = self
            .try_into()
            .map_err(|_| ExprError::CastOutOfRange("IEEE 754 double"))?;
        res.add_to(builder)?;
        Ok(())
    }
}

impl ToJsonb for Int256Ref<'_> {
    fn add_to(self, builder: &mut Builder) -> Result<()> {
        builder.add_string(self.to_text().as_str());
        Ok(())
    }
}

impl ToJsonb for &str {
    fn add_to(self, builder: &mut Builder) -> Result<()> {
        builder.add_string(self);
        Ok(())
    }
}

impl ToJsonb for &[u8] {
    fn add_to(self, builder: &mut Builder) -> Result<()> {
        builder.add_string(self.to_text().as_str());
        Ok(())
    }
}

impl ToJsonb for Date {
    fn add_to(self, builder: &mut Builder) -> Result<()> {
        builder.add_string(self.to_text().as_str());
        Ok(())
    }
}

impl ToJsonb for Time {
    fn add_to(self, builder: &mut Builder) -> Result<()> {
        builder.add_string(self.to_text().as_str());
        Ok(())
    }
}

impl ToJsonb for Interval {
    fn add_to(self, builder: &mut Builder) -> Result<()> {
        builder.add_string(self.to_text().as_str());
        Ok(())
    }
}

impl ToJsonb for Timestamp {
    fn add_to(self, builder: &mut Builder) -> Result<()> {
        builder.display(format_args!("{}T{}", self.0.date(), self.0.time()));
        Ok(())
    }
}

impl ToJsonb for Timestamptz {
    fn add_to(self, builder: &mut Builder) -> Result<()> {
        let instant_local = self.to_datetime_utc();
        builder.display(instant_local.to_rfc3339().as_str());
        Ok(())
    }
}

impl ToJsonb for Serial {
    fn add_to(self, builder: &mut Builder) -> Result<()> {
        builder.add_string(self.to_text().as_str());
        Ok(())
    }
}

impl ToJsonb for JsonbRef<'_> {
    fn add_to(self, builder: &mut Builder) -> Result<()> {
        builder.add_value(self.into());
        Ok(())
    }
}
