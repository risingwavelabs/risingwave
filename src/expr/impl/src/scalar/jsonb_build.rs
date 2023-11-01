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

use jsonbb::Builder;
use risingwave_common::row::Row;
use risingwave_common::types::{DatumRef, JsonbVal, ScalarRefImpl, ToText};
use risingwave_expr::{function, ExprError, Result};

/// Builds a possibly-heterogeneously-typed JSON array out of a variadic argument list.
/// Each argument is converted as per `to_jsonb`.
///
/// # Examples
///
/// ```slt
/// query T
/// select jsonb_build_array(1, 2, 'foo', 4, 5);
/// ----
/// [1, 2, "foo", 4, 5]
/// ```
#[function("jsonb_build_array(...) -> jsonb")]
fn jsonb_build_array(args: impl Row) -> Result<JsonbVal> {
    let mut builder = Builder::<Vec<u8>>::new();
    builder.begin_array();
    for value in args.iter() {
        builder.add_datum(value)?;
    }
    builder.end_array();
    Ok(builder.finish().into())
}

/// Builds a JSON object out of a variadic argument list.
/// By convention, the argument list consists of alternating keys and values.
/// Key arguments are coerced to text; value arguments are converted as per `to_jsonb`.
///
/// # Examples
///
/// ```slt
/// query T
/// select jsonb_build_object('foo', 1, 2, row(3,'bar'));
/// ----
/// {"foo": 1, "2": {"f1": 3, "f2": "bar"}}
/// ```
#[function("jsonb_build_object(...) -> jsonb")]
fn jsonb_build_object(args: impl Row) -> Result<JsonbVal> {
    if args.len() % 2 == 1 {
        return Err(ExprError::InvalidParam {
            name: "args",
            reason: "argument list must have even number of elements".into(),
        });
    }
    let mut builder = Builder::<Vec<u8>>::new();
    builder.begin_object();
    for (i, [key, value]) in args.iter().array_chunks().enumerate() {
        match key {
            Some(s) => builder.display(ToTextDisplay(s)),
            None => {
                return Err(ExprError::InvalidParam {
                    name: "args",
                    reason: format!("argument {}: key must not be null", i * 2 + 1).into(),
                })
            }
        }
        builder.add_datum(value)?;
    }
    builder.end_object();
    Ok(builder.finish().into())
}

trait BuilderExt {
    fn add_datum(&mut self, value: DatumRef<'_>) -> Result<()>;
}

impl BuilderExt for Builder {
    fn add_datum(&mut self, value: DatumRef<'_>) -> Result<()> {
        use ScalarRefImpl::*;
        match value {
            None => self.add_null(),
            Some(Bool(x)) => self.add_bool(x),
            Some(Int16(x)) => self.add_i64(x as i64),
            Some(Int32(x)) => self.add_i64(x as i64),
            Some(Int64(x)) => self.add_i64(x),
            Some(Serial(x)) => self.add_i64(x.into_inner()),
            Some(Float32(x)) => self.add_f64(x.0 as f64),
            Some(Float64(x)) => self.add_f64(x.0),
            Some(Date(x)) => self.display(ToTextDisplay(x)),
            Some(Time(x)) => self.display(ToTextDisplay(x)),
            Some(Timestamp(x)) => self.display(ToTextDisplay(x)),
            Some(Timestamptz(x)) => self.display(ToTextDisplay(x)),
            Some(Interval(x)) => self.display(ToTextDisplay(x)),
            Some(Bytea(x)) => self.display(ToTextDisplay(x)),
            Some(Utf8(x)) => self.add_string(x),
            Some(Jsonb(x)) => self.add_value(x.into()),
            Some(List(array)) => {
                self.begin_array();
                for v in array.iter() {
                    self.add_datum(v)?;
                }
                self.end_array();
            }
            // FIXME: support these types after `to_jsonb`
            // https://github.com/risingwavelabs/risingwave/issues/12834
            Some(Int256(_)) | Some(Decimal(_)) | Some(Struct(_)) => {
                return Err(ExprError::InvalidParam {
                    name: "value",
                    reason: format!(
                        "to jsonb is not supported yet for: {}",
                        ToTextDisplay(value)
                    )
                    .into(),
                })
            }
        }
        Ok(())
    }
}

/// A wrapper type to implement `Display` for `ToText`.
struct ToTextDisplay<T>(T);

impl<T: ToText> std::fmt::Display for ToTextDisplay<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.write(f)
    }
}
