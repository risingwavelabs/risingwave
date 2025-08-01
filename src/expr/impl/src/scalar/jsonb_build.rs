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

use itertools::Either;
use jsonbb::Builder;
use risingwave_common::row::Row;
use risingwave_common::types::{JsonbVal, ScalarRefImpl};
use risingwave_common::util::iter_util::ZipEqDebug;
use risingwave_expr::expr::Context;
use risingwave_expr::{ExprError, Result, function};

use super::{ToJsonb, ToTextDisplay};

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
///
/// query T
/// select jsonb_build_array(variadic array[1, 2, 4, 5]);
/// ----
/// [1, 2, 4, 5]
/// ```
#[function("jsonb_build_array(variadic anyarray) -> jsonb")]
fn jsonb_build_array(args: impl Row, ctx: &Context) -> Result<JsonbVal> {
    let mut builder = Builder::<Vec<u8>>::new();
    builder.begin_array();
    if ctx.variadic {
        for (value, ty) in args.iter().zip_eq_debug(&ctx.arg_types) {
            value.add_to(ty, &mut builder)?;
        }
    } else {
        let ty = ctx.arg_types[0].as_list_element_type();
        for value in args.iter() {
            value.add_to(ty, &mut builder)?;
        }
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
/// select jsonb_build_object('foo', 1, 2, 'bar');
/// ----
/// {"2": "bar", "foo": 1}
///
/// query T
/// select jsonb_build_object(variadic array['foo', '1', '2', 'bar']);
/// ----
/// {"2": "bar", "foo": "1"}
/// ```
#[function("jsonb_build_object(variadic anyarray) -> jsonb")]
fn jsonb_build_object(args: impl Row, ctx: &Context) -> Result<JsonbVal> {
    if args.len() % 2 == 1 {
        return Err(ExprError::InvalidParam {
            name: "args",
            reason: "argument list must have even number of elements".into(),
        });
    }
    let mut builder = Builder::<Vec<u8>>::new();
    builder.begin_object();
    let arg_types = match ctx.variadic {
        true => Either::Left(ctx.arg_types.iter()),
        false => Either::Right(itertools::repeat_n(
            ctx.arg_types[0].as_list_element_type(),
            args.len(),
        )),
    };
    for (i, [(key, _), (value, value_type)]) in args
        .iter()
        .zip_eq_debug(arg_types)
        .array_chunks()
        .enumerate()
    {
        match key {
            Some(ScalarRefImpl::List(_) | ScalarRefImpl::Struct(_) | ScalarRefImpl::Jsonb(_)) => {
                return Err(ExprError::InvalidParam {
                    name: "args",
                    reason: "key value must be scalar, not array, composite, or json".into(),
                });
            }
            // special treatment for bool, `false` & `true` rather than `f` & `t`.
            Some(ScalarRefImpl::Bool(b)) => builder.display(b),
            Some(s) => builder.display(ToTextDisplay(s)),
            None => {
                return Err(ExprError::InvalidParam {
                    name: "args",
                    reason: format!("argument {}: key must not be null", i * 2 + 1).into(),
                });
            }
        }
        value.add_to(value_type, &mut builder)?;
    }
    builder.end_object();
    Ok(builder.finish().into())
}
