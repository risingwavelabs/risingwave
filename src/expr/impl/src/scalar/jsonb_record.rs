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

use risingwave_common::types::{JsonbRef, MapRef, MapValue, Scalar, StructRef, StructValue};
use risingwave_expr::expr::Context;
use risingwave_expr::{ExprError, Result, function};

/// Expands the top-level JSON object to a row having the composite type of the base argument.
/// The JSON object is scanned for fields whose names match column names of the output row type,
/// and their values are inserted into those columns of the output. (Fields that do not correspond
/// to any output column name are ignored.) In typical use, the value of base is just NULL, which
/// means that any output columns that do not match any object field will be filled with nulls.
/// However, if base isn't NULL then the values it contains will be used for unmatched columns.
///
/// # Examples
///
/// ```slt
/// query ITT
/// select (jsonb_populate_record(
///     null::struct<a int, b text[], c struct<d int, e text>>,
///     '{"a": 1, "b": ["2", "a b"], "c": {"d": 4, "e": "a b c"}, "x": "foo"}'
/// )).*;
/// ----
/// 1 {2,"a b"} (4,"a b c")
///
/// query ITT
/// select (jsonb_populate_record(
///     row(1, null, row(4, '5'))::struct<a int, b text[], c struct<d int, e text>>,
///     '{"b": ["2", "a b"], "c": {"e": "a b c"}, "x": "foo"}'
/// )).*;
/// ----
/// 1 {2,"a b"} (4,"a b c")
///
/// query II
/// select * from jsonb_populate_record(
///    null::struct<a int, b int>,
///    '{"a": 1, "b": 2}'
/// );
/// ----
/// 1 2
/// ```
#[function("jsonb_populate_record(struct, jsonb) -> struct")]
fn jsonb_populate_record(
    base: Option<StructRef<'_>>,
    jsonb: JsonbRef<'_>,
    ctx: &Context,
) -> Result<StructValue> {
    let output_type = ctx.return_type.as_struct();
    jsonb.populate_struct(output_type, base).map_err(parse_err)
}

#[function("jsonb_populate_map(anymap, jsonb) -> anymap")]
pub fn jsonb_populate_map(
    base: Option<MapRef<'_>>,
    v: JsonbRef<'_>,
    ctx: &Context,
) -> Result<MapValue> {
    let output_type = ctx.return_type.as_map();
    let jsonb_map = v
        .to_map(output_type)
        .map_err(|e| ExprError::Parse(e.into()))?;
    match base {
        Some(base) => Ok(MapValue::concat(base, jsonb_map.as_scalar_ref())),
        None => Ok(jsonb_map),
    }
}

/// Expands the top-level JSON array of objects to a set of rows having the composite type of the
/// base argument. Each element of the JSON array is processed as described above for
/// `jsonb_populate_record`.
///
/// # Examples
///
/// ```slt
/// query II
/// select * from jsonb_populate_recordset(
///     null::struct<a int, b int>,
///     '[{"a":1,"b":2}, {"a":3,"b":4}]'::jsonb
/// );
/// ----
/// 1 2
/// 3 4
///
/// query II
/// select * from jsonb_populate_recordset(
///     row(0, 0)::struct<a int, b int>,
///     '[{}, {"a":1}, {"b":2}, {"a":1,"b":2}]'::jsonb
/// );
/// ----
/// 0 0
/// 1 0
/// 0 2
/// 1 2
/// ```
#[function("jsonb_populate_recordset(struct, jsonb) -> setof struct")]
fn jsonb_populate_recordset<'a>(
    base: Option<StructRef<'a>>,
    jsonb: JsonbRef<'a>,
    ctx: &'a Context,
) -> Result<impl Iterator<Item = Result<StructValue>> + 'a> {
    let output_type = ctx.return_type.as_struct();
    Ok(jsonb
        .array_elements()
        .map_err(parse_err)?
        .map(move |elem| elem.populate_struct(output_type, base).map_err(parse_err)))
}

/// Expands the top-level JSON object to a row having the composite type defined by an AS clause.
/// The output record is filled from fields of the JSON object, in the same way as described above
/// for `jsonb_populate_record`. Since there is no input record value, unmatched columns are always
/// filled with nulls.
///
/// # Examples
///
/// // FIXME(runji): this query is blocked by parser and frontend support.
/// ```slt,ignore
/// query T
/// select * from jsonb_to_record('{"a":1,"b":[1,2,3],"c":[1,2,3],"e":"bar","r": {"a": 123, "b": "a b c"}}')
/// as x(a int, b text, c int[], d text, r struct<a int, b text>);
/// ----
/// 1 [1,2,3] {1,2,3} NULL (123,"a b c")
/// ```
#[function("jsonb_to_record(jsonb) -> struct", type_infer = "unreachable")]
fn jsonb_to_record(jsonb: JsonbRef<'_>, ctx: &Context) -> Result<StructValue> {
    let output_type = ctx.return_type.as_struct();
    jsonb.to_struct(output_type).map_err(parse_err)
}

/// Expands the top-level JSON array of objects to a set of rows having the composite type defined
/// by an AS clause. Each element of the JSON array is processed as described above for
/// `jsonb_populate_record`.
///
/// # Examples
///
/// // FIXME(runji): this query is blocked by parser and frontend support.
/// ```slt,ignore
/// query IT
/// select * from jsonb_to_recordset('[{"a":1,"b":"foo"}, {"a":"2","c":"bar"}]') as x(a int, b text);
/// ----
/// 1 foo
/// 2 NULL
/// ```
#[function(
    "jsonb_to_recordset(jsonb) -> setof struct",
    type_infer = "unreachable"
)]
fn jsonb_to_recordset<'a>(
    jsonb: JsonbRef<'a>,
    ctx: &'a Context,
) -> Result<impl Iterator<Item = Result<StructValue>> + 'a> {
    let output_type = ctx.return_type.as_struct();
    Ok(jsonb
        .array_elements()
        .map_err(parse_err)?
        .map(|elem| elem.to_struct(output_type).map_err(parse_err)))
}

/// Construct a parse error from String.
fn parse_err(s: String) -> ExprError {
    ExprError::Parse(s.into())
}
