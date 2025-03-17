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

use jsonbb::ValueRef;
use risingwave_common::types::{JsonbRef, JsonbVal};
use risingwave_expr::{ExprError, Result, function};
use sql_json_path::{EvalError, JsonPath, ParseError};
use thiserror_ext::AsReport;

#[function(
    "jsonb_path_exists(jsonb, varchar) -> boolean",
    prebuild = "JsonPath::new($1).map_err(parse_error)?"
)]
fn jsonb_path_exists2(target: JsonbRef<'_>, path: &JsonPath) -> Result<bool> {
    path.exists::<ValueRef<'_>>(target.into())
        .map_err(eval_error)
}

#[function(
    "jsonb_path_exists(jsonb, varchar, jsonb) -> boolean",
    prebuild = "JsonPath::new($1).map_err(parse_error)?"
)]
fn jsonb_path_exists3(target: JsonbRef<'_>, vars: JsonbRef<'_>, path: &JsonPath) -> Result<bool> {
    path.exists_with_vars::<ValueRef<'_>>(target.into(), vars.into())
        .map_err(eval_error)
}

/// Checks whether the JSON path returns any item for the specified JSON value.
/// If the vars argument is specified, it must be a JSON object, and its fields
/// provide named values to be substituted into the jsonpath expression. If the
/// silent argument is specified and is true, the function suppresses the same
/// errors as the @? and @@ operators do.
///
/// # Examples
///
/// ```slt
/// query B
/// select jsonb_path_exists('{"a":[1,2,3,4,5]}', '$.a[*] ? (@ >= $min && @ <= $max)', '{"min":2, "max":4}');
/// ----
/// t
/// ```
#[function(
    "jsonb_path_exists(jsonb, varchar, jsonb, boolean) -> boolean",
    prebuild = "JsonPath::new($1).map_err(parse_error)?"
)]
fn jsonb_path_exists4(
    target: JsonbRef<'_>,
    vars: JsonbRef<'_>,
    silent: bool,
    path: &JsonPath,
) -> Result<Option<bool>> {
    match jsonb_path_exists3(target, vars, path) {
        Ok(x) => Ok(Some(x)),
        Err(_) if silent => Ok(None),
        Err(e) => Err(e),
    }
}

#[function(
    "jsonb_path_match(jsonb, varchar) -> boolean",
    prebuild = "JsonPath::new($1).map_err(parse_error)?"
)]
fn jsonb_path_match2(target: JsonbRef<'_>, path: &JsonPath) -> Result<Option<bool>> {
    let matched = path
        .query::<ValueRef<'_>>(target.into())
        .map_err(eval_error)?;

    if matched.len() != 1 || !matched[0].as_ref().is_boolean() && !matched[0].as_ref().is_null() {
        return Err(ExprError::InvalidParam {
            name: "jsonb_path_match",
            reason: "single boolean result is expected".into(),
        });
    }
    Ok(matched[0].as_ref().as_bool())
}

#[function(
    "jsonb_path_match(jsonb, varchar, jsonb) -> boolean",
    prebuild = "JsonPath::new($1).map_err(parse_error)?"
)]
fn jsonb_path_match3(
    target: JsonbRef<'_>,
    vars: JsonbRef<'_>,
    path: &JsonPath,
) -> Result<Option<bool>> {
    let matched = path
        .query_with_vars::<ValueRef<'_>>(target.into(), vars.into())
        .map_err(eval_error)?;

    if matched.len() != 1 || !matched[0].as_ref().is_boolean() && !matched[0].as_ref().is_null() {
        return Err(ExprError::InvalidParam {
            name: "jsonb_path_match",
            reason: "single boolean result is expected".into(),
        });
    }
    Ok(matched[0].as_ref().as_bool())
}

/// Returns the result of a JSON path predicate check for the specified JSON value.
/// Only the first item of the result is taken into account.
/// If the result is not Boolean, then NULL is returned.
/// The optional vars and silent arguments act the same as for `jsonb_path_exists`.
///
/// # Examples
///
/// ```slt
/// query B
/// select jsonb_path_match('{"a":[1,2,3,4,5]}', 'exists($.a[*] ? (@ >= $min && @ <= $max))', '{"min":2, "max":4}');
/// ----
/// t
/// ```
#[function(
    "jsonb_path_match(jsonb, varchar, jsonb, boolean) -> boolean",
    prebuild = "JsonPath::new($1).map_err(parse_error)?"
)]
fn jsonb_path_match4(
    target: JsonbRef<'_>,
    vars: JsonbRef<'_>,
    silent: bool,
    path: &JsonPath,
) -> Result<Option<bool>> {
    match jsonb_path_match3(target, vars, path) {
        Ok(x) => Ok(x),
        Err(_) if silent => Ok(None),
        Err(e) => Err(e),
    }
}

#[function(
    "jsonb_path_query(jsonb, varchar) -> setof jsonb",
    prebuild = "JsonPath::new($1).map_err(parse_error)?"
)]
fn jsonb_path_query2<'a>(
    target: JsonbRef<'a>,
    path: &JsonPath,
) -> Result<impl Iterator<Item = JsonbVal> + 'a> {
    let matched = path
        .query::<ValueRef<'_>>(target.into())
        .map_err(eval_error)?;
    Ok(matched.into_iter().map(|json| json.into_owned().into()))
}

#[function(
    "jsonb_path_query(jsonb, varchar, jsonb) -> setof jsonb",
    prebuild = "JsonPath::new($1).map_err(parse_error)?"
)]
fn jsonb_path_query3<'a>(
    target: JsonbRef<'a>,
    vars: JsonbRef<'a>,
    path: &JsonPath,
) -> Result<impl Iterator<Item = JsonbVal> + 'a> {
    let matched = path
        .query_with_vars::<ValueRef<'_>>(target.into(), vars.into())
        .map_err(eval_error)?;
    Ok(matched.into_iter().map(|json| json.into_owned().into()))
}

/// Returns all JSON items returned by the JSON path for the specified JSON value.
/// The optional vars and silent arguments act the same as for `jsonb_path_exists`.
///
/// # Examples
///
/// ```slt
/// query I
/// select * from jsonb_path_query(jsonb '{"a":[1,2,3,4,5]}', '$.a[*] ? (@ >= $min && @ <= $max)', jsonb '{"min":2, "max":4}');
/// ----
/// 2
/// 3
/// 4
/// ```
#[function(
    "jsonb_path_query(jsonb, varchar, jsonb, boolean) -> setof jsonb",
    prebuild = "JsonPath::new($1).map_err(parse_error)?"
)]
fn jsonb_path_query4<'a>(
    target: JsonbRef<'a>,
    vars: JsonbRef<'a>,
    silent: bool,
    path: &JsonPath,
) -> Result<Option<impl Iterator<Item = JsonbVal> + 'a>> {
    match jsonb_path_query3(target, vars, path) {
        Ok(x) => Ok(Some(x)),
        Err(_) if silent => Ok(None),
        Err(e) => Err(e),
    }
}

#[function(
    "jsonb_path_query_array(jsonb, varchar) -> jsonb",
    prebuild = "JsonPath::new($1).map_err(parse_error)?"
)]
fn jsonb_path_query_array2(target: JsonbRef<'_>, path: &JsonPath) -> Result<JsonbVal> {
    let matched = path
        .query::<ValueRef<'_>>(target.into())
        .map_err(eval_error)?;
    let array = jsonbb::Value::array(matched.iter().map(|json| json.as_ref()));
    Ok(array.into())
}

#[function(
    "jsonb_path_query_array(jsonb, varchar, jsonb) -> jsonb",
    prebuild = "JsonPath::new($1).map_err(parse_error)?"
)]
fn jsonb_path_query_array3(
    target: JsonbRef<'_>,
    vars: JsonbRef<'_>,
    path: &JsonPath,
) -> Result<JsonbVal> {
    let matched = path
        .query_with_vars::<ValueRef<'_>>(target.into(), vars.into())
        .map_err(eval_error)?;
    let array = jsonbb::Value::array(matched.iter().map(|json| json.as_ref()));
    Ok(array.into())
}

/// Returns all JSON items returned by the JSON path for the specified JSON value, as a JSON array.
/// The optional vars and silent arguments act the same as for `jsonb_path_exists`.
///
/// # Examples
///
/// ```slt
/// query T
/// select jsonb_path_query_array('{"a":[1,2,3,4,5]}', '$.a[*] ? (@ >= $min && @ <= $max)', '{"min":2, "max":4}');
/// ----
/// [2, 3, 4]
/// ```
#[function(
    "jsonb_path_query_array(jsonb, varchar, jsonb, boolean) -> jsonb",
    prebuild = "JsonPath::new($1).map_err(parse_error)?"
)]
fn jsonb_path_query_array4(
    target: JsonbRef<'_>,
    vars: JsonbRef<'_>,
    silent: bool,
    path: &JsonPath,
) -> Result<Option<JsonbVal>> {
    match jsonb_path_query_array3(target, vars, path) {
        Ok(x) => Ok(Some(x)),
        Err(_) if silent => Ok(None),
        Err(e) => Err(e),
    }
}

#[function(
    "jsonb_path_query_first(jsonb, varchar) -> jsonb",
    prebuild = "JsonPath::new($1).map_err(parse_error)?"
)]
fn jsonb_path_query_first2(target: JsonbRef<'_>, path: &JsonPath) -> Result<Option<JsonbVal>> {
    let matched = path
        .query_first::<ValueRef<'_>>(target.into())
        .map_err(eval_error)?;
    Ok(matched
        .into_iter()
        .next()
        .map(|json| json.into_owned().into()))
}

#[function(
    "jsonb_path_query_first(jsonb, varchar, jsonb) -> jsonb",
    prebuild = "JsonPath::new($1).map_err(parse_error)?"
)]
fn jsonb_path_query_first3(
    target: JsonbRef<'_>,
    vars: JsonbRef<'_>,
    path: &JsonPath,
) -> Result<Option<JsonbVal>> {
    let matched = path
        .query_first_with_vars::<ValueRef<'_>>(target.into(), vars.into())
        .map_err(eval_error)?;
    Ok(matched
        .into_iter()
        .next()
        .map(|json| json.into_owned().into()))
}

/// Returns the first JSON item returned by the JSON path for the specified JSON value.
/// Returns NULL if there are no results.
/// The optional vars and silent arguments act the same as for `jsonb_path_exists`.
///
/// # Examples
///
/// ```slt
/// query T
/// select jsonb_path_query_first('{"a":[1,2,3,4,5]}', '$.a[*] ? (@ >= $min && @ <= $max)', '{"min":2, "max":4}');
/// ----
/// 2
/// ```
#[function(
    "jsonb_path_query_first(jsonb, varchar, jsonb, boolean) -> jsonb",
    prebuild = "JsonPath::new($1).map_err(parse_error)?"
)]
fn jsonb_path_query_first4(
    target: JsonbRef<'_>,
    vars: JsonbRef<'_>,
    silent: bool,
    path: &JsonPath,
) -> Result<Option<JsonbVal>> {
    match jsonb_path_query_first3(target, vars, path) {
        Ok(x) => Ok(x),
        Err(_) if silent => Ok(None),
        Err(e) => Err(e),
    }
}

fn parse_error(e: ParseError) -> ExprError {
    ExprError::Parse(e.to_report_string().into())
}

fn eval_error(e: EvalError) -> ExprError {
    ExprError::InvalidParam {
        name: "jsonpath",
        reason: e.to_report_string().into(),
    }
}
