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

use risingwave_common::row::Row;
use risingwave_common::types::{DataType, JsonbRef, ListValue, ScalarRefImpl};
use risingwave_expr::{ExprError, Result, function};

/// Extracts JSON object field with the given key.
///
/// `jsonb -> text → jsonb`
///
/// # Examples
///
/// ```slt
/// query T
/// select '{"a": {"b":"foo"}}'::jsonb -> 'a';
/// ----
/// {"b": "foo"}
/// ```
#[function("jsonb_access(jsonb, varchar) -> jsonb")]
pub fn jsonb_object_field<'a>(v: JsonbRef<'a>, p: &str) -> Option<JsonbRef<'a>> {
    v.access_object_field(p)
}

/// Extracts n'th element of JSON array (array elements are indexed from zero,
/// but negative integers count from the end).
///
/// `jsonb -> integer → jsonb`
///
/// # Examples
///
/// ```slt
/// query T
/// select '[{"a":"foo"},{"b":"bar"},{"c":"baz"}]'::jsonb -> 2;
/// ----
/// {"c": "baz"}
///
/// query T
/// select '[{"a":"foo"},{"b":"bar"},{"c":"baz"}]'::jsonb -> -3;
/// ----
/// {"a": "foo"}
/// ```
#[function("jsonb_access(jsonb, int4) -> jsonb")]
pub fn jsonb_array_element(v: JsonbRef<'_>, p: i32) -> Option<JsonbRef<'_>> {
    let idx = if p < 0 {
        let Ok(len) = v.array_len() else {
            return None;
        };
        if ((-p) as usize) > len {
            return None;
        } else {
            len - ((-p) as usize)
        }
    } else {
        p as usize
    };
    v.access_array_element(idx)
}

/// Extracts JSON sub-object at the specified path, where path elements can be either field keys or array indexes.
///
/// - `jsonb #> text[] → jsonb`
/// - `jsonb_extract_path ( from_json jsonb, VARIADIC path_elems text[] ) → jsonb`
///
/// # Examples
///
/// ```slt
/// query T
/// select '{"a": {"b": ["foo","bar"]}}'::jsonb #> '{a,b,1}'::text[];
/// ----
/// "bar"
///
/// query T
/// select '{"a": {"b": ["foo","bar"]}}'::jsonb #> '{a,b,null}'::text[];
/// ----
/// NULL
///
/// query T
/// select jsonb_extract_path('{"a": {"b": ["foo","bar"]}}', 'a', 'b', '1');
/// ----
/// "bar"
///
/// query T
/// select jsonb_extract_path('{"a": {"b": ["foo","bar"]}}', variadic array['a', 'b', '1']);
/// ----
/// "bar"
/// ```
#[function("jsonb_extract_path(jsonb, variadic varchar[]) -> jsonb")]
pub fn jsonb_extract_path(v: JsonbRef<'_>, path: impl Row) -> Option<JsonbRef<'_>> {
    let mut jsonb = v;
    for key in path.iter() {
        // return null if any element is null
        let key = key?.into_utf8();
        if jsonb.is_array() {
            // return null if the key is not an integer
            let idx = key.parse().ok()?;
            jsonb = jsonb_array_element(jsonb, idx)?;
        } else if jsonb.is_object() {
            jsonb = jsonb_object_field(jsonb, key)?;
        } else {
            return None;
        }
    }
    Some(jsonb)
}

/// Extracts JSON object field with the given key, as text.
///
/// `jsonb ->> text → text`
///
/// # Examples
///
/// ```slt
/// query T
/// select '{"a":1,"b":2}'::jsonb ->> 'b';
/// ----
/// 2
///
/// query T
/// select '{"a":1,"b":null}'::jsonb ->> 'b';
/// ----
/// NULL
/// ```
#[function("jsonb_access_str(jsonb, varchar) -> varchar")]
pub fn jsonb_object_field_str(
    v: JsonbRef<'_>,
    p: &str,
    writer: &mut impl std::fmt::Write,
) -> Option<()> {
    let jsonb = jsonb_object_field(v, p)?;
    if jsonb.is_jsonb_null() {
        return None;
    }
    jsonb.force_str(writer).unwrap();
    Some(())
}

/// Extracts n'th element of JSON array, as text.
///
/// `jsonb ->> integer → text`
///
/// # Examples
///
/// ```slt
/// query T
/// select '[1,2,3]'::jsonb ->> 2;
/// ----
/// 3
///
/// query T
/// select '[1,2,null]'::jsonb ->> 2;
/// ----
/// NULL
/// ```
#[function("jsonb_access_str(jsonb, int4) -> varchar")]
pub fn jsonb_array_element_str(
    v: JsonbRef<'_>,
    p: i32,
    writer: &mut impl std::fmt::Write,
) -> Option<()> {
    let jsonb = jsonb_array_element(v, p)?;
    if jsonb.is_jsonb_null() {
        return None;
    }
    jsonb.force_str(writer).unwrap();
    Some(())
}

/// Extracts JSON sub-object at the specified path as text.
///
/// - `jsonb #>> text[] → text`
/// - `jsonb_extract_path_text ( from_json jsonb, VARIADIC path_elems text[] ) → text`
///
/// # Examples
///
/// ```slt
/// query T
/// select '{"a": {"b": ["foo","bar"]}}'::jsonb #>> '{a,b,1}'::text[];
/// ----
/// bar
///
/// query T
/// select '{"a": {"b": ["foo",null]}}'::jsonb #>> '{a,b,1}'::text[];
/// ----
/// NULL
///
/// query T
/// select '{"a": {"b": ["foo","bar"]}}'::jsonb #>> '{a,b,null}'::text[];
/// ----
/// NULL
///
/// query T
/// select jsonb_extract_path_text('{"a": {"b": ["foo","bar"]}}', 'a', 'b', '1');
/// ----
/// bar
///
/// query T
/// select jsonb_extract_path_text('{"a": {"b": ["foo","bar"]}}', variadic array['a', 'b', '1']);
/// ----
/// bar
/// ```
#[function("jsonb_extract_path_text(jsonb, variadic varchar[]) -> varchar")]
pub fn jsonb_extract_path_text(
    v: JsonbRef<'_>,
    path: impl Row,
    writer: &mut impl std::fmt::Write,
) -> Option<()> {
    let jsonb = jsonb_extract_path(v, path)?;
    if jsonb.is_jsonb_null() {
        return None;
    }
    jsonb.force_str(writer).unwrap();
    Some(())
}

/// Converts the a JSONB array to a SQL array of JSONB elements.
///
/// This is equivalent to `jsonb_array_elements` followed by `array_agg` or `array` in most cases.
///
/// ```slt
/// query T
/// select
///     input,
///     (select array_agg(v) from jsonb_array_elements(input) as v) as array_agg,
///     array(select jsonb_array_elements(input)),
///     jsonb_to_array(input)
/// from (values
///     (null::jsonb),
///     ('[]'::jsonb)
/// ) as t(input);
/// ----
/// NULL NULL {} NULL
/// []   NULL {} {}
///
/// query T
/// select jsonb_to_array('[1,"foo",null,true,[false,"bar"],{"a":2}]');
/// ----
/// {1,"\"foo\"","null",true,"[false, \"bar\"]","{\"a\": 2}"}
///
/// query error parsing
/// select jsonb_to_array('');
///
/// query error cannot extract elements from a jsonb object
/// select jsonb_to_array('{"a": 1}');
/// ```
#[function("jsonb_to_array(jsonb) -> jsonb[]")]
fn jsonb_to_array(v: JsonbRef<'_>) -> Result<ListValue> {
    let iter = v
        .array_elements()
        .map_err(|e| ExprError::InvalidParam {
            name: "jsonb",
            reason: e.into(),
        })?
        .map(|elem| Some(ScalarRefImpl::Jsonb(elem)));
    Ok(ListValue::from_datum_iter(&DataType::Jsonb, iter))
}
