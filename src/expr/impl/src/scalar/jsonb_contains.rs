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
use risingwave_common::types::{JsonbRef, ListRef};
use risingwave_expr::function;

/// Does the first JSON value contain the second?
///
/// Examples:
///
/// ```slt
/// # Simple scalar/primitive values contain only the identical value:
/// query B
/// SELECT '"foo"'::jsonb @> '"foo"'::jsonb;
/// ----
/// t
///
/// # The array on the right side is contained within the one on the left:
/// query B
/// SELECT '[1, 2, 3]'::jsonb @> '[1, 3]'::jsonb;
/// ----
/// t
///
/// # Order of array elements is not significant, so this is also true:
/// query B
/// SELECT '[1, 2, 3]'::jsonb @> '[3, 1]'::jsonb;
/// ----
/// t
///
/// # Duplicate array elements don't matter either:
/// query B
/// SELECT '[1, 2, 3]'::jsonb @> '[1, 2, 2]'::jsonb;
/// ----
/// t
///
/// # The object with a single pair on the right side is contained
/// # within the object on the left side:
/// query B
/// SELECT '{"product": "PostgreSQL", "version": 9.4, "jsonb": true}'::jsonb @> '{"version": 9.4}'::jsonb;
/// ----
/// t
///
/// # The array on the right side is not considered contained within the
/// # array on the left, even though a similar array is nested within it:
/// query B
/// SELECT '[1, 2, [1, 3]]'::jsonb @> '[1, 3]'::jsonb;
/// ----
/// f
///
/// # But with a layer of nesting, it is contained:
/// query B
/// SELECT '[1, 2, [1, 3]]'::jsonb @> '[[1, 3]]'::jsonb;
/// ----
/// t
///
/// # Similarly, containment is not reported here:
/// query B
/// SELECT '{"foo": {"bar": "baz"}}'::jsonb @> '{"bar": "baz"}'::jsonb;
/// ----
/// f
///
/// # A top-level key and an empty object is contained:
/// query B
/// SELECT '{"foo": {"bar": "baz"}}'::jsonb @> '{"foo": {}}'::jsonb;
/// ----
/// t
///
/// # This array contains the primitive string value:
/// query B
/// SELECT '["foo", "bar"]'::jsonb @> '"bar"'::jsonb;
/// ----
/// t
///
/// # This exception is not reciprocal -- non-containment is reported here:
/// query B
/// SELECT '"bar"'::jsonb @> '["bar"]'::jsonb;
/// ----
/// f
///
/// # Object is not primitive:
/// query B
/// SELECT '[1, {"a":2}]'::jsonb @> '{"a":2}';
/// ----
/// f
///
/// # Array can be nested:
/// query B
/// SELECT '[1, [3, 4]]'::jsonb @> '[[3]]';
/// ----
/// t
///
/// # Recursion shall not include the special rule of array containing primitive:
/// query B
/// SELECT '{"a": [3, 4]}'::jsonb @> '{"a": 3}';
/// ----
/// f
/// ```
#[function("jsonb_contains(jsonb, jsonb) -> boolean")]
fn jsonb_contains(left: JsonbRef<'_>, right: JsonbRef<'_>) -> bool {
    jsonbb_contains(left.into(), right.into(), true)
}

/// Performs `jsonb_contains` on `jsonbb::ValueRef`.
/// `root` indicates whether the current recursion is at the root level.
fn jsonbb_contains(left: ValueRef<'_>, right: ValueRef<'_>, root: bool) -> bool {
    match (left, right) {
        // Both left and right are objects.
        (ValueRef::Object(left_obj), ValueRef::Object(right_obj)) => {
            // Every key-value pair in right should be present in left.
            right_obj.iter().all(|(key, value)| {
                left_obj
                    .get(key)
                    .is_some_and(|left_val| jsonbb_contains(left_val, value, false))
            })
        }

        // Both left and right are arrays.
        (ValueRef::Array(left_arr), ValueRef::Array(right_arr)) => {
            // For every value in right, there should be an equivalent in left.
            right_arr.iter().all(|right_val| {
                left_arr
                    .iter()
                    .any(|left_val| jsonbb_contains(left_val, right_val, false))
            })
        }

        // Left is an array and right is an object.
        (ValueRef::Array(_), ValueRef::Object(_)) => false,

        // Left is an array and right is a primitive value. only at root level.
        (ValueRef::Array(left_arr), right_val) if root => {
            // The right should be present in left.
            left_arr.iter().any(|left_val| left_val == right_val)
        }

        // Both left and right are primitive values.
        (left_val, right_val) => left_val == right_val,
    }
}

/// Is the first JSON value contained in the second?
///
/// Examples:
///
/// ```slt
/// query B
/// select '{"b":2}'::jsonb <@ '{"a":1, "b":2}'::jsonb;
/// ----
/// t
/// ```
#[function("jsonb_contained(jsonb, jsonb) -> boolean")]
fn jsonb_contained(left: JsonbRef<'_>, right: JsonbRef<'_>) -> bool {
    jsonb_contains(right, left)
}

/// Does the text string exist as a top-level key or array element within the JSON value?
///
/// Examples:
///
/// ```slt
/// # String exists as array element:
/// query B
/// SELECT '["foo", "bar", "baz"]'::jsonb ? 'bar';
/// ----
/// t
///
/// # String exists as object key:
/// query B
/// SELECT '{"foo": "bar"}'::jsonb ? 'foo';
/// ----
/// t
///
/// # Object values are not considered:
/// query B
/// SELECT '{"foo": "bar"}'::jsonb ? 'bar';
/// ----
/// f
///
/// # As with containment, existence must match at the top level:
/// query B
/// SELECT '{"foo": {"bar": "baz"}}'::jsonb ? 'bar';
/// ----
/// f
///
/// # A string is considered to exist if it matches a primitive JSON string:
/// query B
/// SELECT '"foo"'::jsonb ? 'foo';
/// ----
/// t
/// ```
#[function("jsonb_exists(jsonb, varchar) -> boolean")]
fn jsonb_exists(left: JsonbRef<'_>, key: &str) -> bool {
    match left.into() {
        ValueRef::Object(object) => object.get(key).is_some(),
        ValueRef::Array(array) => array.iter().any(|val| val.as_str() == Some(key)),
        ValueRef::String(str) => str == key,
        _ => false,
    }
}

/// Do any of the strings in the text array exist as top-level keys or array elements?
///
/// Examples:
///
/// ```slt
/// query B
/// select '{"a":1, "b":2, "c":3}'::jsonb ?| array['b', 'd'];
/// ----
/// t
///
/// query B
/// select '["a", "b", "c"]'::jsonb ?| array['b', 'd'];
/// ----
/// t
///
/// query B
/// select '"b"'::jsonb ?| array['b', 'd'];
/// ----
/// t
/// ```
#[function("jsonb_exists_any(jsonb, varchar[]) -> boolean")]
fn jsonb_exists_any(left: JsonbRef<'_>, keys: ListRef<'_>) -> bool {
    let mut keys = keys.iter().flatten().map(|val| val.into_utf8());
    match left.into() {
        ValueRef::Object(object) => keys.any(|key| object.get(key).is_some()),
        ValueRef::Array(array) => keys.any(|key| array.iter().any(|val| val.as_str() == Some(key))),
        ValueRef::String(str) => keys.any(|key| str == key),
        _ => false,
    }
}

/// Do all of the strings in the text array exist as top-level keys or array elements?
///
/// Examples:
///
/// ```slt
/// query B
/// select '{"a":1, "b":2, "c":3}'::jsonb ?& array['a', 'b'];
/// ----
/// t
///
/// query B
/// select '["a", "b", "c"]'::jsonb ?& array['a', 'b'];
/// ----
/// t
///
/// query B
/// select '"b"'::jsonb ?& array['b'];
/// ----
/// t
/// ```
#[function("jsonb_exists_all(jsonb, varchar[]) -> boolean")]
fn jsonb_exists_all(left: JsonbRef<'_>, keys: ListRef<'_>) -> bool {
    let mut keys = keys.iter().flatten().map(|val| val.into_utf8());
    match left.into() {
        ValueRef::Object(object) => keys.all(|key| object.get(key).is_some()),
        ValueRef::Array(array) => keys.all(|key| array.iter().any(|val| val.as_str() == Some(key))),
        ValueRef::String(str) => keys.all(|key| str == key),
        _ => false,
    }
}
