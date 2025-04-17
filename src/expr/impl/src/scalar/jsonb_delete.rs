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

use std::collections::HashSet;

use jsonbb::{Value, ValueRef};
use risingwave_common::types::{JsonbRef, JsonbVal, ListRef};
use risingwave_expr::{ExprError, Result, function};

/// Removes a key (and its value) from a JSON object, or matching string value(s) from a JSON array.
///
/// Examples:
///
/// ```slt
/// # remove key from object
/// query T
/// SELECT '{"a": "b", "c": "d"}'::jsonb - 'a';
/// ----
/// {"c": "d"}
///
/// # remove matching value from array
/// query T
/// SELECT '["a", "b", "c", "b"]'::jsonb - 'b';
/// ----
/// ["a", "c"]
///
/// query error cannot delete from scalar
/// SELECT '1'::jsonb - 'b';
/// ```
#[function("subtract(jsonb, varchar) -> jsonb")]
fn jsonb_remove(v: JsonbRef<'_>, key: &str) -> Result<JsonbVal> {
    match v.into() {
        ValueRef::Object(obj) => Ok(JsonbVal::from(Value::object(
            obj.iter().filter(|(k, _)| *k != key),
        ))),
        ValueRef::Array(arr) => Ok(JsonbVal::from(Value::array(
            arr.iter().filter(|value| value.as_str() != Some(key)),
        ))),
        _ => Err(ExprError::InvalidParam {
            name: "jsonb",
            reason: "cannot delete from scalar".into(),
        }),
    }
}

/// Deletes all matching keys or array elements from the left operand.
///
/// Examples:
///
/// ```slt
/// query T
/// SELECT '{"a": "b", "c": "d"}'::jsonb - '{a,c}'::text[];
/// ----
/// {}
///
/// query error cannot delete from scalar
/// SELECT '1'::jsonb - '{a,c}'::text[];
/// ```
#[function("subtract(jsonb, varchar[]) -> jsonb")]
fn jsonb_remove_keys(v: JsonbRef<'_>, keys: ListRef<'_>) -> Result<JsonbVal> {
    let keys_set: HashSet<&str> = keys.iter().flatten().map(|s| s.into_utf8()).collect();

    match v.into() {
        ValueRef::Object(obj) => Ok(JsonbVal::from(Value::object(
            obj.iter().filter(|(k, _)| !keys_set.contains(*k)),
        ))),
        ValueRef::Array(arr) => {
            Ok(JsonbVal::from(Value::array(arr.iter().filter(
                |value| match value.as_str() {
                    Some(s) => !keys_set.contains(s),
                    None => true,
                },
            ))))
        }
        _ => Err(ExprError::InvalidParam {
            name: "jsonb",
            reason: "cannot delete from scalar".into(),
        }),
    }
}

/// Deletes the array element with the specified index (negative integers count from the end).
/// Throws an error if JSON value is not an array.
///
/// Examples:
///
/// ```slt
/// query T
/// SELECT '["a", "b"]'::jsonb - 1;
/// ----
/// ["a"]
///
/// query T
/// SELECT '["a", "b"]'::jsonb - -1;
/// ----
/// ["a"]
///
/// query T
/// SELECT '["a", "b"]'::jsonb - 2;
/// ----
/// ["a", "b"]
///
/// query T
/// SELECT '["a", "b"]'::jsonb - -3;
/// ----
/// ["a", "b"]
///
/// query error cannot delete from scalar
/// SELECT '1'::jsonb - 1;
///
/// query error cannot delete from object using integer index
/// SELECT '{"a": 1}'::jsonb - 1;
/// ```
#[function("subtract(jsonb, int4) -> jsonb")]
fn jsonb_remove_index(v: JsonbRef<'_>, index: i32) -> Result<JsonbVal> {
    let array = match v.into() {
        ValueRef::Array(array) => array,
        ValueRef::Object(_) => {
            return Err(ExprError::InvalidParam {
                name: "jsonb",
                reason: "cannot delete from object using integer index".into(),
            });
        }
        _ => {
            return Err(ExprError::InvalidParam {
                name: "jsonb",
                reason: "cannot delete from scalar".into(),
            });
        }
    };
    let Some(idx) = normalize_array_index(array.len(), index) else {
        // out of bounds index returns original value
        return Ok(JsonbVal::from(v));
    };
    Ok(JsonbVal::from(Value::array(
        array
            .iter()
            .enumerate()
            .filter(|&(i, _)| i != idx)
            .map(|(_, v)| v),
    )))
}

/// Deletes the field or array element at the specified path, where path elements can be
/// either field keys or array indexes.
///
/// Examples:
///
/// ```slt
/// # Basic test case
/// query T
/// SELECT '["a", {"b":1}]'::jsonb #- '{1,b}';
/// ----
/// ["a", {}]
///
/// # Invalid path
/// query error path element at position 1 is null
/// SELECT '["a", {"b":1}]'::jsonb #- array[null];
///
/// # Removing non-existent key from an object
/// query T
/// SELECT '{"a": 1, "b": 2}'::jsonb #- '{c}';
/// ----
/// {"a": 1, "b": 2}
///
/// # Removing an existing key from an object
/// query T
/// SELECT '{"a": 1, "b": 2}'::jsonb #- '{a}';
/// ----
/// {"b": 2}
///
/// # Removing an item from an array by positive index
/// query T
/// SELECT '["a", "b", "c"]'::jsonb #- '{1}';
/// ----
/// ["a", "c"]
///
/// # Removing an item from an array by negative index
/// query T
/// SELECT '["a", "b", "c"]'::jsonb #- '{-1}';
/// ----
/// ["a", "b"]
///
/// # Removing a non-existent index from an array
/// query T
/// SELECT '["a", "b", "c"]'::jsonb #- '{3}';
/// ----
/// ["a", "b", "c"]
///
/// # Path element is not an integer for array
/// query error path element at position 1 is not an integer: "a"
/// SELECT '["a", "b", "c"]'::jsonb #- '{a}';
///
/// # Path to deeply nested value
/// query T
/// SELECT '{"a": {"b": {"c": [1, 2, 3]}}}'::jsonb #- '{a,b,c,1}';
/// ----
/// {"a": {"b": {"c": [1, 3]}}}
///
/// # Path terminates early (before reaching the final depth of the JSON)
/// query T
/// SELECT '{"a": {"b": {"c": [1, 2, 3]}}}'::jsonb #- '{a}';
/// ----
/// {}
///
/// # Removing non-existent path in nested structure
/// query T
/// SELECT '{"a": {"b": {"c": [1, 2, 3]}}}'::jsonb #- '{a,x}';
/// ----
/// {"a": {"b": {"c": [1, 2, 3]}}}
///
/// # Path is longer than the depth of the JSON structure
/// query T
/// SELECT '{"a": 1}'::jsonb #- '{a,b}';
/// ----
/// {"a": 1}
///
/// # Edge case: Removing root
/// query T
/// SELECT '{"a": 1}'::jsonb #- '{}';
/// ----
/// {"a": 1}
///
/// # Edge case: Empty array
/// query T
/// SELECT '[]'::jsonb #- '{a}';
/// ----
/// []
///
/// # Edge case: Empty object
/// query T
/// SELECT '{}'::jsonb #- '{null}';
/// ----
/// {}
///
/// query error cannot delete path in scalar
/// SELECT '1'::jsonb #- '{}';
/// ```
#[function("jsonb_delete_path(jsonb, varchar[]) -> jsonb")]
fn jsonb_delete_path(v: JsonbRef<'_>, path: ListRef<'_>) -> Result<JsonbVal> {
    if v.is_scalar() {
        return Err(ExprError::InvalidParam {
            name: "jsonb",
            reason: "cannot delete path in scalar".into(),
        });
    }
    if path.is_empty() {
        return Ok(JsonbVal::from(v));
    }
    let jsonb: ValueRef<'_> = v.into();
    let mut builder = jsonbb::Builder::<Vec<u8>>::with_capacity(jsonb.capacity());
    jsonbb_remove_path(jsonb, path, 0, &mut builder)?;
    Ok(JsonbVal::from(builder.finish()))
}

// Recursively remove `path[i..]` from `jsonb` and write the result to `builder`.
// Panics if `i` is out of bounds.
fn jsonbb_remove_path(
    jsonb: ValueRef<'_>,
    path: ListRef<'_>,
    i: usize,
    builder: &mut jsonbb::Builder,
) -> Result<()> {
    match jsonb {
        ValueRef::Object(obj) => {
            if obj.is_empty() {
                builder.add_value(jsonb);
                return Ok(());
            }
            let key = path
                .get(i)
                .unwrap()
                .ok_or_else(|| ExprError::InvalidParam {
                    name: "path",
                    reason: format!("path element at position {} is null", i + 1).into(),
                })?
                .into_utf8();
            if !obj.contains_key(key) {
                builder.add_value(jsonb);
                return Ok(());
            }
            builder.begin_object();
            for (k, v) in obj.iter() {
                if k != key {
                    builder.add_string(k);
                    builder.add_value(v);
                    continue;
                }
                if i != path.len() - 1 {
                    builder.add_string(k);
                    // recursively remove path[i+1..] from v
                    jsonbb_remove_path(v, path, i + 1, builder)?;
                }
            }
            builder.end_object();
            Ok(())
        }
        ValueRef::Array(array) => {
            if array.is_empty() {
                builder.add_value(jsonb);
                return Ok(());
            }
            let key = path
                .get(i)
                .unwrap()
                .ok_or_else(|| ExprError::InvalidParam {
                    name: "path",
                    reason: format!("path element at position {} is null", i + 1).into(),
                })?
                .into_utf8();
            let idx = key.parse::<i32>().map_err(|_| ExprError::InvalidParam {
                name: "path",
                reason: format!(
                    "path element at position {} is not an integer: \"{}\"",
                    i + 1,
                    key
                )
                .into(),
            })?;
            let Some(idx) = normalize_array_index(array.len(), idx) else {
                // out of bounds index returns original value
                builder.add_value(jsonb);
                return Ok(());
            };
            builder.begin_array();
            for (j, v) in array.iter().enumerate() {
                if j != idx {
                    builder.add_value(v);
                    continue;
                }
                if i != path.len() - 1 {
                    // recursively remove path[i+1..] from v
                    jsonbb_remove_path(v, path, i + 1, builder)?;
                }
            }
            builder.end_array();
            Ok(())
        }
        _ => {
            builder.add_value(jsonb);
            Ok(())
        }
    }
}

/// Normalizes an array index to `0..len`.
/// Negative indices count from the end. i.e. `-len..0 => 0..len`.
/// Returns `None` if index is out of bounds.
fn normalize_array_index(len: usize, index: i32) -> Option<usize> {
    if index < -(len as i32) || index >= (len as i32) {
        return None;
    }
    if index >= 0 {
        Some(index as usize)
    } else {
        Some((len as i32 + index) as usize)
    }
}

/// Recursively removes all object fields that have null values from the given JSON value.
/// Null values that are not object fields are untouched.
///
/// Examples:
///
/// ```slt
/// query T
/// SELECT jsonb_strip_nulls('[{"f1":1, "f2":null}, 2, null, 3]');
/// ----
/// [{"f1": 1}, 2, null, 3]
/// ```
#[function("jsonb_strip_nulls(jsonb) -> jsonb")]
fn jsonb_strip_nulls(v: JsonbRef<'_>) -> JsonbVal {
    let jsonb: ValueRef<'_> = v.into();
    let mut builder = jsonbb::Builder::<Vec<u8>>::with_capacity(jsonb.capacity());
    jsonbb_strip_nulls(jsonb, &mut builder);
    JsonbVal::from(builder.finish())
}

/// Recursively removes all object fields that have null values from the given JSON value.
fn jsonbb_strip_nulls(jsonb: ValueRef<'_>, builder: &mut jsonbb::Builder) {
    match jsonb {
        ValueRef::Object(obj) => {
            builder.begin_object();
            for (k, v) in obj.iter() {
                if let ValueRef::Null = v {
                    continue;
                }
                builder.add_string(k);
                jsonbb_strip_nulls(v, builder);
            }
            builder.end_object();
        }
        ValueRef::Array(array) => {
            builder.begin_array();
            for v in array.iter() {
                jsonbb_strip_nulls(v, builder);
            }
            builder.end_array();
        }
        _ => builder.add_value(jsonb),
    }
}
