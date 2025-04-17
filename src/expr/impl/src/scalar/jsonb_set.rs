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
use risingwave_common::types::{JsonbRef, JsonbVal, ListRef};
use risingwave_expr::{ExprError, Result, function};

/// Returns `target` with the item designated by `path` replaced by `new_value`, or with `new_value`
/// added if `create_if_missing` is true (which is the default) and the item designated by path does
/// not exist. All earlier steps in the path must exist, or the `target` is returned unchanged. As
/// with the path oriented operators, negative integers that appear in the path count from the end
/// of JSON arrays.
///
/// If the last path step is an array index that is out of range, and `create_if_missing` is true,
/// the new value is added at the beginning of the array if the index is negative, or at the end of
/// the array if it is positive.
///
/// # Examples
///
/// ```slt
/// query T
/// SELECT jsonb_set('[{"f1":1,"f2":null},2,null,3]', '{0,f1}', '[2,3,4]', false);
/// ----
/// [{"f1": [2, 3, 4], "f2": null}, 2, null, 3]
///
/// query T
/// SELECT jsonb_set('[{"f1":1,"f2":null},2]', '{0,f3}', '[2,3,4]');
/// ----
/// [{"f1": 1, "f2": null, "f3": [2, 3, 4]}, 2]
/// ```
#[function("jsonb_set(jsonb, varchar[], jsonb, boolean) -> jsonb")]
fn jsonb_set4(
    target: JsonbRef<'_>,
    path: ListRef<'_>,
    new_value: JsonbRef<'_>,
    create_if_missing: bool,
) -> Result<JsonbVal> {
    if target.is_scalar() {
        return Err(ExprError::InvalidParam {
            name: "jsonb",
            reason: "cannot set path in scalar".into(),
        });
    }
    let target: ValueRef<'_> = target.into();
    let new_value: ValueRef<'_> = new_value.into();
    let mut builder = jsonbb::Builder::<Vec<u8>>::with_capacity(target.capacity());
    jsonbb_set_path(target, path, 0, new_value, create_if_missing, &mut builder)?;
    Ok(JsonbVal::from(builder.finish()))
}

#[function("jsonb_set(jsonb, varchar[], jsonb) -> jsonb")]
fn jsonb_set3(
    target: JsonbRef<'_>,
    path: ListRef<'_>,
    new_value: JsonbRef<'_>,
) -> Result<JsonbVal> {
    jsonb_set4(target, path, new_value, true)
}

/// Recursively set `path[i..]` in `target` to `new_value` and write the result to `builder`.
///
/// Panics if `i` is out of bounds.
fn jsonbb_set_path(
    target: ValueRef<'_>,
    path: ListRef<'_>,
    i: usize,
    new_value: ValueRef<'_>,
    create_if_missing: bool,
    builder: &mut jsonbb::Builder,
) -> Result<()> {
    let last_step = i == path.len() - 1;
    match target {
        ValueRef::Object(obj) => {
            let key = path
                .get(i)
                .unwrap()
                .ok_or_else(|| ExprError::InvalidParam {
                    name: "path",
                    reason: format!("path element at position {} is null", i + 1).into(),
                })?
                .into_utf8();
            builder.begin_object();
            for (k, v) in obj.iter() {
                builder.add_string(k);
                if k != key {
                    builder.add_value(v);
                } else if last_step {
                    builder.add_value(new_value);
                } else {
                    // recursively set path[i+1..] in v
                    jsonbb_set_path(v, path, i + 1, new_value, create_if_missing, builder)?;
                }
            }
            if create_if_missing && last_step && !obj.contains_key(key) {
                builder.add_string(key);
                builder.add_value(new_value);
            }
            builder.end_object();
            Ok(())
        }
        ValueRef::Array(array) => {
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
                // out of bounds index
                if create_if_missing {
                    builder.begin_array();
                    // the new value is added at the beginning of the array if the index is negative
                    if idx < 0 {
                        builder.add_value(new_value);
                    }
                    for v in array.iter() {
                        builder.add_value(v);
                    }
                    // or at the end of the array if it is positive.
                    if idx >= 0 {
                        builder.add_value(new_value);
                    }
                    builder.end_array();
                } else {
                    builder.add_value(target);
                }
                return Ok(());
            };
            builder.begin_array();
            for (j, v) in array.iter().enumerate() {
                if j != idx {
                    builder.add_value(v);
                    continue;
                }
                if last_step {
                    builder.add_value(new_value);
                } else {
                    // recursively set path[i+1..] in v
                    jsonbb_set_path(v, path, i + 1, new_value, create_if_missing, builder)?;
                }
            }
            builder.end_array();
            Ok(())
        }
        _ => {
            builder.add_value(target);
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
