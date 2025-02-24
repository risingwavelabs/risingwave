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

use jsonbb::Builder;
use risingwave_common::types::{JsonbVal, ListRef};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_expr::{ExprError, Result, function};

/// Builds a JSON object out of a text array.
///
/// The array must have either exactly one dimension with an even number of members,
/// in which case they are taken as alternating key/value pairs, or two dimensions
/// such that each inner array has exactly two elements, which are taken as a key/value pair.
/// All values are converted to JSON strings.
///
/// # Examples
///
/// ```slt
/// query T
/// select jsonb_object('{a, 1, b, def, c, 3.5}' :: text[]);
/// ----
/// {"a": "1", "b": "def", "c": "3.5"}
///
/// query error array must have even number of elements
/// select jsonb_object('{a, 1, b, "def", c}' :: text[]);
///
/// query error null value not allowed for object key
/// select jsonb_object(array[null, 'b']);
///
/// query T
/// select jsonb_object(array['a', null]);
/// ----
/// {"a": null}
/// ```
#[function("jsonb_object(varchar[]) -> jsonb")]
fn jsonb_object_1d(array: ListRef<'_>) -> Result<JsonbVal> {
    if array.len() % 2 == 1 {
        return Err(ExprError::InvalidParam {
            name: "array",
            reason: "array must have even number of elements".into(),
        });
    }
    let mut builder = Builder::<Vec<u8>>::new();
    builder.begin_object();
    for [key, value] in array.iter().array_chunks() {
        match key {
            Some(s) => builder.add_string(s.into_utf8()),
            None => {
                return Err(ExprError::InvalidParam {
                    name: "array",
                    reason: "null value not allowed for object key".into(),
                });
            }
        }
        match value {
            Some(s) => builder.add_string(s.into_utf8()),
            None => builder.add_null(),
        }
    }
    builder.end_object();
    Ok(builder.finish().into())
}

/// Builds a JSON object out of a text array.
///
/// The array must have either exactly one dimension with an even number of members,
/// in which case they are taken as alternating key/value pairs, or two dimensions
/// such that each inner array has exactly two elements, which are taken as a key/value pair.
/// All values are converted to JSON strings.
///
/// # Examples
///
/// ```slt
/// query T
/// select jsonb_object('{{a, 1}, {b, def}, {c, 3.5}}' :: text[][]);
/// ----
/// {"a": "1", "b": "def", "c": "3.5"}
///
/// # FIXME: `null` should be parsed as a null value instead of a "null" string.
/// # query error null value not allowed for object key
/// # select jsonb_object('{{a, 1}, {null, "def"}, {c, 3.5}}' :: text[][]);
///
/// query error array must have two columns
/// select jsonb_object('{{a, 1, 2}, {b, "def"}, {c, 3.5}}' :: text[][]);
/// ```
#[function("jsonb_object(varchar[][]) -> jsonb")]
fn jsonb_object_2d(array: ListRef<'_>) -> Result<JsonbVal> {
    let mut builder = Builder::<Vec<u8>>::new();
    builder.begin_object();
    for kv in array.iter() {
        let Some(kv) = kv else {
            return Err(ExprError::InvalidParam {
                name: "array",
                reason: "Unexpected array element.".into(),
            });
        };
        let kv = kv.into_list();
        if kv.len() != 2 {
            return Err(ExprError::InvalidParam {
                name: "array",
                reason: "array must have two columns".into(),
            });
        }
        match kv.get(0).unwrap() {
            Some(s) => builder.add_string(s.into_utf8()),
            None => {
                return Err(ExprError::InvalidParam {
                    name: "array",
                    reason: "null value not allowed for object key".into(),
                });
            }
        }
        match kv.get(1).unwrap() {
            Some(s) => builder.add_string(s.into_utf8()),
            None => builder.add_null(),
        }
    }
    builder.end_object();
    Ok(builder.finish().into())
}

/// This form of `jsonb_object` takes keys and values pairwise from separate text arrays.
/// Otherwise it is identical to the one-argument form.
///
/// # Examples
///
/// ```slt
/// query T
/// select jsonb_object('{a,b}', '{1,2}');
/// ----
/// {"a": "1", "b": "2"}
///
/// query error mismatched array dimensions
/// select jsonb_object('{a,b}', '{1,2,3}');
///
/// # FIXME: `null` should be parsed as a null value instead of a "null" string.
/// # query error null value not allowed for object key
/// # select jsonb_object('{a,null}', '{1,2}');
/// ```
#[function("jsonb_object(varchar[], varchar[]) -> jsonb")]
fn jsonb_object_kv(keys: ListRef<'_>, values: ListRef<'_>) -> Result<JsonbVal> {
    if keys.len() != values.len() {
        return Err(ExprError::InvalidParam {
            name: "values",
            reason: "mismatched array dimensions".into(),
        });
    }
    let mut builder = Builder::<Vec<u8>>::new();
    builder.begin_object();
    for (key, value) in keys.iter().zip_eq_fast(values.iter()) {
        match key {
            Some(s) => builder.add_string(s.into_utf8()),
            None => {
                return Err(ExprError::InvalidParam {
                    name: "keys",
                    reason: "null value not allowed for object key".into(),
                });
            }
        }
        match value {
            Some(s) => builder.add_string(s.into_utf8()),
            None => builder.add_null(),
        }
    }
    builder.end_object();
    Ok(builder.finish().into())
}
