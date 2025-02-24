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

use std::fmt::Write;

use risingwave_common::types::JsonbRef;
use risingwave_expr::{ExprError, Result, function};

#[function("jsonb_typeof(jsonb) -> varchar")]
pub fn jsonb_typeof(v: JsonbRef<'_>, writer: &mut impl Write) {
    writer.write_str(v.type_name()).unwrap()
}

#[function("jsonb_array_length(jsonb) -> int4")]
pub fn jsonb_array_length(v: JsonbRef<'_>) -> Result<i32> {
    v.array_len()
        .map(|n| n as i32)
        .map_err(|e| ExprError::InvalidParam {
            name: "",
            reason: e.into(),
        })
}

#[function("is_json(varchar) -> boolean")]
pub fn is_json_value(s: &str) -> bool {
    serde_json::from_str::<serde::de::IgnoredAny>(s).is_ok()
}

#[function("is_json(varchar, varchar) -> boolean")]
pub fn is_json_type(s: &str, t: &str) -> bool {
    serde_json::from_str::<serde::de::IgnoredAny>(s).is_ok_and(|_| {
        let s = s.trim_start();
        match t {
            "ARRAY" => s.starts_with('['),
            "OBJECT" => s.starts_with('{'),
            "SCALAR" => !s.starts_with('[') && !s.starts_with('{'),
            // forward compatible in case we always pass the default later
            "VALUE" => true,
            // After #11134, validate during expr build and pass enum to avoid this
            _ => unreachable!(),
        }
    })
}

/// Converts the given JSON value to pretty-printed, indented text.
///
/// # Examples
// TODO: enable docslt after sqllogictest supports multiline output
/// ```text
/// query T
/// select jsonb_pretty('[{"f1":1,"f2":null}, 2]');
/// ----
/// [
///     {
///         "f1": 1,
///         "f2": null
///     },
///     2
/// ]
/// ```
#[function("jsonb_pretty(jsonb) -> varchar")]
pub fn jsonb_pretty(v: JsonbRef<'_>, writer: &mut impl Write) {
    v.pretty(writer).unwrap()
}
