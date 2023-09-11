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

use risingwave_common::types::JsonbVal;
use risingwave_expr_macro::aggregate;
use serde_json::Value;

use crate::{ExprError, Result};

#[aggregate("jsonb_agg(boolean) -> jsonb")]
#[aggregate("jsonb_agg(*int) -> jsonb")]
#[aggregate("jsonb_agg(*float) -> jsonb")]
#[aggregate("jsonb_agg(varchar) -> jsonb")]
#[aggregate("jsonb_agg(jsonb) -> jsonb")]
fn jsonb_agg(state: Option<JsonbVal>, input: Option<impl Into<Value>>) -> JsonbVal {
    let mut jsonb = state.unwrap_or_else(|| Value::Array(Vec::with_capacity(1)).into());
    match jsonb.as_serde_mut() {
        Value::Array(a) => a.push(input.map_or(Value::Null, Into::into)),
        _ => unreachable!("invalid jsonb state"),
    };
    jsonb
}

#[aggregate("jsonb_object_agg(varchar, boolean) -> jsonb")]
#[aggregate("jsonb_object_agg(varchar, *int) -> jsonb")]
#[aggregate("jsonb_object_agg(varchar, *float) -> jsonb")]
#[aggregate("jsonb_object_agg(varchar, varchar) -> jsonb")]
#[aggregate("jsonb_object_agg(varchar, jsonb) -> jsonb")]
fn jsonb_object_agg(
    state: Option<JsonbVal>,
    key: Option<&str>,
    value: Option<impl Into<Value>>,
) -> Result<JsonbVal> {
    let key = key.ok_or(ExprError::FieldNameNull)?;
    let mut jsonb = state.unwrap_or_else(|| Value::Object(Default::default()).into());
    match jsonb.as_serde_mut() {
        Value::Object(map) => map.insert(key.into(), value.map_or(Value::Null, Into::into)),
        _ => unreachable!("invalid jsonb state"),
    };
    Ok(jsonb)
}
